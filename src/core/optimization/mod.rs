use std::str::FromStr;

use actix_web::web;
use chrono::Utc;
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, oid, serde_helpers::deserialize_hex_string_from_object_id},
    options::{FindOptions, IndexOptions},
    Client, Collection, IndexModel,
};
use serde::{Deserialize, Serialize};
use tech_indicators::{fuzzy::fuzzy_indicator, Ohlc};

use self::swarm::{gen_rho, Individual, IndividualGroup};

use super::{
    backtest::{
        backtest, generate_report, get_valid_data, save_backtest_report, BacktestMetadata,
        BacktestResult, BacktestResultWithRequest, SignalCondition,
    },
    error::{map_internal_err, CustomError},
    fetch_symbol,
    fuzzy::{create_fuzzy_engine, create_input},
    settings::{
        fetch_fuzzy_rules, fetch_setting, get_rules_coll, get_setting_coll, FuzzyRuleModel,
        FuzzyRuleModelWithOutId, LinguisticVarPresetModel, LinguisticVarsModel,
    },
    users::User,
    Interval, DB_NAME,
};

pub mod swarm;

#[derive(Deserialize, Serialize, Clone)]
pub struct Strategy {
    epoch: usize,
    capital: f64,
    start_time: i64,
    train_end_time: i64,
    validation_end_time: i64,
    signal_conditions: Vec<SignalCondition>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct TrainProgress {
    epoch: usize,
    group: usize,
    f: f64,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct TrainResult {
    username: String,
    preset: String,
    backtest_id: String,
    train_progress: Vec<TrainProgress>,
    validation_f: f64,
    start_time: i64,
    train_end_time: i64,
    validation_end_time: i64,
    run_at: i64,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct TrainResultWithId {
    #[serde(flatten)]
    traint_result: TrainResult,
    #[serde(deserialize_with = "deserialize_hex_string_from_object_id")]
    _id: String,
}

pub trait IsTrainResult {}
impl IsTrainResult for TrainResult {}
impl IsTrainResult for TrainResultWithId {}

fn to_particle(linguistic_vars: &LinguisticVarsModel) -> Vec<f64> {
    let particle_pos = linguistic_vars
        .iter()
        .flat_map(|(_, l)| {
            l.shapes
                .iter()
                .flat_map(|(_, s)| s.parameters.values().copied())
        })
        .collect::<Vec<_>>();

    particle_pos
}

fn create_particle_groups(
    start_pos: &[f64],
    group: usize,
    group_size: usize,
) -> Vec<IndividualGroup> {
    let particle = Individual::new(start_pos.to_vec());
    let particles = vec![particle; group_size + 1];

    (0..group)
        .map(|_| IndividualGroup {
            particles: particles[1..].into(),
            lbest_f: f64::MAX,
            lbest_pos: particles[0].position.clone(),
        })
        .collect()
}

fn from_particle(
    linguistic_vars: &LinguisticVarsModel,
    particle_pos: &mut [f64],
) -> LinguisticVarsModel {
    let mut copied = linguistic_vars.clone();

    let mut i = 0;
    for (_, l) in copied.iter_mut() {
        for (_, s) in l.shapes.iter_mut() {
            for (k, v) in s.parameters.iter_mut() {
                /*
                 * this is super ugly, and hard coded
                 * if we want to enforce some constraints on some parameter
                 */
                match (s.shapeType.as_str(), k.as_str()) {
                    (_, "height") => {
                        *v = particle_pos[i].min(1.0).max(0.0);
                    }
                    (_, _) => {
                        *v = particle_pos[i];
                    }
                }
                //*v = particle_pos[i];
                i += 1; // this is kind of bad
            }
        }
    }

    copied
}

fn use_particle(
    setting: &mut LinguisticVarPresetModel,
    particle_pos: &mut [f64],
    strat: &Strategy,
    valid_ohlc: &[Ohlc],
    fuzzy_rules: &[FuzzyRuleModel],
    fuzzy_inputs: Vec<(i64, Vec<Option<f64>>)>,
    is_validation: bool,
) -> BacktestResult {
    setting.vars = from_particle(&setting.vars, particle_pos);
    let fuzzy_engine = create_fuzzy_engine(setting, fuzzy_rules);

    let fuzzy_output = get_valid_data(
        fuzzy_indicator(&fuzzy_engine, fuzzy_inputs.clone()),
        strat.start_time,
        if is_validation {
            strat.validation_end_time
        } else {
            strat.train_end_time
        },
    );

    let positions = backtest(
        valid_ohlc,
        &fuzzy_output,
        &strat.signal_conditions,
        strat.capital,
    );
    generate_report(&positions, strat.capital, strat.start_time)
}

async fn save_linguistic_vars_setting(
    db: &web::Data<Client>,
    data: LinguisticVarPresetModel,
) -> Result<(), CustomError> {
    let coll = get_setting_coll(db).await?;

    coll.insert_one(data, None)
        .await
        .map_err(map_internal_err)?;
    Ok(())
}

async fn save_fuzzy_rules(
    db: &web::Data<Client>,
    data: Vec<FuzzyRuleModel>,
    new_preset_name: &String,
) -> Result<(), CustomError> {
    let dt = data.into_iter().map(|item| FuzzyRuleModelWithOutId {
        input: item.input,
        output: item.output,
        username: item.username,
        valid: item.valid,
        preset: new_preset_name.clone(),
    });

    let coll = get_rules_coll(db).await?;
    coll.insert_many(dt, None).await.map_err(map_internal_err)?;
    Ok(())
}

async fn get_train_result_coll<T: IsTrainResult>(
    db: &web::Data<Client>,
) -> Result<Collection<T>, CustomError> {
    let db_client = (*db).database(DB_NAME);
    let coll = db_client.collection::<T>("pso-results");
    let opts = IndexOptions::builder().unique(true).build();
    let index = IndexModel::builder()
        .keys(doc! { "username": 1, "preset": 1})
        .options(opts)
        .build();
    coll.create_index(index, None)
        .await
        .map_err(map_internal_err)?;
    Ok(coll)
}

async fn save_train_result(
    db: &web::Data<Client>,
    train_result: TrainResult,
) -> Result<(), CustomError> {
    let coll = get_train_result_coll(db).await?;
    coll.insert_one(train_result, None)
        .await
        .map_err(map_internal_err)?;
    Ok(())
}

fn objective_func(result: &BacktestResult, reference: &BacktestResult) -> f64 {
    let profit_change = result.total.pnl_percent - reference.total.pnl_percent;
    let mdd_change = result.maximum_drawdown.percent - reference.maximum_drawdown.percent;

    -1.0 * (profit_change + mdd_change)
    /*
    let profit_trades_p = 100.0 * (result.profit_trades.trades as f64 / p_trades);
    let loss_trades_p = 10.0 * (result.loss_trades.trades as f64 / l_trades);
    -1.0 * (profit_trades_p + result.total.pnl_percent
        - result.maximum_drawdown.percent
        - loss_trades_p)
    */
}

/// Using PSO to optimize linguistic variables
pub async fn linguistic_vars_optimization(
    db: &web::Data<Client>,
    symbol: &String,
    interval: &Interval,
    preset: &String,
    user: &User,
    strat: Strategy,
) -> Result<TrainResult, CustomError> {
    if strat.start_time > strat.train_end_time || strat.train_end_time > strat.validation_end_time {
        return Err(CustomError::InvalidTimeRange);
    }

    if strat.signal_conditions.is_empty() {
        return Err(CustomError::ExpectAtlestOneSignalCondition);
    }

    // data preparation
    let username = &user.username;
    let data = fetch_symbol(db, symbol, &Some(interval.clone())).await;
    let mut setting = fetch_setting(db, username, preset).await?;
    let fuzzy_rules = fetch_fuzzy_rules(db, username, preset).await?;

    // set initial values for each individual
    let fuzzy_inputs = create_input(&setting, &data, user);

    let mut start_pos = to_particle(&setting.vars);
    let mut groups = create_particle_groups(&start_pos, 1, 5);

    // train on training period only
    let valid_ohlc = get_valid_data(data.0.clone(), strat.start_time, strat.train_end_time);
    let first_run_train = use_particle(
        &mut setting,
        &mut start_pos,
        &strat,
        &valid_ohlc,
        &fuzzy_rules,
        fuzzy_inputs.clone(),
        false,
    );

    let mut train_progress: Vec<TrainProgress> = vec![];
    for i in 0..strat.epoch {
        for (k, g) in groups.iter_mut().enumerate() {
            for x in g.particles.iter_mut() {
                let r = use_particle(
                    &mut setting,
                    &mut x.position,
                    &strat,
                    &valid_ohlc,
                    &fuzzy_rules,
                    fuzzy_inputs.clone(),
                    false,
                );

                let f = objective_func(&r, &first_run_train);
                if f < x.f {
                    x.f = f;
                    x.best_pos = x.position.clone();
                }
                if f < g.lbest_f {
                    g.lbest_f = f;
                    g.lbest_pos = x.position.clone();
                }
                x.update_speed(&g.lbest_pos, gen_rho(1.0), gen_rho(1.5));
                x.change_pos();

                train_progress.push(TrainProgress {
                    epoch: i,
                    group: k,
                    f,
                });
            }
        }
    }

    let mut best_group = groups
        .into_iter()
        .reduce(|best, x| if best.lbest_f < x.lbest_f { best } else { x })
        .unwrap();

    let valid_ohlc = get_valid_data(data.0.clone(), strat.start_time, strat.validation_end_time);
    let validation_result = use_particle(
        &mut setting,
        &mut best_group.lbest_pos,
        &strat,
        &valid_ohlc,
        &fuzzy_rules,
        fuzzy_inputs.clone(),
        true,
    );
    let first_run_validation = use_particle(
        &mut setting,
        &mut start_pos,
        &strat,
        &valid_ohlc,
        &fuzzy_rules,
        fuzzy_inputs,
        true,
    );

    let validation_f = objective_func(&validation_result, &first_run_validation);
    let new_preset_name = format!("{}-pso-{}", setting.preset, Utc::now().timestamp());
    let run_at = Utc::now().timestamp_millis();
    let (_, backtest_id) = save_backtest_report(
        db,
        username,
        symbol,
        interval,
        &new_preset_name,
        BacktestResultWithRequest {
            result: validation_result,
            metadata: BacktestMetadata::PsoBackTest(strat.clone()),
        },
        run_at,
    )
    .await?;

    save_fuzzy_rules(db, fuzzy_rules, &new_preset_name).await?;
    setting.preset = new_preset_name.clone();
    save_linguistic_vars_setting(db, setting).await?;

    let train_result = TrainResult {
        username: username.clone(),
        preset: new_preset_name,
        train_progress,
        backtest_id,
        validation_f,
        start_time: strat.start_time,
        train_end_time: strat.train_end_time,
        validation_end_time: strat.validation_end_time,
        run_at,
    };
    save_train_result(db, train_result.clone()).await?;
    Ok(train_result)
}

pub async fn get_train_results(
    db: &web::Data<Client>,
    username: String,
) -> Result<Vec<TrainResultWithId>, CustomError> {
    let coll = get_train_result_coll(db).await?;
    let find_options = FindOptions::builder().sort(doc! { "run_at": - 1}).build();
    Ok(coll
        .find(doc! { "username": username}, find_options)
        .await
        .map_err(map_internal_err)?
        .try_collect::<Vec<_>>()
        .await
        .map_err(map_internal_err)?)
}

pub async fn delete_train_result(db: &web::Data<Client>, id: String) -> Result<(), CustomError> {
    let coll = get_train_result_coll::<TrainResultWithId>(db).await?;
    let obj_id = oid::ObjectId::from_str(&id).map_err(map_internal_err)?;

    let result = coll
        .delete_one(doc! {"_id": obj_id}, None)
        .await
        .map_err(map_internal_err)?;

    if result.deleted_count == 0 {
        return Err(CustomError::TrainResultNotFound);
    }
    Ok(())
}
