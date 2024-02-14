use std::{
    str::FromStr,
    sync::{mpsc::Receiver, Mutex},
};

use actix_web::web::{self, Data};
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
        backtest, generate_report, save_backtest_report, BacktestMetadata, BacktestResult,
        BacktestResultWithRequest, GetTime, SignalCondition,
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

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Strategy {
    epoch: usize,
    capital: f64,
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
    particle_pos: &[f64],
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
                    _ => {
                        *v = particle_pos[i];
                    }
                }
                i += 1; // this is kind of bad
            }
        }
    }

    copied
}

fn use_particle(
    setting: &mut LinguisticVarPresetModel,
    particle_pos: &[f64],
    strat: &Strategy,
    valid_ohlc: &[Ohlc],
    fuzzy_rules: &[FuzzyRuleModel],
    fuzzy_inputs: &[(i64, Vec<Option<f64>>)],
    range: (usize, usize),
) -> BacktestResult {
    setting.vars = from_particle(&setting.vars, particle_pos);
    let fuzzy_engine = create_fuzzy_engine(setting, fuzzy_rules);
    let fuzzy_output = &fuzzy_indicator(&fuzzy_engine, fuzzy_inputs.to_vec())[range.0..range.1];
    let positions = backtest(
        valid_ohlc,
        fuzzy_output,
        &strat.signal_conditions,
        strat.capital,
    );
    let start_time = valid_ohlc
        .first()
        .expect("valid_ohlc should not be empty")
        .get_time();
    generate_report(&positions, strat.capital, start_time)
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
    new_preset_name: &str,
) -> Result<(), CustomError> {
    let dt = data.into_iter().map(|item| FuzzyRuleModelWithOutId {
        input: item.input,
        output: item.output,
        username: item.username,
        valid: item.valid,
        preset: new_preset_name.to_owned(),
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
    let mdd_change = reference.maximum_drawdown.percent - result.maximum_drawdown.percent;

    if result.total.trades == 0 {
        return 100.0;
    }
    -1.0 * (profit_change + mdd_change)
}

/// Using PSO to optimize linguistic variables
pub async fn linguistic_vars_optimization(
    db: &Data<Client>,
    symbol: &String,
    interval: &Interval,
    preset: &String,
    user: &User,
    strat: Strategy,
) -> Result<(), CustomError> {
    if strat.signal_conditions.is_empty() {
        return Err(CustomError::ExpectAtlestOneSignalCondition);
    }
    // data preparation
    let username = &user.username;
    let data = fetch_symbol(db, symbol, &Some(interval.clone())).await;
    let setting = fetch_setting(db, username, preset).await?;
    let fuzzy_rules = fetch_fuzzy_rules(db, username, preset).await?;

    let fuzzy_inputs = create_input(&setting, &data, user);

    // k fold cross validation
    let k = 5;
    let offset = data.0.len() / k;

    // pair of (start, end)
    let folds = (0..k - 1)
        .map(|i| (i * offset, (i + 1) * offset))
        .chain([((k - 1) * offset, data.0.len())]);

    let mut best_validation_f = f64::MAX;
    let mut best_validation_res = None;
    let mut best_train_progress = None;
    let mut best_setting = None;

    for (start, end) in folds {
        let start_pos = to_particle(&setting.vars);
        let mut groups = create_particle_groups(&start_pos, 1, 5);

        let mut trained_setting = setting.clone();

        // use 3/4 of the data for training
        // train on training period only
        let train_end = start + ((end - start) as f32 * 0.75).floor() as usize;
        let ohlc = &data.0[start..train_end];
        let first_run_train = use_particle(
            &mut trained_setting,
            &start_pos,
            &strat,
            ohlc,
            &fuzzy_rules,
            &fuzzy_inputs,
            (start, train_end),
        );

        let mut train_progress: Vec<TrainProgress> = vec![];

        for i in 0..strat.epoch {
            for (k, g) in groups.iter_mut().enumerate() {
                for x in g.particles.iter_mut() {
                    let r = use_particle(
                        &mut trained_setting,
                        &x.position,
                        &strat,
                        ohlc,
                        &fuzzy_rules,
                        &fuzzy_inputs,
                        (start, train_end),
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
        let best_group = groups
            .into_iter()
            .reduce(|best, x| if best.lbest_f < x.lbest_f { best } else { x })
            .unwrap();

        let valid_ohlc = &data.0[train_end..end];
        let first_run_validation = use_particle(
            &mut trained_setting,
            &start_pos,
            &strat,
            valid_ohlc,
            &fuzzy_rules,
            &fuzzy_inputs,
            (train_end, end),
        );
        let validation_result = use_particle(
            &mut trained_setting,
            &best_group.lbest_pos,
            &strat,
            valid_ohlc,
            &fuzzy_rules,
            &fuzzy_inputs,
            (train_end, end),
        );
        let validation_f = objective_func(&validation_result, &first_run_validation);

        log::info!("({}, {}) -> Validation f: {}", start, end, validation_f);

        if validation_f < best_validation_f {
            best_validation_f = validation_f;
            best_validation_res = Some(validation_result);
            best_train_progress = Some(train_progress);
            best_setting = Some(trained_setting);
        }
    }

    let new_preset_name = format!("{}-pso-{}", setting.preset, Utc::now().timestamp());
    let run_at = Utc::now().timestamp_millis();
    let (_, backtest_id) = save_backtest_report(
        db,
        username,
        symbol,
        interval,
        &new_preset_name,
        BacktestResultWithRequest {
            result: best_validation_res.expect("This should not be None"),
            metadata: BacktestMetadata::PsoBackTest(strat.clone()),
        },
        run_at,
    )
    .await?;

    let mut setting = best_setting.expect("This should not be None");
    save_fuzzy_rules(db, fuzzy_rules, &new_preset_name).await?;
    setting.preset = new_preset_name.clone();
    save_linguistic_vars_setting(db, setting).await?;

    let train_result = TrainResult {
        username: username.clone(),
        preset: new_preset_name,
        train_progress: best_train_progress.expect("This should not be None"),
        backtest_id,
        validation_f: best_validation_f,
        run_at,
    };
    save_train_result(db, train_result.clone()).await?;
    Ok(())
}

pub async fn get_train_results(
    db: &web::Data<Client>,
    username: String,
) -> Result<Vec<TrainResultWithId>, CustomError> {
    let coll = get_train_result_coll(db).await?;
    let find_options = FindOptions::builder().sort(doc! { "run_at": - 1}).build();
    coll.find(doc! { "username": username}, find_options)
        .await
        .map_err(map_internal_err)?
        .try_collect::<Vec<_>>()
        .await
        .map_err(map_internal_err)
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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PSOTrainJob {
    pub symbol: String,
    pub interval: Interval,
    pub preset: String,
    pub user: User,
    pub strat: Strategy,
}

#[tokio::main]
pub async fn pso_consumer(
    mongo_uri: String,
    receiver: Receiver<PSOTrainJob>,
    pso_counter: web::Data<Mutex<u32>>,
) {
    let client = Client::with_uri_str(mongo_uri)
        .await
        .expect("Failed to connect to Mongodb");
    let db = web::Data::new(client);

    while let Ok(job) = receiver.recv() {
        log::info!("PSO job started");
        let PSOTrainJob {
            symbol,
            interval,
            preset,
            user,
            strat,
        } = job;

        let r = linguistic_vars_optimization(&db, &symbol, &interval, &preset, &user, strat).await;
        match r {
            Ok(_) => {
                log::info!("PSO job success")
            }
            Err(e) => {
                log::error!("Error in PSO job: {:?}", e);
            }
        }

        {
            let mut c = *pso_counter.lock().unwrap();
            c = c.saturating_sub(1);
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
}
