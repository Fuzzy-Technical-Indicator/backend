use std::{
    str::FromStr,
    sync::{mpsc::Receiver, Arc, Mutex},
    time::Instant,
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

use crate::core::backtest::CapitalManagement;

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
use rayon::prelude::*;

pub mod swarm;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Strategy {
    limit: usize,
    capital: f64,
    signal_conditions: Vec<SignalCondition>,
    validation_period: usize, // in mounth
    test_start: i64,
    particle_groups: usize,
    particle_amount: usize,
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
    validation_progress: Vec<Vec<f64>>,
    test_f: f64,
    run_at: i64,
    time_used: u64,
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
    let particles = (0..group_size + 1)
        .map(|_| Individual::new(start_pos.to_vec()))
        .collect::<Vec<_>>();
    (0..group)
        .map(|_| IndividualGroup {
            particles: particles[1..].into(),
            lbest_f: f64::MAX,
            lbest_pos: start_pos.to_vec(),
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
        return 50.0;
    }
    -1.0 * (profit_change + mdd_change)
}

pub struct PSOTrainResult {
    pub test_result: BacktestResult,
    pub test_f: f64,
    pub best_setting: LinguisticVarPresetModel,
    pub train_progress: Vec<TrainProgress>,
    pub validation_progress: Vec<Vec<f64>>,
}

pub struct PSORunner {
    ohlcs: Vec<Ohlc>,
    strat: Strategy,
    fuzzy_rules: Vec<FuzzyRuleModel>,
    fuzzy_inputs: Vec<(i64, Vec<Option<f64>>)>,
    setting: LinguisticVarPresetModel,
}

impl PSORunner {
    fn get_test_start_index(&self) -> usize {
        self.ohlcs
            .iter()
            .enumerate()
            .min_by_key(|(_, item)| (item.get_time() - self.strat.test_start).abs())
            .map(|(i, _)| i)
            .unwrap_or_default() // this could be weird when unwrapping
    }

    fn use_particle(
        &self,
        particle_pos: &[f64],
        range: (usize, usize),
        range2: Option<(usize, usize)>,
    ) -> BacktestResult {
        let valid_ohlc = &self.ohlcs[range.0..range.1];
        let strat = &self.strat;
        let mut setting = self.setting.clone();
        setting.vars = from_particle(&setting.vars, particle_pos);

        let fuzzy_engine = create_fuzzy_engine(&setting, &self.fuzzy_rules);
        let fuzzy_output = fuzzy_indicator(&fuzzy_engine, self.fuzzy_inputs.clone());
        let positions = backtest(
            valid_ohlc,
            &fuzzy_output[range.0..range.1],
            &strat.signal_conditions,
            strat.capital,
        );
        let start_time = valid_ohlc
            .first()
            .expect("valid_ohlc should not be empty")
            .get_time();

        match range2 {
            Some(range2) => {
                let valid_ohlc = &self.ohlcs[range2.0..range2.1];
                let snd_positions = backtest(
                    valid_ohlc,
                    &fuzzy_output[range2.0..range2.1],
                    &strat.signal_conditions,
                    strat.capital,
                );
                let positions = positions
                    .into_iter()
                    .chain(snd_positions)
                    .collect::<Vec<_>>();
                generate_report(&positions, strat.capital, start_time)
            }
            None => generate_report(&positions, strat.capital, start_time),
        }
    }

    fn use_p_cv(
        &self,
        pos: &[f64],
        curr_fold: usize,
        last_fold: usize,
        valid_start: usize,
        valid_end: usize,
        test_start: usize,
    ) -> BacktestResult {
        if curr_fold == 0 {
            return self.use_particle(pos, (valid_end, test_start), None);
        }
        if curr_fold == last_fold {
            return self.use_particle(pos, (0, valid_start), None);
        }
        self.use_particle(pos, (0, valid_start), Some((valid_end, test_start)))
    }

    pub fn cross_valid_train(&self, interval: &Interval) -> Option<PSOTrainResult> {
        let test_start = self.get_test_start_index();
        let validation_period = self.strat.validation_period;
        let validation_len = match interval {
            // hour in a month
            Interval::OneHour => validation_period * 30 * 24,
            // 4-hours in a month
            Interval::FourHour => validation_period * 30 * 6,
            // day in a month
            Interval::OneDay => validation_period * 30,
        };

        let k = test_start / validation_len;
        log::info!("k: {}", k);
        if k <= 1 {
            return None;
        }
        // pair of (start, end)
        let folds = (0..k - 1)
            .map(|i| (i * validation_len, (i + 1) * validation_len))
            .chain([((k - 1) * validation_len, test_start)])
            .collect::<Vec<_>>();

        let start_pos = to_particle(&self.setting.vars);

        let train_results = folds
            .par_iter()
            .enumerate()
            .map(|(curr_fold, item)| {
                log::info!("fold {}/{}, start training", curr_fold, k - 1);
                let (valid_start, valid_end) = item;
                let mut groups = create_particle_groups(
                    &start_pos,
                    self.strat.particle_groups,
                    self.strat.particle_amount,
                );

                let last = k - 1;
                let ref_run = self.use_p_cv(
                    &start_pos,
                    curr_fold,
                    last,
                    *valid_start,
                    *valid_end,
                    test_start,
                );

                let valid_ref_run = self.use_particle(&start_pos, (*valid_start, *valid_end), None);

                let mut validation_progress = vec![];

                let mut best_validation_f = f64::MAX;
                let mut best_ind = None;

                for _ in 0..self.strat.limit {
                    groups.par_iter_mut().enumerate().for_each(|(_, g)| {
                        for x in g.particles.iter_mut() {
                            let r = self.use_p_cv(
                                &x.position,
                                curr_fold,
                                last,
                                *valid_start,
                                *valid_end,
                                test_start,
                            );

                            let f = objective_func(&r, &ref_run);
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
                        }
                    });

                    let best_group = groups
                        .iter()
                        .reduce(|best, x| if best.lbest_f < x.lbest_f { best } else { x })
                        .unwrap();

                    let validation_result =
                        self.use_particle(&best_group.lbest_pos, (*valid_start, *valid_end), None);
                    let validation_f = objective_func(&validation_result, &valid_ref_run);

                    if validation_f < best_validation_f {
                        best_validation_f = validation_f;
                        best_ind = Some(best_group.lbest_pos.clone());
                    }

                    validation_progress.push(validation_f);
                }
                log::info!("fold {}/{}, {}", curr_fold, k - 1, best_validation_f);
                (
                    validation_progress,
                    best_ind.expect("This should not be None"),
                    best_validation_f,
                )
            })
            .collect::<Vec<_>>();

        let all_valid_progress = train_results
            .iter()
            .fold(&mut vec![], |acc, (vp, _, _)| {
                acc.push(vp.clone());
                acc
            })
            .to_vec();

        let (_, best_ind_pos, _) = train_results
            .into_iter()
            .max_by(|(_, _, f1), (_, _, f2)| f1.total_cmp(f2))
            .unwrap();

        let end = self.ohlcs.len();
        let test_ref_run = self.use_particle(&start_pos, (test_start, end), None);
        let test_result = self.use_particle(&best_ind_pos, (test_start, end), None);
        let test_f = objective_func(&test_result, &test_ref_run);

        let mut best_setting = self.setting.clone();
        best_setting.vars = from_particle(&best_setting.vars, &best_ind_pos);

        Some(PSOTrainResult {
            test_result,
            test_f,
            best_setting,
            train_progress: vec![],
            validation_progress: all_valid_progress,
        })
    }

    pub fn train(&self, interval: &Interval) -> PSOTrainResult {
        log::info!("start training");
        let test_start = self.get_test_start_index();
        let strat = &self.strat;
        let train_end = match interval {
            // hour in a month
            Interval::OneHour => test_start.saturating_sub(strat.validation_period * 30 * 24),
            // 4-hours in a month
            Interval::FourHour => test_start.saturating_sub(strat.validation_period * 30 * 6),
            // day in a month
            Interval::OneDay => test_start.saturating_sub(strat.validation_period * 30),
        };

        let start_pos = to_particle(&self.setting.vars);
        let mut groups = create_particle_groups(
            &start_pos,
            self.strat.particle_groups,
            self.strat.particle_amount,
        );

        let ref_run = self.use_particle(&start_pos, (0, train_end), None);
        let valid_ref_run = self.use_particle(&start_pos, (train_end, test_start), None);

        let train_progress = Arc::new(Mutex::new(vec![]));
        let mut validation_progress = vec![];

        let mut best_validation_f = f64::MAX;
        let mut best_ind = None;

        for i in 0..self.strat.limit {
            groups.par_iter_mut().enumerate().for_each(|(k, g)| {
                for x in g.particles.iter_mut() {
                    let r = self.use_particle(&x.position, (0, train_end), None);

                    let f = objective_func(&r, &ref_run);
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

                    {
                        let mut tp = train_progress.lock().unwrap();
                        (*tp).push(TrainProgress {
                            epoch: i,
                            group: k,
                            f,
                        });
                    }
                }
            });

            let best_group = groups
                .iter()
                .reduce(|best, x| if best.lbest_f < x.lbest_f { best } else { x })
                .unwrap();

            let validation_result =
                self.use_particle(&best_group.lbest_pos, (train_end, test_start), None);
            let validation_f = objective_func(&validation_result, &valid_ref_run);

            if validation_f < best_validation_f {
                best_validation_f = validation_f;
                best_ind = Some(best_group.lbest_pos.clone());
            }

            validation_progress.push(validation_f);
        }

        let end = self.ohlcs.len();
        let best_ind_pos = best_ind.expect("This should not be None");
        let test_ref_run = self.use_particle(&start_pos, (test_start, end), None);
        let test_result = self.use_particle(&best_ind_pos, (test_start, end), None);
        let test_f = objective_func(&test_result, &test_ref_run);

        let mut best_setting = self.setting.clone();
        best_setting.vars = from_particle(&best_setting.vars, &best_ind_pos);
        let train_progress = train_progress.lock().unwrap().to_vec();

        PSOTrainResult {
            test_result,
            test_f,
            best_setting,
            train_progress,
            validation_progress: vec![validation_progress],
        }
    }
}

/// Normal Version
pub async fn linguistic_vars_optimization(
    db: &Data<Client>,
    symbol: &String,
    interval: &Interval,
    preset: &String,
    user: &User,
    strat: Strategy,
    run_type: PSORunType,
) -> Result<(), CustomError> {
    if strat.signal_conditions.is_empty() {
        return Err(CustomError::ExpectAtlestOneSignalCondition);
    }

    let now = Instant::now();
    // data preparation
    let username = &user.username;
    let data = fetch_symbol(db, symbol, &Some(interval.clone())).await;
    let setting = fetch_setting(db, username, preset).await?;
    let fuzzy_rules = fetch_fuzzy_rules(db, username, preset).await?;
    let fuzzy_inputs = create_input(&setting, &data, user);

    let runner = PSORunner {
        ohlcs: data.0,
        strat: strat.clone(),
        fuzzy_rules: fuzzy_rules.clone(),
        fuzzy_inputs,
        setting: setting.clone(),
    };

    let PSOTrainResult {
        test_result,
        test_f,
        train_progress,
        validation_progress,
        mut best_setting,
    } = match run_type {
        PSORunType::CrossValidation => match runner.cross_valid_train(interval) {
            Some(r) => r,
            None => {
                return Err(CustomError::InternalError(
                    "k-fold = 1, specify new validation period".to_string(),
                ))
            }
        },
        PSORunType::Normal => runner.train(interval),
    };

    // hard-coded capital management name by using first signal condition
    let cap_type = match strat.signal_conditions.first() {
        Some(st) => match st.capital_management {
            CapitalManagement::Normal { .. } => "normal".to_string(),
            CapitalManagement::LiquidF { .. } => "liquidf".to_string(),
        },
        None => "normal".to_string(),
    };

    let new_preset_name = format!(
        "{}-{}-{}-pso-{}",
        setting.preset,
        symbol.replace('/', ""),
        cap_type,
        Utc::now().timestamp()
    );
    let run_at = Utc::now().timestamp_millis();
    let (_, backtest_id) = save_backtest_report(
        db,
        username,
        symbol,
        interval,
        &new_preset_name,
        BacktestResultWithRequest {
            result: test_result,
            metadata: BacktestMetadata::PsoBackTest(strat.clone()),
        },
        run_at,
    )
    .await?;

    save_fuzzy_rules(db, fuzzy_rules, &new_preset_name).await?;
    best_setting.preset = new_preset_name.clone();
    save_linguistic_vars_setting(db, best_setting).await?;

    let time_used = now.elapsed().as_secs();
    log::info!("PSO time: {}", time_used);
    let train_result = TrainResult {
        username: username.clone(),
        preset: new_preset_name,
        train_progress,
        backtest_id,
        validation_progress,
        test_f,
        run_at,
        time_used,
    };
    save_train_result(db, train_result).await?;
    Ok(())
}

pub async fn get_train_results(
    db: &web::Data<Client>,
    username: String,
) -> Result<Vec<TrainResultWithId>, CustomError> {
    let coll = get_train_result_coll(db).await?;
    let find_options = FindOptions::builder().sort(doc! { "run_at": - 1}).build();
    coll.find(doc! { "username": username }, find_options)
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PSORunType {
    #[serde(rename = "normal")]
    Normal,
    #[serde(rename = "crossvalid")]
    CrossValidation,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PSOTrainJob {
    pub symbol: String,
    pub interval: Interval,
    pub preset: String,
    pub user: User,
    pub strat: Strategy,
    pub run_type: PSORunType,
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
            run_type,
        } = job;

        let r =
            linguistic_vars_optimization(&db, &symbol, &interval, &preset, &user, strat, run_type)
                .await;
        match r {
            Ok(_) => {
                log::info!("PSO job success")
            }
            Err(e) => {
                log::error!("Error in PSO job: {:?}", e);
            }
        }
        {
            let mut c = pso_counter.lock().unwrap();
            *c = c.saturating_sub(1);
        }
    }
}
