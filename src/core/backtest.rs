use std::{collections::BTreeMap, str::FromStr};

use crate::core::Interval;
use actix_web::web;
use chrono::Utc;
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, oid, serde_helpers::deserialize_hex_string_from_object_id},
    options::FindOptions,
    Client, Collection,
};
use rand::distributions::{Distribution, Uniform};
use serde::{Deserialize, Serialize};
use tech_indicators::{fuzzy::fuzzy_indicator, DTValue, Ohlc};

use super::{
    error::{map_internal_err, CustomError},
    fetch_symbol,
    fuzzy::get_fuzzy_config,
    optimization::Strategy,
    users::User,
    DB_NAME,
};

const COLLECTION_NAME: &str = "backtest-reports";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum PosType {
    #[serde(rename = "long")]
    Long,
    #[serde(rename = "short")]
    Short,
}

#[derive(Debug, Clone, Copy)]
pub struct RealizedInfo {
    pnl: f64,
    exit_price: f64,
    exit_time: i64,
}

#[derive(Debug)]
pub struct Position {
    enter_price: f64,
    enter_time: i64,
    amount: f64,

    take_profit_when: f64,
    stop_loss_when: f64,
    pos_type: PosType,

    realized: Option<RealizedInfo>,
}

impl Position {
    pub fn new(
        enter_price: f64,
        enter_time: i64,
        amount: f64,
        take_profit_when: f64,
        stop_loss_when: f64,
        pos_type: PosType,
    ) -> Position {
        Position {
            enter_price,
            enter_time,
            amount,
            take_profit_when,
            stop_loss_when,
            pos_type,
            realized: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum CapitalManagement {
    Normal {
        entry_size_percent: f64,
        min_entry_size: f64,
    },
    LiquidF {
        min_entry_size: f64,
    },
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SignalCondition {
    signal_index: u64,
    signal_threshold: f64,
    signal_do_command: PosType,
    take_profit_when: f64,
    stop_loss_when: f64,
    capital_management: CapitalManagement,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct BacktestRequest {
    capital: f64,
    start_time: i64,
    end_time: i64,
    signal_conditions: Vec<SignalCondition>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Trades {
    pnl: f64,
    pub pnl_percent: f64,
    pub trades: i64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct MaximumDrawdown {
    amount: f64,
    pub percent: f64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CumalativeReturn {
    time: i64,
    value: f64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BacktestResult {
    pub maximum_drawdown: MaximumDrawdown,
    pub profit_trades: Trades,
    pub loss_trades: Trades,
    pub total: Trades,
    pub cumalative_return: Vec<CumalativeReturn>,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "tag")]
pub enum BacktestMetadata {
    NormalBackTest(BacktestRequest),
    PsoBackTest(Strategy),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BacktestResultWithRequest {
    pub metadata: BacktestMetadata,
    #[serde(flatten)]
    pub result: BacktestResult,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BacktestReport {
    username: String,
    ticker: String,
    interval: Interval,
    fuzzy_preset: String,
    backtest_result: BacktestResultWithRequest,
    run_at: i64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BacktestReportWithId {
    #[serde(deserialize_with = "deserialize_hex_string_from_object_id")]
    _id: String,
    #[serde(flatten)]
    backtest_report: BacktestReport,
}

pub trait IsBacktestReport {}

impl IsBacktestReport for BacktestReport {}
impl IsBacktestReport for BacktestReportWithId {}

pub trait GetTime {
    fn get_time(&self) -> i64;
}

impl GetTime for Ohlc {
    fn get_time(&self) -> i64 {
        self.time.timestamp_millis()
    }
}

impl<T> GetTime for DTValue<T> {
    fn get_time(&self) -> i64 {
        self.time
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BacktestJob {
    pub request: BacktestRequest,
    pub user: User,
    pub symbol: String,
    pub interval: Interval,
    pub preset: String,
}

// TODO classic one

fn to_percent(x: f64, y: f64) -> f64 {
    (x / y) * 100.0
}

pub fn get_backtest_coll<T: IsBacktestReport>(db: &web::Data<Client>) -> Collection<T> {
    let db_client = (*db).database(DB_NAME);
    db_client.collection::<T>(COLLECTION_NAME)
}

pub fn get_valid_data<T: GetTime>(data: Vec<T>, start_time: i64, end_time: i64) -> Vec<T> {
    data.into_iter()
        .filter(|item| item.get_time() >= start_time && item.get_time() <= end_time)
        .collect()
}

fn realize_positions(
    positions: &mut [Position],
    working_capital: &mut f64,
    ohlc: &Ohlc,
    last: bool,
) {
    for p in positions {
        match p.realized {
            Some(_) => continue,
            None => {
                let p_diff = ((ohlc.close - p.enter_price) / p.enter_price) * 100.0;
                match p.pos_type {
                    PosType::Long => {
                        if p_diff >= p.take_profit_when || p_diff <= -p.stop_loss_when || last {
                            let realized_amount = (p.amount / p.enter_price) * ohlc.close;
                            let pnl = realized_amount - p.amount;
                            *working_capital += p.amount + pnl;
                            p.realized = Some(RealizedInfo {
                                pnl,
                                exit_price: ohlc.close,
                                exit_time: ohlc.time.timestamp_millis(),
                            });
                        }
                    }
                    PosType::Short => {
                        if -p_diff >= p.take_profit_when || -p_diff <= -p.stop_loss_when || last {
                            let realized_amount = (p.amount / p.enter_price) * ohlc.close;
                            let pnl = p.amount - realized_amount;
                            *working_capital += p.amount + pnl;
                            p.realized = Some(RealizedInfo {
                                pnl,
                                exit_price: ohlc.close,
                                exit_time: ohlc.time.timestamp_millis(),
                            });
                        }
                    }
                }
            }
        }
    }
}

fn liquid_f_entry_size(
    positions: &[Position],
    min_entry_size: f64,
    working_capital: f64,
    output: f64,
    output_max: f64,
    threshold: f64,
) -> f64 {
    let v = positions
        .iter()
        .filter_map(|pos| pos.realized)
        .map(|pos| pos.pnl)
        .collect::<Vec<_>>();

    if v.is_empty() {
        return min_entry_size;
    }
    let risk_factor = v.iter().min_by(|a, b| a.total_cmp(b)).unwrap();

    let mut max_twr = f64::MIN;
    let mut max_f = 0.0;

    for i in 1..99 {
        let f = (i as f64) / 100.0;
        let twr = v
            .iter()
            .map(|pnl| 1.0 + (f * pnl) / risk_factor)
            .reduce(|acc, item| acc * item)
            .unwrap();

        if twr > max_twr {
            max_twr = twr;
            max_f = f;
        }
    }

    let liquid_f = 0.1 * max_f;
    let size = liquid_f + ((output - threshold) * (max_f - liquid_f)) / (output_max - threshold);
    (size * working_capital)
        .max(min_entry_size)
        .min(working_capital)
}

fn random_backtest(
    valid_ohlc: &[Ohlc],
    initial_capital: f64,
    condition: SignalCondition,
    start_time: i64,
) -> (MaximumDrawdown, Trades) {
    let mut rng = rand::thread_rng();
    let coin = Uniform::from(0..=1);

    let rounds = 5;
    (0..=rounds)
        .map(|_| {
            let mut working_capital = initial_capital;
            let mut positions: Vec<Position> = vec![];
            for ohlc in valid_ohlc {
                realize_positions(&mut positions, &mut working_capital, ohlc, false);

                // toss a coin to determine whether to enter or not
                if coin.sample(&mut rng) == 0 && working_capital <= 0.0 {
                    continue;
                }

                let entry_amount = match condition.capital_management {
                    CapitalManagement::Normal {
                        entry_size_percent,
                        min_entry_size,
                    } => (((entry_size_percent / 100.0) * working_capital).max(min_entry_size))
                        .min(working_capital),
                    CapitalManagement::LiquidF { min_entry_size } => min_entry_size,
                };

                working_capital -= entry_amount;
                positions.push(Position::new(
                    ohlc.close,
                    ohlc.time.timestamp_millis(),
                    entry_amount,
                    condition.take_profit_when,
                    condition.stop_loss_when,
                    condition.signal_do_command.clone(),
                ));
            }
            // realized the remaining positions
            let last_ohlc = valid_ohlc
                .last()
                .expect("valid_ohlc should have at least 1 item");
            realize_positions(&mut positions, &mut working_capital, last_ohlc, true);

            let result = generate_report(&positions, initial_capital, start_time);
            (result.maximum_drawdown, result.total)
        })
        .fold(
            (
                MaximumDrawdown {
                    amount: 0.0,
                    percent: 0.0,
                },
                Trades {
                    pnl_percent: 0.0,
                    pnl: 0.0,
                    trades: 0,
                },
            ),
            |acc, (m, t)| {
                (
                    MaximumDrawdown {
                        amount: acc.0.amount + m.amount / rounds as f64,
                        percent: acc.0.percent + m.percent / rounds as f64,
                    },
                    Trades {
                        pnl: acc.1.pnl + t.pnl / rounds as f64,
                        pnl_percent: acc.1.pnl_percent + t.pnl_percent / rounds as f64,
                        trades: acc.1.trades + t.trades / rounds,
                    },
                )
            },
        )
}

pub fn backtest(
    valid_ohlc: &[Ohlc],
    valid_fuzzy_output: &[DTValue<Vec<f64>>],
    signal_conditions: &[SignalCondition],
    initial_capital: f64,
) -> Vec<Position> {
    use CapitalManagement::*;
    let mut working_capital = initial_capital;
    let mut positions: Vec<Position> = vec![];

    for (ohlc, signal) in valid_ohlc.iter().zip(valid_fuzzy_output.iter()) {
        // check if the previous position need to be closed or not
        realize_positions(&mut positions, &mut working_capital, ohlc, false);

        if working_capital <= 0.0 {
            continue;
        }

        // determine whether we will enter a position or not
        for condition in signal_conditions {
            let signal_v = signal.value[condition.signal_index as usize];
            if signal_v > condition.signal_threshold {
                // if we enter a position, determine the size of the position and enter it
                // maybe we can many methods of position sizing

                let entry_amount = match condition.capital_management {
                    Normal {
                        entry_size_percent,
                        min_entry_size,
                    } => (((entry_size_percent / 100.0) * working_capital).max(min_entry_size))
                        .min(working_capital),
                    LiquidF { min_entry_size } => liquid_f_entry_size(
                        &positions,
                        min_entry_size,
                        working_capital,
                        signal_v,
                        100.0,
                        condition.signal_threshold,
                    ),
                };

                working_capital -= entry_amount;
                positions.push(Position::new(
                    ohlc.close,
                    ohlc.time.timestamp_millis(),
                    entry_amount,
                    condition.take_profit_when,
                    condition.stop_loss_when,
                    condition.signal_do_command.clone(),
                ));
            }
        }
    }

    // realized the remaining positions
    let last_ohlc = valid_ohlc
        .last()
        .expect("valid_ohlc should have at least 1 item");

    realize_positions(&mut positions, &mut working_capital, last_ohlc, true);
    positions
}

pub fn generate_report(
    positions: &[Position],
    initial_capital: f64,
    start_time: i64,
) -> BacktestResult {
    let mut cumalative_return = initial_capital;
    let mut g = BTreeMap::from([(start_time / 1000, cumalative_return)]);
    for p in positions {
        if let Some(rel) = &p.realized {
            cumalative_return += rel.pnl;

            g.entry(rel.exit_time / 1000)
                .and_modify(|v| *v = cumalative_return)
                .or_insert(cumalative_return);
        }
    }

    let g = g
        .into_iter()
        .map(|(time, value)| CumalativeReturn { time, value })
        .collect::<Vec<_>>();

    let mut maximum_drawdown = 0.0;
    let mut gt = 1.0;
    for r in 0..g.len() {
        for t in 0..r {
            let dd = g[t].value - g[r].value;
            if dd > 0.0 && dd > maximum_drawdown {
                maximum_drawdown = dd;
                gt = g[t].value;
            }
        }
    }

    let (profit_trades, loss_trades) =
        positions.iter().fold(((0.0, 0i64), (0.0, 0i64)), |acc, p| {
            if let Some(rel) = &p.realized {
                if rel.pnl >= 0.0 {
                    return ((acc.0 .0 + rel.pnl, acc.0 .1 + 1), acc.1);
                } else {
                    return (acc.0, (acc.1 .0 + rel.pnl, acc.1 .1 + 1));
                }
            }
            acc
        });

    let total = (
        profit_trades.0 + loss_trades.0,
        profit_trades.1 + loss_trades.1,
    );

    BacktestResult {
        maximum_drawdown: MaximumDrawdown {
            amount: maximum_drawdown,
            percent: to_percent(maximum_drawdown, gt),
        },
        profit_trades: Trades {
            pnl: profit_trades.0,
            pnl_percent: to_percent(profit_trades.0, initial_capital),
            trades: profit_trades.1,
        },
        loss_trades: Trades {
            pnl: loss_trades.0,
            pnl_percent: to_percent(loss_trades.0, initial_capital),
            trades: loss_trades.1,
        },
        total: Trades {
            pnl: total.0,
            pnl_percent: to_percent(total.0, initial_capital),
            trades: total.1,
        },
        cumalative_return: g,
    }
}

pub async fn save_backtest_report(
    db: &web::Data<Client>,
    username: &String,
    symbol: &String,
    interval: &Interval,
    preset: &String,
    backtest_result: BacktestResultWithRequest,
    run_at: i64,
) -> Result<(BacktestReport, String), CustomError> {
    let result = BacktestReport {
        username: username.to_string(),
        ticker: symbol.to_string(),
        interval: interval.to_owned(),
        fuzzy_preset: preset.to_string(),
        backtest_result,
        run_at,
    };
    let collection = get_backtest_coll(db);
    let inserted_result = collection
        .insert_one(result.clone(), None)
        .await
        .map_err(map_internal_err)?;

    Ok((
        result,
        inserted_result
            .inserted_id
            .as_object_id()
            .unwrap()
            .to_string(),
    ))
}

pub async fn create_backtest_report(
    db: web::Data<Client>,
    request: BacktestRequest,
    user: &User,
    symbol: &String,
    interval: &Interval,
    preset: &String,
) -> Result<BacktestReport, CustomError> {
    let ohlc_data = fetch_symbol(&db, symbol, &Some(interval.clone())).await;
    let fuzzy_config = get_fuzzy_config(&db, &ohlc_data, preset, user).await?;
    let fuzzy_output = fuzzy_indicator(&fuzzy_config.0, fuzzy_config.1);

    let valid_ohlc = get_valid_data(ohlc_data.0, request.start_time, request.end_time);
    let valid_fuzzy_output = get_valid_data(fuzzy_output, request.start_time, request.end_time);

    let positions = backtest(
        &valid_ohlc,
        &valid_fuzzy_output,
        &request.signal_conditions,
        request.capital,
    );

    let backtest_result = BacktestResultWithRequest {
        result: generate_report(&positions, request.capital, request.start_time),
        metadata: BacktestMetadata::NormalBackTest(request),
    };

    let run_at = Utc::now().timestamp_millis();
    Ok(save_backtest_report(
        &db,
        &user.username,
        symbol,
        interval,
        preset,
        backtest_result,
        run_at,
    )
    .await?
    .0)
}

pub async fn get_backtest_reports(
    db: web::Data<Client>,
    username: String,
) -> Result<Vec<BacktestReportWithId>, CustomError> {
    let collection = get_backtest_coll(&db);
    let find_options = FindOptions::builder().sort(doc! { "run_at": - 1}).build();
    collection
        .find(doc! { "username": username }, find_options)
        .await
        .map_err(map_internal_err)?
        .try_collect::<Vec<_>>()
        .await
        .map_err(map_internal_err)
}

pub async fn get_backtest_report(
    db: &web::Data<Client>,
    id: String,
) -> Result<BacktestReportWithId, CustomError> {
    let collection = get_backtest_coll::<BacktestReportWithId>(db);
    let obj_id = oid::ObjectId::from_str(&id).map_err(map_internal_err)?;
    collection
        .find_one(doc! { "_id": obj_id }, None)
        .await
        .map_err(map_internal_err)?
        .ok_or(CustomError::BacktestReportNotFound)
}

pub async fn delete_backtest_report(db: &web::Data<Client>, id: String) -> Result<(), CustomError> {
    let collection = get_backtest_coll::<BacktestReportWithId>(db);
    let obj_id = oid::ObjectId::from_str(&id).map_err(map_internal_err)?;

    let result = collection
        .delete_one(doc! { "_id": obj_id }, None)
        .await
        .map_err(map_internal_err)?;

    if result.deleted_count == 0 {
        return Err(CustomError::BacktestReportNotFound);
    }
    Ok(())
}

#[derive(Serialize, Deserialize, Clone)]
pub struct RandomBacktestReport {
    maximum_drawdown: MaximumDrawdown,
    total: Trades,
}

pub async fn create_random_backtest_report(
    db: web::Data<Client>,
    request: BacktestRequest,
    symbol: &String,
    interval: &Interval,
) -> RandomBacktestReport {
    let ohlc_data = fetch_symbol(&db, symbol, &Some(interval.clone())).await;
    let valid_ohlc = get_valid_data(ohlc_data.0, request.start_time, request.end_time);

    let (maximum_drawdown, total) = random_backtest(
        &valid_ohlc,
        request.capital,
        request.signal_conditions[0].clone(),
        request.start_time,
    );

    RandomBacktestReport {
        maximum_drawdown,
        total,
    }
}
