use std::collections::BTreeMap;

use crate::core::Interval;
use actix_web::web;
use chrono::Utc;
use futures::TryStreamExt;
use mongodb::{bson::doc, Client};
use serde::{Deserialize, Serialize};
use tech_indicators::{fuzzy::fuzzy_indicator, DTValue, Ohlc};

use super::{
    error::{map_internal_err, CustomError},
    fetch_symbol,
    fuzzy::get_fuzzy_config,
    users::User,
    DB_NAME,
};

const COLLECTION_NAME: &str = "backtest-reports";

#[derive(Serialize, Deserialize, Clone, Debug)]
enum PosType {
    #[serde(rename = "long")]
    Long,
    #[serde(rename = "short")]
    Short,
}

#[derive(Debug)]
struct RealizedInfo {
    pnl: f64,
    exit_price: f64,
    exit_time: i64,
}

#[derive(Debug)]
struct Position {
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

#[derive(Deserialize, Serialize, Clone)]
struct SignalCondition {
    signal_index: u64,
    signal_threshold: f64,
    signal_do_command: PosType,
    entry_size_percent: f64,
    take_profit_when: f64,
    stop_loss_when: f64,
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
    pnl_percent: f64,
    trades: i64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct MaximunDrawdown {
    amount: f64,
    percent: f64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CumalativeReturn {
    time: i64,
    value: f64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BacktestResult {
    backtest_request: BacktestRequest,
    maximum_drawdown: MaximunDrawdown,
    profit_trades: Trades,
    loss_trades: Trades,
    total: Trades,
    cumalative_return: Vec<CumalativeReturn>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BacktestReport {
    username: String,
    ticker: String,
    interval: Interval,
    fuzzy_preset: String,
    backtest_result: BacktestResult,
    run_at: i64,
}

// TODO need to have one randomly entering a postion for backtest

// what to save in db?
// {
//      report,
//      cumlative_return for displaying the graph,
// }

fn to_percent(x: f64, y: f64) -> f64 {
    (x / y) * 100.0
}

fn backtest(
    valid_ohlc: &[Ohlc],
    valid_fuzzy_output: &[DTValue<Vec<f64>>],
    signal_conditions: &[SignalCondition],
    initial_capital: f64,
) -> Vec<Position> {
    let mut working_capital = initial_capital;
    let mut positions: Vec<Position> = vec![];

    for (ohlc, signal) in valid_ohlc.iter().zip(valid_fuzzy_output.iter()) {
        // check if the previous position need to be closed or not
        for p in &mut positions {
            match p.realized {
                Some(_) => continue,
                None => {
                    let p_diff = ((ohlc.close - p.enter_price) / p.enter_price) * 100.0;
                    match p.pos_type {
                        PosType::Long => {
                            if p_diff >= p.take_profit_when || p_diff <= -p.stop_loss_when {
                                let realized_amount = (p.amount / p.enter_price) * ohlc.close;
                                let pnl = realized_amount - p.amount;
                                working_capital += p.amount + pnl;
                                p.realized = Some(RealizedInfo {
                                    pnl,
                                    exit_price: ohlc.close,
                                    exit_time: ohlc.time.timestamp_millis(),
                                });
                            }
                        }
                        PosType::Short => {
                            if -p_diff >= p.take_profit_when || -p_diff <= -p.stop_loss_when {
                                let realized_amount = (p.amount / p.enter_price) * ohlc.close;
                                let pnl = p.amount - realized_amount;
                                working_capital += p.amount + pnl;
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

        if working_capital <= 0.0 {
            continue;
        }

        // determine whether we will enter a position or not
        for condition in signal_conditions {
            if signal.value[condition.signal_index as usize] > condition.signal_threshold {
                // if we enter a position, determine the size of the position and enter it
                // maybe we can many methods of position sizing
                let entry_amount = (((condition.entry_size_percent / 100.0) * working_capital)
                    .max(100f64))
                .min(working_capital);

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
    for p in &mut positions {
        match p.realized {
            Some(_) => continue,
            None => match p.pos_type {
                PosType::Long => {
                    let realized_amount = (p.amount / p.enter_price) * last_ohlc.close;
                    let pnl = realized_amount - p.amount;
                    working_capital += p.amount + pnl;
                    p.realized = Some(RealizedInfo {
                        pnl,
                        exit_price: last_ohlc.close,
                        exit_time: last_ohlc.time.timestamp_millis(),
                    });
                }
                PosType::Short => {
                    let realized_amount = (p.amount / p.enter_price) * last_ohlc.close;
                    let pnl = p.amount - realized_amount;
                    working_capital += p.amount + pnl;
                    p.realized = Some(RealizedInfo {
                        pnl,
                        exit_price: last_ohlc.close,
                        exit_time: last_ohlc.time.timestamp_millis(),
                    });
                }
            },
        }
    }

    positions
}

fn generate_report(
    positions: &[Position],
    initial_capital: f64,
    start_time: i64,
) -> (
    MaximunDrawdown,
    Trades,
    Trades,
    Trades,
    Vec<CumalativeReturn>,
) {
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

    let mut maximum_drawdown = f64::MIN;
    let mut gt = 0.0;
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

    (
        MaximunDrawdown {
            amount: maximum_drawdown,
            percent: to_percent(maximum_drawdown, gt),
        },
        Trades {
            pnl: profit_trades.0,
            pnl_percent: to_percent(profit_trades.0, initial_capital),
            trades: profit_trades.1,
        },
        Trades {
            pnl: loss_trades.0,
            pnl_percent: to_percent(loss_trades.0, initial_capital),
            trades: loss_trades.1,
        },
        Trades {
            pnl: total.0,
            pnl_percent: to_percent(total.0, initial_capital),
            trades: total.1,
        },
        g,
    )
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

    let valid_ohlc = ohlc_data
        .0
        .into_iter()
        .filter(|ohlc| {
            ohlc.time.timestamp_millis() >= request.start_time
                && ohlc.time.timestamp_millis() <= request.end_time
        })
        .collect::<Vec<_>>();
    let valid_fuzzy_output = fuzzy_output
        .into_iter()
        .filter(|output| output.time >= request.start_time && output.time <= request.end_time)
        .collect::<Vec<_>>();

    let positions = backtest(
        &valid_ohlc,
        &valid_fuzzy_output,
        &request.signal_conditions,
        request.capital,
    );

    let (maximum_drawdown, profit_trades, loss_trades, total, cumalative_return) =
        generate_report(&positions, request.capital, request.start_time);

    let backtest_result = BacktestResult {
        backtest_request: request,
        maximum_drawdown,
        profit_trades,
        loss_trades,
        total,
        cumalative_return,
    };
    let result = BacktestReport {
        username: user.username.to_string(),
        ticker: symbol.to_string(),
        interval: interval.to_owned(),
        fuzzy_preset: preset.to_string(),
        backtest_result,
        run_at: Utc::now().timestamp_millis(),
    };
    let db_client = (*db).database(DB_NAME);
    let collection = db_client.collection::<BacktestReport>(COLLECTION_NAME);
    collection
        .insert_one(result.clone(), None)
        .await
        .map_err(map_internal_err)?;

    Ok(result)
}

pub async fn get_backtest_reports(
    db: web::Data<Client>,
    username: String,
) -> Result<Vec<BacktestReport>, CustomError> {
    let db_client = (*db).database(DB_NAME);
    let collection = db_client.collection::<BacktestReport>(COLLECTION_NAME);

    Ok(collection
        .find(doc! { "username": username }, None)
        .await
        .map_err(map_internal_err)?
        .try_collect::<Vec<_>>()
        .await
        .map_err(map_internal_err)?)
}
