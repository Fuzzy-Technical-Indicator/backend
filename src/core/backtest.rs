use crate::core::{Interval};
use actix_web::web;
use chrono::Utc;
use mongodb::Client;
use serde::{Deserialize, Serialize};
use tech_indicators::{fuzzy::fuzzy_indicator, DTValue, Ohlc};

use super::{
    fetch_symbol,
    fuzzy::{get_fuzzy_config},
    users::User,
};

#[derive(Debug)]
enum PosType {
    Long,
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
    signal_do_command: i8, // 0 is long, 1 is short
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

#[derive(Serialize, Deserialize)]
pub struct Trades {
    pnl: f64,
    pnl_percent: f64,
    trades: i64,
}

#[derive(Serialize, Deserialize)]
pub struct MaximunDrawdown {
    amount: f64,
    percent: f64,
}

#[derive(Serialize, Deserialize)]
pub struct BacktestResult {
    backtest_request: BacktestRequest,
    maximum_drawdown: MaximunDrawdown,
    profit_trades: Trades,
    loss_trades: Trades,
    total: Trades,
    run_at: i64,
}

#[derive(Serialize, Deserialize)]
pub struct BacktestReport {
    user: String,
    ticker: String,
    interval: Interval,
    fuzzy_preset: String,
    backtest_result: BacktestResult,
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
                                working_capital += pnl;
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
                                working_capital += pnl;
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
                let entry_amount =
                    ((condition.entry_size_percent / 100.0) * working_capital).min(working_capital);

                working_capital -= entry_amount;
                positions.push(Position::new(
                    ohlc.close,
                    ohlc.time.timestamp_millis(),
                    entry_amount,
                    condition.take_profit_when,
                    condition.stop_loss_when,
                    match condition.signal_do_command {
                        0 => PosType::Long,
                        1 => PosType::Short,
                        _ => todo!(),
                    },
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
            None => {
                let realized_amount = (p.amount / p.enter_price) * last_ohlc.close;
                let pnl = realized_amount - p.amount;
                p.realized = Some(RealizedInfo {
                    pnl,
                    exit_price: last_ohlc.close,
                    exit_time: last_ohlc.time.timestamp_millis(),
                });
            }
        }
    }

    positions
}

fn generate_report(
    positions: &[Position],
    initial_capital: f64,
) -> (MaximunDrawdown, Trades, Trades, Trades) {
    let mut cumalative_return = initial_capital;
    let mut g = vec![cumalative_return];
    for p in positions {
        if let Some(rel) = &p.realized {
            cumalative_return += rel.pnl;
            g.push(cumalative_return);
        }
    }

    let mut maximum_drawdown = f64::MAX;
    let mut gt = None;
    for r in 0..g.len() {
        for t in 0..r {
            let dd = g[t] - g[r];
            if dd < 0.0 && dd < maximum_drawdown {
                maximum_drawdown = dd;
                gt = Some(g[t]);
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
            percent: to_percent(maximum_drawdown, gt.unwrap()),
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
    )
}

pub async fn run_backtest(
    db: web::Data<Client>,
    request: BacktestRequest,
    user: &User,
    symbol: &String,
    interval: &Interval,
    preset: &String,
) -> BacktestReport {
    let ohlc_data = fetch_symbol(&db, symbol, &Some(interval.clone())).await;
    let fuzzy_config = get_fuzzy_config(&db, &ohlc_data, preset, user).await;
    let fuzzy_output = match fuzzy_config {
        Ok(v) => fuzzy_indicator(&v.0, v.1),
        Err(_) => todo!(),
    };

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

    let (maximum_drawdown, profit_trades, loss_trades, total) =
        generate_report(&positions, request.capital);

    //let db_client = (*db).database(DB_NAME);
    //let collection = db_client.collection("backtest-reports");

    let backtest_result = BacktestResult {
        backtest_request: request,
        maximum_drawdown,
        profit_trades,
        loss_trades,
        total,
        run_at: Utc::now().timestamp_millis(),
    };

    BacktestReport {
        user: user.username.to_string(),
        ticker: symbol.to_string(),
        interval: interval.to_owned(),
        fuzzy_preset: preset.to_string(),
        backtest_result,
    }
}
