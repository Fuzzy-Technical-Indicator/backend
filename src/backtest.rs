use crate::core::{UserOhlc, Interval};
use chrono::Utc;
use tech_indicators::DTValue;
use serde::{Deserialize, Serialize};

enum PosType {
    Long,
    Short,
}


struct Position {
    at_price: f64,
    at_time: i64,
    amount: f64,
    take_profit_when: f64,
    stop_loss_when: f64,
    pos_type: PosType,
    realize: bool,
}


impl Position {
    pub fn new(at_price: f64, at_time: i64, amount: f64, take_profit_when: f64, stop_loss_when: f64, pos_type: PosType, realize: bool) -> Position {
        Position {
            at_price,
            at_time,
            amount,
            take_profit_when,
            stop_loss_when,
            pos_type,
            realize,
        }
    }
}


#[derive(Deserialize, Serialize, Clone)]
struct SignalCondition {
    signal_index: u64,
    signal_threshold: f64,
    signal_do_command: i8,  // 0 is long, 1 is short
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


#[derive(Serialize)]
pub struct BacktestResult {
    backtest_request: BacktestRequest,
    maximum_drawdown: f64,
    total_profit: f64,
    total_loss: f64,
    total_trade: i64,
    net_profit: f64,
    run_at: i64
}


impl BacktestResult {
    pub fn new(backtest_request: BacktestRequest, maximum_drawdown: f64, total_profit: f64, total_loss: f64, total_trade: i64, net_profit: f64, run_at: i64) -> BacktestResult {
        BacktestResult {
            backtest_request,
            maximum_drawdown,
            total_profit,
            total_loss,
            total_trade,
            net_profit,
            run_at,
        }
    }
}


#[derive(Serialize)]
pub struct BacktestReport {
    user: String,
    ticker: String,
    interval: Option<Interval>,
    fuzzy_preset: String,
    backtest_result: BacktestResult
}

impl BacktestReport {
    pub fn new(user: String, ticker: String, interval: Option<Interval>, fuzzy_preset: String, backtest_result: BacktestResult) -> BacktestReport {
        BacktestReport {
            user,
            ticker,
            interval,
            fuzzy_preset,
            backtest_result
        }
    }
}


// fn get_ohlc_in_timerange(ohlc_datas: Vec<UserOhlc>, start_time: i64, end_time: i64) -> Vec<UserOhlc> {
//     return ohlc_datas.iter()
//         .cloned()
//         .filter(|ohlc| ohlc.time >= start_time && ohlc.time <= end_time)
//         .collect();
// }


// fn get_fuzzy_output_in_timerange(fuzzy_output:Vec<DTValue<Vec<f64>>>, start_time: i64, end_time: i64) -> Vec<DTValue<Vec<f64>>> {
//     return fuzzy_output.iter()
//         .cloned()
//         .filter(|output| output.time >= start_time && output.time <= end_time)
//         .collect();
// }


// fn ordering_by_fuzzy(request: BacktestRequest, ohlc_datas: Vec<UserOhlc>, fuzzy_output: Vec<DTValue<Vec<f64>>>) -> Vec<Position> {

//     let mut working_capital = request.capital;
//     let mut orders: Vec<Position> = vec![];

//     for (_, (ohlc, signal)) in ohlc_datas.iter().zip(fuzzy_output.iter()).enumerate() {

//         if working_capital <= 0.0 {break};

//         for condition in request.signal_conditions {

//             if signal.value[condition.signal_index as usize] > condition.signal_threshold {

//                 let entry_amount = (condition.entry_size_percent / 100.0) * request.capital;

//                 if working_capital - entry_amount <= 0.0 {continue;}

//                 // Long
//                 if condition.signal_do_command == 0 {
//                     working_capital -= entry_amount;
//                     orders.push(Position::new(ohlc.close, ohlc.time, entry_amount, condition.take_profit_when, condition.stop_loss_when, PosType::Long, false))
//                 }

//                 // Short
//                 else if condition.signal_do_command == 1 {
//                     working_capital -= entry_amount;
//                     orders.push(Position::new(ohlc.close, ohlc.time, entry_amount, condition.take_profit_when, condition.stop_loss_when, PosType::Short, false))
//                 }

//             }
//         }
//     }

//     return orders
// }



// - Symbol
// - Period
// - Initial deposit
// - Total net profit
// - Profit factor
// - Absolute drawdown
// - Total trades
// - Maximum drawdown
// - Short positions (won %)
// - Profit trades (% of total)
// - Largest profit trade
// - Average profit trade
// - Maximum consecutive wins (profit in money)
// - Maximal consecutive profit (count of wins)
// - Average consecutive wins
// - Relative drawdown
// - Long positions (won %)
// - Loss trades (% of total)
// - Largest loss trade
// - Average loss trade
// - Maximum consecutive losses (loss in money)
// - Maximal consecutive loss (count of losses)
// - Average consecutive losses

// fn realizing_orders(ohlc_datas: Vec<UserOhlc>, orders: &Vec<Position>) -> (f64, f64, f64, usize, f64) {

//     let mut profit: Vec<f64> = vec![];
//     let mut losses: Vec<f64> = vec![];
//     let mut out_orders: Vec<Position> = vec![];

//     for (idx, c) in ohlc_datas.iter().enumerate() {
//         let p = c.close;
//         for j in 0..orders.len() {
//             if c.time <= orders[j].at_time || orders[j].realize == true {
//                 continue;
//             }
//             let diff = (p - orders[j].at_price) / orders[j].at_price * 100.0;
//             match orders[j].pos_type {
//                 PosType::Long => {
//                     if diff > orders[j].take_profit_when {
//                         profit.push((p - orders[j].at_price) * orders[j].amount);
//                         orders[j].realize = true;
//                         out_orders.push(Position::new(c.close, c.time, orders[j].amount, orders[j].take_profit_when, orders[j].stop_loss_when, PosType::Long, true))
//                     } else if diff < orders[j].stop_loss_when {
//                         losses.push((p - orders[j].at_price) * orders[j].amount);
//                         orders[j].realize = true;
//                         out_orders.push(Position::new(c.close, c.time, orders[j].amount, orders[j].take_profit_when, orders[j].stop_loss_when, PosType::Long, true))
//                     }
//                 }
//                 PosType::Short => {
//                     if diff < orders[j].take_profit_when {
//                         profit.push(-1.0 * (p - orders[j].at_price) * orders[j].amount);
//                         orders[j].realize = true;
//                         out_orders.push(Position::new(c.close, c.time, orders[j].amount, orders[j].take_profit_when, orders[j].stop_loss_when, PosType::Short, true))
//                     } else if diff > orders[j].stop_loss_when {
//                         losses.push(-1.0 * (p - orders[j].at_price) * orders[j].amount);
//                         orders[j].realize = true;
//                         out_orders.push(Position::new(c.close, c.time, orders[j].amount, orders[j].take_profit_when, orders[j].stop_loss_when, PosType::Short, true))
//                     }
//                 },
//             }
//         }
//     }

//     let maximum_drawdown = 0.0;
//     let total_profit = profit.iter().fold(0.0, |s, x| s + x);
//     let total_losses = losses.iter().fold(0.0, |s, x| s + x);
//     let total_trade = out_orders.len();
//     let net_profit = total_profit + total_losses;

//     println!("total trade: {:.3}", total_trade);
//     println!("net profit: {:.3}", net_profit);
//     println!("count: {}, profits: {:.3}", profit.len(), total_profit);
//     println!("count: {}, losses: {:.3}", losses.len(), total_losses);
//     println!("---------------");

//     return (maximum_drawdown, total_profit, total_losses, total_trade, net_profit)
// }


pub async fn run_backtest(request: BacktestRequest, ohlc_datas: Vec<UserOhlc>, fuzzy_output: Vec<DTValue<Vec<f64>>>) -> BacktestResult {

    // let ohlc_datas_inrange = get_ohlc_in_timerange(ohlc_datas, request.start_time, request.end_time);
    // let fuzzy_output_inrange = get_fuzzy_output_in_timerange(fuzzy_output, request.start_time, request.end_time);

    // let orders = ordering_by_fuzzy(&request, &ohlc_datas_inrange, &fuzzy_output_inrange);

    // let (maximum_drawdown, total_profit, total_losses, total_trade, net_profit) = realizing_orders(ohlc_datas_inrange, &orders);

    let ohlc_datas_inrange: Vec<UserOhlc> = ohlc_datas.iter()
                                                .cloned()
                                                .filter(|ohlc| ohlc.time >= request.start_time && ohlc.time <= request.end_time)
                                                .collect();

    let fuzzy_output_inrange: Vec<DTValue<Vec<f64>>> = fuzzy_output.iter()
                                                        .cloned()
                                                        .filter(|output| output.time >= request.start_time && output.time <= request.end_time)
                                                        .collect();


    let mut working_capital = request.capital;
    let mut orders: Vec<Position> = vec![];
                                                    
    for (_, (ohlc, signal)) in ohlc_datas_inrange.iter().zip(fuzzy_output_inrange.iter()).enumerate() {
                                                    
        if working_capital <= 0.0 {break};
                                                    
        for condition in &request.signal_conditions {
                                                    
            if signal.value[condition.signal_index as usize] > condition.signal_threshold {
                                                    
                let entry_amount = (condition.entry_size_percent / 100.0) * request.capital;
                                                    
                if working_capital - entry_amount <= 0.0 {continue;}
                                                    
                // Long
                if condition.signal_do_command == 0 {
                    working_capital -= entry_amount;
                    orders.push(Position::new(ohlc.close, ohlc.time, entry_amount, condition.take_profit_when, condition.stop_loss_when, PosType::Long, false))
                }
                                                    
                // Short
                else if condition.signal_do_command == 1 {
                    working_capital -= entry_amount;
                    orders.push(Position::new(ohlc.close, ohlc.time, entry_amount, condition.take_profit_when, condition.stop_loss_when, PosType::Short, false))
                }
                                                    
            }
        }
    }

    let mut profit: Vec<f64> = vec![];
    let mut losses: Vec<f64> = vec![];
    let mut out_orders: Vec<Position> = vec![];

    for (_, c) in ohlc_datas_inrange.iter().enumerate() {
        let p = c.close;
        for j in 0..orders.len() {
            if c.time <= orders[j].at_time || orders[j].realize == true {
                continue;
            }
            let diff = (p - orders[j].at_price) / orders[j].at_price * 100.0;
            match orders[j].pos_type {
                PosType::Long => {
                    if diff > orders[j].take_profit_when {
                        profit.push((p - orders[j].at_price) * orders[j].amount);
                        orders[j].realize = true;
                        out_orders.push(Position::new(c.close, c.time, orders[j].amount, orders[j].take_profit_when, orders[j].stop_loss_when, PosType::Long, true))
                    } else if diff < orders[j].stop_loss_when {
                        losses.push((p - orders[j].at_price) * orders[j].amount);
                        orders[j].realize = true;
                        out_orders.push(Position::new(c.close, c.time, orders[j].amount, orders[j].take_profit_when, orders[j].stop_loss_when, PosType::Long, true))
                    }
                }
                PosType::Short => {
                    if diff < orders[j].take_profit_when {
                        profit.push(-1.0 * (p - orders[j].at_price) * orders[j].amount);
                        orders[j].realize = true;
                        out_orders.push(Position::new(c.close, c.time, orders[j].amount, orders[j].take_profit_when, orders[j].stop_loss_when, PosType::Short, true))
                    } else if diff > orders[j].stop_loss_when {
                        losses.push(-1.0 * (p - orders[j].at_price) * orders[j].amount);
                        orders[j].realize = true;
                        out_orders.push(Position::new(c.close, c.time, orders[j].amount, orders[j].take_profit_when, orders[j].stop_loss_when, PosType::Short, true))
                    }
                },
            }
        }
    }

    let maximum_drawdown = 0.0;
    let total_profit = profit.iter().fold(0.0, |s, x| s + x);
    let total_losses = losses.iter().fold(0.0, |s, x| s + x);
    let total_trade = out_orders.len();
    let net_profit = total_profit + total_losses;

    let current_time = Utc::now();

    let run_at = current_time.timestamp_millis();

    return BacktestResult::new(request, maximum_drawdown, total_profit, total_losses, total_trade as i64, net_profit, run_at);
}