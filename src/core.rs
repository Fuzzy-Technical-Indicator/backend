use std::time::Instant;

use chrono::{Timelike, Utc};
use cached::proc_macro::cached;
use rocket::serde::json::Json;
use tech_indicators::{Ohlc, DTValue, fuzzy::fuzzy_indicator, rsi, bb};

use crate::Interval;

const DEBUG: bool = true;

pub fn measure_time<T, F: FnOnce() -> T>(f: F, msg: &str) -> T {
    let now = Instant::now();
    let result = f();
    println!("{}, time elapsed: {}ms", msg, now.elapsed().as_millis());
    result
}

pub fn cachable_dt() -> (u32, bool) {
    let curr = Utc::now();
    let gt_thirty = if curr.minute() > 30 { true } else { false };
    (curr.hour(), gt_thirty)
}

#[cached(
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", _symbol, _interval, _dt) }"#
)]
pub fn fuzzy_f(
    data: &Vec<Ohlc>,
    _symbol: &str,
    _interval: Option<Interval>,
    _dt: (u32, bool),
) -> Json<Vec<DTValue<Vec<f64>>>> {
    if DEBUG {
        let rsi_v = measure_time(|| rsi(data, 14), "rsi");
        let bb_v = measure_time(|| bb(data, 20, 2.0), "bb");
        let price = measure_time(|| data.iter().map(|x| x.close).collect(), "price");
        let result = measure_time(|| fuzzy_indicator(rsi_v, bb_v, price), "fuzzy");
        return measure_time(|| Json(result), "json");
    }

    let rsi_v = rsi(data, 14);
    let bb_v =bb(data, 20, 2.0);
    let price = data.iter().map(|x| x.close).collect();
    let result = fuzzy_indicator(rsi_v, bb_v, price);
    Json(result)
}
