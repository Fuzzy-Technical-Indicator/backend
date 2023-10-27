use actix_web::web;
use cached::proc_macro::cached;
use chrono::{Timelike, Utc};
use futures::stream::TryStreamExt;
use mongodb::{
    bson::{doc, Document},
    Client, Collection
};
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tech_indicators::{
    accum_dist, adx, aroon, bb, fuzzy::fuzzy_indicator, macd, naranjo_macd, obv, rsi, stoch,
    DTValue, Ohlc,
};

use crate::Interval;

const DEBUG: bool = false;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct UserOhlc {
    pub ticker: String,
    pub time: i64,
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub volume: u64,
}

pub fn measure_time<T, F: FnOnce() -> T>(f: F, msg: &str) -> T {
    let now = Instant::now();
    let result = f();
    println!("{}, time elapsed: {}ms", msg, now.elapsed().as_millis());
    result
}

pub fn cachable_dt() -> (u32, bool) {
    let curr = Utc::now();
    let gt_thirty = curr.minute() > 30;
    (curr.hour(), gt_thirty)
}

fn aggrdoc_to_ohlc(docs: Vec<Document>) -> Vec<Ohlc> {
    let mut result = docs.iter()
        .map(|x| Ohlc {
            ticker: x
                .get_document("_id")
                .unwrap()
                .get_str("ticker")
                .unwrap()
                .to_string(),
            time: *x.get_document("_id").unwrap().get_datetime("time").unwrap(),
            open: x.get_f64("open").unwrap(),
            close: x.get_f64("close").unwrap(),
            high: x.get_f64("high").unwrap(),
            low: x.get_f64("low").unwrap(),
            volume: x.get_i64("volume").unwrap() as u64,
        })
        .collect::<Vec<Ohlc>>();
    result.sort_by_key(|dt| dt.time);
    result
}

async fn aggr_fetch(collection: &Collection<Ohlc>, interval: &Option<Interval>) -> Vec<Ohlc> {
    // Our free mongoDB instance doesn't support allowDiskUse so, we are sorting in the app code
    let result = collection
        .aggregate(
            vec![
                doc! {"$group" : {
                    "_id" : {
                        "ticker": "$ticker",
                        "time": {
                            "$dateTrunc" : {
                                "date": "$time",
                                "unit": "hour",
                                "binSize": match interval {
                                    Some(Interval::OneHour) => 1,
                                    Some(Interval::FourHour) => 4,
                                    Some(Interval::OneDay) => 24,
                                    None => 1,
                                }
                            }
                        }
                    },
                    "open": {"$first": "$open"},
                    "close": {"$last": "$close"},
                    "high": {"$max": "$high"},
                    "low": {"$min": "$low"},
                    "volume": {"$sum": "$volume"},
                }}
            ],
            None,
        )
        .await
        .unwrap();

    aggrdoc_to_ohlc(result.try_collect::<Vec<Document>>().await.unwrap())
}

#[cached(
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", symbol, interval, cachable_dt()) }"#
)]
pub async fn fetch_symbol(
    db: web::Data<Client>,
    symbol: &str,
    interval: &Option<Interval>,
) -> Vec<Ohlc> {
    let now = Instant::now();

    let db_client = (*db).database("StockMarket");
    let collection = db_client.collection::<Ohlc>(symbol);
    let result = aggr_fetch(&collection, interval).await;

    if DEBUG {
        println!(
            "fetch_symbol, time elapsed: {}ms",
            now.elapsed().as_millis()
        );
    }

    result
}

#[cached(
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", symbol, interval, cachable_dt()) }"#
)]
pub async fn fetch_user_ohlc(
    db: web::Data<Client>,
    symbol: &str,
    interval: &Option<Interval>,
) -> Vec<UserOhlc> {
    let fetch_result = fetch_symbol(db, symbol, interval).await;
    fetch_result
        .iter()
        .map(|x| UserOhlc {
            ticker: x.ticker.clone(),
            time: x.time.timestamp_millis(),
            high: x.high,
            low: x.low,
            open: x.open,
            close: x.close,
            volume: x.volume,
        })
        .collect()
}

#[cached(
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", _symbol, _interval, cachable_dt()) }"#
)]
pub fn fuzzy_cached(
    data: &[Ohlc],
    _symbol: &str,
    _interval: &Option<Interval>,
) -> Vec<DTValue<Vec<f64>>> {
    if DEBUG {
        let rsi_v = measure_time(|| rsi(data, 14), "rsi");
        let bb_v = measure_time(|| bb(data, 20, 2.0), "bb");
        let price = measure_time(|| data.iter().map(|x| x.close).collect(), "price");
        let result = measure_time(|| fuzzy_indicator(rsi_v, bb_v, price), "fuzzy");
        return measure_time(|| result, "_");
    }

    let rsi_v = rsi(data, 14);
    let bb_v = bb(data, 20, 2.0);
    let price = data.iter().map(|x| x.close).collect();
    fuzzy_indicator(rsi_v, bb_v, price)
}

#[cached(
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", _symbol, _interval, cachable_dt()) }"#
)]
pub fn rsi_cached(data: &[Ohlc], _symbol: &str, _interval: &Option<Interval>) -> Vec<DTValue<f64>> {
    rsi(data, 14)
}

#[cached(
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", _symbol, _interval, cachable_dt()) }"#
)]
pub fn bb_cached(
    data: &[Ohlc],
    _symbol: &str,
    _interval: &Option<Interval>,
) -> Vec<DTValue<(f64, f64, f64)>> {
    bb(data, 20, 2.0)
}

#[cached(
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", _symbol, _interval, cachable_dt()) }"#
)]
pub fn macd_cached(
    data: &[Ohlc],
    _symbol: &str,
    _interval: &Option<Interval>,
) -> Vec<DTValue<(f64, f64, f64)>> {
    macd(data, 12, 26, 9)
}

#[cached(
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", _symbol, _interval, cachable_dt()) }"#
)]
pub fn adx_cached(data: &[Ohlc], _symbol: &str, _interval: &Option<Interval>) -> Vec<DTValue<f64>> {
    adx(data, 14)
}

#[cached(
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", _symbol, _interval, cachable_dt()) }"#
)]
pub fn obv_cached(data: &[Ohlc], _symbol: &str, _interval: &Option<Interval>) -> Vec<DTValue<f64>> {
    obv(data)
}

#[cached(
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", _symbol, _interval, cachable_dt()) }"#
)]
pub fn aroon_cached(
    data: &[Ohlc],
    _symbol: &str,
    _interval: &Option<Interval>,
) -> Vec<DTValue<(f64, f64)>> {
    aroon(data, 14)
}

#[cached(
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", _symbol, _interval, cachable_dt()) }"#
)]
pub fn accum_dist_cached(
    data: &[Ohlc],
    _symbol: &str,
    _interval: &Option<Interval>,
) -> Vec<DTValue<f64>> {
    accum_dist(&data)
}

#[cached(
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", _symbol, _interval, cachable_dt()) }"#
)]
pub fn stoch_cached(
    data: &[Ohlc],
    _symbol: &str,
    _interval: &Option<Interval>,
) -> Vec<DTValue<(f64, f64)>> {
    stoch(data, 14, 3, 1)
}

#[cached(
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", _symbol, _interval, cachable_dt()) }"#
)]
pub fn naranjo_macd_cached(
    data: &[Ohlc],
    _symbol: &str,
    _interval: &Option<Interval>,
) -> Vec<DTValue<f64>> {
    naranjo_macd(data)
}
