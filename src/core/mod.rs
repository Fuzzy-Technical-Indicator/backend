pub mod backtest;
pub mod error;
pub mod fuzzy;
pub mod optimization;
pub mod settings;
pub mod users;

use actix_web::web;
use cached::proc_macro::cached;
use chrono::{Timelike, Utc};
use futures::stream::TryStreamExt;

use mongodb::{
    bson::{doc, Document},
    Client, Collection,
};
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tech_indicators::{
    accum_dist, adx, aroon, bb, fuzzy::fuzzy_indicator, macd, naranjo_macd, obv, rsi, stoch,
    DTValue, Ohlc,
};

use self::{
    error::{map_internal_err, CustomError},
    users::User,
};

pub const DB_NAME: &str = "StockMarket";

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone, Hash, Eq)]
pub enum Interval {
    #[serde(rename = "1h")]
    OneHour,
    #[serde(rename = "4h")]
    FourHour,
    #[serde(rename = "1d")]
    OneDay,
}

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
    let mut result = docs
        .iter()
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
            vec![doc! {"$group" : {
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
            }}],
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
    db: &Client,
    symbol: &str,
    interval: &Option<Interval>,
) -> (Vec<Ohlc>, String) {
    let db_client = (*db).database(DB_NAME);
    let collection = db_client.collection::<Ohlc>(symbol);
    let result = aggr_fetch(&collection, interval).await;

    let label = format!("{}{:?}", symbol, interval);
    (result, label)
}

#[cached(
    time = 120,
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", symbol, interval, cachable_dt()) }"#
)]
pub async fn fetch_user_ohlc(
    db: web::Data<Client>,
    symbol: &str,
    interval: &Option<Interval>,
) -> Vec<UserOhlc> {
    let (fetch_result, _) = fetch_symbol(&db, symbol, interval).await;
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

pub async fn fuzzy_cached(
    db: web::Data<Client>,
    data: (Vec<Ohlc>, String),
    preset: &String,
    user: User,
) -> Result<Vec<DTValue<Vec<f64>>>, CustomError> {
    let (fuzzy_engine, inputs) = fuzzy::get_fuzzy_config(&db, &data, preset, &user)
        .await
        .map_err(map_internal_err)?;
    Ok(fuzzy_indicator(&fuzzy_engine, inputs))
}

#[cached(
    time = 120,
    key = "String",
    convert = r#"{ format!("{}{}{:?}", length, data.1, cachable_dt()) }"#
)]
pub fn rsi_cached(data: (Vec<Ohlc>, String), length: usize) -> Vec<DTValue<f64>> {
    rsi(&data.0, length)
}

#[cached(
    time = 120,
    key = "String",
    convert = r#"{ format!("{}{}{:?}{:?}", length, stdev, data.1, cachable_dt()) }"#
)]
pub fn bb_cached(
    data: (Vec<Ohlc>, String),
    length: usize,
    stdev: f64,
) -> Vec<DTValue<(f64, f64, f64)>> {
    bb(&data.0, length, stdev)
}

#[cached(
    time = 120,
    key = "String",
    convert = r#"{ format!("{}{}{}{}{:?}", fast, slow, smooth, data.1, cachable_dt()) }"#
)]
pub fn macd_cached(
    data: (Vec<Ohlc>, String),
    fast: usize,
    slow: usize,
    smooth: usize,
) -> Vec<DTValue<(f64, f64, f64)>> {
    macd(&data.0, fast, slow, smooth)
}

#[cached(
    time = 120,
    key = "String",
    convert = r#"{ format!("{}{}{:?}", length, data.1, cachable_dt()) }"#
)]
pub fn adx_cached(data: (Vec<Ohlc>, String), length: usize) -> Vec<DTValue<f64>> {
    adx(&data.0, length)
}

#[cached(
    time = 120,
    key = "String",
    convert = r#"{ format!("{}{:?}", data.1, cachable_dt()) }"#
)]
pub fn obv_cached(data: (Vec<Ohlc>, String)) -> Vec<DTValue<f64>> {
    obv(&data.0)
}

#[cached(
    time = 120,
    key = "String",
    convert = r#"{ format!("{}{}{:?}", length, data.1, cachable_dt()) }"#
)]
pub fn aroon_cached(data: (Vec<Ohlc>, String), length: usize) -> Vec<DTValue<(f64, f64)>> {
    aroon(&data.0, length)
}

#[cached(
    time = 120,
    key = "String",
    convert = r#"{ format!("{}{:?}", data.1, cachable_dt()) }"#
)]
pub fn accum_dist_cached(data: (Vec<Ohlc>, String)) -> Vec<DTValue<f64>> {
    accum_dist(&data.0)
}

#[cached(
    time = 120,
    key = "String",
    convert = r#"{ format!("{}{}{}{}{:?}", data.1, k, d, length, cachable_dt()) }"#
)]
pub fn stoch_cached(
    data: (Vec<Ohlc>, String),
    k: usize,
    d: usize,
    length: usize,
) -> Vec<DTValue<(f64, f64)>> {
    stoch(&data.0, k, d, length)
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
