use cached::proc_macro::cached;
use chrono::{Timelike, Utc};
use futures::stream::TryStreamExt;
use mongodb::{
    bson::{doc, Document},
    Collection,
};
use rocket::serde::json::Json;
use rocket_db_pools::{mongodb, Connection};
use std::time::Instant;
use tech_indicators::{adx, bb, fuzzy::fuzzy_indicator, macd, my_macd, rsi, DTValue, Ohlc};

use crate::{Interval, MarketData};

const DEBUG: bool = true;

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
    docs.iter()
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
        .collect()
}

async fn aggr_fetch(collection: &Collection<Ohlc>, interval: &Option<Interval>) -> Vec<Ohlc> {
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
                }},
                doc! {"$sort": {"_id.time": 1}},
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
    db: Connection<MarketData>,
    symbol: &str,
    interval: &Option<Interval>,
) -> Vec<Ohlc> {
    let now = Instant::now();

    let db_client = (*db).database("StockMarket");
    let collection = db_client.collection::<Ohlc>(symbol);
    let result = aggr_fetch(&collection, interval).await;

    println!(
        "fetch_symbol, time elapsed: {}ms",
        now.elapsed().as_millis()
    );

    result
}

#[cached(
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", _symbol, _interval, cachable_dt()) }"#
)]
pub fn fuzzy_cached(
    data: &[Ohlc],
    _symbol: &str,
    _interval: Option<Interval>,
) -> Json<Vec<DTValue<Vec<f64>>>> {
    if DEBUG {
        let rsi_v = measure_time(|| rsi(data, 14), "rsi");
        let bb_v = measure_time(|| bb(data, 20, 2.0), "bb");
        let price = measure_time(|| data.iter().map(|x| x.close).collect(), "price");
        let result = measure_time(|| fuzzy_indicator(rsi_v, bb_v, price), "fuzzy");
        return measure_time(|| Json(result), "json");
    }

    let rsi_v = rsi(data, 14);
    let bb_v = bb(data, 20, 2.0);
    let price = data.iter().map(|x| x.close).collect();
    let result = fuzzy_indicator(rsi_v, bb_v, price);
    Json(result)
}

#[cached(
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", _symbol, _interval, cachable_dt()) }"#
)]
pub fn rsi_cached(
    data: &[Ohlc],
    _symbol: &str,
    _interval: Option<Interval>,
) -> Json<Vec<DTValue<f64>>> {
    Json(rsi(data, 14))
}

#[cached(
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", _symbol, _interval, cachable_dt()) }"#
)]
pub fn bb_cached(
    data: &[Ohlc],
    _symbol: &str,
    _interval: Option<Interval>,
) -> Json<Vec<DTValue<(f64, f64, f64)>>> {
    Json(bb(data, 20, 2.0))
}

#[cached(
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", _symbol, _interval, cachable_dt()) }"#
)]
pub fn macd_cached(
    data: &[Ohlc],
    _symbol: &str,
    _interval: Option<Interval>,
) -> Json<Vec<DTValue<(f64, f64, f64)>>> {
    Json(macd(data))
}

#[cached(
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", _symbol, _interval, cachable_dt()) }"#
)]
pub fn adx_cached(
    data: &[Ohlc],
    _symbol: &str,
    _interval: Option<Interval>,
) -> Json<Vec<DTValue<f64>>> {
    Json(adx(data, 14))
}

#[cached(
    key = "String",
    convert = r#"{ format!("{}{:?}{:?}", _symbol, _interval, cachable_dt()) }"#
)]
pub fn mymacd_cached(
    data: &[Ohlc],
    _symbol: &str,
    _interval: Option<Interval>,
) -> Json<Vec<DTValue<f64>>> {
    Json(my_macd(data))
}
