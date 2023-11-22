use actix_web::web;
use cached::proc_macro::cached;
use chrono::{Timelike, Utc};
use futures::stream::TryStreamExt;
use fuzzy_logic::{
    linguistic::LinguisticVar,
    shape::{triangle, zero},
};
use mongodb::{
    bson::{doc, to_bson, Bson, Document},
    options::UpdateOptions,
    Client, Collection,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::HashMap, time::Instant};
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
    db: web::Data<Client>,
    symbol: &str,
    interval: &Option<Interval>,
) -> (Vec<Ohlc>, String) {
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
    let (fetch_result, _) = fetch_symbol(db, symbol, interval).await;
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
    time = 120,
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
    convert = r#"{ format!("{}{:?}", data.1, cachable_dt()) }"#
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

#[derive(Deserialize, Serialize)]
pub struct LinguisticVarSetting {
    labels: Vec<f64>,
    #[serde(rename(serialize = "upperBoundary", deserialize = "upperBoundary"))]
    upper_boundary: f64,
    #[serde(rename(serialize = "lowerBoundary", deserialize = "lowerBoundary"))]
    lower_boundary: f64,
    graphs: HashMap<String, Value>,
}

#[derive(Deserialize, Serialize)]
pub struct Settings {
    #[serde(rename(serialize = "linguisticVariables", deserialize = "linguisticVariables"))]
    linguistic_variables: HashMap<String, LinguisticVarSetting>,
}

fn to_settings(var: &LinguisticVar) -> LinguisticVarSetting {
    let xs = var.get_finite_universe(1.0);
    let mut ys = HashMap::new();
    for (name, set) in var.sets.iter() {
        let data = json!({
            "type": set.membership_f.name,
            "parameters": set.membership_f.parameters,
            "data": xs.iter().map(|x| set.degree_of(*x)).collect::<Vec<f64>>(),
        });

        ys.insert(name.to_string(), data);
    }

    LinguisticVarSetting {
        labels: xs,
        graphs: ys,
        lower_boundary: var.universe.0,
        upper_boundary: var.universe.1,
    }
}

pub async fn get_settings(db: web::Data<Client>) -> Settings {
    let db_client = (*db).database("StockMarket");
    let collection = db_client.collection::<SettingsModel>("settings");

    // hard coded username
    let settings = collection
        .find_one(doc! { "username": "tanat" }, None)
        .await
        .unwrap()
        .unwrap();
    let mut linguistic_variables = HashMap::new();

    for (k, v) in settings.linguistic_variables.iter() {
        let var = LinguisticVar::new(
            v.shapes
                .iter()
                .map(|(name, shape_info)| {
                    let f = match shape_info.shape_type.as_str() {
                        "triangle" => triangle(
                            *shape_info.parameters.get("center").unwrap(),
                            *shape_info.parameters.get("height").unwrap(),
                            *shape_info.parameters.get("width").unwrap(),
                        ),
                        _ => zero(),
                    };
                    return (name.as_str(), f);
                })
                .collect(),
            (v.lower_boundary, v.upper_boundary),
        );
        linguistic_variables.insert(k.to_string(), to_settings(&var));
    }

    Settings {
        linguistic_variables,
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct LinguisticVarShapeInfo {
    parameters: HashMap<String, f64>,
    #[serde(rename(serialize = "shapeType", deserialize = "shapeType"))]
    shape_type: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct LinguisticVarInfo {
    #[serde(rename(serialize = "upperBoundary", deserialize = "upperBoundary"))]
    upper_boundary: f64,
    #[serde(rename(serialize = "lowerBoundary", deserialize = "lowerBoundary"))]
    lower_boundary: f64,
    shapes: HashMap<String, LinguisticVarShapeInfo>,
}

#[derive(Deserialize, Serialize)]
pub struct SettingsModel {
    username: String,
    #[serde(rename(serialize = "linguisticVariables", deserialize = "linguisticVariables"))]
    linguistic_variables: HashMap<String, LinguisticVarInfo>,
}

pub async fn update_settings(db: web::Data<Client>, info: web::Json<SettingsModel>) -> String {
    let db_client = (*db).database("StockMarket");
    let collection = db_client.collection::<SettingsModel>("settings");

    let data = to_bson(
        &info
            .linguistic_variables
            .iter()
            .map(|(name, info)| {
                let lv_name = format!("linguisticVariables.{}", name);
                let lv_info = to_bson(info).unwrap();
                (lv_name, lv_info)
            })
            .collect::<HashMap<String, Bson>>(),
    )
    .unwrap();
    let options = UpdateOptions::builder().upsert(true).build();
    let update_result = collection
        .update_one(
            doc! { "username": info.username.clone()},
            doc! { "$set": data },
            options,
        )
        .await;

    match update_result {
        Ok(res) => format!("{:?}", res),
        Err(err) => format!("{:?}", err),
    }
}
