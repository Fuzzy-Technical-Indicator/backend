pub mod core;

use crate::core::{
    adx_cached, bb_cached, cachable_dt, fetch_symbol, fuzzy_cached, macd_cached, mymacd_cached,
    rsi_cached,
};
use rocket::serde::json::Json;
use rocket::{get, launch, routes, FromFormField};
use rocket_cors::{AllowedOrigins, Cors, CorsOptions};
use rocket_db_pools::{mongodb, Connection, Database};
use tech_indicators::{DTValue, Ohlc};

// we need to specify the database url on Rocket.toml like this
// [default.databases.marketdata]
// url = "..."
#[derive(Database)]
#[database("marketdata")]
pub struct MarketData(mongodb::Client);

#[derive(Debug, PartialEq, FromFormField, Clone, Hash, Eq)]
pub enum Interval {
    #[field(value = "1h")]
    OneHour,
    #[field(value = "4h")]
    FourHour,
    #[field(value = "1d")]
    OneDay,
}

#[get("/ohlc?<symbol>&<interval>")]
async fn ohlc(
    db: Connection<MarketData>,
    symbol: &str,
    interval: Option<Interval>,
) -> Json<Vec<Ohlc>> {
    Json(fetch_symbol(db, symbol, &interval).await)
}

#[get("/indicator/rsi?<symbol>&<interval>")]
async fn indicator_rsi(
    db: Connection<MarketData>,
    symbol: &str,
    interval: Option<Interval>,
) -> Json<Vec<DTValue<f64>>> {
    let data = fetch_symbol(db, symbol, &interval).await;
    rsi_cached(&data, symbol, interval)
}

#[get("/indicator/bb?<symbol>&<interval>")]
async fn indicator_bb(
    db: Connection<MarketData>,
    symbol: &str,
    interval: Option<Interval>,
) -> Json<Vec<DTValue<(f64, f64, f64)>>> {
    let data = fetch_symbol(db, symbol, &interval).await;
    bb_cached(&data, symbol, interval)
}

#[get("/indicator/macd?<symbol>&<interval>")]
async fn indicator_macd(
    db: Connection<MarketData>,
    symbol: &str,
    interval: Option<Interval>,
) -> Json<Vec<DTValue<(f64, f64, f64)>>> {
    let data = fetch_symbol(db, symbol, &interval).await;
    macd_cached(&data, symbol, interval)
}

#[get("/indicator/adx?<symbol>&<interval>")]
async fn indicator_adx(
    db: Connection<MarketData>,
    symbol: &str,
    interval: Option<Interval>,
) -> Json<Vec<DTValue<f64>>> {
    let data = fetch_symbol(db, symbol, &interval).await;
    adx_cached(&data, symbol, interval)
}

#[get("/indicator/mymacd?<symbol>&<interval>")]
async fn indicator_mymacd(
    db: Connection<MarketData>,
    symbol: &str,
    interval: Option<Interval>,
) -> Json<Vec<DTValue<f64>>> {
    let data = fetch_symbol(db, symbol, &interval).await;
    mymacd_cached(&data, symbol, interval)
}

#[get("/fuzzy?<symbol>&<interval>")]
async fn fuzzy_route(
    db: Connection<MarketData>,
    symbol: &str,
    interval: Option<Interval>,
) -> Json<Vec<DTValue<Vec<f64>>>> {
    let data = fetch_symbol(db, symbol, &interval).await;
    fuzzy_cached(&data, symbol, interval)
}

#[launch]
fn rocket() -> _ {
    // Configure CORS options
    let cors_option = CorsOptions::default().allowed_origins(AllowedOrigins::All);
    let cors = Cors::from_options(&cors_option).unwrap();

    rocket::build()
        .attach(cors)
        .attach(MarketData::init())
        .mount(
            "/api",
            routes![
                ohlc,
                indicator_rsi,
                indicator_bb,
                indicator_macd,
                indicator_adx,
                indicator_mymacd,
                fuzzy_route
            ],
        )
}
