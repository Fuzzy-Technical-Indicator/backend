pub mod core;

use crate::core::{
    accum_dist_cached, adx_cached, aroon_cached, bb_cached, fetch_symbol, fetch_user_ohlc,
    fuzzy_cached, macd_cached, mymacd_cached, obv_cached, rsi_cached,
};
use actix_cors::Cors;
use actix_web::{get, middleware::Logger, web, App, HttpServer, Responder};
use env_logger::Env;
use mongodb::Client;
use serde::Deserialize;

#[derive(Deserialize, Debug, PartialEq, Clone, Hash, Eq)]
pub enum Interval {
    #[serde(rename = "1h")]
    OneHour,
    #[serde(rename = "4h")]
    FourHour,
    #[serde(rename = "1d")]
    OneDay,
}

#[derive(Deserialize)]
struct QueryParams {
    symbol: String,
    interval: Option<Interval>,
}

#[get("/ohlc")]
async fn ohlc(db: web::Data<Client>, params: web::Query<QueryParams>) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;
    web::Json(fetch_user_ohlc(db, symbol, interval).await)
}

#[get("/indicator/rsi")]
async fn indicator_rsi(db: web::Data<Client>, params: web::Query<QueryParams>) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;

    let data = fetch_symbol(db, symbol, interval).await;
    web::Json(rsi_cached(&data, symbol, interval))
}

#[get("/indicator/bb")]
async fn indicator_bb(db: web::Data<Client>, params: web::Query<QueryParams>) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;

    let data = fetch_symbol(db, symbol, interval).await;
    web::Json(bb_cached(&data, symbol, interval))
}

#[get("/indicator/macd")]
async fn indicator_macd(db: web::Data<Client>, params: web::Query<QueryParams>) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;

    let data = fetch_symbol(db, symbol, interval).await;
    web::Json(macd_cached(&data, symbol, interval))
}

#[get("/indicator/adx")]
async fn indicator_adx(db: web::Data<Client>, params: web::Query<QueryParams>) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;

    let data = fetch_symbol(db, symbol, interval).await;
    web::Json(adx_cached(&data, symbol, interval))
}

#[get("/indicator/obv")]
async fn indicator_obv(db: web::Data<Client>, params: web::Query<QueryParams>) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;

    let data = fetch_symbol(db, symbol, interval).await;
    web::Json(obv_cached(&data, symbol, interval))
}

#[get("/indicator/mymacd")]
async fn indicator_mymacd(
    db: web::Data<Client>,
    params: web::Query<QueryParams>,
) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;

    let data = fetch_symbol(db, symbol, interval).await;
    web::Json(mymacd_cached(&data, symbol, interval))
}

#[get("/indicator/aroon")]
async fn indicator_aroon(db: web::Data<Client>, params: web::Query<QueryParams>) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;

    let data = fetch_symbol(db, symbol, interval).await;
    web::Json(aroon_cached(&data, symbol, interval))
}

#[get("/indicator/accumdist")]
async fn indicator_accum_dist(
    db: web::Data<Client>,
    params: web::Query<QueryParams>,
) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;

    let data = fetch_symbol(db, symbol, interval).await;
    web::Json(accum_dist_cached(&data, symbol, interval))
}

#[get("/fuzzy")]
async fn fuzzy_route(db: web::Data<Client>, params: web::Query<QueryParams>) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;

    let data = fetch_symbol(db, symbol, interval).await;
    web::Json(fuzzy_cached(&data, symbol, interval))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let uri = dotenvy::var("MONGO_DB_URI").unwrap();

    let client = Client::with_uri_str(uri)
        .await
        .expect("Failed to connect to Mongodb");

    env_logger::init_from_env(Env::default().default_filter_or("info"));

    HttpServer::new(move || {
        let cors = Cors::default().allow_any_origin();

        App::new()
            .wrap(Logger::new("%r %s %bbytes %Dms"))
            .wrap(cors)
            .app_data(web::Data::new(client.clone()))
            .service(
                web::scope("/api")
                    .service(ohlc)
                    .service(fuzzy_route)
                    .service(indicator_macd)
                    .service(indicator_bb)
                    .service(indicator_adx)
                    .service(indicator_rsi)
                    .service(indicator_mymacd)
                    .service(indicator_obv)
                    .service(indicator_aroon)
                    .service(indicator_accum_dist),
            )
    })
    .bind(("127.0.0.1", 8000))?
    .run()
    .await
}
