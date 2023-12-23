pub mod backtest;
pub mod core;

use core::{
    accum_dist_cached, adx_cached, aroon_cached, bb_cached,
    error::{map_custom_err, map_internal_err},
    fetch_symbol, fetch_user_ohlc, fuzzy_cached, macd_cached, obv_cached, rsi_cached, settings,
    stoch_cached,
};

use core::settings::{LinguisticVarsModel, NewFuzzyRule};

use actix_cors::Cors;
use actix_web::HttpResponse;
use actix_web::{
    delete, get, middleware::Logger, post, put, web, App, HttpServer, Responder,
    Result as ActixResult,
};
use env_logger::Env;
use mongodb::Client;
use serde::{Deserialize, Serialize};

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

#[derive(Deserialize)]
struct LengthQueryParam {
    length: usize,
}

#[derive(Deserialize)]
struct BBQueryParams {
    stdev: f64,
}

#[derive(Deserialize)]
struct MacdQueryParams {
    fast: usize,
    slow: usize,
    smooth: usize,
}

#[derive(Deserialize, Serialize)]
struct StochQueryParams {
    k: usize,
    d: usize,
}

#[get("/ohlc")]
async fn ohlc(db: web::Data<Client>, params: web::Query<QueryParams>) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;
    web::Json(fetch_user_ohlc(db, symbol, interval).await)
}

#[get("/rsi")]
async fn indicator_rsi(
    db: web::Data<Client>,
    params: web::Query<QueryParams>,
    length_query: web::Query<LengthQueryParam>,
) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;

    let data = fetch_symbol(&db, symbol, interval).await;
    web::Json(rsi_cached(data, length_query.length))
}

#[get("/bb")]
async fn indicator_bb(
    db: web::Data<Client>,
    params: web::Query<QueryParams>,
    length_query: web::Query<LengthQueryParam>,
    other_params: web::Query<BBQueryParams>,
) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;
    let data = fetch_symbol(&db, symbol, interval).await;

    let length = length_query.length;
    let stdev = other_params.stdev;
    web::Json(bb_cached(data, length, stdev))
}

#[get("/macd")]
async fn indicator_macd(
    db: web::Data<Client>,
    params: web::Query<QueryParams>,
    other_params: web::Query<MacdQueryParams>,
) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;
    let data = fetch_symbol(&db, symbol, interval).await;

    web::Json(macd_cached(
        data,
        other_params.fast,
        other_params.slow,
        other_params.smooth,
    ))
}

#[get("/adx")]
async fn indicator_adx(
    db: web::Data<Client>,
    params: web::Query<QueryParams>,
    length_query: web::Query<LengthQueryParam>,
) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;
    let data = fetch_symbol(&db, symbol, interval).await;

    web::Json(adx_cached(data, length_query.length))
}

#[get("/obv")]
async fn indicator_obv(db: web::Data<Client>, params: web::Query<QueryParams>) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;

    let data = fetch_symbol(&db, symbol, interval).await;
    web::Json(obv_cached(data))
}

#[get("/aroon")]
async fn indicator_aroon(
    db: web::Data<Client>,
    params: web::Query<QueryParams>,
    length_query: web::Query<LengthQueryParam>,
) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;

    let data = fetch_symbol(&db, symbol, interval).await;
    web::Json(aroon_cached(data, length_query.length))
}

#[get("/accumdist")]
async fn indicator_accum_dist(
    db: web::Data<Client>,
    params: web::Query<QueryParams>,
) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;

    let data = fetch_symbol(&db, symbol, interval).await;
    web::Json(accum_dist_cached(data))
}

#[get("/stoch")]
async fn indicator_stoch(
    db: web::Data<Client>,
    params: web::Query<QueryParams>,
    length_query: web::Query<LengthQueryParam>,
    other_params: web::Query<StochQueryParams>,
) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;

    let data = fetch_symbol(&db, symbol, interval).await;
    web::Json(stoch_cached(
        data,
        length_query.length,
        other_params.k,
        other_params.d,
    ))
}

/*
#[get("/indicator/naranjomacd")]
async fn indicator_naranjo_macd(
    db: web::Data<Client>,
    params: web::Query<QueryParams>,
) -> impl Responder {
    let symbol = &params.symbol;
    let interval = &params.interval;

    let data = fetch_symbol(db, symbol, interval).await;
    web::Json(naranjo_macd_cached(&data, symbol, interval))
}
*/

#[get("/fuzzy")]
async fn fuzzy_route(
    db: web::Data<Client>,
    params: web::Query<QueryParams>,
) -> ActixResult<HttpResponse> {
    let symbol = &params.symbol;
    let interval = &params.interval;

    let data = fetch_symbol(&db, symbol, interval).await;
    let result = fuzzy_cached(db, data, symbol, interval)
        .await
        .map_err(map_custom_err)?;

    Ok(HttpResponse::Ok().json(result))
}

#[get("/settings")]
async fn get_settings(db: web::Data<Client>) -> impl Responder {
    web::Json(settings::get_settings(db).await)
}

#[put("/settings/linguisticvars")]
async fn update_linguistic_vars(
    db: web::Data<Client>,
    vars: web::Json<LinguisticVarsModel>,
) -> String {
    settings::update_linguistic_vars(db, vars).await
}

#[delete("/settings/linguisticvars/{name}")]
async fn delete_linguistic_var(db: web::Data<Client>, path: web::Path<String>) -> String {
    let name = path.into_inner();
    settings::delete_linguistic_var(db, name).await
}

#[post("/settings/fuzzyrules")]
async fn add_fuzzy_rules(
    db: web::Data<Client>,
    rules: web::Json<NewFuzzyRule>,
) -> ActixResult<HttpResponse> {
    let result = settings::add_fuzzy_rules(db, rules)
        .await
        .map_err(map_custom_err)?;

    Ok(HttpResponse::Ok().body(result))
}

#[delete("/settings/fuzzyrules/{id}")]
async fn delete_fuzzy_rule(db: web::Data<Client>, path: web::Path<String>) -> String {
    let id = path.into_inner();
    settings::delete_fuzzy_rule(db, id).await
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let uri = dotenvy::var("MONGO_DB_URI").unwrap();
    let ip = dotenvy::var("IP").unwrap_or("127.0.0.1".to_string());
    let port: u16 = match dotenvy::var("PORT") {
        Ok(p) => p.parse().unwrap_or(8000),
        _ => 8000,
    };

    let client = Client::with_uri_str(uri)
        .await
        .expect("Failed to connect to Mongodb");

    env_logger::init_from_env(Env::default().default_filter_or("info"));

    HttpServer::new(move || {
        let cors = Cors::permissive();

        App::new()
            .wrap(Logger::new("%r %s %bbytes %Dms"))
            .wrap(cors)
            .app_data(web::Data::new(client.clone()))
            .service(
                web::scope("/api/indicators")
                    .service(indicator_macd)
                    .service(indicator_bb)
                    .service(indicator_adx)
                    .service(indicator_rsi)
                    .service(indicator_obv)
                    .service(indicator_aroon)
                    .service(indicator_stoch)
                    .service(indicator_accum_dist), //.service(indicator_naranjo_macd),
            )
            .service(
                web::scope("/api")
                    .service(ohlc)
                    .service(fuzzy_route)
                    .service(get_settings)
                    .service(update_linguistic_vars)
                    .service(delete_linguistic_var)
                    .service(add_fuzzy_rules)
                    .service(delete_fuzzy_rule),
            )
    })
    .bind((ip, port))?
    .run()
    .await
}
