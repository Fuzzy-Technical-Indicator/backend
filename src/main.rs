pub mod core;

use core::error::CustomError;
use core::{
    accum_dist_cached, adx_cached, aroon_cached, backtest, bb_cached, error::map_custom_err,
    fetch_symbol, fetch_user_ohlc, fuzzy_cached, macd_cached, obv_cached, optimization, rsi_cached,
    settings, stoch_cached,
};
use core::{users, Interval};

use core::settings::{LinguisticVarsModel, NewFuzzyRule};

use actix_cors::Cors;
use actix_web::dev::ServiceRequest;
use actix_web::http::KeepAlive;
use actix_web::{
    delete, get, middleware::Logger, post, put, web, App, HttpServer, Responder,
    Result as ActixResult,
};
use actix_web::{HttpMessage, HttpRequest, HttpResponse};
use actix_web_httpauth::extractors::bearer::BearerAuth;
use actix_web_httpauth::middleware::HttpAuthentication;
use env_logger::Env;
use mongodb::Client;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct QueryParams {
    symbol: String,
    interval: Option<Interval>,
}

#[derive(Deserialize, Serialize)]
struct PresetQueryParam {
    preset: String,
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
    req: HttpRequest,
) -> ActixResult<HttpResponse> {
    let user = is_user_exist(req)?;

    let symbol = &params.symbol;
    let interval = &params.interval;

    let data = fetch_symbol(&db, symbol, interval).await;
    Ok(HttpResponse::Ok().json(rsi_cached(data, user.rsi.length)))
}

#[get("/bb")]
async fn indicator_bb(
    db: web::Data<Client>,
    params: web::Query<QueryParams>,
    req: HttpRequest,
) -> ActixResult<HttpResponse> {
    let user = is_user_exist(req)?;

    let symbol = &params.symbol;
    let interval = &params.interval;
    let data = fetch_symbol(&db, symbol, interval).await;

    let length = user.bb.length;
    let stdev = user.bb.stdev;
    Ok(HttpResponse::Ok().json(bb_cached(data, length, stdev)))
}

#[get("/macd")]
async fn indicator_macd(
    db: web::Data<Client>,
    params: web::Query<QueryParams>,
    req: HttpRequest,
) -> ActixResult<HttpResponse> {
    let user = is_user_exist(req)?;
    let symbol = &params.symbol;
    let interval = &params.interval;
    let data = fetch_symbol(&db, symbol, interval).await;

    Ok(HttpResponse::Ok().json(macd_cached(
        data,
        user.macd.fast,
        user.macd.slow,
        user.macd.smooth,
    )))
}

#[get("/adx")]
async fn indicator_adx(
    db: web::Data<Client>,
    params: web::Query<QueryParams>,
    req: HttpRequest,
) -> ActixResult<HttpResponse> {
    let user = is_user_exist(req)?;
    let symbol = &params.symbol;
    let interval = &params.interval;
    let data = fetch_symbol(&db, symbol, interval).await;

    Ok(HttpResponse::Ok().json(adx_cached(data, user.adx.length)))
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
    req: HttpRequest,
) -> ActixResult<HttpResponse> {
    let user = is_user_exist(req)?;
    let symbol = &params.symbol;
    let interval = &params.interval;

    let data = fetch_symbol(&db, symbol, interval).await;
    Ok(HttpResponse::Ok().json(aroon_cached(data, user.aroon.length)))
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
    req: HttpRequest,
) -> ActixResult<HttpResponse> {
    let user = is_user_exist(req)?;
    let symbol = &params.symbol;
    let interval = &params.interval;

    let data = fetch_symbol(&db, symbol, interval).await;
    Ok(HttpResponse::Ok().json(stoch_cached(
        data,
        user.stoch.length,
        user.stoch.k,
        user.stoch.d,
    )))
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

#[get("")]
async fn fuzzy_route(
    db: web::Data<Client>,
    params: web::Query<QueryParams>,
    preset_query: web::Query<PresetQueryParam>,
    req: HttpRequest,
) -> ActixResult<HttpResponse> {
    let user = is_user_exist(req)?;
    let symbol = &params.symbol;
    let interval = &params.interval;
    let preset = &preset_query.preset;

    let data = fetch_symbol(&db, symbol, interval).await;
    let result = fuzzy_cached(db, data, preset, user)
        .await
        .map_err(map_custom_err)?;

    Ok(HttpResponse::Ok().json(result))
}

#[get("")]
async fn get_settings(
    db: web::Data<Client>,
    query: web::Query<PresetQueryParam>,
    req: HttpRequest,
) -> ActixResult<HttpResponse> {
    let username = is_user_exist(req)?.username;
    let result = settings::get_setting(db, &query.preset, &username)
        .await
        .map_err(map_custom_err)?;
    Ok(HttpResponse::Ok().json(result))
}

#[put("/linguisticvars")]
async fn update_linguistic_vars(
    db: web::Data<Client>,
    vars: web::Json<LinguisticVarsModel>,
    query: web::Query<PresetQueryParam>,
    req: HttpRequest,
) -> ActixResult<HttpResponse> {
    let username = is_user_exist(req)?.username;
    let result = settings::update_linguistic_vars(db, vars, &query.preset, &username)
        .await
        .map_err(map_custom_err)?;

    Ok(HttpResponse::Ok().body(result))
}

#[delete("/linguisticvars/{name}")]
async fn delete_linguistic_var(
    db: web::Data<Client>,
    path: web::Path<String>,
    query: web::Query<PresetQueryParam>,
    req: HttpRequest,
) -> ActixResult<HttpResponse> {
    let username = is_user_exist(req)?.username;
    let name = path.into_inner();
    let result = settings::delete_linguistic_var(db, &query.preset, name, username)
        .await
        .map_err(map_custom_err)?;

    Ok(HttpResponse::Ok().body(result))
}

#[post("/fuzzyrules")]
async fn add_fuzzy_rules(
    db: web::Data<Client>,
    rules: web::Json<NewFuzzyRule>,
    query: web::Query<PresetQueryParam>,
    req: HttpRequest,
) -> ActixResult<HttpResponse> {
    let username = is_user_exist(req)?.username;
    let result = settings::add_fuzzy_rules(db, rules, &query.preset, username)
        .await
        .map_err(map_custom_err)?;

    Ok(HttpResponse::Ok().body(result))
}

#[delete("/fuzzyrules/{id}")]
async fn delete_fuzzy_rule(
    db: web::Data<Client>,
    path: web::Path<String>,
) -> ActixResult<HttpResponse> {
    // currently other user can delete another user rule
    let id = path.into_inner();
    let result = settings::delete_fuzzy_rule(db, id)
        .await
        .map_err(map_custom_err)?;

    Ok(HttpResponse::Ok().body(result))
}

#[get("/presets")]
async fn get_presets(db: web::Data<Client>, req: HttpRequest) -> ActixResult<HttpResponse> {
    let username = is_user_exist(req)?.username;
    let result = settings::get_presets(&db, username)
        .await
        .map_err(map_custom_err)?;
    Ok(HttpResponse::Ok().json(result))
}

#[post("/presets/{preset}")]
async fn add_preset(
    db: web::Data<Client>,
    path: web::Path<String>,
    req: HttpRequest,
) -> ActixResult<HttpResponse> {
    let username = is_user_exist(req)?.username;
    let result = settings::add_preset(&db, path.into_inner(), username)
        .await
        .map_err(map_custom_err)?;
    Ok(HttpResponse::Ok().body(result))
}

#[delete("/presets/{preset}")]
async fn delete_preset(
    db: web::Data<Client>,
    path: web::Path<String>,
    req: HttpRequest,
) -> ActixResult<HttpResponse> {
    let username = is_user_exist(req)?.username;
    let result = settings::delete_preset(&db, path.into_inner(), username)
        .await
        .map_err(map_custom_err)?;
    Ok(HttpResponse::Ok().body(result))
}

#[post("/users/{username}")]
async fn register(db: web::Data<Client>, path: web::Path<String>) -> ActixResult<HttpResponse> {
    users::register(&db, path.into_inner())
        .await
        .map_err(map_custom_err)?;
    Ok(HttpResponse::Ok().into())
}

#[put("/users")]
async fn update_user_setting(
    db: web::Data<Client>,
    data: web::Json<users::TASetting>,
    req: HttpRequest,
) -> ActixResult<HttpResponse> {
    let username = is_user_exist(req)?.username;
    users::update_user_setting(&db, username, data)
        .await
        .map_err(map_custom_err)?;
    Ok(HttpResponse::Ok().into())
}

#[get("/users")]
async fn get_user_setting(req: HttpRequest) -> ActixResult<HttpResponse> {
    let user = is_user_exist(req)?;
    Ok(HttpResponse::Ok().json(user))
}

#[post("/run")]
async fn create_backtest_report(
    db: web::Data<Client>,
    params: web::Query<QueryParams>,
    preset_query: web::Query<PresetQueryParam>,
    backtest_request: web::Json<backtest::BacktestRequest>,
    req: HttpRequest,
) -> ActixResult<HttpResponse> {
    let user = is_user_exist(req)?;
    let symbol = &params.symbol;
    let interval = params.interval.as_ref().unwrap_or(&Interval::OneDay);
    let preset = &preset_query.preset;

    let result = backtest::create_backtest_report(
        db,
        backtest_request.into_inner(),
        &user,
        symbol,
        interval,
        preset,
    )
    .await
    .map_err(map_custom_err)?;

    Ok(HttpResponse::Ok().json(result))
}

#[post("/runrandom")]
async fn create_random_backtest_report(
    db: web::Data<Client>,
    params: web::Query<QueryParams>,
    backtest_request: web::Json<backtest::BacktestRequest>,
) -> ActixResult<HttpResponse> {
    let symbol = &params.symbol;
    let interval = params.interval.as_ref().unwrap_or(&Interval::OneDay);

    let result = backtest::create_random_backtest_report(
        db,
        backtest_request.into_inner(),
        symbol,
        interval,
    )
    .await;

    Ok(HttpResponse::Ok().json(result))
}

#[post("/run")]
async fn run_pso(
    db: web::Data<Client>,
    params: web::Query<QueryParams>,
    preset_query: web::Query<PresetQueryParam>,
    strat: web::Json<optimization::Strategy>,
    req: HttpRequest,
) -> ActixResult<HttpResponse> {
    let user = is_user_exist(req)?;
    let symbol = &params.symbol;
    let interval = params.interval.as_ref().unwrap_or(&Interval::OneDay);
    let preset = &preset_query.preset;

    let result = optimization::linguistic_vars_optimization(
        &db,
        symbol,
        interval,
        preset,
        &user,
        strat.into_inner(),
    )
    .await
    .map_err(map_custom_err)?;

    Ok(HttpResponse::Ok().json(result))
}

#[get("")]
async fn get_backtest_reports(
    db: web::Data<Client>,
    req: HttpRequest,
) -> ActixResult<HttpResponse> {
    let user = is_user_exist(req)?;
    let result = backtest::get_backtest_reports(db, user.username)
        .await
        .map_err(map_custom_err)?;
    Ok(HttpResponse::Ok().json(result))
}

fn is_user_exist(req: HttpRequest) -> Result<users::User, actix_web::Error> {
    if let Some(user) = req.extensions().get::<users::User>() {
        return Ok(user.clone());
    }
    Err(map_custom_err(CustomError::InternalError(
        "Can't get username from actix extension".to_string(),
    )))
}

async fn auth_validator(
    req: ServiceRequest,
    credentials: BearerAuth,
) -> Result<ServiceRequest, (actix_web::Error, ServiceRequest)> {
    let db_opt = req.app_data::<web::Data<Client>>();
    if let Some(db) = db_opt {
        let username = credentials.token().to_string();
        let result = users::auth_user(db, username.as_str())
            .await
            .map_err(map_custom_err);

        match result {
            Ok(user) => {
                req.extensions_mut().insert(user);
                return Ok(req);
            }
            Err(e) => return Err((e, req)),
        }
    }
    Err((
        map_custom_err(CustomError::InternalError(
            "Can't get DB instance on actix".to_string(),
        )),
        req,
    ))
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
                    .wrap(HttpAuthentication::bearer(auth_validator))
                    .service(indicator_macd)
                    .service(indicator_stoch)
                    .service(indicator_accum_dist)
                    .service(indicator_obv)
                    .service(indicator_bb)
                    .service(indicator_rsi)
                    .service(indicator_aroon)
                    .service(indicator_adx),
            )
            .service(
                web::scope("/api/settings")
                    .wrap(HttpAuthentication::bearer(auth_validator))
                    .service(get_settings)
                    .service(update_linguistic_vars)
                    .service(delete_linguistic_var)
                    .service(add_fuzzy_rules)
                    .service(delete_fuzzy_rule)
                    .service(get_presets)
                    .service(add_preset)
                    .service(delete_preset)
                    .service(update_user_setting)
                    .service(get_user_setting),
            )
            .service(
                web::scope("/api/fuzzy")
                    .wrap(HttpAuthentication::bearer(auth_validator))
                    .service(fuzzy_route),
            )
            .service(
                web::scope("/api/backtesting")
                    .wrap(HttpAuthentication::bearer(auth_validator))
                    .service(create_backtest_report)
                    .service(get_backtest_reports)
                    .service(create_random_backtest_report),
            )
            .service(
                web::scope("/api/pso")
                    .wrap(HttpAuthentication::bearer(auth_validator))
                    .service(run_pso)
            )
            .service(web::scope("/api").service(ohlc).service(register))
    })
    .keep_alive(KeepAlive::Os)
    .bind((ip, port))?
    .run()
    .await
}
