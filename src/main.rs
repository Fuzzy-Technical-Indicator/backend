pub mod core;

use core::backtest::backtest_consumer;
use core::error::CustomError;
use core::optimization::pso_consumer;
use core::{
    accum_dist_cached, adx_cached, aroon_cached, backtest, bb_cached, error::map_custom_err,
    fetch_symbol, fetch_user_ohlc, fuzzy_cached, macd_cached, obv_cached, optimization, rsi_cached,
    settings, stoch_cached,
};
use core::{users, Interval};

use core::settings::{LinguisticVarsModel, NewFuzzyRule};

use std::sync::mpsc::Sender;
use std::sync::{mpsc, Mutex};
use std::thread;

use actix_cors::Cors;
use actix_web::dev::ServiceRequest;
use actix_web::error::ErrorInternalServerError;
use actix_web::http::KeepAlive;
use actix_web::{
    delete, get, middleware::Logger, post, put, web, App, HttpServer, Responder,
    Result as ActixResult,
};
use actix_web::{HttpMessage, HttpRequest, HttpResponse};
use actix_web_httpauth::extractors::bearer::BearerAuth;
use actix_web_httpauth::middleware::HttpAuthentication;

use backend::core::transformed_macd;
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

#[get("/macd/transformed")]
async fn indicator_transformed_macd(
    db: web::Data<Client>,
    params: web::Query<QueryParams>,
    req: HttpRequest,
) -> ActixResult<HttpResponse> {
    let user = is_user_exist(req)?;
    let symbol = &params.symbol;
    let interval = &params.interval;
    let data = fetch_symbol(&db, symbol, interval).await;

    Ok(HttpResponse::Ok().json(transformed_macd(
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

#[get("/running")]
async fn get_running_backtest(
    backtest_counter: web::Data<Mutex<u32>>,
) -> ActixResult<HttpResponse> {
    let result = { *backtest_counter.lock().unwrap() };
    Ok(HttpResponse::Ok().json(result))
}

#[post("/run")]
async fn create_backtest_report(
    params: web::Query<QueryParams>,
    preset_query: web::Query<PresetQueryParam>,
    backtest_request: web::Json<backtest::BacktestRequest>,
    req: HttpRequest,
    sender: web::Data<Sender<backtest::BacktestJob>>,
    backtest_counter: web::Data<Mutex<u32>>,
) -> ActixResult<HttpResponse> {
    let user = is_user_exist(req)?;
    let symbol = params.symbol.clone();
    let interval = params.interval.clone().unwrap_or(Interval::OneDay);
    let preset = preset_query.preset.clone();

    let job = backtest::BacktestJob {
        request: backtest_request.into_inner(),
        user,
        symbol,
        interval,
        preset,
    };

    sender
        .send(job)
        .map_err(|e| ErrorInternalServerError(e.to_string()))?;
    {
        *backtest_counter.lock().unwrap() += 1;
    }

    Ok(HttpResponse::Ok().into())
}

#[get("/running")]
async fn running_pso(pso_counter: web::Data<Mutex<u32>>) -> ActixResult<HttpResponse> {
    let result = { *pso_counter.lock().unwrap() };
    Ok(HttpResponse::Ok().json(result))
}

#[derive(Deserialize)]
struct IntervalParam {
    interval: Interval,
}

#[derive(Deserialize)]
struct PSORunTypeParam {
    runtype: optimization::PSORunType,
}

#[post("/run/{preset}/cryptos")]
async fn run_pso_cryptos(
    path: web::Path<String>,
    params: web::Query<IntervalParam>,
    run_type: web::Query<PSORunTypeParam>,
    strat: web::Json<optimization::Strategy>,
    req: HttpRequest,
    pso_sender: web::Data<Sender<optimization::PSOTrainJob>>,
    pso_counter: web::Data<Mutex<u32>>,
) -> ActixResult<HttpResponse> {
    // convenient route
    let user = is_user_exist(req)?;
    let interval = params.interval.clone();
    let preset = path.into_inner();
    let strat = strat.into_inner();
    let run_type = run_type.into_inner().runtype;

    const CRYPTOS: &[&str] = &["ETH/USDT", "BTC/USDT", "BNB/USDT"];

    for symbol in CRYPTOS {
        let job = optimization::PSOTrainJob {
            symbol: symbol.to_string(),
            interval: interval.clone(),
            preset: preset.clone(),
            user: user.clone(),
            strat: strat.clone(),
            run_type: run_type.clone()
        };
        pso_sender
            .send(job)
            .map_err(|e| ErrorInternalServerError(e.to_string()))?;
        {
            *pso_counter.lock().unwrap() += 1;
        }
    }

    Ok(HttpResponse::Ok().into())
}

#[post("/run/{preset}/stocks")]
async fn run_pso_stocks(
    path: web::Path<String>,
    params: web::Query<IntervalParam>,
    run_type: web::Query<PSORunTypeParam>,
    strat: web::Json<optimization::Strategy>,
    req: HttpRequest,
    pso_sender: web::Data<Sender<optimization::PSOTrainJob>>,
    pso_counter: web::Data<Mutex<u32>>,
) -> ActixResult<HttpResponse> {
    // convenient route
    let user = is_user_exist(req)?;
    let interval = params.interval.clone();
    let preset = path.into_inner();
    let strat = strat.into_inner();
    let run_type = run_type.into_inner().runtype;

    const STOCKS: &[&str] = &[
        "AAPL/USD", "IBM/USD", "JPM/USD", "MSFT/USD", "NKE/USD", "TSLA/USD",
    ];

    for symbol in STOCKS {
        let job = optimization::PSOTrainJob {
            symbol: symbol.to_string(),
            interval: interval.clone(),
            preset: preset.clone(),
            user: user.clone(),
            strat: strat.clone(),
            run_type: run_type.clone()
        };
        pso_sender
            .send(job)
            .map_err(|e| ErrorInternalServerError(e.to_string()))?;
        {
            *pso_counter.lock().unwrap() += 1;
        }
    }

    Ok(HttpResponse::Ok().into())
}

#[post("/run/{preset}")]
async fn run_pso(
    path: web::Path<String>,
    params: web::Query<QueryParams>,
    run_type: web::Query<PSORunTypeParam>,
    strat: web::Json<optimization::Strategy>,
    req: HttpRequest,
    pso_sender: web::Data<Sender<optimization::PSOTrainJob>>,
    pso_counter: web::Data<Mutex<u32>>,
) -> ActixResult<HttpResponse> {
    let user = is_user_exist(req)?;
    let symbol = params.symbol.clone();
    let interval = params.interval.clone().unwrap_or(Interval::OneDay);
    let preset = path.into_inner();

    let job = optimization::PSOTrainJob {
        symbol,
        interval,
        preset,
        user,
        strat: strat.into_inner(),
        run_type: run_type.into_inner().runtype
    };

    pso_sender
        .send(job)
        .map_err(|e| ErrorInternalServerError(e.to_string()))?;
    {
        *pso_counter.lock().unwrap() += 1;
    }

    Ok(HttpResponse::Ok().into())
}

#[get("")]
async fn get_pso_result(db: web::Data<Client>, req: HttpRequest) -> ActixResult<HttpResponse> {
    let user = is_user_exist(req)?;
    let result = optimization::get_train_results(&db, user.username)
        .await
        .map_err(map_custom_err)?;
    Ok(HttpResponse::Ok().json(result))
}

#[delete("/{id}")]
async fn delete_pso_result(
    db: web::Data<Client>,
    path: web::Path<String>,
) -> ActixResult<HttpResponse> {
    optimization::delete_train_result(&db, path.into_inner())
        .await
        .map_err(map_custom_err)?;
    Ok(HttpResponse::Ok().into())
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

#[get("/{id}")]
async fn get_backtest_report(
    db: web::Data<Client>,
    path: web::Path<String>,
) -> ActixResult<HttpResponse> {
    let result = backtest::get_backtest_report(&db, path.into_inner())
        .await
        .map_err(map_custom_err)?;
    Ok(HttpResponse::Ok().json(result))
}

#[delete("/{id}")]
async fn delete_backtest_report(
    db: web::Data<Client>,
    path: web::Path<String>,
) -> ActixResult<HttpResponse> {
    backtest::delete_backtest_report(&db, path.into_inner())
        .await
        .map_err(map_custom_err)?;
    Ok(HttpResponse::Ok().into())
}

#[delete("")]
async fn delete_all_backtest_report(
    db: web::Data<Client>,
    req: HttpRequest,
) -> ActixResult<HttpResponse> {
    let user = is_user_exist(req)?;
    backtest::delete_all_becktest_report(&db, user.username)
        .await
        .map_err(map_custom_err)?;
    Ok(HttpResponse::Ok().into())
}

#[get("")]
async fn check_user(req: HttpRequest) -> ActixResult<HttpResponse> {
    let _ = is_user_exist(req)?;
    Ok(HttpResponse::Ok().into())
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
async fn main_server(
    mongodb_uri: String,
    pso_sender: Sender<optimization::PSOTrainJob>,
    pso_counter: web::Data<Mutex<u32>>,
    backtest_sender: Sender<backtest::BacktestJob>,
    backtest_counter: web::Data<Mutex<u32>>,
) -> std::io::Result<()> {
    let ip = dotenvy::var("IP").unwrap_or("127.0.0.1".to_string());
    let port: u16 = match dotenvy::var("PORT") {
        Ok(p) => p.parse().unwrap_or(8000),
        _ => 8000,
    };

    let client = Client::with_uri_str(mongodb_uri)
        .await
        .expect("Failed to connect to Mongodb");

    HttpServer::new(move || {
        let cors = Cors::permissive();

        App::new()
            .wrap(Logger::new("%r %s %bbytes %Dms"))
            .wrap(cors)
            .app_data(web::Data::new(client.clone()))
            .app_data(web::Data::new(pso_sender.clone()))
            .app_data(web::Data::new(backtest_sender.clone()))
            .service(
                web::scope("/api/indicators")
                    .wrap(HttpAuthentication::bearer(auth_validator))
                    .service(indicator_macd)
                    .service(indicator_transformed_macd)
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
                    .app_data(backtest_counter.clone())
                    .wrap(HttpAuthentication::bearer(auth_validator))
                    .service(create_backtest_report)
                    .service(get_running_backtest)
                    .service(get_backtest_reports)
                    .service(get_backtest_report)
                    .service(delete_all_backtest_report)
                    .service(delete_backtest_report),
            )
            .service(
                web::scope("/api/pso")
                    .app_data(pso_counter.clone())
                    .wrap(HttpAuthentication::bearer(auth_validator))
                    .service(run_pso_cryptos)
                    .service(run_pso_stocks)
                    .service(run_pso)
                    .service(running_pso)
                    .service(delete_pso_result)
                    .service(get_pso_result),
            )
            .service(
                web::scope("/api/user")
                    .wrap(HttpAuthentication::bearer(auth_validator))
                    .service(check_user),
            )
            .service(web::scope("/api").service(ohlc).service(register))
    })
    .keep_alive(KeepAlive::Os)
    .bind((ip, port))?
    .run()
    .await
}

fn main() {
    env_logger::init_from_env(Env::default().default_filter_or("info"));

    let mongo_uri = dotenvy::var("MONGO_DB_URI").expect("Failed to get mongo uri");

    let (pso_sender, pso_receiver) = mpsc::channel();
    let (backtest_sender, backtest_receiver) = mpsc::channel();
    let pso_counter = web::Data::new(Mutex::new(0u32));
    let backtest_counter = web::Data::new(Mutex::new(0u32));

    let t0 = thread::spawn({
        let mongo_uri = mongo_uri.clone();
        let pso_counter = pso_counter.clone();
        let backtest_counter = backtest_counter.clone();
        move || {
            main_server(
                mongo_uri,
                pso_sender,
                pso_counter,
                backtest_sender,
                backtest_counter,
            )
            .unwrap();
        }
    });

    let t1 = thread::spawn({
        let mongo_uri = mongo_uri.clone();
        || {
            pso_consumer(mongo_uri, pso_receiver, pso_counter);
        }
    });

    let t2 = thread::spawn(|| {
        backtest_consumer(mongo_uri, backtest_receiver, backtest_counter);
    });

    t0.join().expect("Main Service has panicked");
    t1.join().expect("PSO Consumer has panicked");
    t2.join().expect("Backtest Consumer has panicked");
}
