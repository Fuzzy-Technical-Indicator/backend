use std::thread;

use actix_web::web;
use backend::core::{
    backtest::{create_backtest_report, BacktestJob},
    optimization::{linguistic_vars_optimization, PSOTrainJob},
};
use env_logger::Env;
use mongodb::Client;
use redis::Commands;

#[tokio::main]
async fn backtest_consumer(mongo_uri: String, redis_client: redis::Client) {
    let key = "backtests";
    let client = Client::with_uri_str(mongo_uri)
        .await
        .expect("Failed to connect to Mongodb");
    let db = web::Data::new(client);
    let mut con = redis_client
        .get_connection()
        .expect("Failed to get redis connection");

    loop {
        let llen: i64 = con.llen(key).unwrap();
        if llen == 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            continue;
        }
        let poped: Result<String, redis::RedisError> = con.rpop(key, None);
        match poped {
            Ok(result) => {
                log::info!("Backtest job started");
                let job: BacktestJob = serde_json::from_str(result.as_str()).unwrap();
                let BacktestJob {
                    request,
                    user,
                    symbol,
                    interval,
                    preset,
                } = job;

                let r =
                    create_backtest_report(db.clone(), request, &user, &symbol, &interval, &preset)
                        .await;

                match r {
                    Ok(_) => {
                        log::info!("Backtest job success")
                    }
                    Err(e) => {
                        log::error!("Error in Backtest job: {:?}", e);
                    }
                }
            }
            Err(e) => {
                log::error!("Error: {}", e);
            }
        }
    }
}

#[tokio::main]
async fn pso_consumer(mongo_uri: String, redis_client: redis::Client) {
    let key = "pso";
    let client = Client::with_uri_str(mongo_uri)
        .await
        .expect("Failed to connect to Mongodb");
    let db = web::Data::new(client);
    let mut con = redis_client
        .get_connection()
        .expect("Failed to get redis connection");

    loop {
        let llen: i64 = con.llen(key).unwrap();
        if llen == 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            continue;
        }
        let poped: Result<String, redis::RedisError> = con.rpop(key, None);
        match poped {
            Ok(result) => {
                log::info!("PSO job started");
                let job: PSOTrainJob = serde_json::from_str(result.as_str()).unwrap();
                let PSOTrainJob {
                    symbol,
                    interval,
                    preset,
                    user,
                    strat,
                } = job;

                let r =
                    linguistic_vars_optimization(&db, &symbol, &interval, &preset, &user, strat)
                        .await;

                match r {
                    Ok(_) => {
                        log::info!("PSO job success")
                    }
                    Err(e) => {
                        log::error!("Error in PSO job: {:?}", e);
                    }
                }
            }
            Err(e) => {
                log::error!("Error: {}", e);
            }
        }
    }
}

fn main() {
    env_logger::init_from_env(Env::default().default_filter_or("info"));

    let mongo_uri = dotenvy::var("MONGO_DB_URI").expect("Failed to get mongo uri");
    let redis_url = dotenvy::var("REDIS_URL").expect("Failed to get redis url");
    let redis_client = redis::Client::open(redis_url).expect("Failed to open redis");

    let mongo_uri_cloned = mongo_uri.clone();
    let redis_client_cloned = redis_client.clone();
    let t1 = thread::spawn(|| {
        pso_consumer(mongo_uri_cloned, redis_client_cloned);
    });
    let t2 = thread::spawn(|| {
        backtest_consumer(mongo_uri, redis_client);
    });

    t1.join().unwrap();
    t2.join().unwrap();
}
