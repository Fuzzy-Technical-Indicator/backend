use std::error::Error;

use rocket_db_pools::{mongodb, Database};
#[derive(Database)]
#[database("marketdata")]
struct MarketData(mongodb::Client);

#[macro_use]
extern crate rocket;

async fn fetch_binance() -> Result<String, Box<dyn Error>> {
    let base = "https://api.binance.com";
    let symbol = "BTCUSDT";
    let interval = "1h";
    let params = format!("?symbol={}&interval={}", symbol, interval);

    let resp = reqwest::get(format!("{}/{}{}", base, "api/v3/klines", params))
        .await?
        .text()
        .await?;

    Ok(resp)
}

#[get("/")]
fn index() -> String {
    "Hello World".to_string()
}

#[get("/binance")]
async fn binance() -> String {
    fetch_binance().await.unwrap_or("err".into())
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .attach(MarketData::init())
        .mount("/", routes![index])
        .mount("/", routes![binance])
}
