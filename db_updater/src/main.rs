use std::thread;
use std::time::Duration;

use binance::api::Binance;
use binance::market::Market;
use db_updater::alphavantage::AlphaVantageClient;
use db_updater::{klines, update};
use lambda_runtime::{service_fn, LambdaEvent};
use mongodb::Database;
use mongodb::{options::ClientOptions, Client};
use serde_json::{json, Value};
use tech_indicators::Ohlc;

/// This assume the collection in DB is already existed and meet all requirements.
///
/// API rate limit is 5 API requests per minute and 500 requests per day
/// - `ticker`: e.g. AAPL/USD, TSLA/USD
async fn update_stock(ticker: &String, db: &Database) -> Result<(), lambda_runtime::Error> {
    let collection = db.collection::<Ohlc>(&ticker);

    // this should be obsolete now, we need to change from alphavantage to finnhub
    let apikey = dotenvy_macro::dotenv!("ALPHAVANTAGE_APIKEY");
    let market = AlphaVantageClient::new(apikey);
    let market_data = market
        .intraday(
            ticker.split('/').next().unwrap(),
            "60min",
            None,
            Some("compact"),
            Some("csv"),
        )
        .await
        .expect("can't fetch market data");
    //

    update(&collection, &market_data, 3).await
}

/// - `ticker`: e.g. BTC/USDT, ETH/USDT
async fn update_crypto(
    db: &Database,
    market: &Market,
    ticker: &str,
) -> Result<(), lambda_runtime::Error> {
    let collection = db.collection::<Ohlc>(&ticker);
    let data = klines(&market, &ticker, Some(100), None).await.unwrap();
    update(&collection, &data, 20).await
}

async fn func(_event: LambdaEvent<Value>) -> Result<Value, lambda_runtime::Error> {
    let url = dotenvy_macro::dotenv!("MONGODB_URL");

    let mut client_options = ClientOptions::parse(url).await?;
    client_options.app_name = Some("seeddb".to_string());

    let client = Client::with_options(client_options)?;
    let db = client.database("StockMarket");

    let symbol_list = vec!["AAPL", "IBM", "JPM", "MSFT", "NKE", "TSLA"];
    for symbol in symbol_list {
        update_stock(&format!("{symbol}/USD"), &db).await?;
        thread::sleep(Duration::from_secs(15));
    }

    let market: Market = Binance::new(None, None);
    let coins = vec!["BTC/USDT", "ETH/USDT", "BNB/USDT"];
    for c in coins {
        update_crypto(&db, &market, &c).await?;
    }

    Ok(json!( { "message": "Okay"}))
}

// cargo lambda build --release
#[tokio::main]
pub async fn main() -> Result<(), lambda_runtime::Error> {
    let func = service_fn(func);
    lambda_runtime::run(func).await?;
    Ok(())
}

/// This is not a good testing -_-
#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_update_crypto() {
        let url = dotenvy_macro::dotenv!("MONGODB_URL");
        let mut client_options = ClientOptions::parse(url).await.unwrap();
        client_options.app_name = Some("seeddb".to_string());

        let client = Client::with_options(client_options).unwrap();
        let db = client.database("StockMarket");
        let market: Market = Binance::new(None, None);
        update_crypto(&db, &market, "BTC/USDT").await.unwrap();
    }

    #[tokio::test]
    async fn test_update_stock() {
        let url = dotenvy_macro::dotenv!("MONGODB_URL");

        let mut client_options = ClientOptions::parse(url).await.unwrap();
        client_options.app_name = Some("seeddb".to_string());

        let client = Client::with_options(client_options).unwrap();
        let db = client.database("StockMarket");
        update_stock(&"AAPL/USD".into(), &db).await.unwrap();
    }

    #[tokio::test]
    async fn test_lambda_func() {
        let input: Value = serde_json::from_str("{}").expect("failed to parse event");
        let context = lambda_runtime::Context::default();

        let event = lambda_runtime::LambdaEvent::new(input, context);

        func(event).await.expect("failed to handle event");
    }
}
