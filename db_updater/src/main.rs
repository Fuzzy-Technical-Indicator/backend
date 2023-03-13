use std::collections::HashMap;
use std::thread;
use std::time::Duration;

use alphavantage::{AlphaVantageClient, Ohlc};
use binance::api::Binance;
use binance::market::Market;
use db_updater::klines;
use futures::stream::TryStreamExt;
use lambda_runtime::{service_fn, LambdaEvent};
use mongodb::bson::{doc, to_document};
use mongodb::options::FindOptions;
use mongodb::{options::ClientOptions, Client};
use mongodb::{Collection, Database};
use serde_json::{json, Value};

async fn most_recents(
    limit: i64,
    collection: &Collection<Ohlc>,
) -> Result<Vec<Ohlc>, lambda_runtime::Error> {
    let find_options = FindOptions::builder()
        .sort(doc! {"time": -1})
        .limit(Some(limit))
        .build();
    let cursor = collection
        .find(None, find_options)
        .await
        .expect("collection is empty?");

    Ok(cursor
        .try_collect()
        .await
        .expect("can't collect mongodb cursor"))
}

async fn replace_recents(
    collection: &Collection<Ohlc>,
    old: &Vec<Ohlc>,
    new: &Vec<Ohlc>,
) -> Result<(), lambda_runtime::Error> {
    let old_map: HashMap<_, _> = old.iter().map(|x| (x.time, x)).collect();
    let should_update: Vec<_> = new
        .iter()
        .filter_map(|x| old_map.get(&x.time).map(|y| (*y, x)))
        .collect();
    
    for (query, update) in should_update {
        collection
            .replace_one(to_document(query).unwrap(), update, None)
            .await
            .expect("can't update");
    }
    Ok(())
}

async fn insert_new(
    collection: &Collection<Ohlc>,
    most_recent: &Ohlc,
    new: &Vec<Ohlc>,
) -> Result<(), lambda_runtime::Error> {
    let new_data: Vec<_> = new
        .iter()
        .filter(|x| x.time.gt(&most_recent.time))
        .collect();
    if new_data.len() > 0 {
        collection.insert_many(new_data, None).await.unwrap();
    };
    Ok(())
}

/// This assume the collection in DB is already existed and meet all requirements.
///
/// API rate limit is 5 API requests per minute and 500 requests per day
/// - `ticker` e.g. AAPL/USD, TSLA/USD
async fn update_stock(ticker: &String, db: &Database) -> Result<(), lambda_runtime::Error> {
    let collection = db.collection::<Ohlc>(&ticker);
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

    let old: Vec<Ohlc> = most_recents(3, &collection).await?;
    replace_recents(&collection, &old, &market_data).await?;
    if let Some(most_recent) = old.first() {
        insert_new(&collection, most_recent, &market_data).await?;
    }
    Ok(())
}

async fn update_crypto(
    db: &Database,
    market: &Market,
    ticker: &str,
) -> Result<(), lambda_runtime::Error> {
    let collection = db.collection::<Ohlc>(&ticker);
    let data = klines(&market, &ticker, Some(100), None).await.unwrap();
    let old: Vec<Ohlc> = most_recents(20, &collection).await?;
    replace_recents(&collection, &old, &data).await?;
    if let Some(most_recent) = old.first() {
        insert_new(&collection, most_recent, &data).await?;
    }
    Ok(())
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

/// This is no a good testing -_-
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
