use std::collections::HashMap;
use std::thread;
use std::time::Duration;

use alphavantage::{AlphaVantageClient, Ohlc};
use futures::stream::{TryStreamExt};
use lambda_runtime::{service_fn, LambdaEvent};
use mongodb::bson::{doc, to_document};
use mongodb::options::FindOptions;
use mongodb::Database;
use mongodb::{options::ClientOptions, Client};
use serde_json::{json, Value};

/// This assume the collection in DB is already existed and meet all requirements.
///
/// API rate limit is 5 API requests per minute and 500 requests per day
/// - `ticker` e.g. AAPL/USD, TSLA/USD
async fn update_db(ticker: &String, db: &Database) -> Result<(), lambda_runtime::Error> {
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

    let find_options = FindOptions::builder()
        .sort(doc! {"time": -1})
        .limit(Some(3))
        .build();
    let cursor = collection
        .find(None, find_options)
        .await
        .expect("collection is empty?");
    let v: Vec<Ohlc> = cursor
        .try_collect()
        .await
        .expect("can't collect mongodb cursor");

    // update logic, we always replace the 3 most recent data from mongodb with data from API
    let v_map: HashMap<_, _> = v.iter().map(|x| (x.time, x)).collect();
    let should_update: Vec<_> = market_data
        .iter()
        .filter_map(|x| v_map.get(&x.time).map(|y| (x, *y)))
        .collect();

    for (query, update) in should_update {
        collection
            .replace_one(to_document(query).unwrap(), update, None)
            .await
            .expect("can't update");
    }

    if let Some(most_recent) = v.first() {
        let new_data: Vec<_> = market_data
            .iter()
            .filter(|x| x.time.gt(&most_recent.time))
            .collect();
        if new_data.len() == 0 {
            return Ok(());
        };
        collection.insert_many(new_data, None).await.unwrap();
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
        update_db(&format!("{symbol}/USD"), &db).await?;
        thread::sleep(Duration::from_secs(15));
    }

    Ok(json!( { "message": "Okay"}))
}

#[tokio::main]
pub async fn main() -> Result<(), lambda_runtime::Error> {
    let func = service_fn(func);
    lambda_runtime::run(func).await?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_update_db() {
        let url = dotenvy_macro::dotenv!("MONGODB_URL");

        let mut client_options = ClientOptions::parse(url).await.unwrap();
        client_options.app_name = Some("seeddb".to_string());

        let client = Client::with_options(client_options).unwrap();
        let db = client.database("StockMarket");
        update_db(&"AAPL/USD".into(), &db).await.unwrap();
    }

    #[tokio::test]
    async fn test_lambda_func() {
        let input: Value = serde_json::from_str("{}").expect("failed to parse event");
        let context = lambda_runtime::Context::default();

        let event = lambda_runtime::LambdaEvent::new(input, context);

        func(event).await.expect("failed to handle event");
    }
}
