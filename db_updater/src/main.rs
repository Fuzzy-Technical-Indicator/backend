use alphavantage::{AlphaVantageClient, Ohlc};
use lambda_runtime::{service_fn, LambdaEvent};
use mongodb::bson::doc;
use mongodb::options::{FindOneOptions};
use mongodb::{options::ClientOptions, Client};
use mongodb::{Database};
use serde_json::{json, Value};

/// This assume the collection in DB is already existed and meet all requirements.
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
        .unwrap();

    let find_options = FindOneOptions::builder().sort(doc! {"time": -1}).build();
    let result = collection.find_one(None, Some(find_options)).await.unwrap();

    if let Some(res) = result {
        let filterd_data: Vec<_> = market_data
            .iter()
            .filter(|x| x.time.gt(&res.time))
            .collect();

        if filterd_data.len() == 0 {
            return Ok(());
        }
        collection.insert_many(filterd_data, None).await.unwrap();
    }
    else {
        collection.insert_many(market_data, None).await.unwrap();
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
    }

    Ok(json!( { "message": "Okay"}))
}

#[tokio::main]
pub async fn main() -> Result<(), lambda_runtime::Error> {
    let func = service_fn(func);
    lambda_runtime::run(func).await?;
    Ok(())
}