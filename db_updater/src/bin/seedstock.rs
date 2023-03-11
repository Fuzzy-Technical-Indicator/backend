use alphavantage::{AlphaVantageClient, Ohlc};
use dotenvy::dotenv;
use mongodb::bson::doc;
use mongodb::options::{FindOneOptions, IndexOptions};
use mongodb::{options::ClientOptions, Client};
use mongodb::{Database, IndexModel};
use std::error::Error;

/// - `ticker` examples: AAPL/USD, TSLA/USD
pub async fn seed_market<S>(ticker: S, db: &Database) -> Result<(), Box<dyn Error>>
where
    S: Into<String>,
{
    let ticker: String = ticker.into();
    let collection = db.collection::<Ohlc>(&ticker);

    let index_options = IndexOptions::builder().unique(Some(true)).build();
    let index = IndexModel::builder()
        .keys(doc! {"time" : 1})
        .options(Some(index_options))
        .build();
    collection.create_index(index, None).await?;

    let apikey = dotenvy::var("ALPHA_VANTAGE_APIKEY")?;
    let market = AlphaVantageClient::new(apikey);

    let market_data = market
        .intraday_extended(ticker.split('/').next().unwrap(), "60min")
        .await?;

    let find_options = FindOneOptions::builder().sort(doc! {"time": -1}).build();
    let result = collection.find_one(None, Some(find_options)).await?;

    match result {
        Some(res) => {
            let filtered_data: Vec<_> = market_data
                .iter()
                .filter(|x| x.time.gt(&res.time))
                .collect();

            if filtered_data.len() == 0 {
                return Ok(());
            }

            collection.insert_many(filtered_data, None).await?;
        }
        None => {
            collection.insert_many(market_data, None).await?;
        }
    }

    Ok(())
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    let url = dotenvy::var("MONGODB_URL")?;

    let mut client_options = ClientOptions::parse(url).await?;
    client_options.app_name = Some("seeddb".to_string());
    let client = Client::with_options(client_options)?;

    let db = client.database("StockMarket");

    seed_market("NKE/USD", &db).await?;

    Ok(())
}
