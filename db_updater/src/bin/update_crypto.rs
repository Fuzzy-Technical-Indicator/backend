use binance::{api::Binance, market::Market};
use db_updater::{klines, Ohlc};
use mongodb::{options::ClientOptions, Client, Database};

/// Should be deprecated!

/// - `ticker`: e.g. BTC/USDT, ETH/USDT
async fn update_crypto(
    db: &Database,
    market: &Market,
    ticker: &str,
    recents: u16,
) -> Result<(), lambda_runtime::Error> {
    let collection = db.collection::<Ohlc>(&ticker);
    let data = klines(&market, &ticker, Some(recents), None).await.unwrap();
    db_updater::update(&collection, &data, recents as u64).await
}

#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
    let url = dotenvy_macro::dotenv!("MONGODB_URL");

    let mut client_options = ClientOptions::parse(url).await?;
    client_options.app_name = Some("seeddb".to_string());

    let client = Client::with_options(client_options)?;
    let db = client.database("StockMarket");

    let market: Market = Binance::new(None, None);
    let coins = vec!["BTC/USDT", "ETH/USDT", "BNB/USDT"];
    for c in coins {
        update_crypto(&db, &market, &c, 96).await?; // this hard code, replace the most recent 96 records (4 days)
    }
    Ok(())
}
