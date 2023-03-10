use alphavantage::Ohlc;
use binance::{api::Binance, market::Market, model::KlineSummaries};
use chrono::{Duration, TimeZone, Utc};
use dotenvy::dotenv;
use mongodb::{
    bson::doc,
    options::{ClientOptions, FindOneOptions, IndexOptions},
    Client, Database, IndexModel,
};
// https://www.binance.com/en/support/announcement/binance-exchange-launched-date-set-115000599831

/// - `ticker` should be in this format "BTC/USDT", "ETH/USDT", "BNB/USDT"
fn klines1000(market: &Market, ticker: &str, start_time: u64) -> Option<(String, Vec<Ohlc>)> {
    match market.get_klines(ticker.replace("/", ""), "1h", 1000, start_time, None) {
        Err(_) => None,
        Ok(klines) => match klines {
            KlineSummaries::AllKlineSummaries(klines) => {
                return Some((
                    ticker.to_string(),
                    klines
                        .iter()
                        .map(|x| Ohlc {
                            ticker: ticker.to_string(),
                            time: Utc.timestamp_millis_opt(x.open_time).unwrap().into(),
                            open: x.open.parse().unwrap(),
                            close: x.close.parse().unwrap(),
                            high: x.high.parse().unwrap(),
                            low: x.low.parse().unwrap(),
                            volume: x.volume.parse::<f64>().unwrap() as u64,
                        })
                        .collect(),
                ));
            }
        },
    }
}

/// Insert the `db` with a of a given `tickers`
async fn seed_crypto(
    db: &Database,
    collection_name: &str,
    data: &Vec<Ohlc>,
) -> Result<(), Box<dyn std::error::Error>> {
    let collection = db.collection::<Ohlc>(collection_name);

    let index_options = IndexOptions::builder().unique(Some(true)).build();
    let index = IndexModel::builder()
        .keys(doc! {"time" : 1})
        .options(Some(index_options))
        .build();

    collection.create_index(index, None).await?;

    let find_options = FindOneOptions::builder().sort(doc! {"time": -1}).build();
    let result = collection.find_one(None, Some(find_options)).await?;

    match result {
        Some(res) => {
            let filtered_data: Vec<_> = data.iter().filter(|x| x.time.gt(&res.time)).collect();
            if filtered_data.len() > 0 {
                collection.insert_many(filtered_data, None).await?;
            }
        }
        None => {
            collection.insert_many(data, None).await?;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    let url = dotenvy::var("MONGODB_URL").unwrap();

    let mut client_options = ClientOptions::parse(url).await.unwrap();
    client_options.app_name = Some("seeddb".to_string());

    let client = Client::with_options(client_options).unwrap();

    let db = client.database("StockMarket");
    let market: Market = Binance::new(None, None);
    let coins = vec!["BTC/USDT", "ETH/USDT", "BNB/USDT"];

    for c in coins {
        println!("Working on {c}");
        let mut t = Utc.with_ymd_and_hms(2017, 6, 14, 0, 0, 0).unwrap();

        while t < Utc::now() {
            let data = klines1000(&market, &c, t.timestamp_millis() as u64).unwrap();
            seed_crypto(&db, &data.0, &data.1).await.unwrap();
            t = t + Duration::hours(1000);
        }
    }
}
