use chrono::TimeZone;
use dotenvy::dotenv;
use mongodb::bson::{self, doc};
use mongodb::options::{FindOneOptions, IndexOptions};
use mongodb::{options::ClientOptions, Client};
use mongodb::{Database, IndexModel};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::thread;
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct IntradayExtended {
    ticker: String,
    time: bson::DateTime,
    open: f64,
    close: f64,
    high: f64,
    low: f64,
    volume: u64,
}

fn parse_intraday_extended(
    resp: String,
    symbol: String,
) -> Result<Vec<IntradayExtended>, Box<dyn Error>> {
    let mut rdr = csv::Reader::from_reader(resp.as_bytes());
    let mut res: Vec<IntradayExtended> = vec![];

    for result in rdr.records() {
        let data = result?;

        let datetime = chrono::Utc.datetime_from_str(&data[0], "%Y-%m-%d %H:%M:%S")?;
        res.push(IntradayExtended {
            ticker: symbol.clone(),
            time: datetime.into(),
            open: data[1].parse()?,
            high: data[2].parse()?,
            low: data[3].parse()?,
            close: data[4].parse()?,
            volume: data[5].parse()?,
        });
    }
    Ok(res)
}

pub struct AlphaVantageClient {
    apikey: String,
    base_url: String,
}

impl AlphaVantageClient {
    pub fn new<S>(apikey: S) -> AlphaVantageClient
    where
        S: Into<String>,
    {
        AlphaVantageClient {
            apikey: apikey.into(),
            base_url: "https://www.alphavantage.co/query".to_string(),
        }
    }

    /// This could be 5 - 10 mins fetching
    /// - `symbol` The name of the equity of your choice. For example: symbol=IBM
    /// - `interval` Time interval between two consecutive data points in the time series.
    /// The following values are supported: 1min, 5min, 15min, 30min, 60min
    pub async fn intraday_extended<S>(
        &self,
        symbol: S,
        interval: S,
    ) -> Result<Vec<IntradayExtended>, Box<dyn Error>>
    where
        S: Into<String>,
    {
        let call_type = "TIME_SERIES_INTRADAY_EXTENDED";
        let symbol: String = symbol.into();
        let interval: String = interval.into();

        let base_params = format!(
            "?function={call_type}&symbol={symbol}&interval={interval}&apikey={}",
            self.apikey
        );

        let mut result: Vec<IntradayExtended> = vec![];
        // fetch all slices start at year1month1 to year2month12
        for y in 1..=2 {
            for m in 1..=12 {
                let slice = format!("year{}month{}", y, m);
                let params = format!("{base_params}&slice={slice}");
                println!("Fetching {slice}");

                let resp = reqwest::get(format!("{}{params}", self.base_url))
                    .await?
                    .text()
                    .await?;

                result.append(&mut parse_intraday_extended(resp, format!("{symbol}/USD"))?);
                thread::sleep(Duration::from_secs(12));
            }
        }
        Ok(result)
    }
}

/// - `ticker` examples: AAPL/USD, TSLA/USD
pub async fn seed_market<S>(ticker: S, db: &Database) -> Result<(), Box<dyn Error>>
where
    S: Into<String>,
{
    let ticker: String = ticker.into();
    let collection = db.collection::<IntradayExtended>(&ticker);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_data() {
        let datetime = chrono::Utc
            .datetime_from_str("2022-12-19 05:00:00", "%Y-%m-%d %H:%M:%S")
            .unwrap();

        let expected1 = IntradayExtended {
            ticker: "AAPL/USD".into(),
            time: datetime.into(),
            open: 135.82828321847023,
            high: 135.82828321847023,
            low: 135.0292933171851,
            close: 135.1191796810797,
            volume: 58474,
        };

        let datetime = chrono::Utc
            .datetime_from_str("2023-01-13 20:00:00", "%Y-%m-%d %H:%M:%S")
            .unwrap();

        let expected2 = IntradayExtended {
            ticker: "AAPL/USD".into(),
            time: datetime.into(),
            open: 134.53,
            high: 134.6,
            low: 134.5,
            close: 134.55,
            volume: 36689,
        };

        let result = parse_intraday_extended(
            "time,open,high,low,close,volume
            2022-12-19 05:00:00,135.82828321847023,135.82828321847023,135.0292933171851,135.1191796810797,58474
            2023-01-13 20:00:00,134.53,134.6,134.5,134.55,36689".into(),
            "AAPL/USD".into(),
        )
        .unwrap();

        assert_eq!(expected1, result[0]);
        assert_eq!(expected2, result[1]);
    }

    #[tokio::test]
    async fn test_fetch() {
        //fetch_alphavantage().await.unwrap();
    }
}
