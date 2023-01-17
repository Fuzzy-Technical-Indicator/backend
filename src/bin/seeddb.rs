use chrono::TimeZone;
use dotenvy::dotenv;
use mongodb::bson::{self, doc};
use mongodb::options::IndexOptions;
use mongodb::IndexModel;
use mongodb::{options::ClientOptions, Client};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::thread;
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct OHLC {
    ticker: String,
    time: bson::DateTime,
    open: f64,
    close: f64,
    high: f64,
    low: f64,
    volume: u64,
}

fn parse_data(resp: String, ticker: String) -> Result<Vec<OHLC>, Box<dyn Error>> {
    let mut res: Vec<OHLC> = vec![];

    let mut line_iter = resp.lines();

    while let Some(l) = line_iter.next() {
        // this is kind of ugly maybe use `csv` crate instead
        if l.trim() == "" || l.contains("time") {
            continue;
        }

        let data: Vec<&str> = l.split(',').collect();

        let datetime = chrono::Utc.datetime_from_str(data[0], "%Y-%m-%d %H:%M:%S")?;

        res.push(OHLC {
            ticker: ticker.clone(),
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

pub async fn fetch_alphavantage() -> Result<Vec<OHLC>, Box<dyn Error>> {
    dotenv().ok();
    let apikey = dotenvy::var("ALPHA_VANTAGE_APIKEY")?;

    let base = "https://www.alphavantage.co/query";
    let call_type = "TIME_SERIES_INTRADAY_EXTENDED";
    let symbol = "AAPl";
    let interval = "60min";

    let mut result: Vec<OHLC> = vec![];

    // fetch all slices of the stock data
    for y in 1..=2 {
        for m in 1..=12 {
            let slice = format!("year{}month{}", y, m);
            let params = format!("?function={call_type}&symbol={symbol}&interval={interval}&slice={slice}&apikey={apikey}");
            println!("Fetching {slice}...");

            let resp = reqwest::get(format!("{base}{params}"))
                .await?
                .text()
                .await?;

            result.append(&mut parse_data(resp, format!("{symbol}/USD"))?);
            thread::sleep(Duration::from_secs(12));
        }
    }
    Ok(result)
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    let url = dotenvy::var("MONGODB_URL")?;

    let mut client_options = ClientOptions::parse(url).await?;
    client_options.app_name = Some("seeddb".to_string());
    let client = Client::with_options(client_options)?;

    let db = client.database("StockMarket");
    let collection = db.collection::<OHLC>("AAPL/USD");

    let index_options = IndexOptions::builder().unique(Some(true)).build();
    let index = IndexModel::builder()
        .keys(doc! {"time" : 1})
        .options(Some(index_options))
        .build();
    collection.create_index(index, None).await?;

    collection
        .insert_many(&fetch_alphavantage().await?, None)
        .await?;

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

        let expected1 = OHLC {
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

        let expected2 = OHLC {
            ticker: "AAPL/USD".into(),
            time: datetime.into(),
            open: 134.53,
            high: 134.6,
            low: 134.5,
            close: 134.55,
            volume: 36689,
        };

        let result = parse_data(
            "2022-12-19 05:00:00,135.82828321847023,135.82828321847023,135.0292933171851,135.1191796810797,58474
            \n2023-01-13 20:00:00,134.53,134.6,134.5,134.55,36689".into(),
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
