use chrono::{TimeZone, Utc};
use std::error::Error;
use std::thread;
use std::time::Duration;

use crate::Ohlc;

fn parsecsv_to_ohlc(resp: String, symbol: String) -> Result<Vec<Ohlc>, Box<dyn Error>> {
    let mut rdr = csv::Reader::from_reader(resp.as_bytes());
    let mut res: Vec<Ohlc> = vec![];

    for result in rdr.records() {
        let data = result?;

        let datetime = Utc.datetime_from_str(&data[0], "%Y-%m-%d %H:%M:%S")?;
        res.push(Ohlc {
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
    ) -> Result<Vec<Ohlc>, Box<dyn Error>>
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

        let mut result: Vec<Ohlc> = vec![];
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

                result.append(&mut parsecsv_to_ohlc(resp, format!("{symbol}/USD"))?);
                thread::sleep(Duration::from_secs(12));
            }
        }
        Ok(result)
    }

    /// Remember that for free-tier API key the rate limit is 5 API requests per minute and 500 requests per day
    pub async fn intraday<S>(
        &self,
        symbol: S,
        interval: S,
        adjusted: Option<bool>,
        outputsize: Option<S>,
        datatype: Option<S>,
    ) -> Result<Vec<Ohlc>, Box<dyn Error>>
    where
        S: Into<String>,
    {
        let call_type = "TIME_SERIES_INTRADAY";
        let symbol: String = symbol.into();
        let base_params = format!(
            "?function={call_type}&symbol={}&interval={}&apikey={}",
            &symbol,
            &interval.into(),
            self.apikey
        );

        let mut params = base_params;
        if let Some(adj) = adjusted {
            params = format!("{params}&adjusted={}", adj.to_string());
        }
        if let Some(outputsize) = outputsize {
            params = format!("{params}&outputsize={}", outputsize.into());
        }

        match datatype {
            Some(datatype) => {
                let dt_type: String = datatype.into();
                params = format!("{params}&datatype={}", &dt_type);
                println!("{}", format!("{}{params}", self.base_url));
                let resp = reqwest::get(format!("{}{params}", self.base_url))
                    .await?
                    .text()
                    .await?;

                if dt_type == "json" {
                    // do something
                } else if dt_type == "csv" {
                    return Ok((parsecsv_to_ohlc(resp, format!("{}/USD", &symbol)))?);
                }
            }
            None => {
                // json: do something
            }
        }

        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_data() {
        let datetime = chrono::Utc
            .datetime_from_str("2022-12-19 05:00:00", "%Y-%m-%d %H:%M:%S")
            .unwrap();

        let expected1 = Ohlc {
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

        let expected2 = Ohlc {
            ticker: "AAPL/USD".into(),
            time: datetime.into(),
            open: 134.53,
            high: 134.6,
            low: 134.5,
            close: 134.55,
            volume: 36689,
        };

        let result = parsecsv_to_ohlc(
            "time,open,high,low,close,volume
            2022-12-19 05:00:00,135.82828321847023,135.82828321847023,135.0292933171851,135.1191796810797,58474
            2023-01-13 20:00:00,134.53,134.6,134.5,134.55,36689".into(),
            "AAPL/USD".into(),
        )
        .unwrap();

        assert_eq!(expected1, result[0]);
        assert_eq!(expected2, result[1]);
    }
}
