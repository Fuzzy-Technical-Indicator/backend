use std::error::Error;

use chrono::{DateTime, Duration, TimeZone, Timelike, Utc};
use itertools::izip;
use serde::Deserialize;

use crate::Ohlc;

#[derive(Deserialize, Debug)]
struct StockCandle {
    c: Vec<f64>,
    h: Vec<f64>,
    l: Vec<f64>,
    o: Vec<f64>,
    s: String,
    t: Vec<i64>,
    v: Vec<u64>,
}

#[derive(Deserialize, Debug)]
struct Quote {
    /// Current price
    c: f64,
    /// Change
    d: f64,
    /// Percent change
    dp: f64,
    h: f64,
    l: f64,
    o: f64,
    /// Previous close price
    pc: f64,
    t: i64,
}

pub struct FinnhubClient {
    apikey: String,
    base_url: String,
}

impl FinnhubClient {
    pub fn new(apikey: &str) -> Self {
        Self {
            apikey: apikey.to_string(),
            base_url: "https://finnhub.io/api/v1".to_string(),
        }
    }

    /// - symbol: e.g. AAPL, IBM
    /// - resolution: 1, 5, 15, 30, 60, D, W, M
    /// - from (unix timestamp in seconds): e.g. 1572651390
    /// - to (unix timestamp in seconds): e.g. 1572910590
    pub async fn stock_candle(
        &self,
        symbol: &str,
        resolution: &str,
        from: i64,
        to: i64,
    ) -> Result<Vec<Ohlc>, Box<dyn Error>> {
        let url = format!(
            "{}/stock/candle?symbol={symbol}&resolution={resolution}&from={from}&to={to}&token={}",
            self.base_url, self.apikey,
        );

        let resp = reqwest::get(url).await?.json::<StockCandle>().await?;

        Ok(izip!(&resp.c, &resp.h, &resp.l, &resp.o, &resp.t, &resp.v)
            .map(|(&c, &h, &l, &o, &t, &v)| Ohlc {
                ticker: format!("{symbol}/USD"),
                time: Utc.timestamp_opt(t, 0).unwrap().into(),
                open: o,
                close: c,
                high: h,
                low: l,
                volume: v,
            })
            .collect())
    }

    pub async fn quote(&self, symbol: &str) -> Result<Ohlc, Box<dyn Error>> {
        let url = format!(
            "{}/quote?symbol={symbol}&token={}",
            self.base_url, self.apikey,
        );
        let resp = reqwest::get(url).await?.json::<Quote>().await?;

        Ok(Ohlc {
            ticker: format!("{symbol}/USD"),
            time: Utc.timestamp_opt(resp.t, 0).unwrap().into(),
            open: resp.o,
            close: resp.c,
            high: resp.h,
            low: resp.l,
            volume: 0,
        })
    }
}

/// TODO: make more tests
#[tokio::test]
async fn t() {
    let apikey = dotenvy_macro::dotenv!("FINNHUB_APIKEY");
    let client = FinnhubClient::new(apikey);

    let now = Utc::now();
    let from = now - Duration::days(1);

    println!(
        "{:?}",
        client
            .stock_candle("AAPL", "60", from.timestamp(), now.timestamp())
            .await
            .unwrap()
    );
}
