use alphavantage::Ohlc;
use binance::{market::Market, rest_model::KlineSummaries};
use chrono::{TimeZone, Utc};

/// - `ticker` should be in this format "BTC/USDT", "ETH/USDT", "BNB/USDT"
/// - limit shoud be in range 0-1000
pub async fn klines(
    market: &Market,
    ticker: &str,
    limit: Option<u16>,
    start_time: Option<u64>,
) -> Option<Vec<Ohlc>> {
    match market
        .get_klines(ticker.replace("/", ""), "1h", limit, start_time, None)
        .await
    {
        Err(_) => None,
        Ok(klines) => match klines {
            KlineSummaries::AllKlineSummaries(klines) => {
                return Some(
                    klines
                        .iter()
                        .map(|x| Ohlc {
                            ticker: ticker.to_string(),
                            time: Utc.timestamp_millis_opt(x.open_time).unwrap().into(),
                            open: x.open,
                            close: x.close,
                            high: x.high,
                            low: x.low,
                            volume: x.volume as u64,
                        })
                        .collect(),
                );
            }
        },
    }
}
