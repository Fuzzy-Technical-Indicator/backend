use std::collections::HashMap;

use alphavantage::Ohlc;
use binance::{market::Market, rest_model::KlineSummaries};
use chrono::{TimeZone, Utc};
use futures::stream::TryStreamExt;
use mongodb::{Collection, bson::{to_document, doc}, options::FindOptions, Database};

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

pub async fn most_recents(
    collection: &Collection<Ohlc>,
    limit: u64
) -> Result<Vec<Ohlc>, lambda_runtime::Error> {
    let find_options = FindOptions::builder()
        .sort(doc! {"time": -1})
        .limit(Some(limit as i64))
        .build();
    let cursor = collection
        .find(None, find_options)
        .await
        .expect("collection is empty?");

    Ok(cursor
        .try_collect()
        .await
        .expect("can't collect mongodb cursor"))
}

pub async fn replace_recents(
    collection: &Collection<Ohlc>,
    old: &Vec<Ohlc>,
    new: &Vec<Ohlc>,
) -> Result<(), lambda_runtime::Error> {
    let old_map: HashMap<_, _> = old.iter().map(|x| (x.time, x)).collect();
    let should_update: Vec<_> = new
        .iter()
        .filter_map(|x| old_map.get(&x.time).map(|y| (*y, x)))
        .collect();
    
    for (query, update) in should_update {
        collection
            .replace_one(to_document(query).unwrap(), update, None)
            .await
            .expect("can't update");
    }
    Ok(())
}

pub async fn insert_new(
    collection: &Collection<Ohlc>,
    most_recent: &Ohlc,
    new: &Vec<Ohlc>,
) -> Result<(), lambda_runtime::Error> {
    let new_data: Vec<_> = new
        .iter()
        .filter(|x| x.time.gt(&most_recent.time))
        .collect();
    if new_data.len() > 0 {
        collection.insert_many(new_data, None).await?;
    };
    Ok(())
}

/// replace #`recents` of old data with new data, and insert the remainings
pub async fn update(collection: &Collection<Ohlc>, new_data: &Vec<Ohlc>, recents: u64) -> Result<(), lambda_runtime::Error> {
    let old: Vec<Ohlc> = most_recents(&collection, recents).await?;
    replace_recents(&collection, &old, &new_data).await?;
    if let Some(most_recent) = old.first() {
        insert_new(&collection, most_recent, &new_data).await?;
    }
    Ok(())
}