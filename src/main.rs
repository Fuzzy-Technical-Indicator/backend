use db_updater::Ohlc;
use futures::stream::TryStreamExt;
use mongodb::bson::{doc, Document};
use mongodb::Collection;
use rocket::serde::json::Json;
use rocket::{get, launch, routes, FromFormField};
use rocket_cors::{CorsOptions, AllowedOrigins, Cors};
use rocket_db_pools::{mongodb, Connection, Database};

// we need to specify the database url on Rocket.toml like this
// [default.databases.marketdata]
// url = "..."
#[derive(Database)]
#[database("marketdata")]
struct MarketData(mongodb::Client);

#[derive(Debug, PartialEq, FromFormField)]
enum Interval {
    #[field(value = "1h")]
    OneHour,
    #[field(value = "4h")]
    FourHour,
    #[field(value = "1d")]
    OneDay,
}

fn aggrdoc_to_ohlc(docs: Vec<Document>) -> Vec<Ohlc> {
    docs.iter()
        .map(|x| Ohlc {
            ticker: x.get_document("_id").unwrap().get_str("ticker").unwrap().to_string(),
            time: x
                .get_document("_id")
                .unwrap()
                .get_datetime("time")
                .unwrap()
                .clone(),
            open: x.get_f64("open").unwrap(),
            close: x.get_f64("close").unwrap(),
            high: x.get_f64("high").unwrap(),
            low: x.get_f64("low").unwrap(),
            volume: x.get_i64("volume").unwrap() as u64,
        })
        .collect()
}

async fn aggr_fetch(collection: &Collection<Ohlc>, interval: Option<Interval>) -> Vec<Ohlc> {
    let result = collection
        .aggregate(
            vec![
                doc! {"$group" : {
                    "_id" : {
                        "ticker": "$ticker",
                        "time": {
                            "$dateTrunc" : {
                                "date": "$time",
                                "unit": "hour",
                                "binSize": match interval {
                                    Some(Interval::OneHour) => 1,
                                    Some(Interval::FourHour) => 4,
                                    Some(Interval::OneDay) => 24,
                                    None => 1,
                                }
                            }
                        }
                    },
                    "open": {"$first": "$open"},
                    "close": {"$last": "$close"},
                    "high": {"$max": "$high"},
                    "low": {"$min": "$low"},
                    "volume": {"$sum": "$volume"},
                }},
                doc! {"$sort": {"_id.time": 1}},
            ],
            None,
        )
        .await
        .unwrap();

    aggrdoc_to_ohlc(result.try_collect::<Vec<Document>>().await.unwrap())
}

#[get("/ohlc?<symbol>&<interval>")]
async fn ohlc(
    db: Connection<MarketData>,
    symbol: &str,
    interval: Option<Interval>,
) -> Json<Vec<Ohlc>> {
    let marketdata = &*db;
    let db = marketdata.database("StockMarket");
    let collection = db.collection::<Ohlc>(symbol);

    Json(aggr_fetch(&collection, interval).await)
}

#[launch]
fn rocket() -> _ {
    // Configure CORS options
    let cors_option = CorsOptions::default()
        .allowed_origins(AllowedOrigins::All);         
    let cors = Cors::from_options(&cors_option).unwrap();

    rocket::build()
        .attach(cors)
        .attach(MarketData::init())
        .mount("/api", routes![ohlc])
}
