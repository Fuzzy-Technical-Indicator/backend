use db_updater::Ohlc;
use futures::stream::TryStreamExt;
use mongodb::bson::doc;
use mongodb::options::FindOptions;
use rocket::serde::json::Json;
use rocket::{get, launch, routes};
use rocket_db_pools::{mongodb, Connection, Database};

// we need to specify the database url on Rocket.toml like this
// [default.databases.marketdata]
// url = "..."
#[derive(Database)]
#[database("marketdata")]
struct MarketData(mongodb::Client);

#[get("/ohlc?<symbol>")]
async fn ohlc(db: Connection<MarketData>, symbol: &str) -> Json<Vec<Ohlc>> {
    let marketdata = &*db;
    let db = marketdata.database("StockMarket");
    let collection = db.collection::<Ohlc>(symbol);

    let find_options = FindOptions::builder().sort(doc! {"time": -1}).build();
    let result = collection.find(None, find_options).await.unwrap();
    let data = result.try_collect::<Vec<Ohlc>>().await.unwrap();
    Json(data)
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .attach(MarketData::init())
        .mount("/api", routes![ohlc])
}
