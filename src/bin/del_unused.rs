use actix_web::web;
use backend::core::{
    backtest,
    settings::{delete_preset, LinguisticVarPresetModel},
    DB_NAME,
};
use futures::TryStreamExt;
use mongodb::{bson::doc, Client};

#[tokio::main]
pub async fn main() {
    let uri = dotenvy::var("MONGO_DB_URI").unwrap();
    let db = Client::with_uri_str(uri)
        .await
        .expect("Failed to connect to Mongodb");

    let db_client = db.database(DB_NAME);
    let coll = db_client.collection::<LinguisticVarPresetModel>("linguistic-vars");

    let temp = coll
        .find(
            doc! { "username": "r", "preset": { "$regex": "rsi-bb-.*-pso" } },
            None,
        )
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let client = web::Data::new(db);
    for p in temp.into_iter().map(|x| x.preset) {
        let _ = delete_preset(&client, p, "r".to_string()).await;
    }

    let backtest_coll = db_client.collection::<backtest::BacktestReportWithId>("backtest-reports");

    let _ = backtest_coll.delete_many(
        doc! { "username": "r", "fuzzy_preset": {"$regex": "rsi-bb-.*-pso"}},
        None,
    ).await;
}
