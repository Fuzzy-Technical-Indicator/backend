use backend::core::{
    backtest,
    users::{self, LengthSetting, MacdSetting, StochSetting},
    DB_NAME,
};
use mongodb::{
    bson::{doc, to_bson},
    Client,
};

#[tokio::main]
pub async fn main() {
    let uri = dotenvy::var("MONGO_DB_URI").unwrap();
    let db = Client::with_uri_str(uri)
        .await
        .expect("Failed to connect to Mongodb");

    let db_client = db.database(DB_NAME);
    let coll = db_client.collection::<backtest::BacktestReport>("backtest-reports");

    /*
    let bb = to_bson(&BBSetting::default()).unwrap();
    coll.update_many(
        doc! {},
        doc! { "$set": { "bb": bb }},
        None,
    ).await.unwrap();
    */

    /*
    let data = to_bson(&LengthSetting::default()).unwrap();
    coll.update_many(
        doc! {},
        doc! { "$set": { "adx": data.clone(), "aroon": data }},
        None,
    )
    .await
    .unwrap();

    let dt1 = to_bson(&MacdSetting::default()).unwrap();
    let dt2 = to_bson(&StochSetting::default()).unwrap();
    coll.update_many(
        doc! {},
        doc! { "$set": { "macd": dt1,  "stoch": dt2 }},
        None,
    )
    .await
    .unwrap();
    */

    coll.update_many(doc!{ }, doc! { 
        "$set": {
            "backtest_result.metadata.signal_conditions.$[].capital_management": {
                "type": "Normal",
                "entry_size_percent": "$backtest_result.metadata.signal_conditions.$[].entry_size_percent",
                "min_entry_size": "$backtest_result.metadata.signal_conditions.$[].min_entry_size"
            }
        },
        "$unset": {
            "backtest_result.metadata.signal_conditions.$[].entry_size_percent": "",
            "backtest_result.metadata.signal_conditions.$[].min_entry_size": ""
        }}, None).await.unwrap();
}
