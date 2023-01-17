use mongodb::{options::ClientOptions, Client};
use mongodb::bson::{doc, Document};
use dotenvy::dotenv;

///
/// struct OHLC {
///     ticker
///     time
///     open
///     close
///     high
///     low
/// }


#[tokio::main]
pub async fn main() {
    dotenv().ok();
    let url = dotenvy::var("MONGODB_URL").unwrap();

    let mut client_options = ClientOptions::parse(url)
        .await
        .unwrap();

    client_options.app_name = Some("seeddb".to_string());
    
    let client = Client::with_options(client_options).unwrap();
    
    let db = client.database("StockMarket");

    // Get a handle to a collection in the database.
    let collection = db.collection::<Document>("t");

    let docs = vec![
        doc! { "ticker": "BTC/BUSD", "data": {"ticker": "BTC/BUSD", "data": vec![1,2,3], "open": vec![20,10,30] },  },
    ];

    // Insert some documents into the "mydb.books" collection.
    collection.insert_many(docs, None).await.unwrap();


    for collection_name in db.list_collection_names(None).await.unwrap() {
        println!("{}", collection_name);
    }
}
