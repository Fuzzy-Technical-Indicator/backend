use actix_web::web;
use mongodb::{bson::doc, options::IndexOptions, Client, IndexModel};
use serde::{Deserialize, Serialize};

use super::{
    error::{map_internal_err, CustomError},
    DB_NAME,
};

#[derive(Deserialize, Serialize)]
pub struct User {
    username: String,
}

async fn get_user_coll(db: &web::Data<Client>) -> Result<mongodb::Collection<User>, CustomError> {
    let db_client = (*db).database(DB_NAME);
    let collection = db_client.collection::<User>("users");
    let opts = IndexOptions::builder().unique(true).build();
    let index = IndexModel::builder()
        .keys(doc! { "username": 1, "token": 1 })
        .options(opts)
        .build();
    collection
        .create_index(index, None)
        .await
        .map_err(map_internal_err)?;
    Ok(collection)
}

pub async fn register(db: &web::Data<Client>, username: String) -> Result<(), CustomError> {
    let collection = get_user_coll(db).await?;
    collection
        .insert_one(User { username }, None)
        .await
        .map_err(map_internal_err)?;
    Ok(())
}

pub async fn auth_user(db: &web::Data<Client>, username: &str) -> Result<(), CustomError> {
    let collection = get_user_coll(db).await?;
    let result = collection
        .find_one(doc! { "username": username }, None)
        .await
        .map_err(map_internal_err)?;

    if result.is_some() {
        Ok(())
    } else {
        Err(CustomError::UserNotFound(username.to_string()))
    }
}
