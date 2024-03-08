use actix_web::web;
use mongodb::{
    bson::{doc, to_bson},
    options::IndexOptions,
    Client, IndexModel,
};
use serde::{Deserialize, Serialize};
use cached::proc_macro::cached;

use super::{
    error::{map_internal_err, CustomError},
    DB_NAME,
};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BBSetting {
    pub length: usize,
    pub stdev: f64,
}

impl Default for BBSetting {
    fn default() -> Self {
        BBSetting {
            length: 20,
            stdev: 2.0,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LengthSetting {
    pub length: usize,
}

impl Default for LengthSetting {
    fn default() -> Self {
        LengthSetting { length: 14 }
    }
}
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MacdSetting {
    pub fast: usize,
    pub slow: usize,
    pub smooth: usize,
}

impl Default for MacdSetting {
    fn default() -> Self {
        MacdSetting {
            fast: 12,
            slow: 26,
            smooth: 9,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct StochSetting {
    pub k: usize,
    pub d: usize,
    pub length: usize,
}

impl Default for StochSetting {
    fn default() -> Self {
        StochSetting {
            k: 14,
            d: 3,
            length: 1,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct User {
    pub username: String,
    pub bb: BBSetting,
    pub rsi: LengthSetting,
    pub adx: LengthSetting,
    pub aroon: LengthSetting,
    pub macd: MacdSetting,
    pub stoch: StochSetting,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum TASetting {
    #[serde(rename = "bb")]
    BB(BBSetting),
    #[serde(rename = "rsi")]
    Rsi(LengthSetting),
    #[serde(rename = "adx")]
    Adx(LengthSetting),
    #[serde(rename = "aroon")]
    Aroon(LengthSetting),
    #[serde(rename = "macd")]
    Macd(MacdSetting),
    #[serde(rename = "stoch")]
    Stoch(StochSetting),
}

pub async fn get_user_coll(
    db: &web::Data<Client>,
) -> Result<mongodb::Collection<User>, CustomError> {
    let db_client = (*db).database(DB_NAME);
    let collection = db_client.collection::<User>("users");
    let opts = IndexOptions::builder().unique(true).build();
    let index = IndexModel::builder()
        .keys(doc! { "username": 1 })
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
        .insert_one(
            User {
                username,
                bb: BBSetting::default(),
                rsi: LengthSetting::default(),
                adx: LengthSetting::default(),
                aroon: LengthSetting::default(),
                macd: MacdSetting::default(),
                stoch: StochSetting::default(),
            },
            None,
        )
        .await
        .map_err(map_internal_err)?;
    Ok(())
}

#[cached(
    result=true,
    key = "String",
    convert = r#"{ format!("{}", username) }"#
)]
pub async fn auth_user(db: &web::Data<Client>, username: &str) -> Result<User, CustomError> {
    let collection = get_user_coll(db).await?;
    let result = collection
        .find_one(doc! { "username": username }, None)
        .await
        .map_err(map_internal_err)?;

    match result {
        Some(user) => Ok(user),
        None => Err(CustomError::UserNotFound(username.to_string())),
    }
}

pub async fn update_user_setting(
    db: &web::Data<Client>,
    username: String,
    data: web::Json<TASetting>,
) -> Result<(), CustomError> {
    use TASetting::*;

    let collection = get_user_coll(db).await?;

    let dt = data.into_inner();
    let data = match dt {
        BB(x) => {
            doc! { "bb": to_bson(&x).map_err(map_internal_err)? }
        }
        Rsi(x) => {
            doc! { "rsi": to_bson(&x).map_err(map_internal_err)? }
        }
        Adx(x) => {
            doc! { "adx": to_bson(&x).map_err(map_internal_err)? }
        }
        Aroon(x) => {
            doc! { "aroon": to_bson(&x).map_err(map_internal_err)? }
        }
        Macd(x) => {
            doc! { "macd": to_bson(&x).map_err(map_internal_err)? }
        }
        Stoch(x) => {
            doc! { "stoch": to_bson(&x).map_err(map_internal_err)? }
        }
    };

    collection
        .update_one(doc! { "username": username }, doc! { "$set": data }, None)
        .await
        .map_err(map_internal_err)?;
    Ok(())
}
