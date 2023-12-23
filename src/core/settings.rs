#![allow(non_snake_case)]
use actix_web::{error::ErrorNotFound, web};
use futures::{TryStreamExt, TryFutureExt};
use fuzzy_logic::{
    linguistic::LinguisticVar,
    shape::{trapezoid, triangle, zero},
    FuzzyEngine,
};
use mongodb::{
    bson::{doc, oid, serde_helpers::deserialize_hex_string_from_object_id, to_bson, Bson},
    error::{ErrorKind, WriteFailure},
    options::IndexOptions,
    Client, Database, IndexModel,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, error::Error, str::FromStr};
use tech_indicators::Ohlc;

use super::{bb_cached, error::{CustomError, map_internal_err}, rsi_cached, get_rules_coll};

#[derive(Deserialize, Serialize, Clone)]
pub enum LinguisticVarKind {
    #[serde(rename(serialize = "input", deserialize = "input"))]
    Input,
    #[serde(rename(serialize = "output", deserialize = "output"))]
    Output,
}

#[derive(Deserialize, Serialize)]
pub struct ShapeDTO {
    shapeType: Option<String>,
    parameters: Option<BTreeMap<String, f64>>,
    latex: Option<Vec<String>>,
}

#[derive(Deserialize, Serialize)]
pub struct LinguisticVarDTO {
    upperBoundary: f64,
    lowerBoundary: f64,
    shapes: BTreeMap<String, ShapeDTO>,
    kind: LinguisticVarKind,
}

#[derive(Deserialize, Serialize)]
pub struct FuzzyRuleDTO {
    id: String,
    input: FuzzyRuleData,
    output: FuzzyRuleData,
    valid: bool,
}

#[derive(Deserialize, Serialize)]
pub struct SettingsDTO {
    linguisticVariables: BTreeMap<String, LinguisticVarDTO>,
    fuzzyRules: Vec<FuzzyRuleDTO>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ShapeModel {
    parameters: BTreeMap<String, f64>,
    shapeType: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct LinguisticVarModel {
    upperBoundary: f64,
    lowerBoundary: f64,
    shapes: BTreeMap<String, ShapeModel>,
    kind: LinguisticVarKind,
}

impl LinguisticVarModel {
    pub fn to_real(&self) -> LinguisticVar {
        LinguisticVar::new(
            self.shapes
                .iter()
                .map(|(name, shape_info)| {
                    let parameters = &shape_info.parameters;
                    let f = match shape_info.shapeType.as_str() {
                        "triangle" => triangle(
                            *parameters.get("center").unwrap(),
                            *parameters.get("height").unwrap(),
                            *parameters.get("width").unwrap(),
                        ),
                        "trapezoid" => trapezoid(
                            *parameters.get("a").unwrap(),
                            *parameters.get("b").unwrap(),
                            *parameters.get("c").unwrap(),
                            *parameters.get("d").unwrap(),
                            *parameters.get("height").unwrap(),
                        ),
                        _ => zero(),
                    };
                    return (name.as_str(), f);
                })
                .collect(),
            (self.lowerBoundary, self.upperBoundary),
        )
    }

    pub fn to_dto(&self) -> LinguisticVarDTO {
        let var = self.to_real();
        let mut shapes = BTreeMap::new();
        for (name, set) in var.sets.iter() {
            let data = ShapeDTO {
                shapeType: set.membership_f.name.clone(),
                parameters: set
                    .membership_f
                    .parameters
                    .as_ref()
                    .map(|x| x.to_owned().into_iter().collect()),
                latex: set.membership_f.latex.clone(),
            };
            shapes.insert(name.to_string(), data);
        }

        LinguisticVarDTO {
            shapes,
            lowerBoundary: var.universe.0,
            upperBoundary: var.universe.1,
            kind: self.kind.clone(),
        }
    }
}

pub type FuzzyRuleData = BTreeMap<String, Option<String>>;

#[derive(Deserialize, Serialize)]
pub struct NewFuzzyRule {
    input: FuzzyRuleData,
    output: FuzzyRuleData,
}

#[derive(Deserialize, Serialize)]
pub struct FuzzyRuleModel {
    #[serde(deserialize_with = "deserialize_hex_string_from_object_id")]
    _id: String,
    input: FuzzyRuleData,
    output: FuzzyRuleData,
    username: String,
    valid: bool,
}

#[derive(Deserialize, Serialize)]
pub struct FuzzyRuleModelWithOutId {
    input: FuzzyRuleData,
    output: FuzzyRuleData,
    username: String,
    valid: bool,
}

pub type LinguisticVarsModel = BTreeMap<String, LinguisticVarModel>;

#[derive(Deserialize, Serialize)]
pub struct SettingsModel {
    username: String,
    linguisticVariables: LinguisticVarsModel,
}

async fn get_fuzzy_rules(db_client: &Database) -> Vec<FuzzyRuleDTO> {
    let collection = db_client.collection::<FuzzyRuleModel>("fuzzy-rules");

    let fuzzyRules = collection
        .find(doc! { "username": "tanat" }, None)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    fuzzyRules
        .into_iter()
        .map(|item| FuzzyRuleDTO {
            id: item._id.to_string(),
            input: item.input,
            output: item.output,
            valid: item.valid,
        })
        .collect()
}

pub async fn get_settings(db: web::Data<Client>) -> SettingsDTO {
    let db_client = (*db).database("StockMarket");
    let collection = db_client.collection::<SettingsModel>("settings");

    // hard coded username
    let settings = collection
        .find_one(doc! { "username": "tanat" }, None)
        .await
        .unwrap()
        .unwrap();

    let linguistic_variables = settings
        .linguisticVariables
        .iter()
        .map(|(name, var_info)| (name.to_string(), var_info.to_dto()))
        .collect::<BTreeMap<String, LinguisticVarDTO>>();

    SettingsDTO {
        linguisticVariables: linguistic_variables,
        fuzzyRules: get_fuzzy_rules(&db_client).await,
    }
}
/*
* When this is updated, what should happend with the linguistic variables?
*/
pub async fn update_linguistic_vars(
    db: web::Data<Client>,
    linguisticVariables: web::Json<LinguisticVarsModel>,
) -> String {
    let db_client = (*db).database("StockMarket");
    let collection = db_client.collection::<SettingsModel>("settings");

    let data = to_bson(
        &linguisticVariables
            .iter()
            .map(|(name, var)| {
                let lv_name = format!("linguisticVariables.{}", name);
                let lv_info = to_bson(var).unwrap();
                (lv_name, lv_info)
            })
            .collect::<BTreeMap<String, Bson>>(),
    )
    .unwrap();
    let update_result = collection
        .update_one(doc! { "username": "tanat"}, doc! { "$set": data }, None)
        .await;

    match update_result {
        Ok(res) => format!("{:?}", res),
        Err(err) => format!("{:?}", err),
    }
}

pub async fn delete_linguistic_var(db: web::Data<Client>, name: String) -> String {
    let db_client = (*db).database("StockMarket");
    let collection = db_client.collection::<SettingsModel>("settings");

    let target = doc! {
        format!("linguisticVariables.{}", name): ""
    };

    // still hard coded username
    let result = collection
        .update_one(
            doc! { "username": "tanat" },
            doc! { "$unset": target },
            None,
        )
        .await;

    match result {
        Ok(res) => format!("{:?}", res),
        Err(err) => format!("{:?}", err),
    }
}

pub async fn add_fuzzy_rules(
    db: web::Data<Client>,
    rule: web::Json<NewFuzzyRule>,
) -> Result<String, CustomError> {
    let db_client = (*db).database("StockMarket");
    let setting_coll = db_client.collection::<SettingsModel>("settings");

    let doc_opt = setting_coll
        .find_one(doc! { "username": "tanat" }, None)
        .await
        .map_err(|_| CustomError::SettingsNotFound)?;

    match doc_opt {
        Some(doc) => {
            for (k, v) in rule.input.iter().chain(rule.output.iter()) {
                let Some(var) = doc.linguisticVariables.get(k) else {
                    return Err(CustomError::LinguisticVarNotFound(k.to_string()));
                };

                if let Some(set) = v {
                    if !var.shapes.contains_key(set) {
                        return Err(CustomError::LinguisticVarShapeNotFound(set.to_string()));
                    }
                }
            }
        }
        None => return Err(CustomError::SettingsNotFound),
    }

    let rules_coll = get_rules_coll(&db_client).await.map_err(map_internal_err)?;

    let data = FuzzyRuleModelWithOutId {
        input: rule.input.clone(),
        output: rule.output.clone(),
        username: "tanat".to_string(),
        valid: true,
    };

    rules_coll
        .insert_one(data, None)
        .await
        .map_err(|e| match *e.kind {
            ErrorKind::Write(WriteFailure::WriteError(w_err)) => {
                if w_err.code == 11000 {
                    CustomError::RuleAlreadyExist
                } else {
                    CustomError::InternalError(format!("{:?}", w_err))
                }
            }
            _ => CustomError::InternalError(e.to_string()),
        })?;

    Ok("The rule is added successfully".to_string())
}

pub async fn delete_fuzzy_rule(db: web::Data<Client>, id: String) -> String {
    let db_client = (*db).database("StockMarket");
    let collection = db_client.collection::<FuzzyRuleModelWithOutId>("fuzzy-rules");

    let obj_id = oid::ObjectId::from_str(&id).unwrap();
    let result = collection.delete_one(doc! { "_id": obj_id }, None).await;

    match result {
        Ok(res) => format!("{:?}", res),
        Err(err) => format!("{:?}", err),
    }
}

pub fn create_fuzzy_engine(
    setting: &SettingsModel,
    fuzzy_rules: &Vec<FuzzyRuleModel>,
) -> FuzzyEngine {
    let mut fuzzy_engine = FuzzyEngine::new();

    for (_, var_info) in setting.linguisticVariables.iter() {
        let var = var_info.to_real();
        match var_info.kind {
            LinguisticVarKind::Output => fuzzy_engine = fuzzy_engine.add_output(var),
            LinguisticVarKind::Input => fuzzy_engine = fuzzy_engine.add_cond(var),
        }
    }

    for rule in fuzzy_rules {
        let a = rule
            .input
            .values()
            .map(|opt| opt.as_ref().map(|v| v.as_str()))
            .collect::<Vec<Option<&str>>>();
        let b = rule
            .output
            .values()
            .map(|opt| opt.as_ref().map(|v| v.as_str()))
            .collect::<Vec<Option<&str>>>();
        fuzzy_engine = fuzzy_engine.add_rule(a, b);
    }
    fuzzy_engine
}

fn bb_percent(price: f64, v: (f64, f64, f64)) -> f64 {
    let (sma, lower, upper) = v;

    if price > sma {
        return (price - sma) / (upper - sma);
    }

    (sma - price) / (sma - lower)
}

pub fn create_input(
    setting: &SettingsModel,
    data: &(Vec<Ohlc>, String),
) -> Vec<(i64, Vec<Option<f64>>)> {
    let mut dt = data
        .0
        .iter()
        .map(|x| (x.time.to_chrono().timestamp_millis(), Vec::new()))
        .collect::<Vec<(i64, Vec<Option<f64>>)>>(); // based on time

    for (name, var_info) in setting.linguisticVariables.iter() {
        if let LinguisticVarKind::Input = var_info.kind {
            match name.as_str() {
                "rsi" => rsi_cached(data.clone(), 14)
                    .iter()
                    .zip(dt.iter_mut())
                    .for_each(|(rsi_v, x)| x.1.push(Some(rsi_v.value))),
                "bb" => bb_cached(data.clone(), 20, 2.0)
                    .iter()
                    .zip(dt.iter_mut())
                    .zip(data.0.iter())
                    .for_each(|((bb_v, x), ohlc)| {
                        x.1.push(Some(bb_percent(ohlc.close, bb_v.value)))
                    }),
                _ => {}
            };
        }
    }

    dt
}

pub async fn get_fuzzy_config(
    db: &web::Data<Client>,
    data: &(Vec<Ohlc>, String),
) -> Result<(FuzzyEngine, Vec<(i64, Vec<Option<f64>>)>), Box<dyn Error>> {
    let db_client = (*db).database("StockMarket");
    let setting_coll = db_client.collection::<SettingsModel>("settings");
    let rules_coll = db_client.collection::<FuzzyRuleModel>("fuzzy-rules");

    let setting = match setting_coll
        .find_one(doc! { "username": "tanat" }, None)
        .await?
    {
        Some(doc) => doc,
        None => return Err("Settings not found".into()),
    };
    let fuzzy_rules = rules_coll
        .find(doc! { "username": "tanat" }, None)
        .await?
        .try_collect::<Vec<_>>()
        .await?;

    Ok((
        create_fuzzy_engine(&setting, &fuzzy_rules),
        create_input(&setting, data),
    ))
}
