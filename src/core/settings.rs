#![allow(non_snake_case)]
use actix_web::web;
use futures::TryStreamExt;
use fuzzy_logic::{
    linguistic::LinguisticVar,
    shape::{trapezoid, triangle, zero},
    FuzzyEngine,
};
use mongodb::{
    bson::{doc, oid, serde_helpers::deserialize_hex_string_from_object_id, to_bson, Bson},
    error::{ErrorKind, WriteFailure},
    Client, Database,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, error::Error, str::FromStr};
use tech_indicators::Ohlc;

use super::{
    bb_cached,
    error::{map_internal_err, CustomError},
    get_rules_coll, rsi_cached,
};

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum LinguisticVarKind {
    #[serde(rename(serialize = "input", deserialize = "input"))]
    Input,
    #[serde(rename(serialize = "output", deserialize = "output"))]
    Output,
}

impl std::string::ToString for LinguisticVarKind {
    fn to_string(&self) -> String {
        match self {
            LinguisticVarKind::Input => "input".to_string(),
            LinguisticVarKind::Output => "output".to_string(),
        }
    }
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

pub async fn update_linguistic_vars(
    db: web::Data<Client>,
    linguisticVariables: web::Json<LinguisticVarsModel>,
) -> Result<String, CustomError> {
    let db_client = (*db).database("StockMarket");
    let setting_coll = db_client.collection::<SettingsModel>("settings");

    let setting = match setting_coll
        .find_one(doc! { "username": "tanat" }, None)
        .await
        .map_err(map_internal_err)?
    {
        Some(doc) => doc,
        None => return Err(CustomError::SettingsNotFound),
    };

    let mut rules_filter = vec![];
    for (k, var1) in setting.linguisticVariables {
        if let Some(var2) = linguisticVariables.get(&k) {
            if var1.kind != var2.kind {
                return Err(CustomError::InternalError(
                    "The kind of the linguistic variable cannot be changed".to_string(),
                ));
            }

            for x in var1.shapes.keys().filter(|x| !var2.shapes.contains_key(*x)) {
                rules_filter.push(doc! {
                    format!("{}.{}", var1.kind.to_string(), k): x
                });
            }
        }
    }
    if !rules_filter.is_empty() {
        let rules_coll = db_client.collection::<FuzzyRuleModel>("fuzzy-rules");
        rules_coll
            .update_many(
                doc! { "$or": rules_filter },
                doc! { "$set": { "valid": false } },
                None,
            )
            .await
            .map_err(map_internal_err)?;
    }

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
    .map_err(map_internal_err)?;

    setting_coll
        .update_one(doc! { "username": "tanat"}, doc! { "$set": data }, None)
        .await
        .map_err(map_internal_err)?;

    Ok("Linguisitc variables have been updated successfully".to_string())
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

                if let Some(shape) = v {
                    if !var.shapes.contains_key(shape) {
                        return Err(CustomError::LinguisticVarShapeNotFound(shape.to_string()));
                    }
                }
            }
        }
        None => return Err(CustomError::SettingsNotFound),
    }

    if rule.input.values().all(|x| x.is_none()) | rule.output.values().all(|x| x.is_none()) {
        return Err(CustomError::RuleNotValid);
    };

    let data = FuzzyRuleModelWithOutId {
        input: rule.input.clone(),
        output: rule.output.clone(),
        username: "tanat".to_string(),
        valid: true,
    };

    let rules_coll = get_rules_coll(&db_client).await?;
    rules_coll
        .insert_one(data, None)
        .await
        .map_err(|e| match *e.kind {
            ErrorKind::Write(WriteFailure::WriteError(w_err)) => {
                // error on unique index constraint
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

pub async fn delete_fuzzy_rule(db: web::Data<Client>, id: String) -> Result<String, CustomError> {
    let db_client = (*db).database("StockMarket");
    let collection = get_rules_coll(&db_client).await?;
    let obj_id = oid::ObjectId::from_str(&id).map_err(map_internal_err)?;

    let result = collection
        .delete_one(doc! { "_id": obj_id }, None)
        .await
        .map_err(map_internal_err)?;

    if result.deleted_count == 0 {
        return Err(CustomError::RuleNotFound(id));
    }
    Ok("The rule has been deleted successfully".to_string())
}

fn to_rule_params<'a>(
    linguistic_var: &'a Vec<String>,
    rule_linguistic_var: &'a BTreeMap<String, Option<String>>,
) -> Vec<Option<&'a str>> {
    linguistic_var
        .iter()
        .map(|name| match rule_linguistic_var.get(name) {
            Some(x) => x.as_ref().map(|v| v.as_str()),
            None => None,
        })
        .collect()
}

pub fn create_fuzzy_engine(
    setting: &SettingsModel,
    fuzzy_rules: &Vec<FuzzyRuleModel>,
) -> FuzzyEngine {
    let mut fuzzy_engine = FuzzyEngine::new();

    let mut linguistic_var_inputs = vec![];
    let mut linguistic_var_outputs = vec![];

    for (name, var_info) in setting.linguisticVariables.iter() {
        let var = var_info.to_real();
        match var_info.kind {
            LinguisticVarKind::Output => {
                fuzzy_engine = fuzzy_engine.add_output(var);
                linguistic_var_outputs.push(name.to_owned())
            }
            LinguisticVarKind::Input => {
                fuzzy_engine = fuzzy_engine.add_cond(var);
                linguistic_var_inputs.push(name.to_owned())
            }
        }
    }

    for rule in fuzzy_rules {
        let a = to_rule_params(&linguistic_var_inputs, &rule.input);
        let b = to_rule_params(&linguistic_var_outputs, &rule.output);
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
        None => return Err(CustomError::SettingsNotFound.into()),
    };
    let fuzzy_rules = rules_coll
        .find(doc! { "username": "tanat", "valid": true }, None)
        .await?
        .try_collect::<Vec<_>>()
        .await?;

    Ok((
        create_fuzzy_engine(&setting, &fuzzy_rules),
        create_input(&setting, data),
    ))
}
