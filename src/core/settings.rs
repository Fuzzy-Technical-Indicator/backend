#![allow(non_snake_case)]
use actix_web::web;
use futures::{StreamExt, TryStreamExt};
use fuzzy_logic::{
    linguistic::LinguisticVar,
    shape::{trapezoid, triangle, zero},
};
use mongodb::{
    bson::{doc, serde_helpers::deserialize_hex_string_from_object_id, to_bson, Bson},
    Client, Database,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
    _id: String,
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

fn to_dto(var: &LinguisticVar, kind: &LinguisticVarKind) -> LinguisticVarDTO {
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
        kind: kind.clone(),
    }
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

    fuzzyRules.into_iter().map(|item| FuzzyRuleDTO {
        _id: item._id.to_string(),
        input: item.input,
        output: item.output,
        valid: item.valid,
    }).collect()
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
        .map(|(name, var_info)| {
            let var = LinguisticVar::new(
                var_info
                    .shapes
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
                (var_info.lowerBoundary, var_info.upperBoundary),
            );
            (name.to_string(), to_dto(&var, &var_info.kind))
        })
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

pub async fn dalete_linguistic_var(db: web::Data<Client>, name: String) -> String {
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

pub async fn add_fuzzy_rule(db: web::Data<Client>, rule: web::Json<NewFuzzyRule>) -> String {
    let db_client = (*db).database("StockMarket");
    let setting_coll = db_client.collection::<SettingsModel>("settings");

    let doc_opt = setting_coll
        .find_one(doc! { "username": "tanat" }, None)
        .await
        .unwrap();

    match doc_opt {
        Some(doc) => {
            for (k, v) in rule.input.iter().chain(rule.output.iter()) {
                if let Some(var) = doc.linguisticVariables.get(k) {
                    if let Some(set) = v {
                        if !var.shapes.contains_key(set) {
                            return format!(
                                "This linguistic variable set \"{}\" does not exist",
                                set
                            );
                        }
                    }
                } else {
                    return format!("This linguistic variable \"{}\" does not exist", k);
                }
            }
        }
        None => return "Settings not found".to_string(),
    }
    let collection = db_client.collection::<FuzzyRuleModelWithOutId>("fuzzy-rules");
    let data = FuzzyRuleModelWithOutId {
        input: rule.input.clone(),
        output: rule.output.clone(),
        username: "tanat".to_string(),
        valid: true,
    };
    let result = collection.insert_one(data, None).await;

    match result {
        Ok(res) => format!("{:?}", res),
        Err(err) => format!("{:?}", err),
    }
}
