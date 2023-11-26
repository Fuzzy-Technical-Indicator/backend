use actix_web::web;

use fuzzy_logic::{
    linguistic::LinguisticVar,
    shape::{trapezoid, triangle, zero},
};
use mongodb::{
    bson::{doc, to_bson, Bson},
    options::UpdateOptions,
    Client,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Deserialize, Serialize)]
pub struct ShapeDTO {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    shape_type: Option<String>,
    parameters: Option<HashMap<String, f64>>,
    latex: Option<Vec<String>>,
}

#[derive(Deserialize, Serialize)]
pub struct LinguisticVarDTO {
    #[serde(rename(serialize = "upperBoundary", deserialize = "upperBoundary"))]
    upper_boundary: f64,
    #[serde(rename(serialize = "lowerBoundary", deserialize = "lowerBoundary"))]
    lower_boundary: f64,
    shapes: BTreeMap<String, ShapeDTO>,
}

#[derive(Deserialize, Serialize)]
pub struct SettingsDTO {
    #[serde(rename(serialize = "linguisticVariables", deserialize = "linguisticVariables"))]
    linguistic_variables: BTreeMap<String, LinguisticVarDTO>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ShapeModel {
    parameters: BTreeMap<String, f64>,
    #[serde(rename(serialize = "shapeType", deserialize = "shapeType"))]
    shape_type: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct LinguisticVarModel {
    #[serde(rename(serialize = "upperBoundary", deserialize = "upperBoundary"))]
    upper_boundary: f64,
    #[serde(rename(serialize = "lowerBoundary", deserialize = "lowerBoundary"))]
    lower_boundary: f64,
    shapes: BTreeMap<String, ShapeModel>,
}

#[derive(Deserialize, Serialize)]
pub struct SettingsModel {
    username: String,
    #[serde(rename(serialize = "linguisticVariables", deserialize = "linguisticVariables"))]
    linguistic_variables: BTreeMap<String, LinguisticVarModel>,
}

fn to_settings(var: &LinguisticVar) -> LinguisticVarDTO {
    let mut shapes = BTreeMap::new();
    for (name, set) in var.sets.iter() {
        let data = ShapeDTO {
            shape_type: set.membership_f.name.clone(),
            parameters: set.membership_f.parameters.clone(),
            latex: set.membership_f.latex.clone()
        };
        shapes.insert(name.to_string(), data);
    }

    LinguisticVarDTO {
        shapes,
        lower_boundary: var.universe.0,
        upper_boundary: var.universe.1,
    }
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
        .linguistic_variables
        .iter()
        .map(|(k, v)| {
            let var = LinguisticVar::new(
                v.shapes
                    .iter()
                    .map(|(name, shape_info)| {
                        let parameters = &shape_info.parameters;
                        let f = match shape_info.shape_type.as_str() {
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
                (v.lower_boundary, v.upper_boundary),
            );
            (k.to_string(), to_settings(&var))
        })
        .collect::<BTreeMap<String, LinguisticVarDTO>>();

    SettingsDTO {
        linguistic_variables,
    }
}

pub async fn update_settings(db: web::Data<Client>, info: web::Json<SettingsModel>) -> String {
    let db_client = (*db).database("StockMarket");
    let collection = db_client.collection::<SettingsModel>("settings");

    let data = to_bson(
        &info
            .linguistic_variables
            .iter()
            .map(|(name, info)| {
                let lv_name = format!("linguisticVariables.{}", name);
                let lv_info = to_bson(info).unwrap();
                (lv_name, lv_info)
            })
            .collect::<HashMap<String, Bson>>(),
    )
    .unwrap();
    let options = UpdateOptions::builder().upsert(true).build();
    let update_result = collection
        .update_one(
            doc! { "username": info.username.clone()},
            doc! { "$set": data },
            options,
        )
        .await;

    match update_result {
        Ok(res) => format!("{:?}", res),
        Err(err) => format!("{:?}", err),
    }
}
