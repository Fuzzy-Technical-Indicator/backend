#![allow(non_snake_case)]
use actix_web::web;
use futures::{FutureExt, TryStreamExt};
use fuzzy_logic::{
    linguistic::LinguisticVar,
    shape::{trapezoid, triangle, zero},
};
use mongodb::{
    bson::{doc, oid, serde_helpers::deserialize_hex_string_from_object_id, to_bson},
    error::{ErrorKind, WriteFailure},
    options::IndexOptions,
    Client, Collection, IndexModel,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, str::FromStr};

use super::{
    error::{map_internal_err, CustomError},
    fuzzy::create_fuzzy_engine,
    DB_NAME,
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
    pub parameters: BTreeMap<String, f64>,
    pub shapeType: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct LinguisticVarModel {
    pub upperBoundary: f64,
    pub lowerBoundary: f64,
    pub shapes: BTreeMap<String, ShapeModel>,
    pub kind: LinguisticVarKind,
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

#[derive(Deserialize, Serialize, Clone)]
pub struct FuzzyRuleModel {
    #[serde(deserialize_with = "deserialize_hex_string_from_object_id")]
    _id: String,
    pub input: FuzzyRuleData,
    pub output: FuzzyRuleData,
    pub username: String,
    pub preset: String,
    pub valid: bool,
}

#[derive(Deserialize, Serialize)]
pub struct FuzzyRuleModelWithOutId {
    pub input: FuzzyRuleData,
    pub output: FuzzyRuleData,
    pub username: String,
    pub preset: String,
    pub valid: bool,
}

pub type LinguisticVarsModel = BTreeMap<String, LinguisticVarModel>;

#[derive(Deserialize, Serialize, Clone)]
pub struct LinguisticVarPresetModel {
    username: String,
    pub preset: String,
    pub vars: LinguisticVarsModel,
}

pub async fn fetch_setting(
    db: &web::Data<Client>,
    username: &String,
    preset: &String,
) -> Result<LinguisticVarPresetModel, CustomError> {
    let setting_coll = get_setting_coll(db).await?;
    let setting = match setting_coll
        .find_one(doc! { "username": username, "preset": preset }, None)
        .await
        .map_err(map_internal_err)?
    {
        Some(doc) => doc,
        None => return Err(CustomError::SettingsNotFound),
    };

    Ok(setting)
}

pub async fn fetch_fuzzy_rules(
    db: &web::Data<Client>,
    username: &String,
    preset: &String,
) -> Result<Vec<FuzzyRuleModel>, CustomError> {
    let db_client = (*db).database(DB_NAME);
    let rules_coll = db_client.collection::<FuzzyRuleModel>("fuzzy-rules");
    let fuzzy_rules = rules_coll
        .find(
            doc! { "username": username, "preset": preset, "valid": true },
            None,
        )
        .await
        .map_err(map_internal_err)?
        .try_collect::<Vec<_>>()
        .await
        .map_err(map_internal_err)?;

    Ok(fuzzy_rules)
}

pub async fn get_rules_coll(
    db: &web::Data<Client>,
) -> Result<Collection<FuzzyRuleModelWithOutId>, CustomError> {
    let db_client = (*db).database(DB_NAME);
    let rules_coll = db_client.collection::<FuzzyRuleModelWithOutId>("fuzzy-rules");
    let opts = IndexOptions::builder().unique(true).build();
    let index = IndexModel::builder()
        .keys(doc! { "input": 1, "output": 1, "username": 1, "preset": 1})
        .options(opts)
        .build();
    rules_coll
        .create_index(index, None)
        .await
        .map_err(map_internal_err)?;
    Ok(rules_coll)
}

async fn get_fuzzy_rules(
    db: &web::Data<Client>,
    preset: &String,
    username: &String,
) -> Result<Vec<FuzzyRuleDTO>, CustomError> {
    let db_client = (*db).database(DB_NAME);
    let collection = db_client.collection::<FuzzyRuleModel>("fuzzy-rules");

    let fuzzyRules = collection
        .find(doc! { "username": username, "preset": preset }, None)
        .await
        .map_err(map_internal_err)?
        .try_collect::<Vec<_>>()
        .await
        .map_err(map_internal_err)?;

    Ok(fuzzyRules
        .into_iter()
        .map(|item| FuzzyRuleDTO {
            id: item._id.to_string(),
            input: item.input,
            output: item.output,
            valid: item.valid,
        })
        .collect())
}

async fn get_linguistic_variables(
    db: &web::Data<Client>,
    preset: &String,
    username: &String,
) -> Result<BTreeMap<String, LinguisticVarDTO>, CustomError> {
    let collection = get_setting_coll(db).await?;
    let doc_opt = collection
        .find_one(doc! { "username": username, "preset": preset }, None)
        .await
        .map_err(map_internal_err)?;
    if let Some(doc) = doc_opt {
        return Ok(doc
            .vars
            .iter()
            .map(|(name, var_info)| (name.to_string(), var_info.to_dto()))
            .collect());
    }
    Err(CustomError::SettingsNotFound) // TODO Change error message
}

pub async fn get_setting(
    db: web::Data<Client>,
    preset: &String,
    username: &String,
) -> Result<SettingsDTO, CustomError> {
    Ok(SettingsDTO {
        linguisticVariables: get_linguistic_variables(&db, preset, username).await?,
        fuzzyRules: get_fuzzy_rules(&db, preset, username).await?,
    })
}

pub async fn update_linguistic_vars(
    db: web::Data<Client>,
    linguisticVariables: web::Json<LinguisticVarsModel>,
    preset: &String,
    username: &String,
) -> Result<String, CustomError> {
    let setting_coll = get_setting_coll(&db).await?;
    let setting = match setting_coll
        .find_one(doc! { "username": username, "preset": preset }, None)
        .await
        .map_err(map_internal_err)?
    {
        Some(doc) => doc,
        None => return Err(CustomError::SettingsNotFound),
    };

    let mut rules_filter = vec![];
    for (k, var1) in setting.vars {
        if let Some(var2) = linguisticVariables.get(&k) {
            if var1.kind != var2.kind {
                return Err(CustomError::InternalError(
                    "The kind of the linguistic variable cannot be changed".to_string(),
                ));
            }

            for x in var1.shapes.keys().filter(|x| !var2.shapes.contains_key(*x)) {
                rules_filter.push(doc! {
                    format!("{}.{}", var1.kind.to_string(), k): x, "preset": preset
                });
            }
        }
    }
    if !rules_filter.is_empty() {
        let db_client = (*db).database(DB_NAME);
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
                if name.is_empty() {
                    return Err(CustomError::InternalError(
                        "Linguistic variable name is empty".to_string(),
                    ));
                }
                let lv_name = format!("vars.{}", name);

                var.shapes
                    .iter()
                    .try_for_each(|shape| {
                        if shape.0.is_empty() {
                            return Err(CustomError::InternalError(
                                "Shape name is empty".to_string(),
                            ));
                        }
                        Ok(())
                    })
                    .map_err(map_internal_err)?;

                let lv_info = to_bson(var).map_err(map_internal_err)?;
                Ok((lv_name, lv_info))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?,
    )
    .map_err(map_internal_err)?;

    setting_coll
        .update_one(
            doc! { "username": username, "preset": preset },
            doc! { "$set": data },
            None,
        )
        .await
        .map_err(map_internal_err)?;

    Ok("Linguisitc variables have been updated successfully".to_string())
}

pub async fn delete_linguistic_var(
    db: web::Data<Client>,
    preset: &String,
    name: String,
    username: String,
) -> Result<String, CustomError> {
    let collection = get_setting_coll(&db).await?;
    let target = doc! {
        format!("vars.{}", name): ""
    };
    let result = collection
        .update_one(
            doc! { "username": username, "preset": preset },
            doc! { "$unset": target },
            None,
        )
        .await
        .map_err(map_internal_err)?;

    if result.modified_count == 0 {
        return Err(CustomError::LinguisticVarNotFound(name.to_string()));
    }

    Ok("Linguisitc variables have been deleted successfully".to_string())
}

pub async fn add_fuzzy_rules(
    db: web::Data<Client>,
    rule: web::Json<NewFuzzyRule>,
    preset: &String,
    username: String,
) -> Result<String, CustomError> {
    let setting_coll = get_setting_coll(&db).await?;

    let doc_opt = setting_coll
        .find_one(
            doc! { "username": username.clone(), "preset": preset },
            None,
        )
        .await
        .map_err(|_| CustomError::SettingsNotFound)?;

    match doc_opt {
        Some(doc) => {
            for (k, v) in rule.input.iter().chain(rule.output.iter()) {
                let Some(var) = doc.vars.get(k) else {
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
        username,
        valid: true,
        preset: preset.clone(),
    };

    let rules_coll = get_rules_coll(&db).await?;
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
    let collection = get_rules_coll(&db).await?;
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

pub async fn get_setting_coll(
    db: &web::Data<Client>,
) -> Result<Collection<LinguisticVarPresetModel>, CustomError> {
    let db_client = (*db).database(DB_NAME);
    let coll = db_client.collection::<LinguisticVarPresetModel>("linguistic-vars");
    let opts = IndexOptions::builder().unique(true).build();
    let index = IndexModel::builder()
        .keys(doc! { "username": 1, "preset": 1})
        .options(opts)
        .build();
    coll.create_index(index, None)
        .await
        .map_err(map_internal_err)?;
    Ok(coll)
}

pub async fn add_preset(
    db: &web::Data<Client>,
    preset: String,
    username: String,
) -> Result<String, CustomError> {
    let linguistic_vars_coll = get_setting_coll(db).await?;
    let data = LinguisticVarPresetModel {
        username,
        preset,
        vars: BTreeMap::new(),
    };
    linguistic_vars_coll
        .insert_one(data, None)
        .await
        .map_err(map_internal_err)?;
    Ok("Added new preset successfully".to_string())
}

async fn delete_preset_transaction(
    session: &mut mongodb::ClientSession,
    preset: String,
    username: String,
) -> Result<(), mongodb::error::Error> {
    let linguistic_vars_coll = session
        .client()
        .database(DB_NAME)
        .collection::<LinguisticVarPresetModel>("linguistic-vars");
    linguistic_vars_coll
        .delete_one(
            doc! { "username": username.clone(), "preset": preset.clone() },
            None,
        )
        .await?;

    let rules_coll = session
        .client()
        .database(DB_NAME)
        .collection::<FuzzyRuleModel>("fuzzy-rules");
    rules_coll
        .delete_many(
            doc! { "username": username, "preset": preset.clone() },
            None,
        )
        .await?;
    Ok(())
}

pub async fn delete_preset(
    db: &web::Data<Client>,
    preset: String,
    username: String,
) -> Result<String, CustomError> {
    let mut session = db.start_session(None).await.map_err(map_internal_err)?;
    session
        .with_transaction(
            (),
            |session, _| {
                delete_preset_transaction(session, preset.clone(), username.clone()).boxed()
            },
            None,
        )
        .await
        .map_err(map_internal_err)?;

    Ok(format!("Deleted preset \"{}\" successfully", preset))
}

pub async fn get_presets(
    db: &web::Data<Client>,
    username: String,
) -> Result<Vec<(String, bool)>, CustomError> {
    let linguistic_vars_coll = get_setting_coll(db).await?;
    let docs = linguistic_vars_coll
        .find(doc! { "username": username.clone() }, None)
        .await
        .map_err(map_internal_err)?
        .try_collect::<Vec<_>>()
        .await
        .map_err(map_internal_err)?;
    let result = futures::future::join_all(docs.into_iter().map(|item| async {
        let preset = item.preset.clone();
        let fuzzy_rules = fetch_fuzzy_rules(db, &username, &preset).await.unwrap();
        let fuzzy_engine = create_fuzzy_engine(&item, &fuzzy_rules);

        (item.preset, fuzzy_engine.is_valid())
    })).await;
    Ok(result)
}
