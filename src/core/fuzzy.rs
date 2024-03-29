use actix_web::web;

use fuzzy_logic::FuzzyEngine;
use mongodb::Client;

use std::collections::BTreeMap;
use tech_indicators::{DTValue, Ohlc};

use super::{
    accum_dist_cached, adx_cached, aroon_cached, bb_cached,
    error::CustomError, obv_cached, rsi_cached,
    settings::{
        fetch_fuzzy_rules, fetch_setting, FuzzyRuleModel, LinguisticVarKind,
        LinguisticVarPresetModel,
    },
    stoch_cached, transformed_macd,
    users::User,
};
use rayon::prelude::*;

fn to_rule_params<'a>(
    linguistic_var: &'a [String],
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
    setting: &LinguisticVarPresetModel,
    fuzzy_rules: &[FuzzyRuleModel],
) -> FuzzyEngine {
    let mut fuzzy_engine = FuzzyEngine::new();

    let mut linguistic_var_inputs = vec![];
    let mut linguistic_var_outputs = vec![];

    for (name, var_info) in setting.vars.iter() {
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

pub fn bb_percent(price: f64, v: (f64, f64, f64)) -> f64 {
    let (sma, lower, upper) = v;

    let scale = if price > sma {
        upper - sma
    }
    else {
        sma - lower
    };

    ((price - sma) / scale) * 100.0
}

/// Compares the current value of data to it's value `length` bars ago and return the normalized
/// difference
fn normalized_change(data: Vec<DTValue<f64>>, n: usize) -> Vec<Option<f64>> {
    let main_iter =
        rayon::iter::repeat(None)
            .take(n)
            .chain(
                data.par_iter()
                    .zip(data.par_iter().skip(n))
                    .map(|(prev, curr)| {
                        let prevval = prev.value;
                        let currval = curr.value;

                        match (prevval.is_nan(), currval.is_nan()) {
                            (false, false) => Some((currval - prevval) / prevval),
                            (_, _) => None,
                        }
                    }),
            );

    let (vmax, vmin) = main_iter
        .clone()
        .fold(
            || None::<(f64, f64)>,
            |acc, val| match val {
                Some(v) => match acc {
                    Some((max, min)) => Some((max.max(v), min.min(v))),
                    None => Some((v, v)),
                },
                None => None,
            },
        )
        .reduce(
            || None::<(f64, f64)>,
            |acc, val| match (acc, val) {
                (Some((max, min)), Some((a, b))) => Some((max.max(a), min.min(b))),
                (Some((max, min)), None) => Some((max, min)),
                (None, Some((a, b))) => Some((a, b)),
                (None, None) => None,
            },
        )
        .unwrap();

    main_iter
        .map(|vopt| vopt.map(|x| (x - vmin) / (vmax - vmin) * 200.0 - 100.0))
        .collect::<Vec<_>>()
}

pub fn create_input(
    setting: &LinguisticVarPresetModel,
    data: &(Vec<Ohlc>, String),
    user: &User,
) -> Vec<(i64, Vec<Option<f64>>)> {
    let mut dt = data
        .0
        .iter()
        .map(|x| (x.time.to_chrono().timestamp_millis(), Vec::new()))
        .collect::<Vec<(i64, Vec<Option<f64>>)>>(); // based on time

    for (name, var_info) in setting.vars.iter() {
        if let LinguisticVarKind::Input = var_info.kind {
            match name.as_str() {
                // use the value directly, range [0, 100]
                "rsi" => rsi_cached(data.clone(), user.rsi.length)
                    .iter()
                    .zip(dt.iter_mut())
                    .for_each(|(rsi_v, x)| x.1.push(Some(rsi_v.value))),
                // percent difference from the middle band, range [-200, 200]
                "bb" => bb_cached(data.clone(), user.bb.length, user.bb.stdev)
                    .iter()
                    .zip(dt.iter_mut())
                    .zip(data.0.iter())
                    .for_each(|((bb_v, x), ohlc)| {
                        x.1.push(Some(bb_percent(ohlc.close, bb_v.value)))
                    }),
                // use the value directly, range [0, 100]
                "adx" => adx_cached(data.clone(), user.adx.length)
                    .iter()
                    .zip(dt.iter_mut())
                    .for_each(|(v, x)| x.1.push(Some(v.value))),
                // use the value directly, range [0, inf]
                "obv" => normalized_change(obv_cached(data.clone()), 14)
                    .iter()
                    .zip(dt.iter_mut())
                    .for_each(|(v, x)| x.1.push(*v)),
                // use the value directly, range [0, inf]
                "accumdist" => normalized_change(accum_dist_cached(data.clone()), 14)
                    .iter()
                    .zip(dt.iter_mut())
                    .for_each(|(v, x)| x.1.push(*v)),
                // use macd that has been transformed to be one line that detect the cross-over
                // range [0, 100]
                "macd" => transformed_macd(
                    data.clone(),
                    user.macd.fast,
                    user.macd.slow,
                    user.macd.smooth,
                )
                .iter()
                .map(|v| match v.value.is_nan() {
                    true => None,
                    false => Some(v.value),
                })
                .zip(dt.iter_mut())
                .for_each(|(v, x)| x.1.push(v)),
                // use the value directly, range [0, 100]
                "stoch" => {
                    stoch_cached(data.clone(), user.stoch.k, user.stoch.d, user.stoch.length)
                        .iter()
                        .zip(dt.iter_mut())
                        .for_each(|(v, x)| x.1.push(Some(v.value.0)))
                }
                // use the value directly, range [0, 100]
                "aroonup" => aroon_cached(data.clone(), user.aroon.length)
                    .iter()
                    .zip(dt.iter_mut())
                    .for_each(|(v, x)| x.1.push(Some(v.value.0))),
                // use the value directly, range [0, 100]
                "aroondown" => aroon_cached(data.clone(), user.aroon.length)
                    .iter()
                    .zip(dt.iter_mut())
                    .for_each(|(v, x)| x.1.push(Some(v.value.1))),
                _ => {}
            };
        }
    }
    dt
}

pub async fn get_fuzzy_config(
    db: &web::Data<Client>,
    data: &(Vec<Ohlc>, String),
    preset: &String,
    user: &User,
) -> Result<(FuzzyEngine, Vec<(i64, Vec<Option<f64>>)>), CustomError> {
    let username = &user.username;
    let setting = fetch_setting(db, username, preset).await?;
    let fuzzy_rules = fetch_fuzzy_rules(db, username, preset).await?;
    let fuzzy_engine = create_fuzzy_engine(&setting, &fuzzy_rules);

    if !fuzzy_engine.is_valid() {
        return Err(CustomError::RequireAtleastOneValidRule);
    }

    Ok((
        fuzzy_engine,
        create_input(&setting, data, user),
    ))
}
