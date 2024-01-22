use actix_web::web;
use mongodb::Client;
use serde::{Deserialize, Serialize};
use tech_indicators::fuzzy::fuzzy_indicator;

use self::swarm::{gen_rho, Individual, IndividualGroup};

use super::{
    backtest::{
        backtest, generate_report, get_valid_data, save_backtest_report, BacktestMetadata,
        BacktestResultWithRequest, SignalCondition,
    },
    error::CustomError,
    fetch_symbol,
    fuzzy::{create_fuzzy_engine, create_input},
    settings::{fetch_fuzzy_rules, fetch_setting, LinguisticVarsModel},
    users::User,
    Interval,
};

pub mod swarm;

#[derive(Deserialize, Serialize, Clone)]
pub struct Strategy {
    capital: f64,
    train_start_time: i64,
    train_end_time: i64,
    validation_start_time: i64,
    validation_end_time: i64,
    signal_conditions: Vec<SignalCondition>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct TrainResult {
    train_progress: Vec<(usize, usize, f64)>,
    validation_f: f64,
}

fn to_particle(linguistic_vars: &LinguisticVarsModel) -> Vec<f64> {
    let particle_pos = linguistic_vars
        .iter()
        .flat_map(|(_, l)| {
            l.shapes
                .iter()
                .flat_map(|(_, s)| s.parameters.values().copied())
        })
        .collect::<Vec<_>>();

    particle_pos
}

fn create_particle_groups(
    start_pos: Vec<f64>,
    group: usize,
    group_size: usize,
) -> Vec<IndividualGroup> {
    let particle = Individual::new(start_pos);
    let particles = vec![particle; group_size + 1];

    (0..group)
        .map(|_| IndividualGroup {
            particles: particles[1..].into(),
            lbest_f: f64::MAX,
            lbest_pos: particles[0].position.clone(),
        })
        .collect()
}

fn from_particle(
    linguistic_vars: &LinguisticVarsModel,
    particle_pos: &Vec<f64>,
) -> LinguisticVarsModel {
    let mut copied = linguistic_vars.clone();

    let mut i = 0;
    for (_, l) in copied.iter_mut() {
        for (_, s) in l.shapes.iter_mut() {
            for (_, v) in s.parameters.iter_mut() {
                *v = particle_pos[i];
                i += 1; // this is kind of bad
            }
        }
    }

    copied
}

/// Using PSO to optimize linguistic variables
pub async fn linguistic_vars_optimization(
    db: &web::Data<Client>,
    symbol: &String,
    interval: &Interval,
    preset: &String,
    user: &User,
    strat: Strategy,
) -> Result<TrainResult, CustomError> {
    let data = fetch_symbol(db, symbol, &Some(interval.clone())).await;

    let username = &user.username;
    let mut setting = fetch_setting(db, username, preset).await?;
    let fuzzy_rules = fetch_fuzzy_rules(db, username, preset).await?;

    let mut fuzzy_engine;
    let fuzzy_inputs = create_input(&setting, &data, user);

    let start_pos = to_particle(&setting.vars);
    let mut groups = create_particle_groups(start_pos, 3, 5);

    let valid_ohlc = get_valid_data(data.0.clone(), strat.train_start_time, strat.train_end_time);

    let mut train_progress = vec![];
    let max_epoch = 10;
    for i in 0..max_epoch {
        for (k, g) in groups.iter_mut().enumerate() {
            for x in g.particles.iter_mut() {
                println!("epoch: {}, group: {}, particle: {:?}", i, k, x.position);
                setting.vars = from_particle(&setting.vars, &x.position);
                fuzzy_engine = create_fuzzy_engine(&setting, &fuzzy_rules);
                let fuzzy_output = get_valid_data(
                    fuzzy_indicator(&fuzzy_engine, fuzzy_inputs.clone()),
                    strat.train_start_time,
                    strat.train_end_time,
                );

                let positions = backtest(
                    &valid_ohlc,
                    &fuzzy_output,
                    &strat.signal_conditions,
                    strat.capital,
                );
                let r = generate_report(&positions, strat.capital, strat.train_start_time);

                let f = -1.0 * (r.total.pnl_percent - r.maximum_drawdown.percent);

                if f < x.f {
                    x.f = f;
                    x.best_pos = x.position.clone();
                }
                if f < g.lbest_f {
                    g.lbest_f = f;
                    g.lbest_pos = x.position.clone();
                }
                x.update_speed(&g.lbest_pos, gen_rho(1.0), gen_rho(1.5));
                x.change_pos();

                train_progress.push((i, k, f));
            }
        }
    }

    let best_group = groups
        .iter()
        .reduce(|best, x| if best.lbest_f < x.lbest_f { best } else { x })
        .unwrap();

    let gbest = best_group
        .particles
        .iter()
        .reduce(|best, ind| if best.f < ind.f { best } else { ind })
        .unwrap();

    let valid_ohlc = get_valid_data(
        data.0.clone(),
        strat.train_start_time,
        strat.validation_end_time,
    );
    setting.vars = from_particle(&setting.vars, &gbest.position);
    fuzzy_engine = create_fuzzy_engine(&setting, &fuzzy_rules);
    let fuzzy_output = get_valid_data(
        fuzzy_indicator(&fuzzy_engine, fuzzy_inputs.clone()),
        strat.train_start_time,
        strat.validation_end_time,
    );
    let positions = backtest(
        &valid_ohlc,
        &fuzzy_output,
        &strat.signal_conditions,
        strat.capital,
    );
    let validation_result = generate_report(&positions, strat.capital, strat.train_start_time);
    let validation_f =
        1.0 / (validation_result.total.pnl_percent - validation_result.maximum_drawdown.percent);

    save_backtest_report(
        db,
        username,
        symbol,
        interval,
        preset,
        BacktestResultWithRequest {
            result: validation_result,
            metadata: BacktestMetadata::PsoBackTest(strat),
        },
    )
    .await?;

    Ok(TrainResult {
        train_progress,
        validation_f,
    })
}
