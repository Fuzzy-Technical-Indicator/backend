use std::collections::{BTreeMap, HashMap};

use actix_web::web::Data;
use backend::core::{
    backtest::{
        self, create_backtest_report, BacktestRequest, CapitalManagement, CumalativeReturn,
        PosType, Position, SignalCondition,
    },
    users::{auth_user, User},
    Interval,
};
use mongodb::Client;

const CRYPTOS: &'static [&str] = &["ETH/USDT", "BTC/USDT", "BNB/USDT"];

const STOCKS: &'static [&str] = &[
    "AAPL/USD", "IBM/USD", "JPM/USD", "MSFT/USD", "NKE/USD", "TSLA/USD",
];

const TEST_START: i64 = 1696093200000;
const TEST_END: i64 = 1708771859000;
const CAPITAL: f64 = 3000.0;
const INTERVAL: Interval = Interval::OneHour;

enum FuzzyKind {
    Normal,
    WithCapitalManagement,
    PSO,
    PSOWithCapitalManagement,
}

enum Asset {
    Crypto,
    Stock,
}

fn transform_positions(ls: Vec<Vec<Position>>) -> Vec<(f64, i64)> {
    let mut result = ls
        .into_iter()
        .flatten()
        .filter_map(|pos| pos.realized.map(|r| (r.pnl, r.exit_time)))
        .collect::<Vec<_>>();

    result.sort_by(|a, b| a.1.cmp(&b.1));
    result
}

fn calc_cumlative_return(
    positions: Vec<(f64, i64)>,
    initial_capital: f64,
    start_time: i64,
) -> BTreeMap<i64, f64> {
    let mut cumalative_return = initial_capital;
    let mut g = BTreeMap::from([(start_time / 1000, cumalative_return)]);

    for (pnl, exit_time) in positions {
        cumalative_return += pnl;

        g.entry(exit_time / 1000)
            .and_modify(|v| *v = cumalative_return)
            .or_insert(cumalative_return);
    }

    g
}

async fn buy_and_hold(
    db: &Data<Client>,
    asset: &Asset,
) -> (HashMap<String, f64>, BTreeMap<i64, f64>) {
    let mut pnls_list = vec![];
    let mut net_profits = HashMap::new();

    match asset {
        Asset::Crypto => {
            for symbol in CRYPTOS {
                let (net_profit, pnls) = backtest::buy_and_hold(
                    db,
                    symbol,
                    &INTERVAL,
                    CAPITAL / CRYPTOS.len() as f64,
                    TEST_START,
                    TEST_END,
                )
                .await;

                net_profits.insert(symbol.to_string(), net_profit);
                pnls_list.push(pnls);
            }
        }
        Asset::Stock => {
            for symbol in STOCKS {
                let (net_profit, pnls) = backtest::buy_and_hold(
                    db,
                    symbol,
                    &INTERVAL,
                    CAPITAL / STOCKS.len() as f64,
                    TEST_START,
                    TEST_END,
                )
                .await;

                net_profits.insert(symbol.to_string(), net_profit);
                pnls_list.push(pnls);
            }
        }
    };

    let mut flattend = pnls_list.into_iter().flatten().collect::<Vec<_>>();
    flattend.sort_by(|a, b| a.1.cmp(&b.1));
    let mut g = BTreeMap::from([(TEST_START / 1000, CAPITAL)]);
    for (pnl, exit_time) in flattend {
        g.entry(exit_time / 1000)
            .and_modify(|v| *v += pnl)
            .or_insert(CAPITAL + pnl);
    }
    return (net_profits, g);
}

async fn classical(db: &Data<Client>, asset: &Asset) -> BTreeMap<i64, f64> {
    let min_entry_size = 30.0;
    let entry_size_percent = 5.0;
    let (asset_list, c_len, (take_profit, stop_loss)) = match asset {
        Asset::Crypto => (&CRYPTOS, CRYPTOS.len(), (20.0, 10.0)),
        Asset::Stock => (&STOCKS, STOCKS.len(), (10.0, 5.0)),
    };

    let mut positions_list = vec![];

    for symbol in *asset_list {
        let positions = backtest::classical(
            db,
            symbol,
            &INTERVAL,
            CAPITAL / c_len as f64,
            TEST_START,
            TEST_END,
            take_profit,
            stop_loss,
            min_entry_size,
            entry_size_percent,
        )
        .await;
        positions_list.push(positions);
    }

    let result = calc_cumlative_return(transform_positions(positions_list), CAPITAL, TEST_START);
    result
}

async fn fuzzy(
    db: &Data<Client>,
    user: &User,
    kind: FuzzyKind,
    asset: &Asset,
) -> (HashMap<String, f64>, BTreeMap<i64, f64>) {
    use FuzzyKind::*;

    let capital_management = match kind {
        Normal | PSO => CapitalManagement::Normal {
            entry_size_percent: 5.0,
            min_entry_size: 30.0,
        },
        WithCapitalManagement | PSOWithCapitalManagement => CapitalManagement::LiquidF {
            min_entry_size: 30.0,
        },
    };

    let (asset_list, c_len, (take_profit, stop_loss)) = match asset {
        Asset::Crypto => (&CRYPTOS, CRYPTOS.len(), (20.0, 10.0)),
        Asset::Stock => (&STOCKS, STOCKS.len(), (10.0, 5.0)),
    };

    let request = BacktestRequest {
        capital: CAPITAL / c_len as f64,
        start_time: TEST_START,
        end_time: TEST_END,
        signal_conditions: vec![
            SignalCondition {
                signal_index: 0,
                signal_threshold: 30.0,
                signal_do_command: PosType::Long,
                take_profit_when: take_profit,
                stop_loss_when: stop_loss,
                capital_management: capital_management.clone(),
            },
            SignalCondition {
                signal_index: 1,
                signal_threshold: 30.0,
                signal_do_command: PosType::Long,
                take_profit_when: take_profit,
                stop_loss_when: stop_loss,
                capital_management,
            },
        ],
    };

    let mut positions_list = vec![];
    let mut net_profits = HashMap::new();

    let preset_map = HashMap::from([
        ("ETH/USDT", "great 2-ETH/USDT-pso-1709048641"),
        ("BTC/USDT", "great 2-BTC/USDT-pso-1709048054"),
        ("BNB/USDT", "great 2-BNB/USDT-pso-1709049133"),
        ("AAPL/USD", "great 2-AAPL/USD-pso-1709047732"),
        ("IBM/USD", "great 2-IBM/USD-pso-1709047753"),
        ("JPM/USD", "great 2-JPM/USD-pso-1709047774"),
        ("MSFT/USD", "great 2-MSFT/USD-pso-1709047794"),
        ("NKE/USD", "great 2-NKE/USD-pso-1709047812"),
        ("TSLA/USD", "great 2-TSLA/USD-pso-1709047837"),
    ]);

    for symbol in *asset_list {
        let preset = match kind {
            Normal | WithCapitalManagement => "great 2".to_string(),
            PSO | PSOWithCapitalManagement => preset_map.get(symbol).unwrap().to_string(),
        };

        let (r, pos) = create_backtest_report(
            db.clone(),
            request.clone(),
            user,
            &symbol.to_string(),
            &INTERVAL,
            &preset,
        )
        .await
        .unwrap();

        let result = r.get_backtest_result();

        net_profits.insert(symbol.to_string(), result.total.pnl);
        positions_list.push(pos);
    }

    let result = calc_cumlative_return(transform_positions(positions_list), CAPITAL, TEST_START);

    (net_profits, result)
}

async fn do_shit(db: &Data<Client>, user: &User, asset: &Asset) {
    let gc = classical(&db, asset).await;

    let (_, g1) = buy_and_hold(&db, asset).await;
    let (_, g2) = fuzzy(&db, &user, FuzzyKind::Normal, asset).await;
    let (_, g3) = fuzzy(&db, &user, FuzzyKind::WithCapitalManagement, asset).await;
    let (_, g4) = fuzzy(&db, &user, FuzzyKind::PSO, asset).await;
    let (_, g5) = fuzzy(&db, &user, FuzzyKind::PSOWithCapitalManagement, asset).await;

    let mut data = g1
        .iter()
        .map(|(k, v)| (*k, vec![Some(*v)]))
        .collect::<BTreeMap<_, _>>();

    for (k, _) in g1 {
        let v1 = gc.get(&k).map(|x| *x);
        let v2 = g2.get(&k).map(|x| *x);
        let v3 = g3.get(&k).map(|x| *x);
        let v4 = g4.get(&k).map(|x| *x);
        let v5 = g5.get(&k).map(|x| *x);
        data.entry(k)
            .and_modify(|ls| ls.append(&mut vec![v1, v2, v3, v4, v5]));
    }

    let mut writer = csv::Writer::from_path(match asset {
        Asset::Crypto => "experiment_graph/data.csv",
        Asset::Stock => "experiment_graph/data_stock.csv",
    })
    .unwrap();
    writer
        .write_record(&[
            "time",
            "B&H",
            "Classical",
            "Fuzzy",
            "Fuzzy C",
            "Fuzzy PSO",
            "Fuzzy C PSO",
        ])
        .unwrap();

    for (k, v) in data {
        writer
            .serialize((k, v[0], v[1], v[2], v[3], v[4], v[5]))
            .unwrap();
    }

    writer.flush().unwrap();
}

#[tokio::main]
async fn main() {
    let uri = dotenvy::var("MONGO_DB_URI").unwrap();
    let db = Data::new(
        Client::with_uri_str(uri)
            .await
            .expect("Failed to connect to Mongodb"),
    );

    let user = auth_user(&db, "tanat").await.unwrap();

    do_shit(&db, &user, &Asset::Crypto).await;
    do_shit(&db, &user, &Asset::Stock).await;
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_something() {}
}
