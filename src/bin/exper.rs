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

const CRYPTOS: [&str; 3] = ["ETH/USDT", "BTC/USDT", "BNB/USDT"];

const STOCKS: [&str; 6] = [
    "AAPL/USD", "IBM/USD", "JPM/USD", "MSFT/USD", "NKE/USD", "TSLA/USD",
];

const TEST_START: i64 = 1696093200000;
const TEST_END: i64 = 1708771859000;
const CAPITAL: f64 = 3000.0;
const INTERVAL: Interval = Interval::OneDay;

enum FuzzyKind {
    Normal,
    WithCapitalManagement,
    PSO,
    PSOWithCapitalManagement,
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

async fn buy_and_hold(db: &Data<Client>) -> (HashMap<String, f64>, BTreeMap<i64, f64>) {
    let mut pnls_list = vec![];
    let mut net_profits = HashMap::new();
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

async fn fuzzy(
    db: &Data<Client>,
    user: &User,
    kind: FuzzyKind,
) -> (HashMap<String, f64>, BTreeMap<i64, f64>) {
    use FuzzyKind::*;

    let preset = match kind {
        Normal | WithCapitalManagement => "aaa",
        PSO | PSOWithCapitalManagement => "aaa", // TODO
    }
    .to_string();

    let capital_management = match kind {
        Normal | PSO => CapitalManagement::Normal {
            entry_size_percent: 10.0,
            min_entry_size: 30.0,
        },
        WithCapitalManagement | PSOWithCapitalManagement => CapitalManagement::LiquidF {
            min_entry_size: 30.0,
        },
    };

    let request = BacktestRequest {
        capital: CAPITAL / CRYPTOS.len() as f64,
        start_time: TEST_START,
        end_time: TEST_END,
        signal_conditions: vec![SignalCondition {
            signal_index: 0,
            signal_threshold: 30.0,
            signal_do_command: PosType::Long,
            take_profit_when: 20.0,
            stop_loss_when: 10.0,
            capital_management,
        }],
    };

    let mut positions_list = vec![];
    let mut net_profits = HashMap::new();

    for symbol in CRYPTOS {
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

#[tokio::main]
async fn main() {
    let uri = dotenvy::var("MONGO_DB_URI").unwrap();
    let db = Data::new(
        Client::with_uri_str(uri)
            .await
            .expect("Failed to connect to Mongodb"),
    );

    let user = auth_user(&db, "tanat").await.unwrap();
    let (_, g1) = buy_and_hold(&db).await;

    let (_, g2) = fuzzy(&db, &user, FuzzyKind::Normal).await;
    let (_, g3) = fuzzy(&db, &user, FuzzyKind::WithCapitalManagement).await;

    let mut data = g1
        .iter()
        .map(|(k, v)| (*k, vec![Some(*v)]))
        .collect::<BTreeMap<_, _>>();

    for (k, _) in g1 {
        let v1 = g2.get(&k).map(|x| *x);
        let v2 = g3.get(&k).map(|x| *x);
        data.entry(k).and_modify(|ls| ls.append(&mut vec![v1, v2]));
    }

    let mut writer = csv::Writer::from_path("data.csv").unwrap();
    writer.write_record(&["time", "bh", "fuzzy", "fuzzy c"]).unwrap();

    for (k, v) in data {
        writer.serialize((k, v[0], v[1], v[2])).unwrap();
    }

    writer.flush().unwrap();
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_something() {}
}
