use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::Write,
};

use actix_web::web::Data;
use backend::core::{
    backtest::{
        self, create_backtest_report, BacktestRequest, CapitalManagement, PosType, Position,
        SignalCondition,
    },
    users::{auth_user, User},
    Interval,
};
use mongodb::Client;

//const CRYPTOS: &'static [&str] = &["ETH/USDT", "BTC/USDT", "BNB/USDT"];

const STOCKS: &'static [&str] = &[
    "AAPL/USD", "IBM/USD", "JPM/USD", "MSFT/USD", "NKE/USD", "TSLA/USD",
];


const CRYPTOS: &'static [&str] = &["ETH/USDT"];

//const TEST_START: i64 = 1696093200000;
//const TEST_END: i64 = 1709830800000;

//const TEST_START: i64 = 1636909200000;
//const TEST_END: i64 = 1650049200000;

const TEST_START: i64 = 1657904400000;
const TEST_END: i64 = 1672851600000;

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
) -> (BTreeMap<String, String>, BTreeMap<i64, f64>) {
    let mut pnls_list = vec![];
    let mut net_profits = BTreeMap::new();

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

                net_profits.insert(symbol.to_string(), format!("{:.2}", net_profit));
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

                net_profits.insert(symbol.to_string(), format!("{:.2}", net_profit));
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

async fn classical(
    db: &Data<Client>,
    asset: &Asset,
) -> (BTreeMap<String, String>, BTreeMap<i64, f64>) {
    let min_entry_size = 30.0;
    let entry_size_percent = 5.0;
    let (asset_list, c_len, (take_profit, stop_loss)) = match asset {
        Asset::Crypto => (&CRYPTOS, CRYPTOS.len(), (20.0, 10.0)),
        Asset::Stock => (&STOCKS, STOCKS.len(), (10.0, 5.0)),
    };

    let mut positions_list = vec![];
    let mut net_profits = BTreeMap::new();

    for symbol in *asset_list {
        /*
        let (net_profit, positions) = backtest::classical_aroon_macd(
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
        */
        let (net_profit, positions) = backtest::classical_rsi_bb(
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
        net_profits.insert(symbol.to_string(), format!("{:.2}", net_profit));
    }

    let result = calc_cumlative_return(transform_positions(positions_list), CAPITAL, TEST_START);
    (net_profits, result)
}

async fn fuzzy(
    db: &Data<Client>,
    user: &User,
    kind: FuzzyKind,
    asset: &Asset,
) -> (BTreeMap<String, String>, BTreeMap<i64, f64>) {
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
                signal_threshold: 25.0, // 25 for rsi-bb, 30 for aroon-macd
                signal_do_command: PosType::Long,
                take_profit_when: take_profit,
                stop_loss_when: stop_loss,
                capital_management: capital_management.clone(),
            },
            SignalCondition {
                signal_index: 1,
                signal_threshold: 25.0,
                signal_do_command: PosType::Short,
                take_profit_when: take_profit,
                stop_loss_when: stop_loss,
                capital_management,
            },
        ],
    };

    let mut positions_list = vec![];
    let mut net_profits = BTreeMap::new();

    let preset_map = HashMap::from([
        ("ETH/USDT", "aroon-macd-ETHUSDT-normal-pso-1710615885"),
        ("BTC/USDT", "aroon-macd-BTCUSDT-normal-pso-1710616861"),
        ("BNB/USDT", "aroon-macd-BNBUSDT-normal-pso-1710617808"),
        ("AAPL/USD", "aroon-macd-AAPLUSD-normal-pso-1710622641"),
        ("IBM/USD", "aroon-macd-IBMUSD-normal-pso-1710622867"),
        ("JPM/USD", "aroon-macd-JPMUSD-normal-pso-1710623109"),
        ("MSFT/USD", "aroon-macd-MSFTUSD-normal-pso-1710623361"),
        ("NKE/USD", "aroon-macd-NKEUSD-normal-pso-1710623590"),
        ("TSLA/USD", "aroon-macd-TSLAUSD-normal-pso-1710623845"),
    ]);

    let with_cap_preset_map = HashMap::from([
        ("ETH/USDT", "aroon-macd-ETHUSDT-liquidf-pso-1710619473"),
        ("BTC/USDT", "aroon-macd-BTCUSDT-liquidf-pso-1710620823"),
        ("BNB/USDT", "aroon-macd-BNBUSDT-liquidf-pso-1710622383"),
        ("AAPL/USD", "aroon-macd-AAPLUSD-liquidf-pso-1710624107"),
        ("IBM/USD", "aroon-macd-IBMUSD-liquidf-pso-1710624332"),
        ("JPM/USD", "aroon-macd-JPMUSD-liquidf-pso-1710624577"),
        ("MSFT/USD", "aroon-macd-MSFTUSD-liquidf-pso-1710624825"),
        ("NKE/USD", "aroon-macd-NKEUSD-liquidf-pso-1710625057"),
        ("TSLA/USD", "aroon-macd-TSLAUSD-liquidf-pso-1710625338"),
    ]);

    /*
    let preset_map = HashMap::from([
        ("ETH/USDT", "rsi-bb-ETHUSDT-normal-pso-1710626789"),
        ("BTC/USDT", "rsi-bb-BTCUSDT-normal-pso-1710628229"),
        ("BNB/USDT", "rsi-bb-BNBUSDT-normal-pso-1710629636"),
        ("AAPL/USD", "rsi-bb-AAPLUSD-normal-pso-1710634919"),
        ("IBM/USD", "rsi-bb-IBMUSD-normal-pso-1710635251"),
        ("JPM/USD", "rsi-bb-JPMUSD-normal-pso-1710635611"),
        ("MSFT/USD", "rsi-bb-MSFTUSD-normal-pso-1710635978"),
        ("NKE/USD", "rsi-bb-NKEUSD-normal-pso-1710636319"),
        ("TSLA/USD", "rsi-bb-TSLAUSD-normal-pso-1710636697"),
    ]);

    let with_cap_preset_map = HashMap::from([
        ("ETH/USDT", "rsi-bb-ETHUSDT-liquidf-pso-1710631245"),
        ("BTC/USDT", "rsi-bb-BTCUSDT-liquidf-pso-1710632839"),
        ("BNB/USDT", "rsi-bb-BNBUSDT-liquidf-pso-1710634536"),
        ("AAPL/USD", "rsi-bb-AAPLUSD-liquidf-pso-1710637075"),
        ("IBM/USD", "rsi-bb-IBMUSD-liquidf-pso-1710637406"),
        ("JPM/USD", "rsi-bb-JPMUSD-liquidf-pso-1710637765"),
        ("MSFT/USD", "rsi-bb-MSFTUSD-liquidf-pso-1710638132"),
        ("NKE/USD", "rsi-bb-NKEUSD-liquidf-pso-1710638473"),
        ("TSLA/USD", "rsi-bb-TSLAUSD-liquidf-pso-1710638854"),
    ]);
    */

    for symbol in *asset_list {
        let preset = match kind {
            Normal | WithCapitalManagement => "rsi-bb".to_string(),
            PSO => preset_map.get(symbol).unwrap().to_string(),
            PSOWithCapitalManagement => with_cap_preset_map.get(symbol).unwrap().to_string(),
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

        net_profits.insert(symbol.to_string(), format!("{:.2}", result.total.pnl));
        positions_list.push(pos);
    }

    let result = calc_cumlative_return(transform_positions(positions_list), CAPITAL, TEST_START);

    (net_profits, result)
}

async fn do_shit(db: &Data<Client>, user: &User, asset: &Asset) {
    let (c_net, gc) = classical(&db, asset).await;

    let (bh_net, g1) = buy_and_hold(&db, asset).await;
    let (f1_net, g2) = fuzzy(&db, &user, FuzzyKind::Normal, asset).await;
    let (f2_net, g3) = fuzzy(&db, &user, FuzzyKind::WithCapitalManagement, asset).await;
    let (f3_net, g4) = fuzzy(&db, &user, FuzzyKind::PSO, asset).await;
    let (f4_net, g5) = fuzzy(&db, &user, FuzzyKind::PSOWithCapitalManagement, asset).await;

    /*
    let mut net_string = format!("classic: {:?} \n\n", c_net);
    net_string.push_str(&format!("f: {:?} \n\n", f1_net));
    net_string.push_str(&format!("f c: {:?} \n\n", f2_net));
    net_string.push_str(&format!("f pso: {:?} \n\n", f3_net));
    net_string.push_str(&format!("f c pso: {:?} \n\n", f4_net));
    net_string.push_str(&format!("bh: {:?} \n\n", bh_net));

    let mut file = match asset {
        Asset::Crypto => File::create("experiment_graph/net_profits2.txt").unwrap(),
        Asset::Stock => File::create("experiment_graph/net_profits_stock2.txt").unwrap(),
    };
    file.write_all(net_string.as_bytes()).unwrap();
    */

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
        Asset::Crypto => "experiment_graph/data7.csv",
        Asset::Stock => "experiment_graph/data_stock4.csv",
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

    let user = auth_user(&db, "r").await.unwrap();

    do_shit(&db, &user, &Asset::Crypto).await;
    //do_shit(&db, &user, &Asset::Stock).await;
}
