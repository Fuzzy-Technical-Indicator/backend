use actix_web::web::Data;
use backend::core::{
    backtest::{CapitalManagement, PosType, SignalCondition},
    optimization::{linvar_multistrat, Strategy},
    users::{auth_user, User},
    Interval,
};
use env_logger::Env;
use mongodb::Client;

const TEST_START: i64 = 1696093200000;
const TEST_END: i64 = 1709830800000;
const CAPITAL: f64 = 3000.0;
const INTERVAL: Interval = Interval::OneDay;

const CRYPTOS: &'static [&str] = &["ETH/USDT", "BTC/USDT", "BNB/USDT"];

const STOCKS: &'static [&str] = &[
    "AAPL/USD", "IBM/USD", "JPM/USD", "MSFT/USD", "NKE/USD", "TSLA/USD",
];

enum Asset {
    Crypto,
    Stock,
}

struct Runner {
    db: Data<Client>,
    user: User,
}
impl Runner {
    async fn train(
        &self,
        capital_management: &CapitalManagement,
        preset: &String,
        threshold: f64,
        asset: Asset,
    ) {
        let (asset_list, clen, (take_profit, stop_loss)) = match asset {
            Asset::Crypto => (&CRYPTOS, CRYPTOS.len(), (20.0, 10.0)),
            Asset::Stock => (&STOCKS, STOCKS.len(), (10.0, 5.0)),
        };

        let base_strat = Strategy {
            limit: 10,
            particle_groups: 5,
            particle_amount: 10,
            capital: CAPITAL / clen as f64,
            signal_conditions: vec![
                SignalCondition {
                    signal_index: 0,
                    signal_threshold: threshold, // 25 for rsi-bb, 30 for aroon-macd
                    signal_do_command: PosType::Long,
                    take_profit_when: take_profit,
                    stop_loss_when: stop_loss,
                    capital_management: capital_management.clone(),
                },
                SignalCondition {
                    signal_index: 1,
                    signal_threshold: threshold,
                    signal_do_command: PosType::Short,
                    take_profit_when: take_profit,
                    stop_loss_when: stop_loss,
                    capital_management: capital_management.clone(),
                },
            ],
            validation_period: 6, // in mounth
            test_start: TEST_START,
            test_end: TEST_END,
        };

        let mut strat1 = base_strat.clone();
        strat1.particle_groups = 10;
        let mut strat2 = base_strat.clone();
        strat2.particle_groups = 15;

        for symbol in *asset_list {
            let _ = linvar_multistrat(
                &self.db,
                &symbol.to_string(),
                &INTERVAL,
                preset,
                &self.user,
                &[base_strat.clone(), strat1.clone(), strat2.clone()],
            )
            .await;
        }
    }
}

#[tokio::main]
async fn main() {
    env_logger::init_from_env(Env::default().default_filter_or("info"));

    let uri = dotenvy::var("MONGO_DB_URI").unwrap();
    let db = Data::new(
        Client::with_uri_str(uri)
            .await
            .expect("Failed to connect to Mongodb"),
    );

    let user = auth_user(&db, "r").await.unwrap();

    let normal = CapitalManagement::Normal {
        entry_size_percent: 5.0,
        min_entry_size: 30.0,
    };
    let liquid_f = CapitalManagement::LiquidF {
        min_entry_size: 30.0,
    };

    let runner = Runner { db, user };

    // AROON-MACD
    runner
        .train(&normal, &"aroon-macd".to_string(), 30.0, Asset::Crypto)
        .await;
    runner
        .train(&liquid_f, &"aroon-macd".to_string(), 30.0, Asset::Crypto)
        .await;

    runner
        .train(&normal, &"aroon-macd".to_string(), 30.0, Asset::Stock)
        .await;
    runner
        .train(&liquid_f, &"aroon-macd".to_string(), 30.0, Asset::Stock)
        .await;

    // RSI-BB
    runner
        .train(&normal, &"rsi-bb".to_string(), 25.0, Asset::Crypto)
        .await;
    runner
        .train(&liquid_f, &"rsi-bb".to_string(), 25.0, Asset::Crypto)
        .await;

    runner
        .train(&normal, &"rsi-bb".to_string(), 25.0, Asset::Stock)
        .await;
    runner
        .train(&liquid_f, &"rsi-bb".to_string(), 25.0, Asset::Stock)
        .await;
}
