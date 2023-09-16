use tech_indicators::Ohlc;

use crate::core::fuzzy_cached;

pub enum PosType {
    Long,
    Short,
}

pub struct Position {
    at_price: f64,
    at_time: usize,
    amount: f64,
    pos_type: PosType,
}

/*
impl Position {
    pub fn new(at_price: f64, at_time: u64, amount: f64, pos_type: PosType) -> Position {
        Position {
            at_price,
            at_time,
            amount,
            pos_type,
        }
    }
}

pub struct SimpleFuzzyStrat {
    data: Vec<Ohlc>,
}

impl SimpleFuzzyStrat {
    pub fn new(data: &[Ohlc], captial: f64) -> SimpleFuzzyStrat {
        SimpleFuzzyStrat { data }
    }

    pub fn run(&self, capital: f64) {
        // 0 is long, 1 is short
        let f = fuzzy_cached(&self.data, "ETH/USDT", "1d");

        let mut working_capital = capital;
        let mut pos_list = vec![];

        let amount = 0.1 * capital;
        for (i, (ohlc, signal)) in self.data.iter().zip(f.iter()).enumerate() {
            // long
            if signal[0] >= 40.0 {
                working_capital -= amount;
                pos_list.push(Position::new(ohlc.close, i, amount, PosType::Long))
            }

            // short
            if signal[1] >= 40.0 {
                working_capital -= amount;
                pos_list.push(Position::new(ohlc.close, i, amount, PosType::Short))
            }
        }
    }
}
*/
