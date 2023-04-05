use mongodb::bson;
use serde::{Serialize, Deserialize};

use crate::Ohlc;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct RsiValue {
    time: bson::DateTime,
    value: f64,
}

fn compute_rsi(avg_gain: f64, avg_loss: f64) -> f64 {
    100f64 - 100f64 / (1f64 + (avg_gain / avg_loss))
}

fn compute_gainloss(data: &Vec<Ohlc>) -> Vec<(bool, f64)> {
    data.iter()
        .zip(data.iter().skip(1))
        .map(|(prev, curr)| {
            (
                curr.close - prev.close > 0.0,
                (curr.close - prev.close).abs(),
            )
        })
        .collect()
}

fn avgs_gainloss(gain_loss: &Vec<(bool, f64)>, n: usize) -> (Vec<f64>, Vec<f64>) {
    // first n sessions gains and losses
    let mut avg_gain = vec![
        gain_loss
            .iter()
            .take(n)
            .filter(|(is_gain, _)| *is_gain)
            .map(|(_, change)| change)
            .sum::<f64>()
            / n as f64,
    ];

    let mut avg_loss = vec![
        gain_loss
            .iter()
            .take(n)
            .filter(|(is_gain, _)| !is_gain)
            .map(|(_, change)| change)
            .sum::<f64>()
            / n as f64,
    ];

    for (is_gain, v) in gain_loss.iter().skip(n) {
        if *is_gain {
            avg_gain.push((avg_gain.last().unwrap() * (n - 1) as f64 + v) / n as f64);
            avg_loss.push((avg_loss.last().unwrap() * (n - 1) as f64 + 0.0) / n as f64);
        } else {
            avg_gain.push((avg_gain.last().unwrap() * (n - 1) as f64 + 0.0) / n as f64);
            avg_loss.push((avg_loss.last().unwrap() * (n - 1) as f64 + v) / n as f64);
        }
    }

    (avg_gain, avg_loss)
}

// https://www.omnicalculator.com/finance/rsi
// using closing price
pub fn rsi(data: &Vec<Ohlc>, n: usize) -> Vec<RsiValue> {
    let gain_loss = compute_gainloss(data);
    let (avg_gain, avg_loss) = avgs_gainloss(&gain_loss, n);

    data.iter()
        .skip(n + 1)
        .zip(avg_gain.iter().zip(avg_loss.iter()))
        .map(|(curr, (avg_g, avg_l))| RsiValue {time: curr.time, value: compute_rsi(*avg_g, *avg_l)})
        .collect()
}

#[cfg(test)]
mod test {
    use float_cmp::approx_eq;

    use super::*;

    #[test]
    fn test_compute_rsi() {
        assert_eq!(compute_rsi(0.0, 1.0), 0.0);
        assert_eq!(compute_rsi(1.0, 1.0), 50.0);
        assert!(approx_eq!(
            f64,
            compute_rsi(1.34, 0.83),
            61.78,
            epsilon = 0.1
        ));
    }
    
    fn ohlc_with(close: f64) -> Ohlc {
        Ohlc {
            ticker: "".to_string(),
            time: bson::DateTime::now(),
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close,
            volume: 0,
        }
    }

    fn test_set() -> Vec<Ohlc> {
        vec![
            ohlc_with(140.06),
            ohlc_with(144.28),
            ohlc_with(147.64),
            ohlc_with(150.6),
            ohlc_with(151.92),
            ohlc_with(154.79),
            ohlc_with(152.61),
            ohlc_with(150.26),
            ohlc_with(150.47),
            ohlc_with(146.68),
            ohlc_with(145.14),
            ohlc_with(148.10),
            ohlc_with(148.82),
            ohlc_with(148.91),
            ohlc_with(147.21),
            ohlc_with(142.84),
            ohlc_with(145.48),
        ]
    }

    #[test]
    fn test_rsi() {
        // manual test for now, need to write some automated test after
        let dt = test_set();
        let gain_loss = compute_gainloss(&dt);
        let (avg_gain, avg_loss) = avgs_gainloss(&gain_loss, 14);
        println!("{:?}", gain_loss);
        println!("{:?}", avg_gain);
        println!("{:?}", avg_loss);
    }
}
