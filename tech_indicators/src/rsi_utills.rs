use crate::{embed_datetime, rma, DTValue, Ohlc};

fn nan_iter(n: usize) -> impl Iterator<Item = f64> {
    std::iter::repeat(f64::NAN).take(n)
}

fn compute_gainloss(data: &[Ohlc]) -> (Vec<Option<f64>>, Vec<Option<f64>>) {
    std::iter::once((None, None))
        .chain(data.iter().zip(data.iter().skip(1)).map(|(prev, curr)| {
            (
                Some((curr.close - prev.close).max(0f64)),
                Some((prev.close - curr.close).max(0f64)),
            )
        }))
        .unzip()
}

fn avg_first_n(data: &[f64], n: usize) -> f64 {
    data.iter().take(n).sum::<f64>() / n as f64
}

fn smooth_fn(last_avg: f64, curr: f64, n: usize) -> f64 {
    (last_avg * (n - 1) as f64 + curr) / n as f64
}

pub fn smooth_rs(gain: &[f64], loss: &[f64], n: usize) -> Vec<f64> {
    // first n sessions gains and losses
    let mut avg_gain = nan_iter(n - 1)
        .chain(vec![avg_first_n(gain, n)])
        .collect::<Vec<f64>>();
    let mut avg_loss = nan_iter(n - 1)
        .chain(vec![avg_first_n(loss, n)])
        .collect::<Vec<f64>>();

    for (g, l) in gain.iter().skip(n).zip(loss.iter().skip(n)) {
        avg_gain.push(smooth_fn(*avg_gain.last().unwrap(), *g, n));
        avg_loss.push(smooth_fn(*avg_loss.last().unwrap(), *l, n));
    }
    avg_gain
        .iter()
        .zip(avg_loss.iter())
        .map(|(g, l)| g / l)
        .collect()
}

pub fn rma_rs(gain: &[Option<f64>], loss: &[Option<f64>], n: usize) -> Vec<Option<f64>> {
    rma(gain, n)
        .iter()
        .zip(rma(loss, n).iter())
        .map(|(g, l)| {
            if let (Some(g), Some(l)) = (g, l) {
                Some(g / l)
            } else {
                None
            }
        })
        .collect()
}

type RsF = fn(&[Option<f64>], &[Option<f64>], usize) -> Vec<Option<f64>>;

pub fn compute_rsi_vec(data: &[Ohlc], n: usize, rs_fn: RsF) -> Vec<DTValue<f64>> {
    let (gain, loss) = compute_gainloss(data);
    let rs_vec = rs_fn(&gain, &loss, n);
    let rsi = rs_vec.iter().map(|rs_o| {
        if let Some(rs) = rs_o {
            100.0 - 100.0 / (1.0 + rs)
        } else {
            100.0 - 100.0 / (1.0 + f64::NAN)
        }
    });

    embed_datetime(rsi, data)
}

#[cfg(test)]
mod test {
    use super::*;

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

        /*
        let dt = test_set();
        let (gain, loss) = compute_gainloss(&dt);
        let rs = smooth_rs(&gain, &loss, 14);
        println!("{:?}", gain);
        println!("{:?}", loss);
        println!("{:?}", rs);
        */
    }
}
