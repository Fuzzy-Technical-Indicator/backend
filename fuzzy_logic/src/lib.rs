pub mod pure;
pub mod set;


pub fn arange(start: f64, stop: f64, interval: f64) -> Vec<f64> {
    if stop < start {
        panic!("end can not be less than start");
    } else if interval <= 0f64 {
        panic!("interval must be > 0");
    }
    let r = 1.0 / interval;

    (0..)
        .map(|i| start + i as f64 * interval)
        .map(|x| (x * r).round() / r)
        .take_while(|&x| x <= stop)
        .collect::<Vec<f64>>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arange() {
        assert_eq!(arange(0f64, 5f64, 1f64), vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0]);
        assert_eq!(
            arange(0f64, 0.5f64, 0.1f64),
            vec![0.0, 0.1, 0.2, 0.3, 0.4, 0.5]
        );
    }
}