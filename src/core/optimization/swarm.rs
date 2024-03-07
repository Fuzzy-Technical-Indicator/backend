/// initial code from https://github.com/RiwEZ/MLPOnRust/blob/main/src/swarm/mod.rs
use rand::{distributions::Uniform, prelude::Distribution};

#[derive(Debug, Clone)]
pub struct Individual {
    pub best_pos: Vec<f64>,
    pub position: Vec<f64>,
    pub f: f64, // evaluation of this individual
    pub speed: Vec<f64>,
}

impl Individual {
    pub fn new(position: Vec<f64>) -> Individual {
        let mut rand = rand::thread_rng();
        let dist = Uniform::from(-1.0..=1.0);
        let speed: Vec<f64> = position.iter().map(|_i| dist.sample(&mut rand)).collect();

        // generate pos based on the position max and min
        let max = position
            .iter()
            .max_by(|a, b| a.total_cmp(b))
            .expect("position should have atleast 1 element");
        let min = position
            .iter()
            .min_by(|a, b| a.total_cmp(b))
            .expect("position should have atleast 1 element");
        let pos_dist = Uniform::from(*min..=*max);
        let pos = position
            .iter()
            .map(|_| pos_dist.sample(&mut rand))
            .collect::<Vec<_>>();

        Individual {
            best_pos: pos.clone(),
            position: pos,
            f: f64::MAX,
            speed,
        }
    }

    /// Individual best speed updater
    pub fn ind_update_speed(&mut self, rho: f64) {
        self.speed
            .iter_mut()
            .zip(self.best_pos.iter().zip(self.position.iter()))
            .for_each(|(v, (x_b, x))| {
                *v += rho * (*x_b - *x);
            });
    }

    /// Speed updator with social component included
    pub fn update_speed(&mut self, other_best: &[f64], rho1: f64, rho2: f64) {
        let w = 1.0;
        self.speed
            .iter_mut()
            .zip(
                self.position
                    .iter()
                    .zip(self.best_pos.iter().zip(other_best.iter())),
            )
            .for_each(|(v, (x, (x_b, x_gb)))| {
                *v = w * *v + rho1 * (*x_b - *x) + rho2 * (*x_gb - *x);
            });
    }

    pub fn change_pos(&mut self) {
        self.position
            .iter_mut()
            .zip(self.speed.iter())
            .for_each(|(x, v)| {
                *x += v;
            });
    }
}

pub fn gen_rho(c: f64) -> f64 {
    let mut rand = rand::thread_rng();
    let dist = Uniform::from(0.0..=1.0);
    dist.sample(&mut rand) * c
}

pub struct IndividualGroup {
    pub particles: Vec<Individual>,
    pub lbest_f: f64,
    pub lbest_pos: Vec<f64>,
}

impl IndividualGroup {
    pub fn add(&mut self, individual: Individual) {
        self.particles.push(individual);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_speed() {
        fn f(pos: &Vec<f64>) -> f64 {
            pos[0].powi(2) + 2.0 * pos[1]
        }

        let mut p1 = Individual::new(vec![1.0, 1.0]);
        p1.f = 4.0;
        p1.speed = vec![0.5, 0.5];

        let gbest = vec![0.5, 1.0];

        // trainning
        let eval_result = f(&p1.position);
        if eval_result < p1.f {
            p1.f = eval_result;
            p1.best_pos = p1.position.clone();
        }

        p1.update_speed(&gbest, 1.0, 1.0);
        p1.change_pos();

        assert_eq!(p1.speed, vec![0.0, 0.5]);
        assert_eq!(p1.position, vec![1.0, 1.5]);
    }
}
