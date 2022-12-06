pub trait Shape {
    fn function(&self, x: f64) -> f64;
}

pub struct Triangular {
    a: f64,
    b: f64,
    s: f64,
}

impl Shape for Triangular {
    fn function(&self, x: f64) -> f64 {
        if (self.a - self.s) <= x && x <= (self.a + self.s) {
            return self.b * (1.0 - (x - self.a).abs() / self.s);
        }
        0.0
    }
}

pub fn triangular(a: f64, b: f64, s: f64) -> Triangular {
    Triangular { a, b, s }
}

pub struct Trapezoidal {
    a: f64,
    b: f64,
    c: f64,
    d: f64,
    e: f64,
}

impl Shape for Trapezoidal {
    fn function(&self, x: f64) -> f64 {
        if x >= self.a && x < self.b {
            return ((x - self.a) * self.e) / (self.b - self.a);
        } else if x >= self.b && x <= self.c {
            return self.e;
        } else if x > self.c && x <= self.d {
            return self.e * (1.0 - (x - self.c).abs() / (self.d - self.c));
        }
        0.0
    }
}

pub fn trapezoidal(a: f64, b: f64, c: f64, d: f64, e: f64) -> Trapezoidal {
    Trapezoidal { a, b, c, d, e }
}