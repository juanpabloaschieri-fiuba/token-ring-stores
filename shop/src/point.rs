
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    x: i32,
    y: i32
}

impl Point {
    pub fn new(x: i32, y: i32) -> Point {
        Point{
            x,
            y
        }
    }

    pub fn calculate_distance(&self, point: &Point) -> f32 {
        let delta_x = self.x - point.x;
        let delta_y = self.y - point.y;
        (((delta_x * delta_x) + (delta_y * delta_y))as f32).sqrt()
    }

    pub fn nearest_point(&self, points: Vec<&Point>) -> Option<Point> {
        let res = points.into_iter().min_by(|p1, p2| {
            let d1 = p1.calculate_distance(self);
            let d2 = p2.calculate_distance(self);
            d1.total_cmp(&d2)
        });

        res.cloned()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const P1: Point = Point{x:2, y:3};
    const P2: Point = Point{x:10, y:20};
    const P3: Point = Point{x:0, y:0};

    #[test]
    fn point_test_1_calculate_distance() {
        assert_eq!(P1.calculate_distance(&P1), 0.0);
        assert_eq!(P1.calculate_distance(&P3), (13 as f32).sqrt());
        assert_eq!(P1.calculate_distance(&P2), P2.calculate_distance(&P1));
    }

    #[test]
    fn point_test_2_nearest_point(){
        let nearest = P3.nearest_point(vec![&P1,&P2]).unwrap();
        assert_eq!(nearest,P1);
    }
}