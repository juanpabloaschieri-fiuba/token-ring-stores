use crate::{token_ring::TokenRingError, token_ring_messages::ByteConvertable};

/// Struct that represents a point in a 2D space
/// Useful for identifying the nearest shop for a given online order 
#[derive(Debug, Clone, Copy, PartialEq,Eq)]
pub struct Point {
    pub x: i32,
    pub y: i32
}

impl Point {
    /// Creates a new point given the x and y coordinates
    pub fn new(x: i32, y: i32) -> Point {
        Point{
            x,
            y
        }
    }

    /// Given another point, calculates the arithmetical distance between both points
    pub fn calculate_distance(&self, point: &Point) -> i32 {
        let delta_x = self.x - point.x;
        let delta_y = self.y - point.y;
        (((delta_x * delta_x) + (delta_y * delta_y))as f32).sqrt() as i32
    }

    /// Given a list of points, returns the nearest point to the current point using 
    /// the arithmetical distance. In case of a tie, returns either of the points
    pub fn nearest_point(&self, points: Vec<&Point>) -> Option<Point> {
        let res = points.into_iter().min_by(|p1, p2| {
            let d1 = p1.calculate_distance(self);
            let d2 = p2.calculate_distance(self);
            d1.cmp(&d2)
        });

        res.cloned()
    }
}

impl ByteConvertable for Point{
    type Type=Point;

    fn to_bytes(&self)-> Vec<u8> {
        vec![self.x, self.y].into_iter().flat_map(|distance| distance.to_be_bytes()).collect()
    }

    fn from_bytes(bytes :&[u8]) -> Result<Self::Type, TokenRingError> {
        if bytes.len() != 8{
            return Err(TokenRingError::ErrorInvalidByteSequence);
        }
        let mut bytes = bytes;
        let aux; 
        (aux, bytes) = bytes.split_at(4);
        let x:[u8;4] = aux.try_into().map_err(|_| TokenRingError::ErrorInvalidByteSequence)?;
        let x = i32::from_be_bytes(x);

        let y:[u8;4] = bytes.try_into().map_err(|_| TokenRingError::ErrorInvalidByteSequence)?;
        let y = i32::from_be_bytes(y);

        Ok(Point::new(x,y))
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
        assert_eq!(P1.calculate_distance(&P1), 0);
        assert_eq!(P1.calculate_distance(&P3), (13 as f32).sqrt() as i32);
        assert_eq!(P1.calculate_distance(&P2), P2.calculate_distance(&P1));
    }

    #[test]
    fn point_test_2_nearest_point(){
        let nearest = P3.nearest_point(vec![&P1,&P2]).unwrap();
        assert_eq!(nearest,P1);
    }

    #[test]
    fn test_3_to_bytes() {
        let bytes = P1.to_bytes();
        assert_eq!(bytes.len(), 8);
        assert_eq!(bytes, vec![0,0,0,2,0,0,0,3]);
    }

    #[test]
    fn test_4_from_bytes() {
        let bytes = vec![0,0,0,2,0,0,0,3];
        let invalid_bytes = vec![0,0,0,2,0,0,0];
        let point = Point::from_bytes(&bytes).unwrap();
        assert_eq!(point, P1);
        assert!(Point::from_bytes(&invalid_bytes).is_err())
    }
}