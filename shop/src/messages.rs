use actix::{Message, Addr};
use crate::{point::Point, shop::ShopError, ecomerce::{EcommerceError, Ecommerce}, token_ring_messages::ByteConvertable, token_ring::TokenRingError};

const POINT_SIZE: usize = 8;

/// Struct that represents a request
/// 
/// ## Attributes
/// 
/// * `product_ids`: Vector of the ids of the products that the request contains
/// * `product_amount`: Vector of the amount of each product that the request contains
/// * `destination`: Point that represents the destination of the request
#[derive(Message, Debug,PartialEq, Eq, Clone)]
#[rtype(result = "()")]
pub struct Request{
    pub product_ids: Vec<u32>,
    pub product_amount: Vec<u32>,
    pub destination: Point,
}

impl Request{

    /// Creates a new Request given the product ids, the amount of each product and the destination
    pub fn new(product_ids: Vec<u32>, product_amount: Vec<u32>, destination: Point)-> Request{
        Request{product_ids,product_amount,destination}
    }

    pub fn to_bytes(&self)->Vec<u8>{
        let mut bytes: Vec<u8> = self.product_ids.iter().flat_map(|distance| distance.to_be_bytes()).collect();

        bytes.extend(self.product_amount.iter().flat_map(|distance| distance.to_be_bytes()));
        bytes.extend(self.destination.to_bytes());
        bytes
    }

    pub fn from_bytes(bytes: Vec<u8>, amount_of_diferent_products: u8)-> Result<Request, TokenRingError>{
        if bytes.len() != 2*4*(amount_of_diferent_products as usize) + POINT_SIZE {
            return Err(TokenRingError::ErrorInvalidByteSequence)
        }
        let mut bytes = bytes.as_slice();
        let mut aux;
        let mut product_ids = Vec::new();
        let mut product_amounts = Vec::new();
        for _ in 0..amount_of_diferent_products{
            (aux, bytes) = bytes.split_at(4);
            let id:[u8;4] = aux.try_into().map_err(|_| TokenRingError::ErrorInvalidByteSequence)?;
            product_ids.push(u32::from_be_bytes(id));
        };

        for _ in 0..amount_of_diferent_products{
            (aux, bytes) = bytes.split_at(4);
            let amount:[u8;4] = aux.try_into().map_err(|_| TokenRingError::ErrorInvalidByteSequence)?;
            product_amounts.push(u32::from_be_bytes(amount));
        };
        
        Ok(Request::new(product_ids, product_amounts, Point::from_bytes(bytes)?))
    }

    /// Returns the amount of diferent products that the request contains
    pub fn amount_of_diferent_products(&self)->u8{
        self.product_ids.len() as u8
    }
}

/// Struct that represents a message that contains the address of the ecommerce
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct EcommerceAddrMsg{
    pub ecommerce_addr: Addr<Ecommerce>
}

impl EcommerceAddrMsg{

    /// Creates a new EcommerceAddrMsg given the address of the ecommerce
    pub fn new(ecommerce_addr: Addr<Ecommerce>)->EcommerceAddrMsg{
        EcommerceAddrMsg{ecommerce_addr}
    }
}

/// Struct that represents a message that contains the request and the id of the request
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct EnoughStockMsg{
    pub request:Request,
    pub req_id: usize,
}

impl EnoughStockMsg{

    /// Creates a new EnoughStockMsg given the request and the id of the request
    pub fn new(request: Request, req_id:usize)->EnoughStockMsg{
        EnoughStockMsg{request, req_id}
    }
}

/// Enum that represents if there is enough stock or not for a request 
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub enum StockAvailabilityMsg{
    EnoughStock(usize, i32),
    NotEnoughStock(usize)
}

/// Enum that indicates if a delivery arrived in time or not
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub enum FinishedDelivery{
    ArrivedInTime(Request),
    ArrivedLate(Request)
}

/// Struct that represents a request that cant be handled by the shop
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct CantHandleRequestMsg{
    pub request: Request
}

impl CantHandleRequestMsg{

    /// Creates a new CantHandleRequestMsg given the request
    pub fn new(request: Request)->CantHandleRequestMsg{
        CantHandleRequestMsg{request}
    }
}

#[derive(Debug)]
pub enum MessageError{
    ShopError(ShopError),
    EcommerceError(EcommerceError)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_1_request_to_bytes(){
        let product_ids = vec![2,3];
        let product_amount = vec![4,5];
        let destination = Point::new(0,0);
        let request = Request::new(product_ids.clone(), product_amount.clone(), destination);
        let mut bytes: Vec<u8> = product_ids.iter().map(|id| id.to_be_bytes()).flatten().collect();
        bytes.extend(product_amount.iter().map(|amount| amount.to_be_bytes()).flatten());
        bytes.extend(request.destination.to_bytes());

        assert_eq!(bytes, request.to_bytes())
    }

    #[test]
    fn test_2_request_from_bytes() -> Result<(), TokenRingError>{
        let product_ids = vec![2, 3];
        let product_amount = vec![4, 5];
        let destination = Point::new(0, 0);
        let request = Request::new(product_ids.clone(), product_amount.clone(), destination);

        assert_eq!(request, Request::from_bytes(request.to_bytes(), 2)?);
        Ok(())
    }
}