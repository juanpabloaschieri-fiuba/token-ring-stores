use actix::{Message, Addr};
use crate::{point::Point, shop::ShopError, ecomerce::{EcommerceError, Ecommerce}};


#[derive(Message, Debug,PartialEq)]
#[rtype(result = "Result<(), MessageError>")]
pub struct Request{
    pub product_ids: Vec<u32>,
    pub product_amount: Vec<u32>,
    pub destination: Point,
}

impl Request{
    pub fn new(product_ids: Vec<u32>, product_amount: Vec<u32>, destination: Point)-> Request{
        Request{product_ids,product_amount,destination}
    }
}

#[derive(Message, Debug,PartialEq)]
#[rtype(result = "()")]
pub struct NotEnoughStockMsg{
    request: Request
}

impl NotEnoughStockMsg{
    pub fn new(request: Request)->NotEnoughStockMsg{
        NotEnoughStockMsg{request}
    }
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct EcommerceAddrMsg{
    pub ecommerce_addr: Addr<Ecommerce>
}

impl EcommerceAddrMsg{
    pub fn new(ecommerce_addr: Addr<Ecommerce>)->EcommerceAddrMsg{
        EcommerceAddrMsg{ecommerce_addr}
    }
}

#[derive(Debug)]
pub enum MessageError{
    ShopError(ShopError),
    EcommerceError(EcommerceError)
}