use actix::{Actor, Context, Handler, System, Addr, Message, WrapFuture};
use crate::shop::Shop;
use crate::messages::{Request, MessageError, NotEnoughStockMsg};

#[derive(Debug)]
pub struct Ecommerce{
    shop_addr: Addr<Shop>,
}

impl Actor for Ecommerce{
    type Context = Context<Self>;
}

impl Ecommerce{
    pub fn new(shop_addr: Addr<Shop>)-> Ecommerce{
        Ecommerce { shop_addr }
    }
}

//#[rtype(result = "Result<(), EcommerceError>;")]
impl Handler<Request> for Ecommerce{
    type Result = Result<(), MessageError>;
    
    fn handle(&mut self, _msg:Request , _ctx: &mut Context<Self>) -> Self::Result {
        
        self.shop_addr.try_send(_msg).map_err(|_| MessageError::EcommerceError(EcommerceError::ErrorCantCommunicateWithShop))
        
    }
}

impl Handler<NotEnoughStockMsg> for Ecommerce{
    type Result = ();
    
    fn handle(&mut self, _msg:NotEnoughStockMsg , _ctx: &mut Context<Self>) -> Self::Result {
        
        //hacer otra eleccion
        todo!()
    }
}

#[derive(Debug)]
pub enum EcommerceError{
    ErrorCantCommunicateWithShop,
}

/*
#[derive(Message, Debug,PartialEq)]
#[rtype(result = "bool")]
struct CanTakeCareOfRequest{
    request: OnlineRequest
}
*/
