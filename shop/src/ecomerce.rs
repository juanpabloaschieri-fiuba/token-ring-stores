use std::collections::HashMap;
use std::sync::mpsc::Sender;

use crate::messages::{CantHandleRequestMsg, EnoughStockMsg, Request, StockAvailabilityMsg};
use crate::shop::Shop;
use crate::token_ring_messages::{CoordinateRequest, RingMessage};
use actix::{Actor, Addr, Context, Handler};
use colored::*;

/// Struct that represents an ecommerce
///
/// An Ecommerce is an actor that represents the online instance of a shop
///
/// ## Attributes
///
/// - `shop_addr`: Address of the shop actor
/// - `ongoing_coordinate_requests`: HashMap that contains the ongoing coordinate requests
/// - `next_id`: Next id to be used for a coordinate request
/// - `message_sender`: Sender of RingMessages
#[derive(Debug)]
pub struct Ecommerce {
    shop_addr: Addr<Shop>,
    ongoing_coordinate_requests: HashMap<usize, CoordinateRequest>,
    next_id: usize,
    message_sender: Sender<RingMessage>,
}

impl Actor for Ecommerce {
    type Context = Context<Self>;
}

impl Ecommerce {
    /// Creates a new Ecommerce given the address of the shop and the sender of RingMessages
    pub fn new(shop_addr: Addr<Shop>, message_sender: Sender<RingMessage>) -> Ecommerce {
        Ecommerce {
            shop_addr,
            ongoing_coordinate_requests: HashMap::new(),
            next_id: 0,
            message_sender,
        }
    }

    /// Continues a coordinate request given the id of the request and the distance to the next node
    fn continue_coordinated_request(&mut self, req_id: usize, distance: i32) {
        if let Some(mut msg) = self.ongoing_coordinate_requests.remove(&req_id) {
            msg.distances.push(distance);
            print(format!("Sending {:?}", msg));
            let _ = self
                .message_sender
                .send(RingMessage::from_coordinate_request(msg));
        }
    }

    /// Starts a new coordinate request given the request and the distance to the next node
    fn start_new_coordinate_request(&self, request: Request, my_distance: i32) {
        let amount_of_diferent_products = request.amount_of_diferent_products();
        let mut recoordinate =
            CoordinateRequest::start(u16::MAX, request, amount_of_diferent_products);
        recoordinate.distances.push(my_distance);
        let recoordinate_ring = RingMessage::from_coordinate_request(recoordinate);

        let _ = self.message_sender.send(recoordinate_ring);
    }

    /// Adds an ongoing coordinate request
    fn add_ongoing_coordinate_request(&mut self, msg: CoordinateRequest) {
        self.ongoing_coordinate_requests.insert(self.next_id, msg);
        self.next_id += 1;
    }
}

impl Handler<Request> for Ecommerce {
    type Result = ();

    /// Handles a Request message
    fn handle(&mut self, _msg: Request, _ctx: &mut Context<Self>) -> Self::Result {
        print(format!("Taking care of {}", _msg));
        self.shop_addr.do_send(_msg);
    }
}

impl Handler<StockAvailabilityMsg> for Ecommerce {
    type Result = ();

    /// Handles a StockAvailabilityMsg message
    fn handle(&mut self, _msg: StockAvailabilityMsg, _ctx: &mut Context<Self>) -> Self::Result {
        let (req_id, distance) = match _msg {
            StockAvailabilityMsg::EnoughStock(req_id, distance) => (req_id, distance),
            StockAvailabilityMsg::NotEnoughStock(req_id) => (req_id, -1),
        };
        self.continue_coordinated_request(req_id, distance);
    }
}

impl Handler<CantHandleRequestMsg> for Ecommerce {
    type Result = ();

    /// Handles a CantHandleRequestMsg message
    fn handle(&mut self, msg: CantHandleRequestMsg, _ctx: &mut Self::Context) -> Self::Result {
        print(format!("Rehandling {}", msg.request));
        self.start_new_coordinate_request(msg.request, -1);
    }
}

impl Handler<CoordinateRequest> for Ecommerce {
    type Result = ();

    /// Handles a CoordinateRequest message
    fn handle(&mut self, msg: CoordinateRequest, _ctx: &mut Self::Context) -> Self::Result {
        print(format!("Coordinating {}", msg.request));
        self.shop_addr
            .do_send(EnoughStockMsg::new(msg.request.clone(), self.next_id)); //WIP el error dios nos ayude, capas pasarlo a un do
        self.add_ongoing_coordinate_request(msg);
    }
}

fn print(string: String) {
    println!("{} {}", "[Ecomerce]".bright_green(), string);
}

#[derive(Debug)]
pub enum EcommerceError {
    ErrorCantCommunicateWithShop,
    ErrorSharingData,
}

#[cfg(test)]
mod test {
    use std::sync::mpsc::{self, Receiver};

    use crate::{point::Point, token_ring_messages::MsgId};

    use super::*;

    fn create_test_ecomerce() -> (Ecommerce, Receiver<RingMessage>) {
        let (tx, rx) = mpsc::channel();
        let (delivery_tx, _delivery_rx) = mpsc::channel();
        let shop_addr = Shop::new(HashMap::new(), Point::new(0, 0), delivery_tx).start();
        (Ecommerce::new(shop_addr, tx), rx)
    }

    #[actix_rt::test]
    async fn test_1_start_new_coordinate_request() {
        let (ecommerce, message_rx) = create_test_ecomerce();
        let request = Request::new(vec![0, 1], vec![30, 45], Point::new(2, 10));
        ecommerce.start_new_coordinate_request(request.clone(), 5);

        let mut sent_ring_message: RingMessage = message_rx.try_recv().unwrap();
        sent_ring_message.sign(0);
        let new_coordinate_request =
            CoordinateRequest::from_ring_message(sent_ring_message).unwrap();
        assert_eq!(new_coordinate_request.request, request);
        assert_eq!(new_coordinate_request.distances, vec![5]);
    }

    #[actix_rt::test]
    async fn test_2_continue_with_coordinate_request() {
        let (mut ecommerce, message_rx) = create_test_ecomerce();
        let request = Request::new(vec![0, 1], vec![30, 45], Point::new(2, 10));
        let coordinated_request = CoordinateRequest::new(
            MsgId::new(0, 1),
            vec![2, 3],
            2,
            vec![-1, 2],
            request.clone(),
        );
        ecommerce.add_ongoing_coordinate_request(coordinated_request);
        ecommerce.continue_coordinated_request(0, 5);

        let mut sent_ring_message: RingMessage = message_rx.try_recv().unwrap();
        sent_ring_message.sign(0);
        let coordinate_request = CoordinateRequest::from_ring_message(sent_ring_message).unwrap();

        assert_eq!(coordinate_request.request, request);
        assert_eq!(coordinate_request.distances, vec![-1, 2, 5]);
        assert!(ecommerce.ongoing_coordinate_requests.is_empty());
    }
}

//recibe ganador y sos vos pero no tenes el producto asi que se inica la votacion nuevamente lo manda al sender y no al ecommerce
