use std::sync::mpsc::{Receiver, Sender};
use std::sync::mpsc;
use std::thread::{JoinHandle, self};
use std::time::{Instant, Duration};
use crate::shop::Shop;
use crate::messages::{Request, FinishedDelivery};
use crate::point::*;
use std::collections::BinaryHeap;
use rand::prelude::*;
use actix::Addr;

const TIME_PER_DISTANCE_UNIT: i32 = 1; // WIP por que lo dejamos en 1?
const DROP_PROBABILITY: f32 = 0.1;


#[derive(PartialEq,Eq, Debug)]
struct DeliveryOrder{
    time_to_arrive: u64,
    request: Request
}

impl DeliveryOrder{
    fn new(time_to_arrive: u64, request: Request) -> DeliveryOrder{
        DeliveryOrder{
            time_to_arrive,
            request,
        }
    }

    fn arrived_in_time(&self)->bool{
        rand::thread_rng().gen_range(0.0, 1.0) >= DROP_PROBABILITY
    }
}

impl Ord for DeliveryOrder{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.time_to_arrive.cmp(&self.time_to_arrive)
    }
}

impl PartialOrd for DeliveryOrder{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {Some(self.cmp(other))}
}


struct Delivery{
    shop_addr: Addr<Shop>,
    receiver: Receiver<Option<Request>>,
    ongoing_deliveries: BinaryHeap<DeliveryOrder>,
    shop_location:  Point,
    starting_instant: Instant
}

impl Delivery{
    fn new(shop_addr: Addr<Shop>, receiver: Receiver<Option<Request>>, shop_location: Point)->Delivery{
        Delivery{
            shop_addr,
            receiver,
            ongoing_deliveries: BinaryHeap::new(),
            shop_location, 
            starting_instant: Instant::now(),
        }
    }

    fn recv_timeout(&mut self, timeout: Duration)->Result<Option<DeliveryOrder>, DeliveryError>{
        match self.receiver.recv_timeout(timeout){
            Ok(request) => {
                match request{
                    Some(req) => {
                        self.add_request(req);
                        Ok(None)
                    },
                    None => Err(DeliveryError::ErrorFinishingSimulation),
                }
            },
            Err(error) => {
                if let mpsc::RecvTimeoutError::Timeout = error{
                    let delivery_order = self.ongoing_deliveries.pop();
                    Ok(delivery_order)
                } else {
                    Err(DeliveryError::ErrorReceivingRequests)
                }
            }
        }
    }

    fn rcv(&mut self)->Result<Option<DeliveryOrder>, DeliveryError>{
        let request = self.receiver.recv().map_err(|_| DeliveryError::ErrorReceivingRequests)?;
        match request{
            Some(req) => {
                self.add_request(req);
                Ok(None)
            },
            None => Err(DeliveryError::ErrorFinishingSimulation),
        }
    }

    fn wait_until_finish_or_new_delivery(&mut self)->Result<Option<DeliveryOrder>, DeliveryError>{
        if let Some(delivery_order) = self.ongoing_deliveries.peek(){
            let duration = ((delivery_order.time_to_arrive as f64) - self.starting_instant.elapsed().as_secs_f64()).max(0.0);
            let timeout = Duration::from_secs_f64(duration);
            return self.recv_timeout(timeout);
        }
        self.rcv()
    }                               

    fn add_request(&mut self, request: Request){
        let arrival_time = self.calculate_time(request.destination) + self.starting_instant.elapsed().as_secs();
        let delivery_order = DeliveryOrder::new(arrival_time, request);
        self.ongoing_deliveries.push(delivery_order) 
        
    }

    fn calculate_time(&self, destination: Point) -> u64{
        (destination.calculate_distance(&self.shop_location) * TIME_PER_DISTANCE_UNIT) as u64
    }
    
    fn inform_shop(&self, delivery_order: DeliveryOrder) -> Result<(), DeliveryError> {
        let msg = if delivery_order.arrived_in_time(){
            FinishedDelivery::ArrivedInTime(delivery_order.request)
        } else {
            FinishedDelivery::ArrivedLate(delivery_order.request)
        };
        self.shop_addr.try_send(msg).map_err(|_| DeliveryError::ErrorCommunicatingWithShop)
    }
}   

pub struct DeliveryHandler {
    handle: JoinHandle<Result<(), DeliveryError>>,
    finish_sender: Sender<Option<Request>>
}

impl DeliveryHandler{

    pub fn new(shop_addr: Addr<Shop>, receiver: Receiver<Option<Request>>, finish_sender: Sender<Option<Request>>,shop_location: Point)-> DeliveryHandler {
        let handle = thread::spawn(move|| deliver(shop_addr, receiver, shop_location));
        DeliveryHandler{handle, finish_sender}
    }

    pub fn stop(self) -> Result<(), DeliveryError>{
        self.finish_sender.send(None).map_err(|_| DeliveryError::ErrorFinishingSimulation)?;
        let res = self.handle.join().map_err(|_| DeliveryError::ErrorJoiningSimulationThread)?;
        println!("Delivery finished: {:?}", res);
        Ok(())
    }
}




#[derive(Debug)]
pub enum DeliveryError{
    ErrorReceivingRequests,
    ErrorCommunicatingWithShop,
    TimeoutError,
    ErrorFinishingSimulation,
    ErrorJoiningSimulationThread,
}

pub fn deliver(shop_addr: Addr<Shop>, receiver: Receiver<Option<Request>>,shop_location: Point) -> Result<(), DeliveryError>{
    let mut delivery = Delivery::new(shop_addr, receiver,shop_location);
    loop{
        match delivery.wait_until_finish_or_new_delivery(){
            Ok(delivery_order) => if let Some(delivery_order) = delivery_order{
                delivery.inform_shop(delivery_order)?;
            },
            Err(error) => match error{
                DeliveryError::ErrorFinishingSimulation => return Ok(()),
                _ => return Err(error),
            }
        }
    }
    
}

#[cfg(test)]
mod test {
    use actix::Actor;

    use super::*;
    
    #[test]
    fn test_1_deliveries_are_in_ascending_time_order(){
        let mut heap = BinaryHeap::new();
        let order_1 = DeliveryOrder::new(5, Request::new(vec![], vec![], Point::new(0,0)));
        let order_2 = DeliveryOrder::new(7, Request::new(vec![], vec![], Point::new(0,0)));
        heap.push(order_1);
        heap.push(order_2);
        assert_eq!(heap.peek().unwrap().time_to_arrive, 5)
    }

    #[actix_rt::test]
    async fn test_2_delivery_wakes_up_on_finished_delivery()-> Result<(),DeliveryError>{
        let (sx, rx) = std::sync::mpsc::channel();
        let shop_addr = Shop::from("test_files/shop_test_1.txt".to_string(), sx.clone()).unwrap().start();
        let order_1 = Request::new(vec![], vec![], Point::new(1,1));
        let order_2 = Request::new(vec![], vec![], Point::new(5,5));
        let mut delivery = Delivery::new(shop_addr, rx, Point::new(0,0));
        
        sx.send(Some(order_1)).unwrap();
        sx.send(Some(order_2)).unwrap();

        assert_eq!(delivery.wait_until_finish_or_new_delivery()?, None);
        assert_eq!(delivery.wait_until_finish_or_new_delivery()?, None);
        assert_eq!(delivery.wait_until_finish_or_new_delivery()?.unwrap().request.destination, Point::new(1,1));
        assert_eq!(delivery.wait_until_finish_or_new_delivery()?.unwrap().request.destination, Point::new(5,5));

        Ok(())
    }
}
