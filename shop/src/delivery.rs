use std::cmp::Reverse;
use std::sync::mpsc::Receiver;
use std::sync::{mpsc, Condvar};
use std::time::{Instant, Duration, SystemTime, UNIX_EPOCH};
use actix::{Addr};
use crate::shop::Shop;
use crate::messages::{Request};
use crate::point::*;
use std::collections::BinaryHeap;
use rand::prelude::*;

const TIME_PER_KILOMETER: f32 = 500.0;
const DROP_PROBABILITY: f32 = 0.1;


#[derive(PartialEq,Eq)]
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
}

impl Ord for DeliveryOrder{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.time_to_arrive.cmp(&other.time_to_arrive)
    }
}

impl PartialOrd for DeliveryOrder{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.time_to_arrive.partial_cmp(&other.time_to_arrive)
    }
}


struct Delivery{
    shop_addr: Addr<Shop>,
    receiver: Receiver<Request>,
    ongoing_deliveries: BinaryHeap<DeliveryOrder>,
    shop_location:  Point,
    starting_instant: Instant
}

impl Delivery{
    fn new(shop_addr: Addr<Shop>, receiver: Receiver<Request>, shop_location: Point)->Delivery{
        Delivery{
            shop_addr,
            receiver,
            ongoing_deliveries: BinaryHeap::new(),
            shop_location, 
            starting_instant: Instant::now(),
        }
    }

    fn wait_until_finish_or_new_delivery(&mut self)->Result<Option<DeliveryOrder>, DeliveryError>{
        if let Some(delivery_order) = self.ongoing_deliveries.peek(){
            let timeout = Duration::from_secs_f64((delivery_order.time_to_arrive as f64) - self.starting_instant.elapsed().as_secs_f64());
            match self.receiver.recv_timeout(timeout){
                Ok(request) => {
                    self.add_request(request);
                    return Ok(None)
                },
                Err(error) => {
                    if let mpsc::RecvTimeoutError::Timeout = error{
                        let delivery_order = self.ongoing_deliveries.pop();
                        return Ok(delivery_order)
                    }else{
                        return Err(DeliveryError::ErrorReceivingRequests)
                    }
                }
            }
        }
        
        let request = self.receiver.recv().map_err(|_| DeliveryError::ErrorReceivingRequests)?;
        self.add_request(request);
        Ok(None)
    }                               

    fn add_request(&mut self, request: Request){
        let arraival_time = self.calculate_time(request.destination) + self.starting_instant.elapsed().as_secs();
        let delivery_order = DeliveryOrder::new(arraival_time, request);
        self.ongoing_deliveries.push(Reverse(delivery_order)) //WIP
        
    }

    fn calculate_time(&self, destination: Point) -> u64{
        (destination.calculate_distance(&self.shop_location) * TIME_PER_KILOMETER) as u64
    }
    
}

enum DeliveryError{
    ErrorReceivingRequests,
    TimeoutError
}

fn deliver(shop_addr: Addr<Shop>, receiver: Receiver<Request>,shop_location: Point) -> Result<Option<DeliveryOrder>, DeliveryError>{
    let mut delivery = Delivery::new(shop_addr, receiver,shop_location);
    
    loop{
        //si no hay que finiquitar
        let deliveryOrder = delivery.wait_until_finish_or_new_delivery()?;
        if let Some(delivery_order) = deliveryOrder{
            let prob: f32 = rand::thread_rng().gen_range(0.0, 1.0);
            if prob < DROP_PROBABILITY{
                //WIP avisar al shop que NO se entrego
            } else {
                //WIP avisar al shop que se entrego, esto mepa que no hace falta
            }
        }
    }
}