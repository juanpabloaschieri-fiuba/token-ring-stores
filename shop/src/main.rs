use actix_rt::time::sleep;
use shop::ecomerce::Ecommerce;
use shop::{shop::Shop, point::Point};
use shop::simulator_physical_requests::simulate_physical_requests;
use shop::messages::EcommerceAddrMsg;
use std::collections::HashMap;
use std::thread;
use std::time::Duration;
use actix::{Actor, Addr,System, MailboxError};
use std::sync::{mpsc};

async fn load_ecommerce_addr(shop_addr: Addr<Shop>,ecommerce_addr: Addr<Ecommerce>)->Result<(), MailboxError>{
    shop_addr.send(EcommerceAddrMsg::new(ecommerce_addr)).await
}

#[actix_rt::main]
async fn main() {
    let shop_location = Point::new(0,0);
    let (tx,rx) = mpsc::Channel();
    let shop_addr: Addr<Shop> = Shop::new(HashMap::new(), shop_location).start();       //WIP agregar tx del channel
    let ecommerce_addr: Addr<Ecommerce> = Ecommerce::new(shop_addr.clone()).start();
    let delivery_handler = thread::spawn(move|| {
        delivery(shop_addr,rx.clone(),shop_location);
    });
    if load_ecommerce_addr(shop_addr.clone(),ecommerce_addr).await.is_err(){
        println!("tengo miedo");
        return
    };
    let handler = simulate_physical_requests("test_files/physical_requests_test_1.txt".to_string(), shop_addr, shop_location).await;
    //let res = shop_addr.send(HayZapatillas{}).await.unwrap();
    //if res{
    //    println!("Hay zapatillas en thread {:?}",thread::current().id())
    //}
    System::current().stop();
}



/*
println!("Antes del sleep");
let a = actix::clock::sleep(Duration::from_secs(5)).into_actor(self);
println!("Despues del sleep");
*/