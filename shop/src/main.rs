use actix_rt::time::sleep;
use shop::{shop::Shop, point::Point};
use shop::physical_requests::simulate_physical_requests;
use std::collections::HashMap;
use std::thread;
use std::time::Duration;
use actix::{Actor, Addr,System};

#[actix_rt::main]
async fn main() {
    println!("a");
    let shop_addr: Addr<Shop> = Shop::new(HashMap::new(), Point::new(0, 0)).start();
    let handler = simulate_physical_requests("test_files/physical_requests_test_1.txt".to_string(), shop_addr).await;
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