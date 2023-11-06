use ::shop::*;

#[actix_rt::main]
async fn main() {
    println!("a");
    /*let shop_addr: Addr<Shop> = Shop{shoes: HashMap::new()}.start();
    let res = shop_addr.send(HayZapatillas{}).await.unwrap();
    if res{
        println!("Hay zapatillas en thread {:?}",thread::current().id())
    }
    System::current().stop();*/
}



/*
println!("Antes del sleep");
let a = actix::clock::sleep(Duration::from_secs(5)).into_actor(self);
println!("Despues del sleep");
*/