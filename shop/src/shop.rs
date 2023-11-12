use actix::{Actor, Context, Handler, System, Addr, Message, WrapFuture, AsyncContext};
use std::{collections::HashMap, thread, time::Duration, fs::File, io::{BufRead, BufReader}};
use crate::{point::*, ecomerce::Ecommerce};
use crate::messages::*;

// Positions of the coordinates in the header of the file
const X_POS: usize = 0;
const Y_POS: usize = 1;

// Positions of the data in the body of the file
const ID_POS: usize = 1;
const AMOUNT_POS: usize = 2;

#[derive(PartialEq, Debug)]
pub struct Shop{
    stock: HashMap<u32, (u32, u32)>,
    location: Point,
    ecomerce_addr: Option<Addr<Ecommerce>>,
}

impl Actor for Shop{
    type Context = Context<Self>;
}

impl Shop{
    /// Private constructor for Shop
    pub fn new(stock: HashMap<u32, (u32, u32)>, location: Point) -> Shop{
        Shop{stock, location, ecomerce_addr: None}
    }
    
    /// Public constructor for Shop. Receives a path to a file with the shop stock and returns a Shop.
    pub fn from(path: String)-> Result<Shop, ShopError>{
        let file = File::open(path);
        println!("{:?}", file);
        let file = file.map_err(|_| ShopError::ErrorCreatingShop)?;
        let mut reader = BufReader::new(file);

        let (x, y) = obtain_coordinates(&mut reader)?;
        
        let point = Point::new(x, y);

        let stock = charge_stock_from_file(&mut reader)?;
        
        Ok(Shop::new(stock, point))
    }
    
    fn verify_enough_stock(&self, request: &Request)-> bool{

        for (index,product) in request.product_ids.iter().enumerate(){
            match self.stock.get(product){
                Some(amount) => {
                    if request.product_amount[index] > amount.0{
                        return false
                    }
                },
                None => {return false}
            }
        }
        true
    }
    
    fn reduce_stock(&mut self, request: &Request)->Result<(), ShopError>{
        if self.verify_enough_stock(request){
            for (i, product) in request.product_ids.iter().enumerate(){
                if let Some(product) = self.stock.get_mut(product){
                    product.0 -= request.product_amount[i];
                }
            }
            return Ok(())
        }
        Err(ShopError::ErrorNotEnoughStock)
    }

    fn handle_physical_request(&mut self, request: &Request)-> Result<(), ShopError>{
        self.reduce_stock(request)
    }

    fn is_physical_request(&self, request: &Request) -> bool{
        self.location == request.destination
    }

    fn reserve_stock(&mut self, request: &Request)->Result<(), ShopError>{
        if self.verify_enough_stock(request){
            for (i, product) in request.product_ids.iter().enumerate(){
                if let Some(product) = self.stock.get_mut(product){
                    product.0 -= request.product_amount[i];
                    product.1 += request.product_amount[i];
                }
            }
            return Ok(())
        }
        Err(ShopError::ErrorNotEnoughStock)
    }
    

    fn handle_online_request(&mut self, request: &Request)-> Result<(), ShopError>{
        self.reserve_stock(request)?;
        
        //Spawnear repartidor
            // Ves a donde tenes que ir, calculas tiempo
            // Tiras rand pa ver si llega o no
            // Mandas al REPARTIDOR? (OTRO ACTOR/THREAD QUE TE MANDE UN MSJ CUANDO LLEGO O CUANDO LE CANCELAN)
            // Devolver el stock en caso de que lo cancelen
            // Mandar msj al ecommers con el resultado final
        Ok(())
    }



}

#[derive(Debug, PartialEq)]
pub enum ShopError {
    ErrorCreatingShop,
    ErrorReadingFile,
    ErrorParsingData,
    ErrorNotEnoughStock,
    ErrorCantCommunicateWithEcommerce
}

impl Handler<Request> for Shop{
    type Result = Result<(), MessageError>;

    fn handle(&mut self, _msg:Request , _ctx: &mut Context<Self>) -> Self::Result {
        println!("WIP: Entre al handler de un request con id: {:?}",thread::current().id());
        println!("WIP: La request que llego es: {:?}",_msg);
        if self.is_physical_request(&_msg){
            return self.handle_physical_request(&_msg).map_err(|error| MessageError::ShopError(error))
        };
        if let Err(error) = self.handle_online_request(&_msg){
            if let ShopError::ErrorNotEnoughStock = error{
                if let Some(ecommerce_addr) = &self.ecomerce_addr{
                    ecommerce_addr.try_send(StockAvailabilityMsg::NotEnoughStock(_msg)).map_err(|_| MessageError::ShopError(ShopError::ErrorCantCommunicateWithEcommerce))?;
                }
            }
        };
        Ok(())
    }
}

impl Handler<EcommerceAddrMsg> for Shop{
    type Result = ();
    
    fn handle(&mut self, _msg:EcommerceAddrMsg , _ctx: &mut Context<Self>) -> Self::Result {
        self.ecomerce_addr = Some(_msg.ecommerce_addr)
    }
}

impl Handler<EnoughStockMsg> for Shop{
    type Result = bool;
    
    fn handle(&mut self, _msg:EnoughStockMsg , _ctx: &mut Context<Self>) -> Self::Result {
        let enough_stock = self.verify_enough_stock(&_msg.request);
        
        let stock_availability = if enough_stock{
            StockAvailabilityMsg::EnoughStock(_msg.request)
        }else{
            StockAvailabilityMsg::NotEnoughStock(_msg.request)
        };
        if let Some(ecommerce_addr) = &self.ecomerce_addr{
            if ecommerce_addr.try_send(stock_availability).is_err(){
                println!("Failed to communicate with Ecommerce")//WIP
            }
        }
        return enough_stock;
    }
}

/// Charges the stock of the shop from the shop stock file. Receives a BufReader with the file
/// and returns a HashMap with the stock: the keys being the ID of the product and the value being
/// the quantity of each.
fn charge_stock_from_file(reader: &mut BufReader<File>) -> Result<HashMap<u32, (u32,u32)>, ShopError>{
    let mut stock = HashMap::new();

    for res_line in reader.lines(){
        //cargar stock
        let line = res_line.map_err(|_| ShopError::ErrorReadingFile)?;
        let vec: Vec<&str> = line.split(',').collect();

        let id: u32   = vec[ID_POS].parse().map_err(|_| ShopError::ErrorParsingData)?;
        let amount: u32   = vec[AMOUNT_POS].parse().map_err(|_| ShopError::ErrorParsingData)?;
        stock.insert(id, (amount,0));
    };  
    Ok(stock)
}

/// Obtains the coordinates of the shop from the shop stock file, reading only the header of the file.
/// Receives a BufReader with the file and returns a tuple with the coordinates.
fn obtain_coordinates(reader: &mut BufReader<File>) -> Result<(i32, i32), ShopError>{
    let mut buf = String::new();
    reader.read_line(&mut buf).map_err(|_| ShopError::ErrorReadingFile)?;
    buf.pop(); // remove \n
    let vec: Vec<&str> = buf.split(',').collect();
    println!("{:?}", vec);
    let x: i32 = vec[X_POS].parse().map_err(|_| ShopError::ErrorParsingData)?;
    let y: i32 = vec[Y_POS].parse().map_err(|_| ShopError::ErrorParsingData)?;
    Ok((x, y))
}



#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn shop_test_1_new_from_file(){
        let shop_from_file = Shop::from("test_files/shop_test_1.txt".to_string()).unwrap();
        let stock = HashMap::from([(1,(5,0)), (2,(4,0)), (3,(6,0))]);
        let pos = Point::new(2, 3);
        let shop = Shop::new(stock, pos);
        assert_eq!(shop_from_file, shop)
    }

    #[test]
    fn shop_test_2_enouncgh_stock_for_puhysical_request(){
        let shop = Shop::from("test_files/shop_test_1.txt".to_string()).unwrap();
        let invalid_request = Request::new(vec![1], vec![100], Point::new(0,0));
        let valid_request = Request::new( vec![1,2], vec![1,1], Point::new(0,0));
        assert!(!shop.verify_enough_stock(&invalid_request));
        assert!(shop.verify_enough_stock(&valid_request));
    }

    #[test]
    fn shop_test_3_can_reduce_stock(){
        let mut shop = Shop::from("test_files/shop_test_1.txt".to_string()).unwrap();
        let request = Request::new( vec![1,2,3], vec![2,4,2], Point::new(0,0));
        shop.reduce_stock(&request);
        let remaind_of_product_1 = shop.stock[&1];
        let remaind_of_product_2 = shop.stock[&2];
        let remaind_of_product_3 = shop.stock[&3];
        assert_eq!(remaind_of_product_1, (3,0));
        assert_eq!(remaind_of_product_2, (0,0));
        assert_eq!(remaind_of_product_3, (4,0));
    }

    //WIP: Test reserve_stock
    
}