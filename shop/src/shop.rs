use actix::{Actor, Context, Handler, System, Addr, Message, WrapFuture};
use std::{collections::HashMap, thread, time::Duration, fs::File, io::{BufRead, BufReader}};
use crate::point::*;

const X_POS: usize = 0;
const Y_POS: usize = 1;

const ID_POS: usize = 1;
const AMOUNT_POS: usize = 2;


#[derive(Message)]
#[rtype(result = "bool")]
struct HayZapatillas();


#[derive(PartialEq, Debug)]
struct Shop{
    stock: HashMap<u32, u32>,
    position: Point,
}

#[derive(Debug)]
enum ShopError {
    ErrorCreatingShop,
    ErrorReadingFile,
    ErrorParsingData,
}

fn charge_stock_from_file(reader: &mut BufReader<File>) -> Result<HashMap<u32, u32>, ShopError>{
    let mut stock = HashMap::new();

    for res_line in reader.lines(){
        //cargar stock
        let mut line = res_line.map_err(|_| ShopError::ErrorReadingFile)?;
        let vec: Vec<&str> = line.split(',').collect();
        //line.pop();
        let id: u32   = vec[ID_POS].parse().map_err(|_| ShopError::ErrorParsingData)?;
        let amount: u32   = vec[AMOUNT_POS].parse().map_err(|_| ShopError::ErrorParsingData)?;
        stock.insert(id, amount);
    };  
    Ok(stock)
}

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

impl Shop{
    fn new(stock: HashMap<u32, u32>, position: Point) -> Shop{
        Shop{stock, position}
    }
    
    ///"
    /// x,y //posicion
    /// nombre,id,cant
    /// nombre,id,cant
    /// "
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

}

impl Actor for Shop{
    type Context = Context<Self>;
}

impl Handler<HayZapatillas> for Shop{
    type Result = bool;

    fn handle(&mut self, _msg:HayZapatillas , _ctx: &mut Context<Self>) -> Self::Result {
        println!("Entre al handler y digo que hay zapas en thread: {:?}",thread::current().id());
        true
    }
}


#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn shop_test_1_new_from_file(){
        let shop_from_file = Shop::from("test_files/shop_test_1.txt".to_string()).unwrap();
        let stock = HashMap::from([(1,5), (2,4), (3,6)]);
        let pos = Point::new(2, 3);
        let shop = Shop::new(stock, pos);
        assert_eq!(shop_from_file, shop)
    }

}

/*
zapatilla,1,5
remera,2,3
gorra,3,6
*/