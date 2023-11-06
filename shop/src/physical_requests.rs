use std::thread;
use std::thread::JoinHandle;
use std::fs::File;
use std::io::{BufReader, BufRead};
use std::time::Duration;
use actix::prelude::*;
use crate::shop::*;
///13 //tiempo hasta pedido
///ida,5;idb7,8;
/// 

#[derive(Debug)]
enum PhysicalRequestError {
    FileError(FileError),
    ErrorParsingData
}

fn simulate_physical_requests(path: String, shop_addr: Addr<Shop>) -> JoinHandle<Result<(), PhysicalRequestError>>{
    thread::spawn(move || _simulate_physical_requests(path, shop_addr))
}

fn _simulate_physical_requests(path:String, shop_addr: Addr<Shop>)-> Result<(), PhysicalRequestError>{
    let orders = File::open(path).map_err(|_| PhysicalRequestError::FileError(FileError::ErrorOpeningFile))?;
    let mut reader = BufReader::new(orders);
    loop {
        match read_request(&mut reader) {
            Ok(request) => {
                print!("WIP: Voy a dormir por: {:?}",request.delay);
                thread::sleep(request.delay);
                if shop_addr.try_send(request).is_err(){
                    print!("FAILED");
                }
            },
            Err(error) => {
                if let PhysicalRequestError::FileError(FileError::ErrorEof) = error{
                    return Ok(())
                }
                return Err(error)
            }
        }
    }
}

fn read_delay(reader: &mut BufReader<File>) -> Result<Duration, PhysicalRequestError> {
    let delay = read_line(reader).map_err(|file_error| PhysicalRequestError::FileError(file_error))?;
    let delay: u32 = delay.parse().map_err(|_| PhysicalRequestError::ErrorParsingData)?;
    Ok(Duration::from_secs(delay as u64))
}


fn read_order(reader: &mut BufReader<File>) -> Result<(Vec<u32>, Vec<u32>), PhysicalRequestError> {
    let mut request_products: Vec<u32> = Vec::new();
    let mut request_amounts: Vec<u32> = Vec::new();

    let buf = read_line(reader).map_err(|file_error| PhysicalRequestError::FileError(file_error))?;
    let products: Vec<&str> = buf.split(';').collect();
    
    for product in products{
        let product: Vec<&str> = product.split(',').collect();
        let id: u32 = product[0].parse().map_err(|_| PhysicalRequestError::ErrorParsingData)?;
        let amount: u32 = product[1].parse().map_err(|_| PhysicalRequestError::ErrorParsingData)?;
        request_products.push(id);
        request_amounts.push(amount);
    }
    Ok((request_products,request_amounts))
}

fn read_request(reader: &mut BufReader<File>) -> Result<PhysicalRequest,PhysicalRequestError>{
    println!("hola");
    let delay = read_delay(reader)?;
    println!("hola2");
    let (request_products, request_amounts) = read_order(reader)?;
    println!("hola3");
    Ok(PhysicalRequest::new(delay,request_products,request_amounts))
}

#[derive(Debug)]
enum FileError{
    ErrorReadingLine,
    ErrorOpeningFile,
    ErrorEof
}

fn read_line(reader: &mut BufReader<File>)->Result<String, FileError>{
    let mut buf = String::new();
    if reader.read_line(&mut buf).map_err(|_| FileError::ErrorReadingLine)? == 0{
        return Err(FileError::ErrorEof);
    };
    if buf.ends_with('\n'){
        buf.pop();
    }
    Ok(buf)
}

#[cfg(test)]

mod test {
    use super::*;

    fn get_physical_requests_test_1_requests()-> (PhysicalRequest, PhysicalRequest){
        let p1 = PhysicalRequest::new(Duration::from_secs(2), Vec::from([1,2]), Vec::from([2,1]));
        let p2 = PhysicalRequest::new(Duration::from_secs(3), Vec::from([1,3]), Vec::from([1,1]));
        (p1,p2)
    }

    #[test]
    fn test_1_read_request(){
        let path = String::from("test_files/physical_requests_test_1.txt");
        let file = File::open(path).unwrap();
        let mut reader = BufReader::new(file);
        let request1 = read_request(&mut reader).unwrap();
        let request2 = read_request(&mut reader).unwrap();
        let (p1, p2) = get_physical_requests_test_1_requests();
        assert_eq!(request1, p1);
        assert_eq!(request2, p2);
    }
    
}