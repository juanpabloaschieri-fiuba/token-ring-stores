# Trabajo Practico - Tecnicas de Programacion Concurrentes - 2C 2023 - Los Petrifica2++

Informe explicativo sobre la solucion general del TP  

# Entidades creadas
Las entidades creadas fueron modeladas con actores (por ejemplo, el `Shop` y el `E-Commerce`), mientras que otras  fueron creadas con threads tradicionales (por ejemplo, el `Delivery`, el `TokenRingSender` y `TokenRingReceiver`).

# Shop
El Shop es el actor principal del sistema, es el encargado de manejar el stock de los productos, y de recibir los pedidos. Se puede pensar como una sucursal de una cadena de tiendas, y cada Shop tiene un stock propio de productos. Esta implementado de la siguiente forma: 
```rs
pub struct Shop{
    stock: HashMap<u32, (u32, u32)>,
    location: Point,
    ecomerce_addr: Option<Addr<Ecommerce>>,
    delivery_sender: Sender<Request>
}
```
Donde:
* `stock`: Es un HashMap que tiene como clave el id del producto, y como valor una tupla que contiene la cantidad de stock disponible y la cantidad de stock reservada (es el stock que se esta mandando al cliente cuando este lo pidio por el e-commerce). Cabe destacar que cada stock es propio de cada Shop, y que no hay un stock global de productos (por eso tampoco es necesario hacerlo un recurso compartido y que este detras de un lock para que varios threads/actores puedan acceder a el de forma segura).
* `location`: Es la ubicacion del Shop, y esta representada por un `Point` (un struct que tiene dos campos, `x` e `y`, que representan la ubicacion en un plano cartesiano). Esto lo hacemos para poder calcular la distancia entre un Shop y el destino de un pedido, y asi poder determinar cual es el Shop mas cercano al destino para realizar el envio.
* `ecommerce_addr`: Es el Address del actor del E-Commerce, y se usa para poder comunicarse con el E-Commerce para, por ejemplo, notificarle sobre la falta de stock para un pedido.
* `delivery_sender`: Es el Sender end de un `mpsc::channel`, y se usa para poder comunicarse con el Delivery del Shop. El receptor de este canal es el Delivery, y el sender es el Shop. Se usa para mandarle al Delivery los pedidos que deben ser enviados.

La creacion del stock de un Shop se realiza al inicio del programa, dado por un archivo llamado `shop_stock_n.txt`, que tiene el siguiente formato:
```
coordenada_x coordenada_y
nombre_producto_1,id_producto_1,cantidad_stock_disponible_1,
nombre_producto_2,id_producto_2,cantidad_stock_disponible_2,
...
nombre_producto_n,id_producto_n,cantidad_stock_disponible_n,
```
De esta manera, el stock de un Shop se crea a partir de un archivo, y cada Shop tiene su propio archivo de stock, asi como tambien su propia ubicacion.

# E-Commerce
El E-Commerce es el actor que se encarga de recibir los pedidos realizados por la plataforma online. Existe una instacia de E-Commerce por cada Shop, y se encarga de recibir los pedidos, y comunicarse con el Shop correspondiente para verificar que haya stock suficiente. Esta implementado de la siguiente forma:
```rs
pub struct Ecommerce{
    shop_addr: Addr<Shop>,
    ongoing_coordinate_requests: HashMap<usize, CoordinateRequest>,
    next_id: usize,
    message_sender: Sender<RingMessage>,
}
```
Donde:
* `shop_addr`: Es el Address del Shop correspondiente al E-Commerce.
* `ongoing_coordinate_requests`: Es un HashMap que tiene como clave un `usize` que representa el id del pedido, y como valor un `CoordinateRequest` (explicado mas adelante).
* `next_id`: Es un `usize` que representa el id del proximo pedido a ser realizado. Se usa para poder identificar univocamente cada pedido.
* `message_sender`: Es el Sender end de un `mpsc::channel`, y se usa para poder comunicarse con el `TokenRingSender` (explicado mas adelante).
---
Ambas entidades fueron modeladas como actores, porque su Runtime es manejado por la libreria `actix` y no hay que tener una logica de manejo de threads, ademas de que es una buena forma de modelar entidades que tienen un comportamiento concurrente y se deben comunicar entre si. 

# Simulador de pedidos
El simulador de pedidos es el encargado de generar los pedidos que se realizan en el Shop, a partir de un archivo que contiene el tiempo de arribo del pedido como . 

La logica de los pedidos se realiza con la lectura de un archivo con el siguiente formato:
```
tiempo_sleep_1
id_producto_1,cantidad_producto_1;id_producto_2,cantidad_producto_2;...;id_producto_n,cantidad_producto_n
tiempo_sleep_2
id_producto_1,cantidad_producto_1;id_producto_2,cantidad_producto_2;...;id_producto_n,cantidad_producto_m
...
tiempo_sleep_i
id_producto_1,cantidad_producto_1;id_producto_2,cantidad_producto_2;...;id_producto_n,cantidad_producto_j
```

Cada pedido tiene un tiempo de espera asociado (para que se permita tambien la entrada de pedidos desde el E-Commerce), y que cada pedido tiene un formato de `id_producto,cantidad_producto`. Esto se debe a que un cliente puede pedir mas de un producto a la vez, y por eso se usa el `;` como separador de productos.

El simulador corre sobre un thread aparte, y el simulador se inicia escribiendo `simulate` en la consola cuando el programa esta corriendo. El archivo de pedidos se selecciona segun el Shop que se quiera simular (primer argumento para `cargo run`). Cuando se inicia se encarga de leer el archivo de pedidos y realizar el timeout correspondiente, escuchando sobre un canal cuya unica finalidad es notificar al simulador si se quiere terminar el programa prematuramente. Cumplido este timeout, se lee la linea correspondiente al pedido, y se manda el pedido al shop si es fisico o al ecommerce si es online. Luego se repite el proceso hasta que se termine de leer el archivo de pedidos.

## Pedidos presenciales
Los pedidos presenciales son aquellos que se realizan en la sucursal presencial del Shop, y se tratan de la siguiente manera:
1. Primero, se verifica que haya stock suficiente para el pedido. Si no hay stock suficiente, se le notifica al cliente que no se puede realizar el pedido.
2. Si hay stock suficiente, directamente se resta el stock de la tienda porque el cliente se lleva el producto en el momento.

## Pedidos por E-Commerce
Los pedidos realizados por la plataforma del E-Commerce se tratan casi identicamente a aquellos que son realizados presencialmente en el Shop correspondiente. La diferencia es que, para los pedidos realizados por la plataforma online, se debe tener en cuenta el _envio_ del producto hacia el destinatario, y la reserva de stock correspondiente.  
Un requisito del trabajo era que el pedido debia ser entregado a destino por la sucursal mas cercana a la locacion de la solicitud, y si bien esto podia tener un manejo muy simple (modelar de forma aleatoria), optamos por una implementacion mas realista:  
Recordando que un Shop tiene un atributo que es su `location` como un `Point`, y un pedido online debe tener una "direccion" a donde ser enviado (implementado tambien con un `Point`), lo que hacemos es tomar la distancia entre el destino y cada shop para ver cual esta mas cerca para realizar el envio.  
De esta manera en vez de quedar aleatoriamente determinado, aseguramos de que el tiempo de envio sea optimo.  
  
## Envio de pedidos realizados via E-Commerce
Quedando abatido lo anterior, queda por verse el envio en si del producto. Para esta parte, podemos pensar que cada Shop tiene asociado un sistema de _envio de pedidos_ (algo analogo a como , por ejemplo, un restaurante contrata a PedidosYa para realizar el delivery de comida), y cada vez que el Shop quiere mandar un pedido, se lo delega al sistema de Delivery, llamado `Delivery`   

### Implementacion del struct Delivery
Este struct tiene los siguientes campos:
```rs
struct Delivery{
    shop_addr: Addr<Shop>,
    receiver: Receiver<Request>,
    ongoing_deliveries: BinaryHeap<DeliveryOrder>,
    shop_location:  Point,
    starting_instant: Instant
}
```
* `shop_addr`: Contiene el Address correspondiente al Actor del Shop
* `receiver`: El receiver end de un `mpsc::channel`, usado para recibir los pedidos a ser enviados, y para esperar a que llegue un pedido a destino. El sender end de este canal es el `Shop` que delega el pedido al `Delivery`.
* `ongoing_deliveries`: Es un Heap Minimal, contiene los tiempos de llegada relativos de cada pedido. Decidimos usar esta estructura de datos para poder tener en todo momento el tiempo minimo de llegada de un pedido, y asi poder dormir el thread del Delivery hasta que llegue el proximo pedido.
* `shop_location`: La ubicacion del Shop, usada para calcular la distancia entre el Shop y el destino del pedido.
* `starting_instant`: Representa el instante de tiempo donde empieza la entrega de un pedido. Cada vez que llega un nuevo pedido o finaliza la entrega de uno, se actualiza este campo con el instante de tiempo actual.  WIP
  
### Implementacion del struct DeliveryOrder
El `DeliveryOrder` es un struct creado con la finalidad de poder ordenar los pedidos que deben ser enviados por el Delivery en el Heap. Tiene los siguientes campos:
```rs
struct DeliveryOrder{
    time_to_arrive: u64,
    request: Request
}
```
* `time_to_arrive`: El tiempo que le toma al Delivery llegar al destino del pedido. Se calcula como la distancia entre el Shop y el destino, multiplicado por la velocidad del Delivery (definido como una constante) y se le suma el campo `starting_instant` del Delivery.
* `request`: El pedido a ser enviado.

## Funcionamiento del Delivery
Una vez que el Shop haya procurado tener el stock suficiente para el pedido, se lo delega a su Delivery asociado mandando la orden por medio del channel `sender` del Shop. A partir de aca se realizan las siguientes acciones:
1. Primero, se hace la correspondiente reserva del stock del producto en el Shop, como para un determinado id de un producto tenemos dos campos (que nos indican la cantidad de stock disponible y la cantidad reservada), restamos del stock disponible y sumamos a la cantidad reservada la cantidad de productos pedidos.
2. Luego el Shop informa al delivery que debe enviar un pedido, mandando un mensaje por el channel `sender` del Delivery. Este mensaje es un `Request`, que contiene: 
    * El id del producto pedido
    * La cantidad de productos pedidos
    * La ubicacion del destino del pedido
3. El Delivery recibe el mensaje, origina con el un `DeliveryOrder` y lo inserta en el Heap de pedidos pendientes. Los pedidos se ordenan en el heap por el tiempo de llegada a destino, de menor a mayor, teniendo en cuenta tambien el instante de tiempo en el que se encuentra el `Delivery`, que lo podemos pensar como la hora del dia: si el pedido se realiza a las 10 de la mañana, y el tiempo de llegada es de 2 horas, el pedido llegara a las 12 del mediodia. Si el pedido se realiza a las 11 de la mañana, el pedido llegara a las 13 horas.
4. Una vez que el pedido esta en el Heap, el `Delivery` se duerme (modelado como una escucha con timeout desde el receptor `receiver` del channel). Aca pueden pasar dos escenarios:
    * Si el pedido que esta primero en el heap (el que se esta esperando que llegue) ya llego, se lo saca del heap y, dependiendo si llego a tiempo o no (si el pedido fue cancelado), se actualiza el stock del Shop acordemente. Luego se procede a esperar a la llegada del proximo producto para repetir el procedimiento, o a quedarse escuchando del channel si no hay mas pedidos pendientes.
    * Si el pedido que esta primero en el heap todavia no llego, y se le manda al Delivery otro producto a ser enviado, se lo inserta en el heap, y se actualiza el tiempo de espera del Delivery. Esto se cumple y funciona tanto si llega un producto con un tiempo de espera menor o mayor al que teniamos inicialmente, en ambos casos se actualiza el tiempo de espera del Delivery. 

Esta implementacion contempla que el Delivery puede tener mas de un pedido pendiente, y que los mismos se van a ir entregando en el orden que corresponda.  
Cabe destacar que el envio de los pedidos con esta implementacion se simula **paralelamente** gracias al paso del tiempo. Por ejemplo:  
* Si son las 10 de la mañana, y se pide un producto que tarda 2 horas en llegar, el pedido llegara a las 12 del mediodia.
* Si a las 10:30 se pide otro producto que tarda 1 hora en llegar, el pedido llegara a las 11:30.
* Mientras tanto, el pedido que se realizo a las 10 de la mañana sigue en camino, y llegara a las 12 del mediodia.   
  
Con esto, el Delivery de un Shop en particular puede ser pensado como un sistema de envio de pedidos que tiene una cantidad infinita de repartidores, y que cada repartidor manda 1 solo pedido a la vez.

# Token Ring
El Token Ring es un Struct para cada instancia de shop que contiene los siguientes campos:
```rs
pub struct TokenRing{
    sender_join_handle: JoinHandle<Result<(), TokenRingError>>,
    receiver_join_handle: JoinHandle<Result<(), TokenRingError>>,
    node_addr: SocketAddr,
    socket: UdpSocket,
}
```
Donde:
* `sender_join_handle`: Es el JoinHandle del thread que se encarga de enviar los mensajes del Token Ring.
* `receiver_join_handle`: Es el JoinHandle del thread que se encarga de recibir los mensajes del Token Ring.
* `node_addr`: Es la direccion del nodo actual.
* `socket`: Es el socket UDP que se usa para enviar y recibir mensajes al `TokenRingReceiver`.

El struct funciona como un wrapper para iniciar y terminar con el `TokenRingReceiver` y el `TokenRingSender`, ademas de que puede conectar y desconectar ambos threads del Token Ring.

## TokenRingReceiver
El `TokenRingReceiver` es el thread que se encarga de recibir los mensajes que circulan a traves del Token Ring. Esta implementado de la siguiente forma:
```rs
pub struct TokenRingReceiver{
    node_id: u16,
    socket: UdpSocket,
    ecommerce_addr: Addr<Ecommerce>,
    msg_sender: Sender<RingMessage>,
    ring_addresses: SharedRingAddresses,
    ring_topology: SharedRingTopology,
    created_messages: SharedCreatedMessages,
    connected: bool 
}
```
Donde:
* `node_id`: Es el id del nodo actual.
* `socket`: Es el socket UDP que se usa para recibir mensajes, ya sea de otros nodos conectados al Token Ring o de, por ejemplo, el Ecommerce para mandar un mensaje de una request.
* `ecommerce_addr`: Es el Address del Ecommerce, y se usa para poder mandarle mensajes al Ecommerce.
* `msg_sender`: Es el Sender end de un `mpsc::channel`, y se usa para poder mandarle mensajes al `TokenRingSender`.
* `ring_addresses`: Es un hash map que contiene como clave el id de un nodo, y como valor la direccion del socket UDP de ese nodo.
* `ring_topology`: Es una cola que contiene las ids de los nodos conectados al Token Ring relativos al nodo actual.
* `created_messages`: Es un struct que contiene un hash map que tiene como clave el id de un mensaje, y como valor el mensaje en cuestion
* `connected`: Es un booleano que indica si el nodo actual esta conectado al Token Ring o no.

Los campos que tienen el prefijo `Shared` son aquellos que son compartidos entre el `TokenRingReceiver` y el `TokenRingSender`, y se usan para poder comunicarse entre si. Estos estan detras de un `Arc<Mutex<T>>` para poder ser compartidos entre threads de forma segura.

El `TokenRingReceiver` se encarga de recibir los mensajes que circulan a traves del Token Ring, y de mandarle los mensajes al `TokenRingSender` para que este los mande al siguiente nodo. Para esto, el `TokenRingReceiver` se queda escuchando sobre el socket UDP, y cuando recibe un mensaje, lo procesa y lo manda al `TokenRingSender` para que este lo mande al siguiente nodo.

## TokenRingSender
El `TokenRingSender` es el thread que se encarga de enviar los mensajes que circulan a traves del Token Ring. El envio de mensajes a traves del Token Ring se realiza siguiendo el metodo `stop-and-wait`. Para esto, implementamos una ventana de mensajes llamada `Window`, de esta forma podemos tener varios mensajes circulando a traves del Token Ring al mismo tiempo, mandados por el mismo nodo. 
A su vez, como la comunicacion se hace a través de un socket UDP, no tenemos la garantia de que el mensaje llegue a destino, por lo que implementamos un mecanismo de reenvio de mensajes. Este mecanismo se basa en un timeout, y si el mensaje no llega a destino en un tiempo determinado, se lo vuelve a mandar. Para chequear si el mensaje llego a destino, uno de los mensajes que se mandan es un ACK, y si el nodo que manda el mensaje no recibe el ACK en el timeout, se lo vuelve a mandar. 

Si un mensaje timeoutea muchas veces, se considera que el nodo al que se le esta mandando el mensaje se desconecto del Token Ring, y se lo saca de la topologia del Token Ring. Esto se hace para evitar que se queden nodos desconectados del Token Ring, y que se queden esperando mensajes que nunca van a llegar, y tambien causando un corte en la comunicacion con los otros nodos.

Si un mensaje se quiere mandar cuando la ventana esta llena, se lo guarda en una cola de mensajes, y cuando se libera un lugar en la ventana, se manda el mensaje que esta en la cola. De todas formas existe un metodo del `TokenRingSender` que permite mandar un mensaje sin importar si la ventana esta llena o no para casos especificos.
