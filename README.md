# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

 - No se implementa la interfaz del middleware. 
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg  "Mutabilidad de Elementos")
*Fig. 2: Elementos mutables e inmutables*

A modo de referencia, en la *Figura 2* se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.

---

## Informe

### Setup necesario

Para tener un mayor control de los datos que viajan a traves de los distintos nodos de la aplicacion, se agregaron tres utilidades al MessageHandler del gateway:

- Al momento de instanciarlo se crea un clientId para identificar univocamente al cliente (se usa un timestamp con un desvio random para evitar colisiones, podria usarse uuid).

- Cada vez que se llama al metodo SerializeDataMessage, se incrementa en 1 un contador interno el cual lleva el conteo de todos los registros que van siendo serializados (notese que no se suma la amount de cada fruitItem pues no cumpliria con la condicion de uso opaco).

- Al momento que se recibe SerializeEOFMessage se envia un EOFMessage el cual internamente se le carga la cantidad de registros que fueron serializados.

Una vez realizada la serializacion de mensajes, los mismos son enviado por una Queue la cual ira repartiendo los mensajes con una estrategia de round robin a los distintos nodos Sum.

### Coordinacion entre nodos Sum

#### Flujo General
Cada nodo Suma se mantiene escuchando de dos fuentes, de su inputQueue y de su communicationExchange el cual esta configurado para que todo mensaje que se envie por este medio le llegue a todas las instancias de Suma. Esto implica tener 2 go routines consumiendo de las dos fuentes.

Los nodos suman van a operar con normalidad procesando los mensages de datos y llevando cuenta de cuantos registros de procesaron para un cliente en un contador ownCount. Eventualmente llegara el mensaje de EOF, En ese moemnto el nodo que lo recibe lo envia al exchange de comunicacion de esta forma podemos dividir el flujo logico del flujo de datos. 

Cuando los nodos suma reciben el mensaje de EOF previamnete broadcasteado , marcan localmente que el cliente el cual termino su envio ahora esta en etapa de done y el total de registros del mismo (dato parte del mensaje desde el MessageHandler), una vez hecho esto se notifica por medio del communicationExchange la cantidad de registros que proceso cada nodo suma

Al recibir el mensaje que contiene la cantidad de registros procesados por otro nodo Sum de forma se suma localmente en un contador peerCount.

Finalmente en el momento en el que el conteo local y el conteo de peers sea igual a la totalidad de registros enviados del cliente, se pasa a la fase de envio a los nodos de Aggregacion

#### Manejo de Race Conditions
Ya que los nodos Suma tienen dos go routines, Puede pasar el caso de que se procesa un mensaje de EOF sin haber terminado de procesar los mensajes de datos, Para ello se abstrajo la logica de conteo y procesamiento de registros en una Struct Accumulator el cual internamente asegura atomicidad de las operaciones a traves de un Mutex.  

### Coordinacion entre nodos Sum y Aggregation

Al momento del envio de datos desde un Nodo Sum, se toman todos los datos procesados y se les aplica a todos una funcion determinisitica de hashing la cual toma el nombre de la fruta de cada fruitItem, se le aplica la funcion de hashing y se uso el modulo de la cantidad de agregadores del sistema, de esa forma podemos decir que si la funcion de hashing es lo suficiente mente buena, siempre podremos distribuir los fruiItems de forma parcialmente equitativa

Despues de haber enviado la informacion procesada por los nodos Sum, se le envia a todas las intancias de aggregation un mensaje de EOF

Los nodos de Agregation procesaran los mensajes de datos que le vayan llegando, y realizara el top parcial de sus frutas si y solo si, la cantidad de EOF recibida el igual a la cantidad de nodos suma. 

### Coordinacion entre nodos Aggregation y Join

Analogo a la seccion anterior, los nodos de agregacion envian un mensaje con el top parcial procesado y posteriormente se envia un mensaje de EOF

El nodo Join, ira almacenando los tops parciales y realizara el top final si y solo si la cantidad de mensages de EOF recibidos es igual a la cantidad de instancias de nodos de agregacion

### Escalabilidad

Hay varios aspectos a analizar:

- Mayor cantidad de clientes: La aplicacion puede manejar a muchos clientes en simultaneo ya que gracias al clientId se puede identificar sin errores los datos y el estado del pedido de cada cliente.

- Mayor cantidad de datos: De la mano con lo dicho anteriormente, si se tiene un set de datos mas grande aun ya sea porque un cliente tiene muchos datos a procesar o bien mas clientes implican mas datos, el sistema podria levantar mas nodos de Sum para repartir mas trabajo (ya que gracias a rabbit, la distribucion de datos ocurre usando Round Robin)

- Mayor cantidad de tipos de fruta: En caso de haber mayor numero de frutas (numero el cual es infimo en comparacion a la cantidad de datos), podemos decir que distribuimos bien los datos a los nodos de Aggregation siempre y cuando la funcion de hashing utilizada este bien optimizada para distribuir de forma lo suficientemente equitativa. No obstante siempre podemos sumar mas nodos de Aggregation si asi quisieramos
