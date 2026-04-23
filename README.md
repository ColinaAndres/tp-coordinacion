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

Solucion desarrollada en Golang

### Setup necesario

Para tener un mayor control de los datos que viajan a través de los distintos nodos de la aplicación, se agregaron tres utilidades al MessageHandler del gateway:

- Al momento de instanciarlo se crea un `clientId` para identificar unívocamente al cliente (se usa un timestamp con un desvío random para evitar colisiones, podría usarse UUID).

- Cada vez que se llama al método `SerializeDataMessage`, se incrementa en 1 un contador interno el cual lleva el conteo de todos los registros que van siendo serializados (nótese que no se suma la `Amount` de cada `fruitItem` pues no cumpliría con la condición de uso opaco).

- Al momento que se recibe `SerializeEOFMessage` se envía un `EOFMessage` el cual internamente se le carga la cantidad de registros que fueron serializados.

Una vez realizada la serialización de mensajes, los mismos son enviados por una `Queue` la cual irá repartiendo los mensajes con una estrategia de round robin a los distintos nodos `Sum`.

### Coordinación entre nodos Sum

#### Flujo General

Cada nodo `Sum` se mantiene escuchando de dos fuentes, de su `inputQueue` y de su `communicationExchange` el cual está configurado para que todo mensaje que se envíe por este medio le llegue a todas las instancias de `Sum`. Esto implica tener 2 goroutines consumiendo de las dos fuentes.

Los nodos `Sum` van a operar con normalidad procesando los mensajes de datos y llevando cuenta de cuántos registros se procesaron para un cliente en un contador `ownCount`. Eventualmente llegará el `EOFMessage`. En ese momento el nodo que lo recibe lo envía al `communicationExchange`; de esta forma podemos dividir el flujo lógico del flujo de datos.

Cuando los nodos `Sum` reciben el mensaje de EOF previamente broadcasteado, marcan localmente:
- Que el cliente el cual terminó su envío ahora está en etapa de "Done" 
- El total de registros del cliente el cual es almacenado (dato que es parte del mensaje desde el MessageHandler). 

Una vez hecho esto, se notifica por medio del `communicationExchange` la cantidad de registros que procesó cada nodo `Sum`.

Al recibir el mensaje que contiene la cantidad de registros procesados por otro nodo `Sum`, se suma localmente en un contador `peerCount`.

Finalmente, en el momento en el que el conteo local y el conteo de peers sea igual a la totalidad de registros enviados del cliente, se pasa a la fase de envío a los nodos de `Agregation`.

#### Manejo de Race Conditions

Ya que los nodos `Sum` tienen dos goroutines, pueden haber race conditions; para ello se abstrajo la lógica de conteo y procesamiento de registros en una struct `Accumulator` el cual internamente asegura atomicidad de las operaciones a través de un Mutex. Una vez que el Accumulator detecte que se terminó de procesar todos los datos de un cliente, los devuelve atómicamente y los nodos `Sum` pasan a enviarlos.

En el caso borde en que se haya procesado un mensaje de EOF antes de terminar de procesar un mensaje de datos, el `Accumulator` va a procesar los datos con normalidad pero también le indicará al nodo `Sum` que el cliente ya fue marcado como done y por ende debe manejar esos datos nuevos registrados notificándole a sus pares la nueva cantidad sumada.

### Coordinación entre nodos Sum y Aggregation

Al momento del envío de datos desde un Nodo `Sum`, se toman todos los datos procesados y se les aplica a todos una función determinística de hashing la cual toma el nombre de la fruta de cada fruitItem, se le aplica la función de hashing y se usa el módulo de la cantidad de agregadores del sistema. De esa forma podemos decir que, si la función de hashing es lo suficientemente buena, siempre podremos distribuir los `fruitItems` de forma parcialmente equitativa.

Después de haber enviado la información procesada por los nodos `Sum`, se le envía a todas las instancias de Aggregation un mensaje de EOF y finalmente se libera la memoria con los datos del cliente.

Los nodos de `Aggregation` procesarán los mensajes de datos que le vayan llegando, y realizarán el top parcial de sus frutas sí y solo sí la cantidad de EOF recibida es igual a la cantidad de nodos `Sum`.

### Coordinación entre nodos Aggregation y Join

Análogo a la sección anterior, los nodos `Agregation` envían un mensaje con el top parcial procesado, posteriormente se envía un mensaje de EOF y se libera la memoria con los datos del cliente.

El nodo `Join` irá almacenando los tops parciales y realizará el top final sí y solo sí la cantidad de mensajes de EOF recibidos es igual a la cantidad de instancias de nodos de `Agregation`.

Finalmente, el `Join` enviará el top final al `Gateway` y liberará la memoria con los datos del cliente.

### Escalabilidad

Hay varios aspectos a analizar:

- Mayor cantidad de clientes: La aplicación puede manejar a muchos clientes en simultáneo ya que gracias al `clientId` se puede identificar sin errores los datos y el estado del pedido de cada cliente.

- Mayor cantidad de datos: De la mano con lo dicho anteriormente, si se tiene un set de datos más grande aun, ya sea porque un cliente tiene muchos datos a procesar o bien más clientes implican más datos, el sistema podría levantar más nodos de `Sum` para repartir más trabajo (ya que gracias a Rabbit, la distribución de datos ocurre usando Round Robin); en otras palabras, los nodos Sum escalan horizontalmente.

- Mayor cantidad de tipos de fruta: En caso de haber mayor número de frutas (número el cual es ínfimo en comparación a la cantidad de datos), podemos decir que distribuimos bien los datos a los nodos de `Aggregation` siempre y cuando la función de hashing utilizada esté bien optimizada para distribuir de forma lo suficientemente equitativa. No obstante, siempre podemos sumar más nodos de `Aggregation `si así quisiéramos.

- Cuello de botella: El posible cuello de botella se encuentra en el nodo `Join` ya que es solo uno (tiene que escalar verticalmente), pero ya que se procesan muchos de los datos de entrada, tener un solo Join resulta aceptable.
