# Informe de Coordinación — TP Sistemas Distribuidos

## Resumen del sistema

El sistema implementa un pipeline distribuido de procesamiento de stock de
frutas, usando un paradigma similar a **MapReduce**:

```
Client → Gateway → Sum (Map) → Aggregation (Reduce) → Join (Final Reduce) → Gateway → Client
```

Cada una de las etapas intermedias puede tener múltiples instancias corriendo
en paralelo, y el sistema soporta múltiples clientes concurrentes.

## Mecanismos de coordinación

### 1. Aislamiento de flujos por cliente

Cada cliente se identifica mediante un UUID generado por el `MessageHandler`
del gateway. Todos los mensajes internos llevan el `client_id`, lo que permite
que los controles (Sum, Aggregation, Joiner) mantengan **estado separado por
cliente** y procesen múltiples flujos concurrentes sin mezclar datos.

### 2. Coordinación entre instancias de Sum

La `input_queue` es una **work queue** compartida entre los N Sums. RabbitMQ
distribuye los mensajes por round-robin, de modo que cada Sum acumula sumas
parciales de un subconjunto de los registros.

El problema: cuando el gateway envía el EOF de un cliente, solo **un** Sum lo
recibe (por la naturaleza de la work queue). Los demás Sums no se enteran de
que los datos de ese cliente terminaron.

**Solución**: el Sum que recibe el EOF publica una notificación en un
**exchange** (`sum_eof_fanout`). Cada instancia de Sum tiene su propia cola
suscripta a ese exchange, de modo que todos (incluido el originador) reciben
la señal. Al recibirla, cada instancia de Sum vacía (flush) sus acumulados
para ese cliente.

Ambas colas (la work queue de datos y la cola del exchange de EOF) se
consumen en un **único event loop** (`channel.start_consuming()`), aprovechando
que el middleware soporta registrar un segundo callback de control. Esto evita
la necesidad de threads o locks, ya que no hay concurrencia intra-proceso:
los callbacks se ejecutan de forma secuencial en el mismo hilo.

### 3. Particionamiento Sum → Aggregator (Shuffle por hash)

En lugar de hacer broadcast de cada fruta a todos los Aggregators (lo cual
genera procesamiento redundante), cada Sum envía cada fruta al Aggregator
determinado por:

```
aggregator_id = hash(nombre_fruta) % AGGREGATION_AMOUNT
```

Esto garantiza que **todas las ocurrencias de una misma fruta** llegan al
**mismo Aggregator**. Se usa `hash()` builtin de Python, que es
**determinístico** entre todos los contenedores de Sum porque el Dockerfile
fija `PYTHONHASHSEED=0`, deshabilitando el salt aleatorio que Python aplica
por defecto en cada sesión.


### 4. Barrera en Aggregator (conteo de EOFs por sum_id)

Cada Sum, al hacer flush, envía un mensaje EOF al Aggregator incluyendo su
`sum_id`. El Aggregator mantiene un **set de sum_ids recibidos** por cada
`client_id`. Cuando el tamaño del set alcanza `SUM_AMOUNT`, significa que
todos los Sums reportaron para ese cliente, y el Aggregator:

1. Computa su **top parcial** de tamaño `TOP_SIZE` sobre las frutas que le
   fueron asignadas por el hash.
2. Lo envía al Joiner junto con su `aggregation_id`.
3. Limpia el estado de ese cliente.

El uso de un set en vez de un contador evita contar duplicados.

### 5. Consolidación en el Joiner (merge de tops parciales)

El Joiner recibe tops parciales de los M Aggregators. Como cada Aggregator
tiene un subconjunto distinto de frutas (por el particionamiento por hash),
el Joiner debe **mergear** estos tops para obtener el resultado final.

El merge usa `bisect.insort` con `FruitItem` para mantener el orden, y
acumula cantidades si la misma fruta aparece en tops de distintos Aggregators
(en principio no debería ocurrir si el hash distribuye correctamente, pero
el merge lo maneja por robustez).

Cuando el Joiner recibe los aportes de `AGGREGATION_AMOUNT` Aggregators
(rastreados por un set de `aggregation_ids`), emite el top final de
`TOP_SIZE` al gateway.

### 6. Manejo de SIGTERM

Todos los controles (Sum, Aggregation, Join) manejan la señal `SIGTERM`
mediante `signal.signal()`. Al recibirla, cada uno:

1. Detiene el consumo de mensajes (`stop_consuming`).
2. Cierra las conexiones con RabbitMQ (`close`).
3. Termina la ejecución limpiamente.

## Escalabilidad

| Dimensión | Mecanismo |
|---|---|
| Clientes simultáneos | UUID por cliente, estado separado en todos los controles |
| Instancias de Sum | Variable `SUM_AMOUNT`, work queue con RR, barrera fanout |
| Instancias de Aggregation | Variable `AGGREGATION_AMOUNT`, hash-routing, barrera en Joiner |
| Top size | Variable `TOP_SIZE`, configurable en docker-compose |

El sistema escala horizontalmente: agregar más Sums o Aggregators solo
requiere cambiar las variables de entorno en el docker-compose.