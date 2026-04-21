import os
import logging
import signal
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

# Exchange fanout para notificar EOF a todas las instancias de Sum.
SUM_EOF_EXCHANGE = "sum_eof_fanout"
# Routing key unica — en un fanout se ignora, pero la usamos como binding
# para que cada instancia reciba todas las notificaciones.
SUM_EOF_KEY = "eof"


class SumFilter:
    def __init__(self):
        # Cola de datos compartida (work queue, round-robin entre Sums).
        self.data_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        # Exchange para publicar la señal de EOF a los demas Sums.
        self.eof_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_EOF_EXCHANGE, [SUM_EOF_KEY]
        )

        # Exchange para recibir la señal de EOF de los demas Sums.
        # Cada instancia crea su propia cola exclusiva -> todas reciben la copia.
        self.eof_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_EOF_EXCHANGE, [SUM_EOF_KEY]
        )

        # Un exchange por cada Aggregator, con routing key especifico.
        self.agg_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            ex = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.agg_exchanges.append(ex)

        # Estado: acumulados por cliente.
        self.fruits_by_client = {}  # {client_id: {fruta: FruitItem}}
        self.flushed = set()        # client_ids ya vaciados por este Sum

    # --- Procesamiento de datos ---

    def _accumulate(self, client_id, fruit, amount):
        """Acumula la cantidad para una fruta de un cliente."""
        by_fruit = self.fruits_by_client.setdefault(client_id, {})
        by_fruit[fruit] = by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _flush(self, client_id):
        """Envia los acumulados de un cliente a los Aggregators segun hash y
        luego envia un EOF con el sum_id a cada Aggregator."""
        if client_id in self.flushed:
            return
        self.flushed.add(client_id)

        by_fruit = self.fruits_by_client.pop(client_id, {})

        # Enviar cada fruta al Aggregator determinado por hash.
        for item in by_fruit.values():
            agg_idx = hash(item.fruit) % AGGREGATION_AMOUNT
            msg = message_protocol.internal.make_data_msg(
                client_id, item.fruit, item.amount
            )
            self.agg_exchanges[agg_idx].send(
                message_protocol.internal.serialize(msg)
            )

        # Enviar EOF (con sum_id) a cada Aggregator para que cuenten.
        eof = message_protocol.internal.make_agg_eof_msg(client_id, ID)
        serialized_eof = message_protocol.internal.serialize(eof)
        for ex in self.agg_exchanges:
            ex.send(serialized_eof)

    # --- Callbacks ---

    def _on_data(self, message, ack, nack):
        """Callback para mensajes de la work queue de datos."""
        parsed = message_protocol.internal.deserialize(message)
        mtype = message_protocol.internal.msg_type(parsed)

        if mtype == message_protocol.internal.TYPE_DATA:
            self._accumulate(
                message_protocol.internal.msg_client_id(parsed),
                message_protocol.internal.msg_fruit(parsed),
                message_protocol.internal.msg_amount(parsed),
            )
        elif mtype == message_protocol.internal.TYPE_EOF:
            client_id = message_protocol.internal.msg_client_id(parsed)
            # Notificar a todas las instancias de Sum (incluido yo).
            notify = message_protocol.internal.make_sum_eof_msg(client_id)
            self.eof_publisher.send(
                message_protocol.internal.serialize(notify)
            )
            # Hacer flush local.
            self._flush(client_id)
        ack()

    def _on_eof_broadcast(self, message, ack, nack):
        """Callback para mensajes del exchange fanout de EOF entre Sums."""
        parsed = message_protocol.internal.deserialize(message)
        mtype = message_protocol.internal.msg_type(parsed)

        if mtype == message_protocol.internal.TYPE_SUM_EOF:
            client_id = message_protocol.internal.msg_client_id(parsed)
            self._flush(client_id)
        ack()

    def start(self):
        # Thread para consumir del exchange de EOF.
        # Justificacion GIL: ambos threads son I/O-bound (bloqueados en pika
        # socket.recv), asi que el GIL no interfiere con el paralelismo.
        eof_thread = threading.Thread(
            target=self.eof_consumer.start_consuming,
            args=(self._on_eof_broadcast,),
            daemon=True,
        )
        eof_thread.start()

        # Consumo principal de la work queue de datos (bloqueante).
        self.data_queue.start_consuming(self._on_data)

    def shutdown(self):
        logging.info("Shutting down Sum %d", ID)
        try:
            self.data_queue.stop_consuming()
        except Exception:
            pass
        try:
            self.eof_consumer.stop_consuming()
        except Exception:
            pass
        try:
            self.data_queue.close()
        except Exception:
            pass
        try:
            self.eof_consumer.close()
        except Exception:
            pass
        try:
            self.eof_publisher.close()
        except Exception:
            pass
        for ex in self.agg_exchanges:
            try:
                ex.close()
            except Exception:
                pass


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARNING)
    sum_filter = SumFilter()
    signal.signal(signal.SIGTERM, lambda _sig, _frame: sum_filter.shutdown())
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
