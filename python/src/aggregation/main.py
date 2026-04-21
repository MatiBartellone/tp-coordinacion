import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        # Cada Aggregator se suscribe a su propio routing key en el exchange.
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        # Estado por cliente.
        self.fruit_top_by_client = {}   # {client_id: [FruitItem sorted]}
        self.eof_sums_by_client = {}    # {client_id: set(sum_ids)}

    def _process_data(self, client_id, fruit, amount):
        """Acumula la cantidad para una fruta de un cliente, manteniendo el
        orden por bisect para poder extraer el top eficientemente."""
        fruit_top = self.fruit_top_by_client.setdefault(client_id, [])
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                updated = fruit_top.pop(i) + fruit_item.FruitItem(fruit, amount)
                bisect.insort(fruit_top, updated)
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id, sum_id):
        """Registra que un Sum termino. Cuando todos los Sums reportaron EOF,
        computa el top parcial y lo envia al Joiner."""
        eof_sums = self.eof_sums_by_client.setdefault(client_id, set())
        eof_sums.add(sum_id)

        logging.info(
            "Aggregator %d: EOF de Sum %d para cliente %s (%d/%d)",
            ID, sum_id, client_id[:8], len(eof_sums), SUM_AMOUNT,
        )

        if len(eof_sums) < SUM_AMOUNT:
            return

        # Todos los Sums terminaron -> computar top parcial.
        fruit_top = self.fruit_top_by_client.get(client_id, [])
        fruit_chunk = list(fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        top = list(
            map(
                lambda fi: (fi.fruit, fi.amount),
                fruit_chunk,
            )
        )

        # Enviar top parcial al Joiner.
        msg = message_protocol.internal.make_partial_msg(client_id, ID, top)
        self.output_queue.send(message_protocol.internal.serialize(msg))

        # Limpiar estado de este cliente.
        self.fruit_top_by_client.pop(client_id, None)
        self.eof_sums_by_client.pop(client_id, None)

    def process_message(self, message, ack, nack):
        parsed = message_protocol.internal.deserialize(message)
        mtype = message_protocol.internal.msg_type(parsed)

        if mtype == message_protocol.internal.TYPE_DATA:
            self._process_data(
                message_protocol.internal.msg_client_id(parsed),
                message_protocol.internal.msg_fruit(parsed),
                message_protocol.internal.msg_amount(parsed),
            )
        elif mtype == message_protocol.internal.TYPE_AGG_EOF:
            self._process_eof(
                message_protocol.internal.msg_client_id(parsed),
                message_protocol.internal.msg_sum_id(parsed),
            )
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_message)

    def shutdown(self):
        logging.info("Shutting down Aggregator %d", ID)
        try:
            self.input_exchange.stop_consuming()
        except Exception:
            pass
        try:
            self.input_exchange.close()
        except Exception:
            pass
        try:
            self.output_queue.close()
        except Exception:
            pass


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARNING)
    aggregation_filter = AggregationFilter()
    signal.signal(signal.SIGTERM, lambda _sig, _frame: aggregation_filter.shutdown())
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
