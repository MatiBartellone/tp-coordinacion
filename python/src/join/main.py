import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        # Estado por cliente.
        self.merged_by_client = {}      # {client_id: [FruitItem sorted]}
        self.seen_aggs_by_client = {}   # {client_id: set(agg_ids)}

    def _merge_partial(self, client_id, partial_top):
        """Mergea un top parcial con los acumulados de ese cliente usando
        bisect.insort para mantener el orden."""
        merged = self.merged_by_client.setdefault(client_id, [])
        for fruit, amount in partial_top:
            found = False
            for i in range(len(merged)):
                if merged[i].fruit == fruit:
                    updated = merged.pop(i) + fruit_item.FruitItem(fruit, amount)
                    bisect.insort(merged, updated)
                    found = True
                    break
            if not found:
                bisect.insort(merged, fruit_item.FruitItem(fruit, amount))

    def _emit_result(self, client_id):
        """Computa el top final y lo envia al gateway via results_queue."""
        merged = self.merged_by_client.get(client_id, [])
        fruit_chunk = list(merged[-TOP_SIZE:])
        fruit_chunk.reverse()
        top = list(
            map(
                lambda fi: (fi.fruit, fi.amount),
                fruit_chunk,
            )
        )
        msg = message_protocol.internal.make_result_msg(client_id, top)
        self.output_queue.send(message_protocol.internal.serialize(msg))

        # Limpiar estado de este cliente.
        self.merged_by_client.pop(client_id, None)
        self.seen_aggs_by_client.pop(client_id, None)

    def process_message(self, message, ack, nack):
        parsed = message_protocol.internal.deserialize(message)
        mtype = message_protocol.internal.msg_type(parsed)

        if mtype == message_protocol.internal.TYPE_PARTIAL:
            client_id = message_protocol.internal.msg_client_id(parsed)
            agg_id = message_protocol.internal.msg_agg_id(parsed)
            partial_top = message_protocol.internal.msg_top(parsed)

            seen = self.seen_aggs_by_client.setdefault(client_id, set())
            if agg_id in seen:
                ack()
                return
            seen.add(agg_id)

            self._merge_partial(client_id, partial_top)

            logging.info(
                "Joiner: top parcial de Agg %d para cliente %s (%d/%d)",
                agg_id, client_id[:8], len(seen), AGGREGATION_AMOUNT,
            )

            if len(seen) == AGGREGATION_AMOUNT:
                self._emit_result(client_id)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_message)

    def shutdown(self):
        logging.info("Shutting down Joiner")
        try:
            self.input_queue.stop_consuming()
        except Exception:
            pass
        try:
            self.input_queue.close()
        except Exception:
            pass
        try:
            self.output_queue.close()
        except Exception:
            pass


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARNING)
    join_filter = JoinFilter()
    signal.signal(signal.SIGTERM, lambda _sig, _frame: join_filter.shutdown())
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
