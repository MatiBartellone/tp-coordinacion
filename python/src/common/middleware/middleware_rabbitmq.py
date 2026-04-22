import pika
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
)

EXCHANGE_TYPE = 'direct'


def _create_channel(host):
    """Abre una conexion bloqueante contra el broker y devuelve (conexion, canal)."""
    conn = pika.BlockingConnection(pika.ConnectionParameters(host=host))
    return conn, conn.channel()


def _safe_close(connection):
    """Intenta cerrar la conexion; si falla lanza MessageMiddlewareCloseError."""
    try:
        connection.close()
    except Exception as err:
        raise MessageMiddlewareCloseError(
            f"No se pudo cerrar la conexion: {err}"
        )


def _build_delivery_callback(user_callback):
    """Construye el callback con la firma que espera pika a partir del callback del usuario.

    Adapta (ch, method, properties, body) -> user_callback(body, ack_fn, nack_fn).
    Ante una desconexion re-lanza MessageMiddlewareDisconnectedError.
    Cualquier otro error se envuelve en MessageMiddlewareMessageError.
    """
    def _on_delivery(ch, method, _properties, body):
        tag = method.delivery_tag
        ack_fn = lambda: ch.basic_ack(delivery_tag=tag)
        nack_fn = lambda: ch.basic_nack(delivery_tag=tag)
        try:
            user_callback(body, ack_fn, nack_fn)
        except MessageMiddlewareDisconnectedError:
            raise
        except Exception as err:
            raise MessageMiddlewareMessageError(
                f"Error al procesar mensaje: {err}"
            )

    return _on_delivery


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    """Implementacion de Work Queue sobre RabbitMQ.

    Multiples consumidores compiten por los mensajes de una misma cola:
    cada mensaje es entregado a exactamente un consumidor.
    """

    def __init__(self, host, queue_name):
        self.connection, self.channel = _create_channel(host)
        self.queue_name = queue_name
        self.channel.queue_declare(queue=self.queue_name)

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message,
            )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError(
                "Se perdio la conexion al intentar enviar a la cola"
            )

    def start_consuming(self, on_message_callback, control_callback=None, control_queue=None):
        self.channel.basic_qos(prefetch_count=1)
        wrapped = _build_delivery_callback(on_message_callback)
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=wrapped,
            auto_ack=False,
        )
        if control_callback and control_queue:
            wrapped_control = _build_delivery_callback(control_callback)
            self.channel.basic_consume(
                queue=control_queue,
                on_message_callback=wrapped_control,
                auto_ack=False,
            )
        self.channel.start_consuming()

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except Exception:
            pass

    def close(self):
        _safe_close(self.connection)


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    """Implementacion de Exchange (direct) sobre RabbitMQ.

    Cada consumidor declara su propia cola exclusiva y la vincula a las
    routing keys deseadas. Al publicar, el mensaje se envia a cada routing key.
    """

    def __init__(self, host, exchange_name, routing_keys):
        self.connection, self.channel = _create_channel(host)
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.channel.exchange_declare(
            exchange=self.exchange_name, exchange_type=EXCHANGE_TYPE
        )

    def send(self, message):
        try:
            keys = self.routing_keys if self.routing_keys else ['']
            for key in keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=key,
                    body=message,
                )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError(
                "Se perdio la conexion al intentar publicar en el exchange"
            )

    def get_queue_name(self):
        """Declara una cola vinculada al exchange y retorna su nombre.

        Esto permite que otro middleware (ej. Queue) consuma de esta cola
        en su propio event loop, evitando la necesidad de threads.
        La cola es auto_delete (se borra al quedar sin consumidores)
        pero no exclusive (puede consumirse desde otra conexion).
        """
        result = self.channel.queue_declare(queue='', exclusive=False, auto_delete=True)
        self.bound_queue = result.method.queue
        for key in self.routing_keys:
            self.channel.queue_bind(
                exchange=self.exchange_name,
                queue=self.bound_queue,
                routing_key=key,
            )
        return self.bound_queue

    def start_consuming(self, on_message_callback):
        self.channel.basic_qos(prefetch_count=1)
        if not hasattr(self, 'bound_queue'):
            self.get_queue_name()

        wrapped = _build_delivery_callback(on_message_callback)
        self.channel.basic_consume(
            queue=self.bound_queue,
            on_message_callback=wrapped,
            auto_ack=False,
        )
        self.channel.start_consuming()

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except Exception:
            pass

    def close(self):
        _safe_close(self.connection)
