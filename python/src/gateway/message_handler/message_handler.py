import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self):
        self.client_id = str(uuid.uuid4())

    def serialize_data_message(self, message):
        [fruit, amount] = message
        msg = message_protocol.internal.make_data_msg(
            self.client_id, fruit, amount
        )
        return message_protocol.internal.serialize(msg)

    def serialize_eof_message(self, message):
        msg = message_protocol.internal.make_eof_msg(self.client_id)
        return message_protocol.internal.serialize(msg)

    def deserialize_result_message(self, message):
        parsed = message_protocol.internal.deserialize(message)
        if message_protocol.internal.msg_type(parsed) != message_protocol.internal.TYPE_RESULT:
            return None
        if message_protocol.internal.msg_client_id(parsed) != self.client_id:
            return None
        return message_protocol.internal.msg_top(parsed)
