import json


# --- Tipos de mensaje ---

TYPE_DATA = "data"
TYPE_EOF = "eof"
TYPE_SUM_EOF = "sum_eof"
TYPE_PARTIAL = "partial"
TYPE_RESULT = "result"

_KEY_TYPE = "t"
_KEY_CLIENT = "c"
_KEY_FRUIT = "f"
_KEY_AMOUNT = "a"
_KEY_SUM_ID = "s"
_KEY_AGG_ID = "g"
_KEY_TOP = "r"


# --- Serialización / deserialización ---

def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))


# --- Constructoras de mensajes ---

def make_data_msg(client_id, fruit, amount):
    return {
        _KEY_TYPE: TYPE_DATA,
        _KEY_CLIENT: client_id,
        _KEY_FRUIT: fruit,
        _KEY_AMOUNT: amount,
    }


def make_eof_msg(client_id):
    return {_KEY_TYPE: TYPE_EOF, _KEY_CLIENT: client_id}


def make_sum_eof_msg(client_id):
    return {_KEY_TYPE: TYPE_SUM_EOF, _KEY_CLIENT: client_id}


def make_partial_msg(client_id, agg_id, fruit_top):
    return {
        _KEY_TYPE: TYPE_PARTIAL,
        _KEY_CLIENT: client_id,
        _KEY_AGG_ID: agg_id,
        _KEY_TOP: fruit_top,
    }


def make_result_msg(client_id, fruit_top):
    return {
        _KEY_TYPE: TYPE_RESULT,
        _KEY_CLIENT: client_id,
        _KEY_TOP: fruit_top,
    }


# --- Helpers ---

def msg_type(msg):
    return msg[_KEY_TYPE]


def msg_client_id(msg):
    return msg[_KEY_CLIENT]


def msg_fruit(msg):
    return msg[_KEY_FRUIT]


def msg_amount(msg):
    return msg[_KEY_AMOUNT]


def msg_sum_id(msg):
    return msg[_KEY_SUM_ID]


def msg_agg_id(msg):
    return msg[_KEY_AGG_ID]


def msg_top(msg):
    return msg[_KEY_TOP]
