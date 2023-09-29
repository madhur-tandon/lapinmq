import functools
from enum import Enum

class MessageStatus(Enum):
    SUCCESS = 1
    FAIL_RETRY_LATER = 2
    FAIL_DO_NOT_RETRY = 3
    HANDLED_VIA_CALLBACK = 4

class InMemoryMessage:
    def __init__(self, ch, method, properties, body):
        self.ch = ch
        self.method = method
        self.properties = properties
        self.body = body

        basic_ack_fn = functools.partial(ch.basic_ack, delivery_tag=method.delivery_tag)
        basic_reject_fn = functools.partial(
            ch.basic_reject, delivery_tag=method.delivery_tag, requeue=False
        )
        basic_reject_fn_with_retry = functools.partial(
            ch.basic_reject, delivery_tag=method.delivery_tag, requeue=True
        )

        self.ack = functools.partial(
            ch.connection.add_callback_threadsafe,
            basic_ack_fn
        )
        self.reject = functools.partial(
            ch.connection.add_callback_threadsafe,
            basic_reject_fn
        )
        self.reject_retry = functools.partial(
            ch.connection.add_callback_threadsafe,
            basic_reject_fn_with_retry
        )
