import functools
from enum import Enum

class MessageStatus(Enum):
    # send an acknowledgement
    SUCCESS = 1

    # send a negative acknowledgement with requeue as True
    # Examples: running out of memory, failure to connect to an external service, etc.
    FAIL_RETRY_LATER = 2

    # send a negative acknowledgement with requeue as False
    # Examples: error parsing the message, invalid attributes in the message, etc.
    FAIL_DO_NOT_RETRY = 3

    # useful when the underlying task function first consumes a message, and then
    # sends a message (either same or different) to another queue. In this case,
    # we only want to acknowledge from our source queue when the destination queue
    # has received the message.
    # This status is only useful when the publisher is asynchronous.
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
