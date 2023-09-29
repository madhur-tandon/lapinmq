import pika
import functools
from pika.spec import Basic
import threading
from lapinmq.utils import get_parameters, alert_and_crash

class SyncPublisher:
    def __init__(self):
        self.stopped = False

        self.connection = pika.BlockingConnection(get_parameters())
        self.channel = self.connection.channel()
        self.channel.confirm_delivery()

        self.start_publishing_thread = None

    def start(self):
        self.start_publishing_thread = threading.Thread(
            target=self.__start_publishing,
            daemon=True
        )
        self.start_publishing_thread.start()

    def __start_publishing(self):
        while not self.stopped:
            try:
                self.connection.process_data_events(time_limit=0)
            except pika.exceptions.UnroutableError as e:
                alert_and_crash("Message is unroutable, no queue was bound to"
                                "the combination of exchange and routing "
                                f"key supplied: {e}")
            except pika.exceptions.ChannelClosedByBroker as e:
                alert_and_crash("Channel closed by broker. Most probably, the "
                                f"exchange supplied does not exist: {e}")
            except pika.exceptions.ConnectionClosedByBroker as e:
                alert_and_crash("Connection closed by broker. Most probably, "
                                f"RabbitMQ was terminated: {e}")
            except pika.exceptions.ChannelWrongStateError as e:
                alert_and_crash(f"Cannot publish message. Wrong Channel State: {e}")
            except Exception as e:
                alert_and_crash(f"Error: {e}")

    def stop(self):
        self.stopped = True
        self.connection.process_data_events(time_limit=0)

        self.start_publishing_thread.join()
        print("Stopped publishing messages")

        self.channel.close()
        print("Channel closed")

        self.connection.close()
        print("Connection closed")

    def send_message(self, exchange, routing_key, body, expiration=None):
        publish_fn = functools.partial(
            self.channel.basic_publish,
            exchange=exchange,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                expiration=expiration,
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
            ),
            mandatory=True,
        )

        self.connection.add_callback_threadsafe(publish_fn)

    def send_message_to_queue(self, queue_name, body, expiration=None):
        self.send_message(
            exchange="",  # default exchange
            routing_key=queue_name,
            body=body,
            expiration=expiration,
        )


class ASyncPublisher:
    def __init__(self):
        self.stopped = False
        self.publisher_lock = threading.Lock()
        self.publisher_condvar = threading.Condition(self.publisher_lock)

        self.ready = False
        self.message_number = 1
        self.messages_with_pending_confirmations = {}
        self.ack_callbacks = {}
        self.nack_callbacks = {}

        self.start_publishing_thread = None

        self.connection = pika.SelectConnection(
            parameters=get_parameters(),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed,
        )

        self.channel = None
    
    def on_connection_open(self, connection):
        print("Connection opened")
        self.connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        print("Channel opened")
        self.channel = channel
        self.channel.confirm_delivery(self.on_delivery_confirmation, self.on_confirm_ok)
        self.channel.add_on_return_callback(self.on_channel_return)
        self.channel.add_on_close_callback(self.on_channel_closed)
    
    def on_confirm_ok(self, frame):
        print("Turned on Publisher Confirmations")
        with self.publisher_lock:
            self.ready = True
            self.publisher_condvar.notify_all()

    def on_channel_return(self, ch, method, properties, body):
        # let's abort and crash the publisher
        alert_and_crash("Message is unroutable, no queue was bound to the "
                        "combination of exchange and routing key supplied.")

    def on_delivery_confirmation(self, frame):
        confirmation_type = frame.method
        delivery_tag = frame.method.delivery_tag
        multiple = frame.method.multiple

        def call_ack_or_nack_callback(confirmation_type, tag):
            if isinstance(confirmation_type, Basic.Ack):
                print(f"Received an ACK for message #{tag} from the broker.")
                ack_callback = self.ack_callbacks.get(tag)
                if ack_callback is not None:
                    ack_callback()
                del self.ack_callbacks[tag]
            elif isinstance(confirmation_type, Basic.Nack):
                print(f"Received a NACK for message #{tag} from the broker.")
                nack_callback = self.nack_callbacks.get(tag)
                if nack_callback is not None:
                    nack_callback()
                del self.nack_callbacks[tag]

        with self.publisher_lock:
            if multiple:
                for each_tag in list(
                    self.messages_with_pending_confirmations.keys()
                ):
                    if each_tag <= delivery_tag:
                        call_ack_or_nack_callback(confirmation_type, each_tag)
                        del self.messages_with_pending_confirmations[each_tag]
            else:
                call_ack_or_nack_callback(confirmation_type, delivery_tag)
                del self.messages_with_pending_confirmations[delivery_tag]

    def on_channel_closed(self, channel, reason):
        print(f"Channel was closed: {reason}")
        self.connection.ioloop.add_callback_threadsafe(self.connection.close)

    def on_connection_open_error(self, connection, err):
        print(f"Error in opening connection: {err}")
        alert_and_crash(f"Error in opening connection: {err}")

    def on_connection_closed(self, connection, reason):
        if self.stopped:
            print(f"Connection was closed: {reason}")
        else:
            alert_and_crash(f"Connection was closed: {reason}")

    def __send_message(
        self,
        exchange,
        routing_key,
        body,
        expiration=None,
        callbacks={},
    ):
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                expiration=expiration,
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
            ),
            mandatory=True,
        )

        with self.publisher_lock:
            self.messages_with_pending_confirmations[self.message_number] = body
            self.ack_callbacks[self.message_number] = callbacks.get('ack_callback')
            self.nack_callbacks[self.message_number] = callbacks.get('nack_callback')
            self.message_number += 1
    
    def send_message(
        self,
        exchange,
        routing_key,
        body,
        expiration=None, # in ms, has to be a string, eg: '30000' for 30 secs
        callbacks={},
    ):
        with self.publisher_lock:
            while not self.ready:
                print("Waiting for channel to be ready...")
                self.publisher_condvar.wait()

        publish_fn = functools.partial(
            self.__send_message,
            exchange=exchange,
            routing_key=routing_key,
            body=body,
            expiration=expiration,
            callbacks=callbacks,
        )

        self.connection.ioloop.add_callback_threadsafe(publish_fn)

    def send_message_to_queue(
        self, queue_name, body, expiration=None, callbacks={}
    ):
        self.send_message(
            exchange="",  # default exchange
            routing_key=queue_name,
            body=body,
            expiration=expiration,
            callbacks=callbacks,
        )

    def start(self):
        self.start_publishing_thread = threading.Thread(
            target=self.__start_publishing,
            daemon=True
        )
        self.start_publishing_thread.start()

    def __start_publishing(self):
        self.connection.ioloop.start()
    
    def stop(self):
        self.stopped = True

        self.connection.ioloop.add_callback_threadsafe(self.connection.ioloop.stop)
        print("IOLoop stopped")

        self.start_publishing_thread.join()
        print("Stopped publishing messages")

        if self.channel is not None:
            self.channel.close()
            print("Channel closed")

        if self.connection is not None:
            self.connection.close()
            print("Connection closed")

class Publisher:
    def __init__(self, kind):
        self.kind = kind
        if self.kind == "sync":
            self.publisher = SyncPublisher()
        elif self.kind == "async":
            self.publisher = ASyncPublisher()
        else:
            raise ValueError("kind can have the following values: "
                             f"[sync, async], supplied value is {self.kind}")

    def start(self):
        self.publisher.start()

    def stop(self):
        self.publisher.stop()

    def send_message(
        self,
        exchange,
        routing_key,
        body,
        expiration=None,
        callbacks={},
    ):
        print(f"Sending the message: {body}")
        if self.kind == "sync":
            self.publisher.send_message(exchange, routing_key, body, expiration)
        elif self.kind == "async":
            self.publisher.send_message(
                exchange,
                routing_key,
                body,
                expiration,
                callbacks
            )

    def send_message_to_queue(
        self, queue_name, body, expiration=None, callbacks={}
    ):
        self.send_message(
            exchange="",  # default exchange
            routing_key=queue_name,
            body=body,
            expiration=expiration,
            callbacks=callbacks,
        )
