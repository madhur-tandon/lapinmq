import pika
import threading
import functools
from collections import deque

from lapinmq.utils import get_parameters, alert_and_crash
from lapinmq.message import InMemoryMessage, MessageStatus

class Consumer:
    def __init__(self, queue_name, task_function, worker_threads=1):
        self.queue_name = queue_name
        self.task_function = task_function
        self.worker_threads = worker_threads
        
        self.consumer_lock = threading.Lock()
        self.consumer_condvar = threading.Condition(self.consumer_lock)
        self.queue = deque()
        self.workers = []
        self.terminated = False

        self.start_consuming_thread = None

        self.connection = pika.BlockingConnection(get_parameters())
        self.channel = self.connection.channel()

        self.channel.basic_qos(prefetch_count=self.worker_threads)
        self.consumer_tag = self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=self.callback
        )

    def start(self):
        # create `self.worker_threads` amount of threads, which will be re-used
        # as and when messages are ready to be processed.
        for _ in range(self.worker_threads):
            t = threading.Thread(target=self.thread_run, daemon=True)
            t.start()
            self.workers.append(t)

        self.start_consuming_thread = threading.Thread(
            target=self.__start_consuming,
            daemon=True
        )
        self.start_consuming_thread.start()

    def __start_consuming(self):
        print(f"Started receiving messages from queue: {self.queue_name}")
        try:
            self.channel.start_consuming()
        except pika.exceptions.ChannelClosedByBroker as e:
            # The channel also gets closed on Delivery Acknowledgement Timeout failure.
            alert_and_crash(f"Channel closed by broker: {e}")
        except pika.exceptions.AMQPChannelError as e:
            alert_and_crash(f"Channel error: {e}")
        except pika.exceptions.AMQPConnectionError as e:
            alert_and_crash(f"Connection was closed: {e}")
        except Exception as e:
            alert_and_crash(f"Error: {e}")

    def stop(self):
        print(f"Request to terminate receiving messages from {self.queue_name}")

        with self.consumer_lock:
            self.terminated = True
            self.consumer_condvar.notify_all()

        # wait for tasks to finish, this may take some time
        print("Waiting for workers to finish processing...")
        for thread in self.workers:
            # if a worker takes too long, there are 2 cases:
            # a) Consumer Delivery Acknowledgement timeout < Grace Period of K8s
            # --> Crashes the consumer and aborts the process
            # b) Grace Period of K8s < Consumer Delivery Acknowledgement timeout
            # --> K8s will shoot us when the grace period expires
            thread.join()

        consumer_cancel = functools.partial(
            self.channel.basic_cancel, consumer_tag=self.consumer_tag
        )
        self.connection.add_callback_threadsafe(consumer_cancel)
        print(f"Broker acknowledged cancellation of the consumer: {self.consumer_tag}")

        self.start_consuming_thread.join()
        print(f"Consumer {self.consumer_tag} has stopped consuming messages")

        self.channel.close()
        print("Channel closed")

        self.connection.close()
        print("Connection closed")

    def callback(self, ch, method, properties, body):
        print(f"Received message #{method.delivery_tag} with contents: {body}")
        in_memory_message = InMemoryMessage(ch, method, properties, body)

        # process the message in one of the threads we instantiated earlier.
        # this avoids interference with the liveliness mechanism of the consumer.
        with self.consumer_lock:
            self.queue.append(in_memory_message)
            self.consumer_condvar.notify()

    def thread_run(self):
        self.consumer_lock.acquire()

        while True:
            while not self.terminated and len(self.queue) == 0:
                self.consumer_condvar.wait()
            
            if self.terminated:
                break

            in_memory_message = self.queue.popleft()

            self.consumer_lock.release()
            self.kernel_fn(in_memory_message)
            self.consumer_lock.acquire()
        
        self.consumer_lock.release()
        return

    def kernel_fn(self, in_memory_message):
        thread_name = threading.current_thread().name
        print(f"Worker {thread_name} for message "
              f"#{in_memory_message.method.delivery_tag} "
              f"with contents: {in_memory_message.body} in queue {self.queue_name}")
        try:
            try:
                ret = self.task_function(in_memory_message)
            except Exception:
                print(f"Exception in task_function: {self.task_function} "
                      f"while trying to process message "
                      f"#{in_memory_message.method.delivery_tag} with "
                      f"contents: {in_memory_message.body}, "
                      "will retry later...")
                in_memory_message.reject_retry()
                ret = None
            if ret == MessageStatus.SUCCESS:
                print(f"Successfully processed message "
                      f"#{in_memory_message.method.delivery_tag} with "
                      f"contents: {in_memory_message.body}, "
                      "sending acknowledgement")
                in_memory_message.ack()
            elif ret == MessageStatus.FAIL_DO_NOT_RETRY:
                print(f"Message #{in_memory_message.method.delivery_tag} with "
                      f"contents: {in_memory_message.body} failed, "
                      "deleting / sending to dead letter queue.")
                in_memory_message.reject()
            elif ret == MessageStatus.FAIL_RETRY_LATER:
                print(f"Message #{in_memory_message.method.delivery_tag} with "
                      f"contents: {in_memory_message.body} failed, "
                      "will retry later...")
                in_memory_message.reject_retry()
            elif ret == MessageStatus.HANDLED_VIA_CALLBACK:
                print(f"Acknowledgements for message "
                      f"#{in_memory_message.method.delivery_tag} with "
                      f"contents: {in_memory_message.body} "
                      "will be handled via callbacks.")
                pass
            elif ret is not None:
                print(f"Invalid return code from the "
                      f"task_function: {self.task_function} for "
                      f"message #{in_memory_message.method.delivery_tag} with "
                      f"contents: {in_memory_message.body}, "
                      "will retry later...")
                in_memory_message.reject_retry()
            print(f"Graceful exit of worker {thread_name} for queue {self.queue_name}")
        except Exception as e:
            alert_and_crash(f"PANIC: Exception in worker {thread_name} "
                            f"for queue {self.queue_name}: {e}")
