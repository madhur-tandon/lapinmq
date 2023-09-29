import pika
import threading
import functools
from collections import deque

from utils import get_parameters, alert_and_crash
from message import InMemoryMessage, MessageStatus

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
        try:
            self.channel.start_consuming()
        except pika.exceptions.ChannelClosedByBroker as e:
            alert_and_crash(f"Channel closed by broker: {e}")
        except pika.exceptions.AMQPChannelError as e:
            alert_and_crash(f"Channel error: {e}")
        except pika.exceptions.AMQPConnectionError as e:
            alert_and_crash(f"Connection was closed: {e}")
        except Exception as e:
            alert_and_crash(f"Error: {e}")

    def stop(self):
        with self.consumer_lock:
            self.terminated = True
            self.consumer_condvar.notify_all()

        for thread in self.workers:
            thread.join()

        consumer_cancel = functools.partial(
            self.channel.basic_cancel, consumer_tag=self.consumer_tag
        )

        self.connection.add_callback_threadsafe(consumer_cancel)

        self.start_consuming_thread.join()

        self.channel.close()

        self.connection.close()

    def callback(self, ch, method, properties, body):
        in_memory_message = InMemoryMessage(ch, method, properties, body)
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
        try:
            try:
                ret = self.task_function(in_memory_message)
            except Exception:
                in_memory_message.reject_retry()
                ret = None
            
            if ret == MessageStatus.SUCCESS:
                in_memory_message.ack()
            elif ret == MessageStatus.FAIL_DO_NOT_RETRY:
                in_memory_message.reject()
            elif ret == MessageStatus.FAIL_RETRY_LATER:
                in_memory_message.reject_retry()
            elif ret == MessageStatus.HANDLED_VIA_CALLBACK:
                pass
            elif ret is not None:
                in_memory_message.reject_retry()
        except Exception as e:
            alert_and_crash(f"PANIC: Exception in worker thread: {e}")
