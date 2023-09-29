import os
import sys
import pika
import signal
import threading

def alert_and_crash(message):
    print(message)
    sys.stdout.flush()
    os._exit(1)

def get_parameters(blocked_connection_timeout=3600):
    # if RabbitMQ broker is low on resources, it can block connections
    # if blocked > 1 hour, drop the connection
    return pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST"),
        port=os.getenv("RABBITMQ_PORT", 5672),
        credentials=pika.PlainCredentials(
            os.getenv("RABBITMQ_USERNAME"),
            os.getenv("RABBITMQ_PASSWORD")
        ),
        blocked_connection_timeout=blocked_connection_timeout,
    )

class SigtermHandler:
    def __init__(self):
        self.sigterm_received = False
        self.lock = threading.Lock()
        self.condvar = threading.Condition(self.lock)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, stack_frame):
        with self.lock:
            self.sigterm_received = True
            self.condvar.notify()

    def wait_for_sigterm(self, timeout):
        with self.lock:
            self.condvar.wait(timeout)
        return self.sigterm_received

sigterm_handler = SigtermHandler()

def wait_for_sigterm(timeout = None):
    return sigterm_handler.wait_for_sigterm(timeout)
