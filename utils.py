import os
import sys
import pika

def alert_and_crash(message):
    print(message)
    sys.stdout.flush()
    os._exit(1)

def get_parameters(blocked_connection_timeout=3600):
    # if RabbitMQ broker is low on resources, it can block connections
    # if blocked > 1 hour, drop the connection
    return pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST"),
        port=os.getenv("RABBITMQ_PORT"),
        credentials=pika.PlainCredentials(os.getenv("RABBITMQ_USERNAME"), os.getenv("RABBITMQ_PASSWORD")),
        blocked_connection_timeout=blocked_connection_timeout,
    )
