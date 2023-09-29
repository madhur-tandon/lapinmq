# lapinmq

Author: Madhur Tandon

_lapin_ ~ _rabbit_ in french

## TLDR
**lapinmq** consists of utilities for RabbitMQ (or any other AMQP protocol compatible message broker) following best practices such as:
- Long living consumers and publishers, with their heartbeats in background
- Multi-threaded consumers that can process messages in parallel with prefetch and manual acknowledgements
- Option of Asynchronous and Synchronous Publisher Confirmations (for different network latency scenarios) -- details [here](https://www.cloudamqp.com/blog/publishing-throughput-asynchronous-vs-synchronous.html)
- Ability to coordinate b/w asynchronous publisher confirmations and consumer acknowledgements via callbacks, useful when using 2 or more queues as a pipeline.
- Detection of Unroutable messages i.e. the case where the queue doesn't exist for the exchange and routing key supplied

The utilities are built on top of `pika` -- the official recommended library by the RabbitMQ team.

**NOTE:**
- The library offers simple abstractions for sending and receiving messages and may not cover very advanced cases at the moment.
- Currently, the popular pattern of `competing consumers in a work queue` is assumed.
- Temporary queues are not supported yet but may get added in near future.
- Contributions are welcome for improving codebase, examples, documentation, etc.

## Why another wrapper for pika?

- Documentation is primitive in the sense that the tutorials don't showcase how to make the consumers and publishers long living
- Setting prefetch count > 1 simply means multiple messages in RAM of the consumer and there are no proper examples of how to leverage threading to process them in parallel
- Synchronous channel confirmations can lead to a very low sending throughput in high network latency scenarios, thus, the option of an asynchronous publisher

## Usage

Ensure that the following environment variables are set before using the utilities below:
- RABBITMQ_HOST: host address for where the RabbitMQ server can be accessed
- RABBITMQ_PORT: port for the RabbitMQ server (default is 5672)
- RABBITMQ_USERNAME: username for the RabbitMQ server
- RABBITMQ_PASSWORD: password for the RabbitMQ server

### Sending Messages

```py
import time
from lapinmq.publisher import Publisher
from lapinmq.utils import wait_for_sigterm

p = Publisher(kind="sync") # or perhaps p = Publisher(kind="async")
p.start()

for i in range(100):
    p.send_message_to_queue(queue_name='task_queue', body='...')
    # if the publisher is async, can also pass callbacks i.e.
    # p.send_message_to_queue(queue_name='task_queue', body='...', callbacks={})
    time.sleep(300) # 5 minutes in seconds, to showcase long wait times between sending messages

# OR we can also use the `send_message` function for more advanced usage
for i in range(100):
    p.send_message(exchange='', routing_key='task_queue', body='...')
    # no time.sleep() here to showcase 0 wait time between sending messages

sigterm_received = wait_for_sigterm()
assert sigterm_received

p.stop()
```

### Receiving Messages

To receive messages, create an object of class `lapinmq.Consumer` with the queue name and a task function. One can also pass in the number of workers (default is 1).

The task function takes in the received message as an argument and must return one of the 4 statuses below:
- MessageStatus.SUCCESS: if the processing of the message succeeded and the message should be deleted from the queue.
- MessageStatus.FAIL_RETRY_LATER : if the processing of the message failed and the message should be re-tried later.
- MessageStatus.FAIL_DO_NOT_RETRY : if the processing of the message failed and the message should not be re-tried later.
- MessageStatus.HANDLED_VIA_CALLBACK : for advanced usage, more on this later

If the function raises an exception, that case is treated similar to FAIL_RETRY_LATER

```py
from lapinmq.message import MessageStatus
from lapinmq.consumer import Consumer
from lapinmq.utils import wait_for_sigterm

def task(message):
    body = message.body.decode()
    # process the received message body
    return MessageStatus.SUCCESS # or perhaps FAIL_RETRY_LATER or FAIL_DO_NOT_RETRY

c = Consumer(
    queue_name='task_queue',
    task_function=task,
    worker_threads=3 # this consumer can process 3 messages in parallel
)
c.start()

sigterm_received = wait_for_sigterm()
assert sigterm_received

c.stop()
```

---

**Note**: The `p.start()` and `c.start()` methods for the Publisher and Consumer class respectively
spawn a `daemon` thread underneath. This means that the script that calls these methods has to have some
waiting mechanism. Otherwise, if the main thread exits, the publishing / consuming will stop as well.

In the examples above, the waiting mechanism is listening for a SIGTERM.

An alternate usage might be to sleep for a certain time:

```py
p.start() # OR c.start()

print("will terminate after 10 mins")
time.sleep(600)

p.stop() # OR c.stop()
```

But, the following example will not work:

```py
p.start() # OR c.start()

print("main thread exiting now")
```

since here, after the main thread exits, the daemon thread also exits i.e. we have stopped publishing / consuming messages after displaying `main thread exiting now` on stdout.

Lastly, since queues are usually used in ever-running applications such as a web-server, the main thread by definition never exits. Thus, the lack of waiting mechanism is really not a problem there.

---

### Advanced: Consumer that is also a Publisher

Consider two queues -- a source queue from which a consumer gets a message and thereafter, it processes it and sends a message (same or another) to a destination queue, thus also acting as a publisher.

**Note**: source and destination queues can refer to the same queue, but the terminology is used so as to understand the concept.

Essentially, the `task()` function is also responsible for sending a message to the destination queue.

But, we only want to acknowledge (/delete) message from the source queue if and only if the message to the destination queue has reached and we have received a delivery confirmation of it from the broker.

#### Synchronous Publisher

In this case, we won't be able to send a message to the broker unless a confirmation has arrived for the previous message being sent. This gives strong guarantees but can be significantly slower in high network latency scenarios.

```py
from lapinmq.message import MessageStatus
from lapinmq.consumer import Consumer
from lapinmq.publisher import Publisher
from lapinmq.utils import wait_for_sigterm

p = Publisher(kind="sync")
p.start()

def task(message):
    body = message.body.decode()

    # process the received message body

    # send a message to the destination queue
    p.send_message_to_queue(queue_name='destination_queue', body='...')

    return MessageStatus.SUCCESS # or perhaps FAIL_RETRY_LATER or FAIL_DO_NOT_RETRY

c = Consumer(
    queue_name='task_queue',
    task_function=task,
    worker_threads=3 # this consumer can process 3 messages in parallel
)
c.start()

sigterm_received = wait_for_sigterm()
assert sigterm_received

c.stop()
p.stop()
```

#### Asynchronous Publisher

In this case, we don't know when the broker will give confirmations for the messages that were sent to it. However, as soon as we receive a delivery confirmation from the broker, a callback is triggered. We can send acknowledgement to the source queue inside this callback. This mechanism helps resolve timing issues since we should only acknowledge the message from the source queue **after** a delivery confirmation for the sent message is received.

The functions to `acknowledge`, `reject` or `reject with retry` are available inside the message object as `message.ack`, `message.reject`, `message.reject_retry`

```py
from lapinmq.message import MessageStatus
from lapinmq.consumer import Consumer
from lapinmq.publisher import Publisher
from lapinmq.utils import wait_for_sigterm

p = Publisher(kind="async")
p.start()

def task(message):
    body = message.body.decode()

    # process the received message body

    # send a message to the destination queue
    p.send_message_to_queue(
        queue_name='destination_queue',
        body='...',
        callbacks={
            # when the broker confirms that message has been sent to the destination queue,
            # send an acknowledgement to the source queue
            'ack_callback': message.ack,
            # when the broker confirms that message has not been sent to the destination queue,
            # reject the message from the source queue and try again later
            'nack_callback': message.reject_retry,
        })

    # callbacks are responsible for various kinds of acknowlegements to the source queue
    return MessageStatus.HANDLED_VIA_CALLBACK

c = Consumer(
    queue_name='task_queue',
    task_function=task,
    worker_threads=3 # this consumer can process 3 messages in parallel
)
c.start()

sigterm_received = wait_for_sigterm()
assert sigterm_received

c.stop()
p.stop()
```

One can of course pass any function in these callbacks, but ideally, the use-case is to send acknowledgements to the source queue. The only condition is that the function being passed inside the callbacks shouldn't accept any arguments. Functions that accept arguments can be converted to functions that take 0 arguments using the `functools.partial` module.

```py
import json
from functools import partial
from lapinmq.publisher import Publisher

p = Publisher(kind="async")
p.start()

def task(message):
    body = message.body.decode()
    value = json.loads(body).get("eta")
    
    def custom_callback(val):
        # increment some metric
        counter.increment(val)
        # send acknowledgement to the source queue
        message.ack()

    custom_fn = partial(custom_callback, value)

    # send a message to the destination queue
    p.send_message_to_queue(
        queue_name='destination_queue',
        body='...',
        callbacks={
            # when the broker confirms that message has been sent to the destination queue,
            # call the custom_callback function that also sends the acknowledgement to the source queue
            'ack_callback': custom_fn,
            'nack_callback': message.reject_retry,
        })

    return MessageStatus.HANDLED_VIA_CALLBACK


c = Consumer(
    queue_name='task_queue',
    task_function=task,
    worker_threads=3 # this consumer can process 3 messages in parallel
)
c.start()

sigterm_received = wait_for_sigterm()
assert sigterm_received

c.stop()
p.stop()
```

## Potential Improvements

### Connection Multiplexing with Multiple Channels

[Recommended Reading](https://www.cloudamqp.com/blog/the-relationship-between-connections-and-channels-in-rabbitmq.html)

The one potential improvement is multiplexing of a single AMQP connection with multiple channels.

Consider the following code snippet below:

```py
from lapinmq.consumer import Consumer

c1 = Consumer(
    queue_name='preprocessing_task_queue',
    task_function=task,
    worker_threads=3 # this consumer can process 3 messages in parallel
)

c2 = Consumer(
    queue_name='transcoding_task_queue',
    task_function=task,
    worker_threads=5 # this consumer can process 5 messages in parallel
)
```

Currently, the library creates 1 connection and 1 channel for each of the consumers defined above.
Instead, we can create 1 connection and 2 channels instead...

Creating connections is costlier because it is an actual TCP connection
And they are limited by sockets / file descriptors, etc. that the underlying operating system allows.
Further, each new TCP connection involves a lot of handshakes etc.

**Impact**

If a pod running inside a kubernetes cluster has X consumers (X=2 in the snippet immediately above) --> X connections and X channels in the current implementation.

If we scale up to 50 pods, then we have 50X consumers --> 50X connections and 50X channels.

But with connection multiplexing, X consumers will mean 1 connection and X channels.
Thus, while scaling up, 50 pods will mean having 50 connections and 50X channels.

Clearly, the improvement is from 50X to 50 for the number of connections needed.

**Limitation**

`pika` -- the library being used underneath is not thread safe and thus, having one connection per process and by extension, different channels in different threads is not really possible.

The FAQ [here](https://pika.readthedocs.io/en/stable/faq.html) also describes the same and recommends to create one pika connection per thread (already being done in the current implementation).

Thus, this potential improvement is not possible with `pika`.

## Appendix

### Some concepts in simple words:

#### 1. Long living consumer and publisher, with their heartbeats in background

Consumers and Publishers need to send their heartbeats to the RabbitMQ broker to communicate that they are alive, thus signalling to keep the underlying TCP connection open. However,
- if, for a consumer, the processing of a message takes longer than usual (say 10 minutes), this interferes with the heartbeat mechanism of the consumer since the processing of the message blocks the main thread.
- if, for a publisher, we want to wait for a while (say 10 minutes) before sending the next subsequent message, this waiting interferes with the heartbeat mechanism of the publisher since it blocks the main thread.

The idea is thus to offload the heartbeat mechanism in a separate thread, enabling us to keep the underlying connection open.

---

Consider the following scenario (to understand the case of a sender / publisher):
1. Open a fridge, keep an apple in it, close the fridge. Do the same process for 1000 apples.
2. In the above example, opening a fridge ~ opening the TCP connection
3. Keeping an apple in it ~ sending a message to the RabbitMQ broker
4. Closing the fridge ~ closing the TCP connection
5. The heartbeat is we shouting to the house help to not close the fridge, who wants to do it after every few minutes

Clearly, computers (without multithreading or multiprocessing) can only do one task i.e. either keep an apple, or shout to the house help to remind her to not close the fridge. And if we take too long in keeping the apple, the house help will close the fridge.

We want to avoid the above case since TCP connections are costly to create (involving several handshakes) and a new connection for sending each message doesn't make sense.

Thus, a long living publisher, with the underlying TCP connection (/fridge) kept open (in a separate thread) is a better approach and we can then send as many messages (/keep as many apples) to the broker (/in the fridge). When we are done, we can finally close the connection (/fridge).

#### 2. Multi-threaded consumers that can process messages in parallel by setting prefetch > 1

Prefetch count ~ how many messages can consumers keep in RAM i.e. memory at any moment in time.

Clearly, if a consumer keeps > 1 messages in RAM i.e. memory, and processes just one of them at a time, the other messages kept in RAM are just sitting idle.

As an example, assuming a queue has 1000 messages, and 2 consumers, each of them having a prefetch count as 2 and 3 respectively. Then, first consumer gets 2 messages in RAM and second consumer gets 3 messages in RAM, leaving us with 995 messages in queue.

To extract most performance, our first consumer should spawn 2 threads to be able to process these 2 messages in parallel. As soon as one of the 2 messages is processed, the first consumer receives another message (so that at any moment, the number of messages in RAM are at most 2 for this consumer), and the idle thread should pick it up (instead of creating a new thread for each new message) -- ensuring that the number of threads always remain 2, picking up new messages as soon as they are done with previous ones.

**Conclusion**: Essentially, the consumer for RabbitMQ also implements the

_competing consumers pattern (where threads are waiting on messages to arrive) but at the OS level_

i.e. threads are the consumers for the RabbitMQ's consumer's internal queue which stores N number of messages in RAM, determined by the prefetch count.

#### 3. Synchronous vs Asynchronous Publisher Confirmations

While [this article](https://www.cloudamqp.com/blog/publishing-throughput-asynchronous-vs-synchronous.html) gives a great overview about the topic, a simple analogy can also help:

1. Assuming we want to give a 1000 bucks to a friend (/broker), 100 notes of 10 bucks each. Each note is a message.
2. We can give 1 note, wait for our friend to confirm that he received it, and only then we are allowed to give him another note, waiting again for his confirmation for the 2nd note and so on... This is the synchronous approach and can get slower if the distance between us and our friend is large (imagine doing this process when the friend lives in another country!). Thus, synchronous publisher confirmations can get very slow in high network latency scenarios, but are also robust since we guarantee that each message that was sent has been received by the broker before another one can be sent.
3. For the other approach, we can give all 100 notes at once to our friend, and it is upto the friend to give confirmations for the notes -- which he can either give for each note, or in batches or a mix of both. An example can be confirming that he received the first 70 notes, then 1 note, then 1 note, then 25 notes, then 2 notes and then 1 last note i.e. giving 6 confirmations for a total of 100 notes. The only problem is, we don't know when these confirmations will arrive, thus the term asynchronous.

The library has both kinds of publishers and extends help with callbacks if one wishes to use the asynchronous publisher while taking care of solving some timing issues (should be more clear in the upcoming pipeline example).
