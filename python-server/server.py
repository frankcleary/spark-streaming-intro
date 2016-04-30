#!/usr/bin/python
"""
usage: ./server.py

Run a TCP socket server outputting events of the form "user:event" for
consumption by streaming applications.
"""
import SocketServer
import random
import time
from Queue import Queue
from threading import Thread

# Maximum number of pending events
MAX_QUEUE_SIZE = 100
# Where to run the server
HOST = 'localhost'
PORT = 9999

# The time between generated events
EVENT_PERIOD_SECONDS = 1
# Fake data for random events
USERS = ['user-{}'.format(i) for i in range(10)]
EVENTS = ['login', 'purchase']


def generate_event(users, events, delimiter=':'):
    """Choose a random user and event, output a delimited user:event string."""
    user = random.choice(users)
    event = random.choice(events)
    return '{}{}{}'.format(user, delimiter, event)


def run_server(host, port, queue):
    """Run a SocketServer.TCPServer on host, port which writes strings from
    queue to the socket as they become available.
    """
    class QueueTCPHandler(SocketServer.BaseRequestHandler):
        def handle(self):
            while True:
                line = queue.get()
                self.request.sendall(line)

    server = SocketServer.TCPServer((host, port), QueueTCPHandler)
    server.serve_forever()

if __name__ == '__main__':
    input_queue = Queue(maxsize=MAX_QUEUE_SIZE)

    server_thread = Thread(target=run_server, args=(HOST, PORT, input_queue,))
    server_thread.daemon = True
    server_thread.start()

    while True:
        input_queue.put(generate_event(USERS, EVENTS) + '\n')
        time.sleep(EVENT_PERIOD_SECONDS)
