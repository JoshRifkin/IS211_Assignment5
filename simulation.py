# Assignment 5
# Joshua Rifkin

import csv
import random
import requests
import queue


class Server:
    def __init__(self):
        self.current_task = None
        self.time_elapsed = 0
        self.time_remaining = 0

    def tick(self):
        if self.current_task != None:
            self.time_remaining = self.time_remaining - 1
        if self.time_remaining <= 0:
            self.current_task = None

    def busy(self):
        if self.current_task != None:
            return True
        else:
            return False

    def start_next(self, new_task):
        self.current_task = new_task
        self.time_remaining = new_task.get_length()


class Request:
    def __init__(self, time, length):
        self.timestamp = time
        self.length = length
        self.pages = random.randrange(1, 21)

    def get_stamp(self):
        return self.timestamp

    def get_pages(self):
        return self.pages

    def wait_time(self, current_time):
        return current_time - self.timestamp

    def get_length(self):
        return self.length


def simulateOneServer(fileName):
    server = Server()
    requestQ = queue.Queue()
    waiting_times = []

    for line in fileName:
        reqTime = int(line[0])
        reqLen = int(line[2])

        task = Request(reqTime, reqLen)
        requestQ.put(task)

        if (not server.busy()) and (not requestQ.empty()):
            nextReq = requestQ.get()
            waiting_times.append(nextReq.wait_time(reqTime))
            server.start_next(nextReq)

        server.tick()

    average_wait = sum(waiting_times) / len(waiting_times)
    print("Average Wait %6.2f secs %3d tasks remaining."
          % (average_wait, requestQ.qsize()))


def simulateManyServers(fileName, numServers):
    waiting_times = []
    requestQ = queue.Queue()
    servers = []
    i = 0
    while i < int(numServers):
        server = Server()
        servers.append(server)

    for line in fileName:
        reqTime = int(line[0])
        reqLen = int(line[2])

        task = Request(reqTime, reqLen)
        requestQ.put(task)

    for server in servers:
        while not requestQ.empty():
            if (not server.busy()) and (not requestQ.empty()):
                nextReq = requestQ.get()
                waiting_times.append(nextReq.wait_time(reqTime))
                server.start_next(nextReq)
        server.tick()
    average_wait = sum(waiting_times) / len(waiting_times)
    print("Average Wait %6.2f secs %3d tasks remaining."
          % (average_wait, requestQ.qsize()))


def main():
    try:
        # Pull file from internet
        # URL: http://s3.amazonaws.com/cuny-is211-spring2015/requests.csv

        url = input('File Source: ')
#        url = "http://s3.amazonaws.com/cuny-is211-spring2015/requests.csv"
        servers = int(input('Number of Servers: '))
        sourceData = requests.get(url)
        csvFile = sourceData.content.decode()
        csvData = csv.reader(csvFile.splitlines())
    except ValueError:
        print('Invalid URL.')
        exit()

    if (servers == 1):
        simulateOneServer(csvData)
    elif (servers < 1):
        print('Invalid number of servers.')
        exit()
    else:
        simulateManyServers(csvData, servers)


if __name__ == '__main__':
    main()
