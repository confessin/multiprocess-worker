#!/usr/bin/env python
# encoding: utf-8

"""
This is an example code for implementing a worker model with multiprocessing.

1. Create 2 queues for tasks and results. (a dict/map better ?)
2. Define your Task. Whatever you want to do.
3. Define your Consumer. This is the class which continuously executes tasks one after another. Kind of manager class.
4. Calculate number of parallel workers based on number of CPU.
5. Start the consumers, and push results of teak worker task to the results queueu

NOTE: The result is not in order always.
"""

__author__ = 'confessin@gmail.com (Mohammad Rafi)'



import multiprocessing


class Consumer(multiprocessing.Process):

    def __init__(self, task_queue, result_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue

    def run(self):
        proc_name = self.name
        while True:
            next_task = self.task_queue.get()
            if next_task is None:
                # Poison pill means we should exit
                print '%s: Exiting' % proc_name
                self.result_queue.put(None)
                break
            answer = next_task()
            self.result_queue.put(answer)
        return


class ResultProcessor(multiprocessing.Process):
    def __init__(self, result_queue, num_consumers):
        multiprocessing.Process.__init__(self)
        self.result_queue = result_queue
        self.num_consumers = num_consumers

    def run(self):
        """Needs to be implemented iun child class."""
        end_signals = 0
        while True:
            next_result = self.result_queue.get()
            if next_result is None:
                end_signals += 1
                if end_signals >= self.num_consumers:
                    print 'all end signals got. Breaking away.'
                    break
            # Call a process_result func.
            self.process_result(next_result)

    def process_result(self, result):
        raise NotImplementedError('Needs to be implemented iun child class.')


class ResultProcessorChild(ResultProcessor):
    def run(self):
        end_signals = 0
        while True:
            next_result = self.result_queue.get()
            if next_result is None:
                end_signals += 1
                if end_signals >= self.num_consumers:
                    print 'all end signals got. Breaking away.'
                    break


class MultiProcess(object):
    def prepare_consumer(self):
        self.tasks = multiprocessing.Queue()
        self.results = multiprocessing.Queue()

        # Start consumers
        self.num_consumers = multiprocessing.cpu_count() * 2
        print 'Creating %d consumers' % self.num_consumers
        consumers = [ Consumer(self.tasks, self.results)
                    for i in xrange(self.num_consumers)]
        for w in consumers:
            w.start()

    def prepare_result_consumer(self, result_processor=ResultProcessor):
        r = result_processor(self.results, self.num_consumers)
        r.start()

    def enqueue_job(self, job):
        """job is a function which needs to be called."""
        # Enqueue jobs
        self.tasks.put(job)

    def add_end_jobs(self):
        # Add a poison pill for each consumer
        for i in xrange(self.num_consumers):
            self.tasks.put(None)

    # Start printing results
    def result_processor(self, num_jobs):
        while num_jobs:
            result = self.results.get()
            print 'Result:', result
            num_jobs -= 1
