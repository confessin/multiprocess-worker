#!/usr/bin/env python
# encoding: utf-8

"""
Test implementation for multiprocessing worker.
"""

__author__ = 'mohammad.rafi@inmobi.com (Mohammad Rafi)'

from multiprocess import MultiProcess
from multiprocess import ResultProcessor


# Example
# A worker class which have a result processor and a Task class.

# Define a result processor which prints results.
class MyResultProcessor(ResultProcessor):
    def process_result(self, next_result):
        print next_result


# Define a task class.
# This class will be instantiated before queuing but it will be called
# each time by consumer.
class Task(object):
    def __init__(self, a, b):
        self.a = a
        self.b = b

    def __call__(self):
        return self.call()  # pretend to take some time to do our work

    def __str__(self):
        return '%s * %s' % (self.a, self.b)

    def call(self):
        """Raise ImplementationError.
        It needs to be implemented in Child class."""

        return self.a * self.b


def main():
    # Task of getting squares of numbers from 0-100
    m = MultiProcess()
    m.prepare_consumer()
    m.prepare_result_consumer(MyResultProcessor)
    num_jobs = 0
    for i in range(1000):
        m.enqueue_job(Task(i, i))
        num_jobs += 1
    m.add_end_jobs()


if __name__ == '__main__':
    main()
