import sys
import time
import datetime

class Timer:
    def __init__(self):
        self.start_time = time.time()

    def start(self):
        self.start_time = time.time()

    def eltime(self):
        return time.time() - self.start_time

    def eltime_pr(self, outstring, **print_args):
        eltime = time.time() - self.start_time
        elapsed = str(datetime.timedelta(seconds=int(eltime )))
        print( outstring + elapsed, **print_args)
