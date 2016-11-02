from time import time
from collections import deque

class BufferRunner:
    # !! Account for out-of-order messages
    
    def __init__(self, function, maxlen=1e6, count_interval=100, time_interval=3600):
        self.function = function
        
        self.buffer = deque(maxlen=maxlen)
        
        self.all_counter = 0
        self.counter = 0
        self.count_interval = count_interval
        self.time_interval = time_interval
        self.last_run = time()
        
    def add(self, obj):
        self.buffer.append(obj)
        self.counter += 1
        self.all_counter += 1
        if self.should_run():
            return self.run()
    
    def should_run(self):
        if self.counter >= self.count_interval:
            return True
        elif (time() - self.last_run) >= self.time_interval:
            return True
        else:
            return False
    
    def run(self):
        self.counter = 0
        self.last_run = time()
        return self.function(self.buffer)
