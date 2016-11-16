import sh
import codecs
from collections import deque

# !! Account for out-of-order messages

class BufferRunner:
    
    def __init__(self, function, maxlen=1e6, count_interval=1000):
        self.function = function
        
        self.buffer = deque(maxlen=maxlen)
        
        self.counter = 0
        self.count_interval = count_interval
        
    def add(self, obj):
        self.buffer.append(obj)
        self.counter += 1
        if self.should_run():
            return self.run()
    
    def should_run(self):
        return self.counter >= self.count_interval
    
    def run(self):
        self.counter = 0
        return self.function(self.buffer)

class DiskBufferRunner:
    
    def __init__(self, function, filename, maxlen=1e6, count_interval=10000):
        
        self.function = function
        self.filename = filename
        
        # In-memory buffer
        self.membuff = deque(maxlen=maxlen)
        
        # On-disk buffer
        self.diskbuff = codecs.open(filename, 'a', encoding='utf-8')
        
        self.counter = 0
        self.total_counter = 0
        self.maxlen = maxlen
        self.count_interval = count_interval
        
    def add(self, obj_memory=None, obj_disk=None):
        if obj_memory:
            self.membuff.append(obj_memory)
        
        if obj_disk:
            self.diskbuff.write(obj_disk + '\n')
        
        self.counter += 1
        self.total_counter += 1
        if self.should_run():
            return self.run()
    
    def should_run(self):
        return self.counter >= self.count_interval
    
    def run(self):
        self.counter = 0
        
        if self.total_counter > self.maxlen:
            self._doRollover()
        
        return self.function(self.membuff, self.filename)
    
    def _doRollover(self):
        tmp_name = self.filename + '.tmp'
        _ = sh.tail('-n', int(self.maxlen), self.filename, _out=open(tmp_name, 'w'))
        _ = sh.cp(tmp_name, self.filename)

