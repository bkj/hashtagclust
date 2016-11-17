import sh
import sys
import codecs
from collections import deque

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
    
    def __init__(self, function, filenames, maxlen=1e6, count_interval=10000):
        
        self.function = function
        self.filenames = filenames
        
        self.maxlen = maxlen
        self.count_interval = count_interval
        
        # On-disk buffer
        self.diskbuffs = [codecs.open(filename, 'a', encoding='utf-8') for filename in filenames]
        self.counter = self._init_counters(filenames)
        
        print "created DiskBufferRunner | %s | %d" % (str(filenames), self.counter)
    
    def _init_counters(self, filenames):
        counter = 0
        for filename in filenames:
            try:
                counter = max(counter, int(sh.wc('-l', filename).split()[0]))
            except:
                print sys.stderr, "Can't init counters"
        
        return counter
    
    def add(self, objs=None):
        
        for i,obj in enumerate(objs):
            self.diskbuffs[i].write(obj + '\n')
        
        self.counter += 1
        
        if self.counter >= self.count_interval:
            return self.run()
    
    def run(self):
        self.counter = 0
        self._doRollover()
        return self.function(self.filenames)
    
    def _doRollover(self):
        for filename in self.filenames:
            tmp_name = filename + '.tmp'
            _ = sh.tail('-n', int(self.maxlen), filename, _out=open(tmp_name, 'w'))
            _ = sh.cp(tmp_name, filename)

