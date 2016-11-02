import sys
sys.path.append('..')
from buffer_runner import BufferRunner

from time import sleep

def f(x):
    return map(int, x)

print "testing counter..."

chm = BufferRunner(f, count_interval=5, maxlen=10)
for i in range(100):
    x = chm.add(i)
    if x:
        print x

print "testing timer..."
chm = BufferRunner(f, time_interval=2, maxlen=10)
for i in range(100):
    sleep(0.5)
    x = chm.add(i)
    if x:
        print x
    
