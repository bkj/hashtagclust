import os
import codecs
import tempfile

class UnicodeNamedTemporaryFile:
    
    def __init__(self, prefix):
        f = tempfile.NamedTemporaryFile(prefix='hc-%s-' % prefix, delete=False)
        self.name = f.name
        f.close()
    
    def write(self, x):
        tmp = codecs.open(self.name, 'w', encoding='utf-8')
        tmp.write(x)
        tmp.flush()
    
    def close(self):
        os.unlink(self.name)