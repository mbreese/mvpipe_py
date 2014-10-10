import os
import sys
import datetime

class FileLogger(object):
    def __init__(self, fname):
        self.fname = fname
        self.fobj = open(fname, 'a')

        self.sep()
        self.write("New run: %s" % datetime.datetime.now())
        self.write("Command line: %s" % ' '.join(sys.argv))
        self.write("Current directory: %s" % os.getcwd())

    def close(self):
        self.fobj.close()

    def write(self, line):
        self.fobj.write('%s\n' % line)

    def sep(self):
            self.write('----------------------------------------')
