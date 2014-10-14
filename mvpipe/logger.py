import os
import sys
import datetime

class FileLogger(object):
    def __init__(self, fname):
        self.fname = fname
        if fname:
            self.fobj = open(fname, 'a')
        else:
            self.fobj = None

        self.sep()
        self.write("New run: %s" % datetime.datetime.now())
        self.write("Command line: %s" % ' '.join(sys.argv))
        self.write("Current directory: %s" % os.getcwd())

    def close(self):
        if self.fobj:
            self.fobj.close()

    def write(self, line):
        if self.fobj:
            self.fobj.write('%s\n' % line)

    def sep(self):
            self.write('----------------------------------------')

    def set_fname(self, fname):
        if self.fobj:
            self.fobj.close()
        self.fobj = open(fname, 'a')
        self.fname = fname
        self.sep()
        self.write("New run: %s" % datetime.datetime.now())
        self.write("Command line: %s" % ' '.join(sys.argv))
        self.write("Current directory: %s" % os.getcwd())
