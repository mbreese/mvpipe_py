import os
from mvpipe.runner import Runner

class BashRunner(Runner):
    def __init__(self):
        self.funcs = []
        self.pre =  ''
        self.post = ''
        self.body = ''
        self.out = ''

        self._name = 'bash-script'

    def reset(self):
        if self.pre:
            self.out += '%s\n' % (self.pre)
        if self.body:
            self.out += '%s\n' % (self.body)
        if self.post:
            self.out += '%s\n' % (self.post)

        self.pre =  ''
        self.post = ''
        self.body = ''

    def done(self):
        self.reset()

        if os.path.exists('/bin/bash'):
            print '#!/bin/bash'
        elif os.path.exists('/usr/bin/bash'):
            print  '#!/usr/bin/bash'
        elif os.path.exists('/usr/local/bin/bash'):
            print  '#!/usr/local/bin/bash'
        else:
            print  '#!/bin/sh'

        print self.out
        print ""
        for func in self.funcs:
            print "%s" % func


    def submit(self, job):
        if not self.pre:
            self.pre = job.pre
        if not self.post:
            self.post = job.post

        src = job.src

        if src:
            func = "job_%s" % (len(self.funcs) + 1)
            self.funcs.append(func)
            self.body+="%s() {\n%s\n}\n" % (func, src)
            return func
