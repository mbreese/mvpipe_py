import os
from mvpipe.runner import Runner

class BashRunner(Runner):
    def __init__(self, dryrun, verbose, logger, interpreter=None):
        Runner.__init__(self, dryrun, verbose, logger)
        self.funcs = []
        self.pre =  ''
        self.post = ''
        self.body = ''
        self.out = ''

        self._name = 'bash-script'
        if interpreter:
            self.interpreter = interpreter
        else:
            for intp in ['/bin/bash', '/usr/bin/bash', '/usr/local/bin/bash']:
                if os.path.exists(intp):
                    self.interpreter = intp
                    break
            if not self.interpreter:
                self.interpreter = '/bin/sh'

    def set_interpreter(self, interpreter):
        self.interpreter = interpreter

    def reset(self):
        if self.body:
            self.out += '%s\n' % (self.body)

        self.body = ''

    def done(self):
        self.reset()
        if self.out:
            print '#!%s' % self.interpreter
            print self.out
            print ""
            print self.pre
            for func in self.funcs:
                print "%s" % func
            print self.post


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
            job.jobid = func

            # for out in job.outputs:
            #     self._output_jobs[out] = func

