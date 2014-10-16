import os
import subprocess
from mvpipe.runner import Runner

class BashRunner(Runner):
    def __init__(self, dryrun, verbose, logger, interpreter=None, autoexec=False):
        Runner.__init__(self, dryrun, verbose, logger)
        self.funcs = []
        self.pre =  ''
        self.post = ''
        self.body = ''
        self.out = ''
        self.autoexec = autoexec

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
        src = ''
        if self.out:
            src += '#!%s\n' % self.interpreter
            src += "set -o pipefail\n"
            src += '%s\n' % self.out
            src += '\n'
            src += '%s\n' % self.pre

            for func in self.funcs:
                src += '%s\n' % func
            src += '%s\n' % self.post

            print src

            if self.autoexec:
                proc = subprocess.Popen([self.interpreter], stdin=subprocess.PIPE)
                proc.communicate(input=src)
                proc.wait()


    def submit(self, job):
        if not self.pre:
            self.pre = job.pre
        if not self.post:
            self.post = job.post

        src = job.src

        if src:
            func = "job_%s" % (len(self.funcs) + 1)
            self.funcs.append(func)
            self.body += '%s() {\n' % func
            self.body += '%s\n' % src

            test=False

            for out in job.outputs:
                if out[0] != '.':
                    if not test:
                        self.body += 'if [ $? -ne 0 ]; then\n'
                        test = True

                    self.body += '    if [ -e "%s" ]; then rm "%s"; fi\n' % (out, out)
            if test:
                self.body += 'fi\n'
            self.body += '}\n'

            job.jobid = func

            # for out in job.outputs:
            #     self._output_jobs[out] = func

