import subprocess
import mvpipe.config
from mvpipe.runner import Runner

class BashRunner(Runner):
    def __init__(self, dryrun, verbose, logger, autoexec=False):
        Runner.__init__(self, dryrun, verbose, logger)
        self.funcs = []
        self.pre =  ''
        self.post = ''
        self.body = ''
        self.out = ''
        self.autoexec = autoexec

        self._name = 'bash-script'

    def reset(self):
        if self.body:
            self.out += '%s\n' % (self.body)

        self.body = ''

    def done(self):
        self.reset()
        src = ''
        if self.out:
            shell = mvpipe.config.get_shell()
            src += '#!%s\n' % shell
            src += "set -o pipefail\n"
            src += '%s\n' % self.out
            src += '\n'
            src += '%s\n' % self.pre

            for func in self.funcs:
                src += '%s\n' % func
            src += '%s\n' % self.post

            print src

            if self.autoexec:
                proc = subprocess.Popen([shell], stdin=subprocess.PIPE)
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
            tmpbody = ''
            for out in job.outputs:
                if not 'keepfailed' in job.args or not job.args['keepfailed']:
                    if out[0] != '.':
                        test = True
                        tmpbody += '    if [ -e "%s" ]; then rm "%s"; fi\n' % (out, out)


            if test:
                self.body += 'if [ $? -ne 0 ]; then\n'

            self.body += tmpbody

            if test:
                self.body += 'fi\n'

            self.body += '}\n'

            job.jobid = func

            # for out in job.outputs:
            #     self._output_jobs[out] = func

