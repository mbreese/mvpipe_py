'''
Job runner for SJQ - Simple Job Queue

SJQ is included with MVpipe as a single-user batch scheduler

'''

import os
import time
import socket
import string
import multiprocessing
import mvpipe.config
from mvpipe.runner import Runner, Job

import sjq.client
import sjq.server

def_options = {'env': True, 'cwd': os.path.abspath(os.curdir)}

class SJQRunner(Runner):
    def __init__(self, dryrun, verbose, logger, global_hold=False, global_depends=None):
        Runner.__init__(self, dryrun, verbose, logger)
        self.global_hold = global_hold
        self._holding_job = None
        self.global_depends = global_depends if global_depends else []

        self.jobids = []

        self.testjobcount = 1
        self._name = 'sjqjob'

        self._sjq = None

    @property
    def sjq(self):
        if not self._sjq and not self.dryrun:
            try:
                self._sjq = sjq.client.SJQClient(self.verbose)
            except socket.error:
                try:
                    # When we daemonize this, we end up forking *this* process, which
                    # isn't quite what we want. So we need to spawn a subprocess to 
                    # actually start the SJQ server
                    self.log("Missing SJQ server, starting one...")
                    p = multiprocessing.Process(target=sjq.server.start, args=(False, {'sjq.autoshutdown': True, 'sjq.waittime': 60, 'sjq.logfile': '~/.sjq.log'}, True))
                    p.start()
                    time.sleep(2)
                    self._sjq = sjq.client.SJQClient(self.verbose)
                except Exception, e:
                    print e

            if not self._sjq:
                self.log("Cannot start SJQ server - aborting!")
                raise RuntimeError("Cannot start SJQ server - aborting!")

        return self._sjq

    def reset(self):
        pass

    def abort(self):
        if self._holding_job:
            self.kill(self._holding_job.jobid)
        else:
            for jobid in self.jobids:
                self.kill(jobid)

    def done(self):
        if not self.dryrun:
            if self._holding_job:
                self.release(self._holding_job.jobid)
            self.sjq.close()

    def _setup_holding_job(self):
        self._holding_job = Job('sleep 1', name="holding", hold=True, stdout='/dev/null', stderr='/dev/null')
        self.submit(self._holding_job)
        self.global_depends.append(self._holding_job.jobid)

    def check_jobid(self, jobid):
        if not self.dryrun:
            ret = self.sjq.status(jobid)
            for line in ret.split('\n'):
                cols = line.strip().split('\t')
                if cols[0] == str(jobid):
                    if cols[2] in ['H', 'Q', 'U', 'R']:
                        # If the status is anything other than these, then
                        # the output file should have been made or won't be.
                        return True

        return False

    def release(self, jobid):
        if not self.dryrun:
            self.sjq.release(jobid)

    def kill(self, jobid):
        if not self.dryrun:
            self.sjq.kill(jobid)

    def submit(self, job):
        if not job.src:
            return

        if self.global_hold and not self._holding_job:
            self._setup_holding_job()

        body = ''

        if job.pre:
            body = job.pre
            body += '\n'

        body += job.src

        if job.post:
            body += '\n'
            body += job.post

        jobopts = dict(def_options)
        for k in job.args:
            jobopts[k] = job.args[k]

        hold = False
        env = False
        procs = 1
        mem = None
        cwd = None
        stdout = None
        stderr = None
        depends = None
        name = job.name if job.name[0] in string.ascii_letters else 'sjq_%s' % job.name

        if 'hold' in jobopts and jobopts['hold']:
            hold = True

        if 'env' in jobopts and jobopts['env']:
            env = True

        if 'procs' in jobopts and int(jobopts['procs']) > 1:
            procs = int(jobopts['procs'])

        if 'mem' in jobopts:
            mem = jobopts['mem']

        if 'wd' in jobopts:
            cwd = jobopts['wd']

        if 'stdout' in jobopts:
            stdout = os.path.abspath(jobopts['stdout'])

        if 'stderr' in jobopts:
            stderr = os.path.abspath(jobopts['stderr'])

        if job.depids or self.global_depends:
            depids = job.depids
            if self.global_depends:
                depids.extend(self.global_depends)

            depends = ':'.join(depids)

        if 'shell' in jobopts:
            shell = jobopts['shell']
        else:
            shell = mvpipe.config.get_shell()

        src = '#!%s\n' % shell
        src += 'set -o pipefail\nfunc () {\n  %s\n  return $?\n}\n' % body
        src += 'func\n'
        src += 'RETVAL=$?\n'

        tmpbody = ''
        if not 'keepfailed' in jobopts or not jobopts['keepfailed']:
            for out in job.outputs:
                if out[0] != '.':
                    tmpbody += '    if [ -e "%s" ]; then rm "%s"; fi\n' % (out, out)
        
        if tmpbody:
            src += 'if [ $RETVAL -ne 0 ]; then\n'
            src += tmpbody
            src += 'fi\n'
        src += 'exit $RETVAL\n'

        if not self.dryrun:
            ret = self.sjq.submit(src, procs=procs, mem=mem, stderr=stderr, stdout=stdout, env=env, cwd=cwd, name=name, uid=os.getuid(), gid=os.getgid(), depends=depends, hold=hold)
            if ret[:2] == "OK":
                jobid = ret.split(' ')[1]
            else:
                self.log('Error submitting job %s: %s\n' % (job.name, ret), True)
                raise RuntimeError(ret)

        else:
            jobid = 'testjob.%s' % self.testjobcount
            self.testjobcount += 1

        job.jobid = jobid

        print jobid
        self.jobids.append(jobid)

        self.log('job: %s' % jobid)
        for line in src.split('\n'):
            self.log('job: %s' % line.strip('\n'), self.verbose)
