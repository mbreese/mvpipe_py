import os
from mvpipe.runner import Runner, Job

def_options = {'env': True, 'wd': os.path.abspath(os.curdir), 'mail': 'ea', 'hold': False}

class SGERunner(Runner):
    def __init__(self, dryrun, verbose, global_hold=False):
        Runner.__init__(self, dryrun, verbose)
        self.global_hold = global_hold
        self.global_hold_jobid = None

        self.jobids = {}

    def reset(self):
        pass

    def done(self):
        pass

    def holding(self):
        return self.submit(Job('sleep 5', hold=True))

    def submit(self, job):
        src = ''
        if job.pre:
            src = job.pre

        src += job.src

        if job.post:
            src = job.post

        jobopts = dict(def_options)
        for k in job.args:
            jobopts[k] = job.args[k]
