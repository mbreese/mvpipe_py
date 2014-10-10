import os
from mvpipe.runner import Runner

def_options = {'env': True, 'wd': os.path.abspath(os.curdir), 'mail': 'ea', 'hold': False}

class SGERunner(Runner):
    def __init__(self):
        self.jobids = {}

    def reset(self):
        pass

    def done(self):
        pass

    def holding(self):
        return self.submit()

    def submit(self, job):
        if job.pre:
            src = job.pre

        src += job.src

        if job.post:
            src = job.post

        opts = job._clonevals()
        jobopts = def_options
        for k in opts:
            if k[:4] == 'job.':
                jobopts[k[4:]] = opts(k)


        
class SGEJob(object):
    def __init__(self, **kwargs):
        pass
