
def get_runner():
    return mvpipe.runner.bash.BashRunner()


class Job(object):
    def __init__(self, src, name=None, depends=None, pre=None, post=None):
        self.jobid = None
        
        self.src = src
        self.pre = pre
        self.post = post

        if depends:
            self.depends = depends
        else:
            self.depends = []

        if name:
            self.name = name
        else:
            self.name = "job"
            for line in self.src.split('\n'):
                if line.strip() and line.strip()[0] != '#':
                    self.name = line.strip().split(' ')[0]
                    break



class Runner(object):
    def check_file_exists(self, fname):
        return False

    def reset(self):
        raise NotImplementedError

    @property
    def name(self):
        if self._name:
            return self._name
        return self.__class__.__name__

    def done(self):
        raise NotImplementedError

    def submit(self, job):
        raise NotImplementedError

import mvpipe.runner.bash

