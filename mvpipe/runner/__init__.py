import sys

class Job(object):
    def __init__(self, src, outputs=None, name=None, depends=None, pre=None, post=None, **kwargs):
        self.jobid = None

        # these are job runner specific settings
        self.args=kwargs

        self.src = src
        self.outputs = outputs if outputs else []
        self.pre = pre
        self.post = post

        if depends:
            self._depends = set(depends)
        else:
            self._depends = set()

        if name:
            self.name = name
        else:
            self.name = "job"
            for line in self.src.split('\n'):
                if line.strip() and line.strip()[0] != '#':
                    self.name = line.strip().split(' ')[0]
                    break

    @property
    def depids(self):
        depids = []
        for d in self._depends:
            if type(d) == str:
                depids.append(d)
            elif d.jobid:
                depids.append(d.jobid)

        return depids


    def __repr__(self):
        return '<job: %s>' % ','.join(self.outputs)

    def add_dep(self, dep):
        self._depends.add(dep)

    def _dump(self, i=0):
        for dep in self._depends:
            dep._dump(i+1)

        indent = ' ' * (i * 4)
        sys.stderr.write('%s%s\n' % (indent, self))

    def flatten(self, l=None):
        if l is None:
            l = []
        
        for d in self._depends:
            if type(d) == str:
                l.append(d)
            else:
                d.flatten(l)

        if self not in l:
            l.append(self)

        return l


class Runner(object):
    def __init__(self, dryrun, verbose, logger=None):
        self.dryrun = dryrun
        self.verbose = verbose
        self.logger = logger
        self._name = None

        # self._output_jobs = {}

    def check_jobid(self, jobid):
        return False, None

    def reset(self):
        raise NotImplementedError

    def abort(self):
        pass

    def log(self, msg, tostderr=False):
        if self.logger:
            self.logger.write('%s' % msg)
            if tostderr:
                sys.stderr.write('%s\n' % msg)
        else:
            sys.stderr.write('%s\n' % msg)


    @property
    def name(self):
        if self._name:
            return self._name
        return self.__class__.__name__

    def done(self):
        raise NotImplementedError

    def submit(self, job, dryrun=False, verbose=False):
        raise NotImplementedError

