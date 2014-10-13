import sys

class Job(object):
    def __init__(self, src, outputs=None, name=None, depends=None, pre=None, post=None, **kwargs):
        self.jobid = None

        # these are job runner specific settings
        self.args=kwargs

        self.src = src
        self.outputs = outputs
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
            else:
                depids.append(d.jobid)

        return depids


    def add_dep(self, dep):
        self._depends.add(dep)

    def _dump(self, i=0):
        for dep in self._depends:
            dep._dump(i+1)

        indent = ' ' * (i * 4)
        sys.stderr.write('%s%s\n' % (indent, '\,'.join(self.outputs)))

    def flatten(self, l=None):
        if l is None:
            l = set()
        
        for d in self._depends:
            if type(d) == str:
                l.add(d)
            else:
                d.flatten(l)

        l.add(self)

        return l


class Runner(object):
    def __init__(self, dryrun, verbose):
        self.dryrun = dryrun
        self.verbose = verbose
        
    def check_file_exists(self, fname):
        return False, None

    def reset(self):
        raise NotImplementedError

    def abort(self):
        pass

    @property
    def name(self):
        if self._name:
            return self._name
        return self.__class__.__name__

    def done(self):
        raise NotImplementedError

    def submit(self, job, dryrun=False, verbose=False):
        raise NotImplementedError

