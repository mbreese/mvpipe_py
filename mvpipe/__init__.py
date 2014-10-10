import os
import sys

import context
import support
import logger

CONFIG_FILE=os.path.expanduser("~/.mvpiperc")

def load_config(fname):
    config = {}
    if os.path.exists(fname):
        with open(fname) as f:
            for line in f:
                if '=' in line:
                    spl = [x.strip() for x in line.strip().split('=',1)]
                    config[spl[0]] = support.autotype(spl[1])
    return config


def parse(fname, args, verbose=False, logfile=None, **kwargs):
    config = load_config(CONFIG_FILE)
    for k in args:
        config[k] = args[k]

    loader = PipelineLoader(config, verbose=verbose, **kwargs)
    if logfile:
        loader.set_log(logfile)
    loader.load_file(fname)
    return loader


class ParseError(Exception):
    def __init__(self, s, parent=None):
        Exception.__init__(self, s)
        self.parent = parent


class PipelineLoader(object):
    def __init__(self, args, verbose=False):
        self.context = context.RootContext(None, args, loader=self, verbose=verbose)
        self.verbose = verbose
        self.paths = []
        self.logger = None

    def close(self):
        if self.logger:
            self.logger.close()

    def set_log(self, fname):
        if self.logger:
            self.logger.close()
        
        self.logger = logger.FileLogger(fname)

    def log(self, msg):
        if self.logger:
            self.logger.write(msg)
        elif self.verbose:
            sys.stderr.write('%s\n' % msg)

    def load_file(self, fname):
        srcfile = None

        if os.path.exists(fname):
            # abs path (or current dir)
            srcfile = fname

        if not srcfile and self.paths:
            # dir of the current file
            if os.path.exists(os.path.join(self.paths[0], fname)):
                srcfile = os.path.join(self.paths[0],fname)

        if not srcfile:
            # cwd
            if os.path.exists(os.path.join(os.getcwd(), fname)):
                srcfile = os.path.join(os.getcwd(),fname)

        if not srcfile:
            raise ParseError("Error loading file: %s" % fname)

        self.log("Loading file: %s" % (os.path.relpath(srcfile)))

        with open(srcfile) as f:
            self.paths.append(os.path.dirname(os.path.abspath(srcfile)))
            for i, line in enumerate(f):
                if not line or not line.strip():
                    continue

                line = line.strip('\n')

                if line[:2] == '##':
                    continue

                if line[:2] == '#$':
                    spl = line[2:].split('#')
                    line = '#$%s' % spl[0]
                    line = line.strip()
                    if not line:
                        continue

                try:
                    self.context.parse_line(line)
                except ParseError, e:
                    self.log('ERROR: %s\n[%s:%s] %s\n\n' % (e, fname, i+1, line))
                    sys.stderr.write('ERROR: %s\n[%s:%s] %s\n\n' % (e, fname, i+1, line))
                    sys.exit(1)

            self.paths = self.paths[:-1]

        for line in self.context.out:
            self.log(line)


    def build(self, target, runner):
        runner.reset()
        self.log('Attempting to build target: %s' % (target if target else 'default'))
        self.log('Job runner: %s' % runner.name)
        pre = []
        post = []

        for tgt in self.context._targets:
            if '__pre__' in tgt.outputs:
                pre = tgt.eval()

        for tgt in self.context._targets:
            if '__post__' in tgt.outputs:
                post = tgt.eval()

        valid, jobtree = self._build(target, pre, post, runner)

        if valid:
            jobs = jobtree.prune(runner=runner)
            added = True

            submitted = set()
            while added:
                added = False
                for job in jobs:
                    if job in submitted:
                        continue

                    passed = True
                    for dep in job.depends:
                        if dep not in submitted:
                            passed = False
                            break

                    if passed:
                        added = True
                        jobid = runner.submit(job)
                        submitted.add(job)

                        if jobid:
                            job.jobid = jobid
                            if jobid:
                                self.log("Submitted job: %s %s" % (jobid, job.name))
                                if job.outputs:
                                    self.log("      outputs: %s" % ' '.join(job.outputs))
                                if job.depends:
                                    self.log("     requires: %s" % (','.join([x.jobid for x in job.depends if x.jobid])))

                                
                                for i, line in enumerate(job.pre.split('\n')):
                                    if i == 0:
                                        self.log("          pre: %s" % line)
                                    else:
                                        self.log("             : %s" % line)
                                for i, line in enumerate(job.src.split('\n')):
                                    if i == 0:
                                        self.log("          src: %s" % line)
                                    else:
                                        self.log("             : %s" % line)
                                for i, line in enumerate(job.post.split('\n')):
                                    if i == 0:
                                        self.log("         post: %s" % line)
                                    else:
                                        self.log("             : %s" % line)


            if len(submitted) != len(jobs):
                sys.stderr.write("ERROR: Didn't submit as many jobs as we had in the build-graph!")

        else:
            sys.stderr.write("ERROR: Can't build target: %s\n" % (target if target else '*default*'))
            if jobtree.lasterror._exception:
                sys.stderr.write("%s\n" % jobtree.lasterror._exception)


    def _build(self, target, pre, post, runner):
        if target:
            exists = support.target_exists(target)
            if runner and runner.check_file_exists(target):
                exists = True

            if exists:
                return True, None

        # TODO: Keep track of outputs and their jobs at the PipelineLoader level 
        #       - this way you can build more than one target and not repeat dependencies...

        for tgt in self.context._targets:
            if '__pre__' in tgt.outputs or '__post__' in tgt.outputs:
                continue

            match, numargs, outputs = tgt.match_target(target)
            if match:
                good_inputgroup = False
                jobdef = TargetJobDef(tgt, outputs, numargs, pre, post)
                exception = None

                for inputgroup in tgt.inputs:
                    good_inputgroup = False
                    jobdef.reset()

                    for inputstr in inputgroup:
                        good_inputgroup = True

                        try:
                            inputs = tgt.replace_token(inputstr, numargs)
                            if ' ' in inputs:
                                inputs = inputs.split()
                            else:
                                inputs = [inputs,]

                            for next in inputs:
                                jobdef.add_input(next)

                                exists = support.target_exists(next)
                                if runner and runner.check_file_exists(next):
                                    exists = True

                                if not exists:
                                    isvalid, dep = self._build(next, pre, post, runner)
                                    if dep:
                                        jobdef.add_dep(dep)

                                    if not isvalid:
                                        good_inputgroup = False
                                        exception = "Missing file: %s" % next
                                        break

                        except Exception, e:
                            exception = e
                            good_inputgroup = False
                            break

                    if good_inputgroup:
                        break

                if good_inputgroup:
                    return True, jobdef
                else:
                    jobdef.seterror(exception)
                    return False, jobdef

        return False, None


class TargetJobDef(object):
    def __init__(self, target, outputs, numargs, pre=None, post=None):
        self.target = target
        self.outputs = outputs
        self.numargs = numargs
        self._pre = pre
        self._post = post

        self.inputs = []
        self.depends = []

        self._error = False
        self._exception = None

        self._reset_count = 0
        self._src = None
        self.jobid = None

        self._name = None

    def reset(self):
        self._reset_count += 1
        self.inputs = []
        self.depends = []

    def add_input(self, inp):
        self.inputs.append(inp)

    def add_dep(self, child):
        self.depends.append(child)

    def __repr__(self):
        return '<%s|%s>: (%s)' % (', '.join(self.outputs), self._reset_count, ', '.join(self.inputs))

    @property
    def name(self):
        if not self._name:
            if self.target.get('job.name'):
                self._name = self.target.get('job.name')
            else:
                self._name = 'job'
                for line in self.src.split('\n'):
                    if line.strip() and line.strip()[0] != '#':
                        self._name = line.strip().split(' ')[0]
                        break
        return self._name


    @property
    def error(self):
        return self._error

    @property
    def lasterror(self):        
        for c in self.depends:
            if c.lasterror:
                return c

        if self._error:
            return self

        return None

    def seterror(self, exception):
        self._error = True
        self._exception = exception

    @property
    def pre(self):
        if self._pre:
            return '\n'.join(self._pre)
        return ''

    @property
    def post(self):
        if self._post:
            return '\n'.join(self._post)
        return ''

    @property
    def src(self):
        if not self._src:
            src_lines = []

            for line in self.target.eval(self.outputs, self.inputs, self.numargs):
                src_lines.append(line)

            if not src_lines:
                return ''

            self._src = '\n'.join(src_lines)
        return self._src

    def prune(self, jobs=None, outputs=None, runner=None):
        if jobs is None:
            jobs = []
            outputs = {}

        for out in self.outputs:
            if not out in outputs:
                jobs.append(self)
                for o in self.outputs:
                    outputs[o] = self
                break

        # TODO: Convert to return a simple definition of a Job: source code, dependent jobs (either object of jobid)
        for dep in self.depends:
            dep.prune(jobs, outputs)

        self.depends = []

        for inp in self.inputs:
            if not support.target_exists(inp):
                if not runner or not runner.check_file_exists(inp):
                    if not outputs[inp] in self.depends:
                        self.depends.append(outputs[inp])

        return jobs

    def _print(self, lines=None, i=0):
        if not lines:
            lines = []
        
        indent = ' ' * (i*2)
        lines.append('%s%s' % (indent, self))

        for c in self.depends:
            c._print(lines, i+1)

        return '\n'.join(lines)

    def _dump(self):
        # for dep in self.depends:
        #     dep._dump()

        sys.stderr.write('\n%s\n--------\n - (%s)\n - %s\n%s\n\n' % (self, ','.join([str(x) for x in self.depends]), self.target._values, self.src))


