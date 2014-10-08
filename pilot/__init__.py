import os
import sys

import context
import support

def parse(fname, args, verbose=False, **kwargs):
    loader = PipelineLoader(args, verbose=verbose, **kwargs)
    loader.load_file(fname)

    if verbose:
        for line in loader.context.out:
            sys.stderr.write('%s\n' % line)

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
            raise ParseError("Can not load file: %s" % fname)

        if self.verbose:
            sys.stderr.write("Loading file: %s\n" % (os.path.relpath(srcfile)))

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
                    # if self.verbose:
                    #     sys.stderr.write('>>> %s\n' % line)

                    self.context.parse_line(line)

                except ParseError, e:
                    sys.stderr.write('ERROR: %s\n[%s:%s] %s\n\n' % (e, fname, i+1, line))
                    sys.stderr.write('%s\n' % self.context.lastchild)
                    sys.exit(1)

            self.paths = self.paths[:-1]


    def build(self, target):
        pre = []
        post = []

        for tgt in self.context._targets:
            if '__pre__' in tgt.outputs:
                pre = tgt.eval()

        for tgt in self.context._targets:
            if '__post__' in tgt.outputs:
                post = tgt.eval()

        valid, jobtree = self._build(target, pre, post)

        if valid:
            return jobtree.prune()
        else:
            sys.stderr.write("ERROR: Can't build target: %s\n" % (target if target else '*default*'))
            if jobtree.lasterror._exception:
                sys.stderr.write("%s\n" % jobtree.lasterror._exception)


    def _build(self, target, pre, post):
        if target and support.target_exists(target):
            return True, None

        for tgt in self.context._targets:
            if '__pre__' in tgt.outputs or '__post__' in tgt.outputs:
                continue

            match, numargs, outputs = tgt.match_target(target)
            if match:
                good_inputgroup = False
                jobdef = JobDef(tgt, outputs, numargs, pre, post)
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

                                if not support.target_exists(next):
                                    isvalid, dep = self._build(next, pre, post)
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


class JobDef(object):
    def __init__(self, target, outputs, numargs, pre=None, post=None):
        self.target = target
        self.outputs = outputs
        self.numargs = numargs
        self.pre = pre
        self.post = post

        self.inputs = []
        self.depends = []

        self._error = False
        self._exception = None

        self._reset_count = 0

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
    def src(self):
        src_lines = []

        for line in self.target.eval(self.outputs, self.inputs, self.numargs):
            src_lines.append(line)

        if not src_lines:
            return ''
        
        if self.pre:
            for i, line in enumerate(self.pre):
                src_lines.insert(i,line)

        if self.post:
            for line in self.post:
                src_lines.append(line)

        return '\n'.join(src_lines)

    def prune(self, jobs=None, outputs=None):
        if jobs is None:
            jobs = []
            outputs = {}

        for out in self.outputs:
            if not out in outputs:
                jobs.append(self)
                for o in self.outputs:
                    outputs[o] = self
                break

        for dep in self.depends:
            dep.prune(jobs, outputs)

        self.depends = []

        for inp in self.inputs:
            if not support.target_exists(inp):
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


