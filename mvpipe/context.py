import os
import re
import subprocess
import sys

import ops
import mvpipe
import mvpipe.runner

class ExecContext(object):
    def __init__(self, parent=None, initvals=None, verbose=False):
        self.parent = parent
        if parent:
            self.level = parent.level + 1
            self.verbose = parent.verbose
        else:            
            self.verbose = verbose

        self.child = None
        self._values = {}

        self._var_numargs = None
        self._var_outputs = None
        self._var_inputs = None

        self.test = True

        if initvals:
            for k in initvals:
                self._values[k] = initvals[k]

    def __repr__(self):
        return '<%s:%s>' % (self.__class__.__name__, self.level)

    @property
    def var_outputs(self):
        if self._var_outputs:
            return self._var_outputs
        elif self.parent:
            return self.parent.var_outputs
        return None

    @property
    def var_inputs(self):
        if self._var_inputs:
            return self._var_inputs
        elif self.parent:
            return self.parent.var_inputs
        return None

    @property
    def var_numargs(self):
        if self._var_numargs:
            return self._var_numargs
        elif self.parent:
            return self.parent.var_numargs
        return None

    @property
    def root(self):
        if self.parent:
            return self.parent.root
        return self

    @property
    def active(self):
        if not self.test:
            return False

        if self.parent:
            return self.parent.active
        return True

    @property
    def lastchild(self):
        if self.child:
            return self.child.lastchild
        return self

    def get(self, k):
        if k in self._values:
            return self._values[k]

        if self.parent:
            return self.parent.get(k)

        if k in os.environ:
            return os.environ[k]
        return None

    def _contains_append(self, k, v):
        if k in self._values:
            if type(self._values[k]) == list:
                self._values[k].append(v)
            else:
                self._values[k] = [self._values[k],v]

            return True

        if self.parent:
            return self.parent._contains_append(k,v)

    def _contains_set(self, k, v):
        if k in self._values:
            self._values[k] = v
            return True
        if self.parent:
            return self.parent._contains_set(k,v)
        return False

    def contains(self, k):
        if k in self._values:
            return True
        if self.parent:
            return self.parent.contains(k)
        return False

    def set(self, k, v):
        if not self._contains_set(k, v):
            self._values[k] = v

    def set_ine(self, k, v):
        # set the value if it doesn't already exist
        # (allows for defaults to be given on command-line)
        if not self.contains(k):
            self._values[k] = v

    def append(self, k, v):
        if not self._contains_append(k, v):
            self._values[k] = [v,]

    def unset(self, k):
        if k in self._values:
            del self._values[k]
        elif self.parent:
            self.parent.unset(k)

    def _clonevals(self):
        vals = {}
        for k in self._values:
            vals[k] = self._values[k]

        if self.parent:
            pvals = self.parent._clonevals()
            for k in pvals:
                vals[k] = pvals[k]

        return vals

    def parse_line(self, line):
        if self.child:
            return self.child.parse_line(line)

        if not self.eval_line(line):
            raise mvpipe.ParseError("Don't know how to parse line: %s" % line)

    def replace_token(self, token, numargs=None, allow_missing=False):
        if not token:
            return ''
        
        token = token.replace("\\$", "$__ESCAPED_$__")
        token = token.replace("\\@", "$__ESCAPED_@__")

        # print "****"
        # print "Evaluating: %s" % token

        regex_var = re.compile('^(.*)\$\{([a-zA-Z_][a-zA-Z0-9_\.]*\??)\}(.*)$')

        # var-replace
        m = regex_var.match(token)

        while m:
            if m.group(2):
                # print "var found => %s" % m.group(2)

                if m.group(2)[-1] == '?':
                    k = m.group(2)[:-1]
                    allow_missing = True
                else:
                    k = m.group(2)

                val = self.get(k)
                if type(val) == list:
                    val = ' '.join(val)

                if val is None:
                    if not allow_missing:
                        raise mvpipe.ParseError("Variable \"%s\" not found!" % k)
                    else:
                        val = ''

                token = '%s%s%s' % (m.group(1), val, m.group(3))
                m = regex_var.match(token)
            else:
                m = None

        # var-replace arrays - replace foo_@{bar}_baz with space delimited versions... foo_bar1_baz foo_bar2_baz foo_bar3_baz...
        regex_array = re.compile('^(.*?)([A-Za-z0-9_\-\.]*)\@\{([a-zA-Z_][a-zA-Z0-9_\.]*)\}([A-Za-z0-9_\-\.]*)(.*)$')
        m = regex_array.match(token)

        while m:
            if m.group(3):
                # print "1", m.group(1)
                # print "2", m.group(2)
                # print "3", m.group(3)
                # print "4", m.group(4)
                # print "5", m.group(5)
                # print "var found => %s" % m.group(2)

                allow_missing = False
                k = m.group(3)

                val = self.get(k)

                if val is None:
                    raise mvpipe.ParseError("Variable \"%s\" not found!" % k)

                if type(val) != list:
                    val = [val,]

                ins_vals = []
                for v in val:
                    ins_vals.append('%s%s%s' % (m.group(2), v, m.group(4)))

                token = '%s%s%s' % (m.group(1), ' '.join(ins_vals) , m.group(5))
                m = regex_array.match(token)
            else:
                m = None

        # var-replace ranges - replace foo@{1..3}baz with space delimited versions... foo1baz foo2baz foo3baz...
        regex_range = re.compile('^(.*?)([A-Za-z0-9_\-\.]*)\@\{[ \t]*([^ \t]+)[ \t]*\.\.[ \t]*([^ \t]+)[ \t]*\}([A-Za-z0-9_\-\.]*)(.*?)$')
        m = regex_range.match(token)

        while m:
            if m.group(3) and m.group(4):
                # print "var found => %s" % m.group(2)

                allow_missing = False
                frm = mvpipe.support.autotype(self.replace_token(m.group(3)))
                to = mvpipe.support.autotype(self.replace_token(m.group(4)))

                if type(frm) != int:
                    raise mvpipe.ParseError("Invalid range start \"%s\"!" % frm)
                if type(to) != int:
                    raise mvpipe.ParseError("Invalid range end \"%s\"!" % to)

                ins_vals = []
                for i in range(frm, to+1):
                    ins_vals.append('%s%s%s' % (m.group(2), i, m.group(5)))

                token = '%s%s%s' % (m.group(1), ' '.join(ins_vals) , m.group(6))
                m = regex_range.match(token)
            else:
                m = None

        # given numarg-replace
        if numargs:
            regex = re.compile('^(.*)\$\{([0-9]+)\}(.*)$')
            m = regex.match(token)

            while m:
                if m.group(2):
                    mi = int(m.group(2)) - 1
                    if mi < len(numargs):
                        # print "var found => %s" % m.group(2)
                        token = '%s%s%s' % (m.group(1), numargs[mi], m.group(3))
                        m = regex.match(token)
                    else:
                        raise mvpipe.ParseError("Unknown num-arg: ${%s}" % m.group(2))
                else:
                    m = None

        # global numarg-replace
        if self.var_numargs:
            regex = re.compile('^(.*)\$\{([0-9]+)\}(.*)$')
            m = regex.match(token)

            while m:
                if m.group(2):
                    mi = int(m.group(2)) - 1
                    if mi < len(self.var_numargs):
                        # print "var found => %s" % m.group(2)
                        token = '%s%s%s' % (m.group(1), self.var_numargs[mi], m.group(3))
                        m = regex.match(token)
                    else:
                        raise mvpipe.ParseError("Unknown num-arg: ${%s}" % m.group(2))
                else:
                    m = None

        # inputs-replace
        if self.var_inputs:
            regex = re.compile('^(.*)\$\<([0-9]*)(.*)$')
            m = regex.match(token)

            while m:
                if m.group(2):
                    mi = int(m.group(2)) - 1
                    if 0 <= mi and mi < len(self.var_inputs):
                        # print "var found => %s" % m.group(2)
                        token = '%s%s%s' % (m.group(1), self.var_inputs[mi], m.group(3))
                    else:
                        raise mvpipe.ParseError("Unknown input-num: $<%s" % m.group(2))
                else:
                    token = '%s%s%s' % (m.group(1), ' '.join(self.var_inputs), m.group(3))

                m = regex.match(token)

        # outputs-replace
        if self.var_outputs:
            regex = re.compile('^(.*)\$\>([0-9]*)(.*)$')
            m = regex.match(token)

            while m:
                if m.group(2):
                    mi = int(m.group(2)) - 1
                    if mi < len(self.var_outputs):
                        # print "var found => %s" % m.group(2)
                        token = '%s%s%s' % (m.group(1), self.var_outputs[mi], m.group(3))
                    else:
                        raise mvpipe.ParseError("Unknown output-num: $>%s" % m.group(2))
                else:
                    token = '%s%s%s' % (m.group(1), ' '.join(self.var_outputs), m.group(3))
                
                m = regex.match(token)

        # shell-out
        regex = re.compile('^(.*)\$\((.*)\)(.*)$')
        m = regex.match(token)

        while m:
            if m.group(2):
                cmd = m.group(2)
                # print "shell found => %s" % cmd
                
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                out, err = proc.communicate()
                # if out:
                #     sys.stderr.write('%s *> %s\n' % (cmd, out))
                if err:
                    self.root.loader.log('$(%s) => %s\n' % (cmd, err))

                if proc.returncode != 0:
                    raise mvpipe.ParseError("Error running shell command: %s" % cmd)
                token = '%s%s%s' % (m.group(1), out.strip(), m.group(3))

                m = regex.match(token)
            else:
                m = None

        token = token.replace("$__ESCAPED_$__", "$")
        token = token.replace("$__ESCAPED_@__", "@")

        return token

    def eval_line(self, line):
        if line[:2] != '#$':
            if line[0] not in ['#',' ','\t'] and ':' in line:
                self.child = TargetContext(self, line)
                if not self.active:
                    self.child.badtarget=True
                self.root._targets.append(self.child)
                return True

            if self.active:
                self.root.out.append(self.replace_token(line))
            return True

        line = line[2:].strip()

        for op in [ops.setop, ops.setineop, ops.unsetop, ops.appendop, ops.ifop, ops.forop]:
            if op(self, line):
                return True

        return False

class RootContext(ExecContext):
    def __init__(self, parent, initvals=None, loader=None, verbose=False):
        ExecContext.__init__(self, parent, initvals, verbose)
        self.loader = loader
        self._targets = []
        self.out = []
        self.level=0

    def eval_line(self, line):
        if ExecContext.eval_line(self, line):
            return True
        else:
            if line[:2] == '#$':
                # If this isn't a normal line, check for an import
                if ops.includeop(self, line[2:].strip()):
                    return True

                if ops.logop(self, line[2:].strip()):
                    return True

                if ops.outfileop(self, line[2:].strip()):
                    return True

        return False


class IfContext(ExecContext):
    def __init__(self, parent, test):
        ExecContext.__init__(self, parent)
        self.test = test

    def set(self, k, v):
        self.parent.set(k,v)

    def eval_line(self, line):
        if line[:2] == '#$':
            for op in [ops.elseop, ops.endifop]:
                if op(self, line[2:].strip()):
                    return True
        return ExecContext.eval_line(self, line)


class ElseContext(ExecContext):
    def __init__(self, parent):
        ExecContext.__init__(self, parent.parent)
        self.test = not parent.test

    def set(self, k, v):
        self.parent.set(k,v)

    def eval_line(self, line):
        if line[:2] == '#$':
            for op in [ops.endifop,]:
                if op(self, line[2:].strip()):
                    return True

        return ExecContext.eval_line(self, line)


class ForContext(ExecContext):
    def __init__(self, parent, var, varlist):
        ExecContext.__init__(self, parent)
        self.var = var
        self.varlist = varlist
        self._body = []
        self.loop_count = 0

    def set(self, k, v):
        self.parent.set(k,v)

    def eval_line(self, line):
        if line[:2] == '#$':
            tmp = line[2:].strip()

            if self.loop_count == 0 and tmp == 'done':
                self.done()
                self.parent.child = None
                return True

            if line[:2] == '#$':
                tmp = line[2:].strip()
                if tmp.split(' ')[0] == 'for':
                    self.loop_count += 1
                elif tmp == 'done':
                    self.loop_count -= 1

        self._body.append(line)
        return True

    def done(self):
        for i in self.varlist:
            sub = ExecContext(self)
            sub.set(self.var, i)
            self.child = sub

            for line in self._body:
                sub.parse_line(line)
                # self.root.out.append(ret)
        self.child = None
        self.parent.child = None

class TargetContext(ExecContext):
    def __init__(self, parent, defline):
        # we capture a copy of the current scope
        # this way variables in the global scope 
        # can be altered for retaining tasks
        #
        # However, we don't link into the global scope,
        # so targets can't change variables in the global
        # scope... only their own. Very similar to a closure
        # in that regard.

        ExecContext.__init__(self, None, parent._clonevals())
        self.rootctx = parent
        self.verbose = parent.verbose

        self.level = len(parent.root._targets)

        self._body = []
        self.defline = defline
        self._leading_whitespace = ''
        self.out = []
        self.badtarget = False

        spl = defline.split(':')

        # the target will output these files
        try:
            self.outputs = self.replace_token(spl[0].strip()).split()
        except:
            self.badtarget = True
        self.outputs_regex = []

        for out in self.outputs:
            stars = 0
            for ch in out:
                if ch == '%':
                    stars += 1
            if stars > 1:
                raise ValueError("Target names can only have one '%' wildcard.")

            regex = '^%s$' % re.escape(self.replace_token(out)).replace('\%', '(.*)')
            self.outputs_regex.append(re.compile(regex))

        # the target requires one of these groups of files...
        # these are not fully evaluated now, but will need to be at run time
        # (for output-based wildcard matching).

        try:
            self.inputs = [x.strip() for x in self.replace_token(spl[1]).split()]
        except:
            self.badtarget = True
            self.inputs = None

#        print self.defline
#        print self.outputs
#        print [x.pattern for x in self.outputs_regex]
#        if self.inputs:
#            print self.inputs

    def __repr__(self):
        return self.defline

    def eval_line(self, line):
        if line and line[0] in [' ', '\t']:
            if not self._leading_whitespace:
                while line[0] in [' ', '\t']:
                    self._leading_whitespace = '%s%s' % (self._leading_whitespace, line[0])
                    line = line[1:]

            self._body.append(line.rstrip().lstrip(self._leading_whitespace))
            return True
        else:
            self.rootctx.child = None
            self.rootctx.eval_line(line)
            return True

    def match_target(self, target):
        if self.badtarget:
            return False, None, None

        wildcards = []
        outputs = []
        match_target = False
        for out, regex in zip(self.outputs, self.outputs_regex):
            if not target:
                # self.rootctx.root.loader.log("MATCH (null) %s " % target)
                match_target = True
                wildcards.append('')
                outputs.append(out)
            else:
                m = regex.match(target)
                if m:
                    # self.rootctx.root.loader.log("MATCH (%s) %s" % (regex.pattern, target))
                    match_target = True
                    if m.groups():
                        wildcards.append(m.group(1))
                        outputs.append(out.replace("%", m.group(1)))
                    else:
                        wildcards.append('')
                        outputs.append(out)
                # else:
                    # self.rootctx.root.loader.log("NO MATCH (%s)" % regex.pattern)

        if match_target:
            return True, wildcards, outputs
        else:
            return False, None, None

    def eval_src(self, outputs=None, inputs=None, numargs=None):
        ctx = TargetExecContext(self._clonevals(), outputs, inputs, numargs, verbose=self.verbose)
        for line in self._body:
            ctx.parse_line(line)
        return ctx


class TargetExecContext(ExecContext):
    def __init__(self, initvals, outputs, inputs, numargs, verbose=False):
        ExecContext.__init__(self, None, initvals=initvals, verbose=verbose)
        self.out = []
        self._var_outputs = outputs
        self._var_inputs = inputs
        self._var_numargs = numargs
        self.level = 0
