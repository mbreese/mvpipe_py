import re
import pilot
from pilot.support import autotype

regex_set = re.compile('^([A-Za-z_\.][A-Za-z0-9_\.]*)[ \t]*=[ \t]*(.*)$')
def setop(context, line, verbose=False):
    m = regex_set.match(line)
    if m:
        if not context.active:
            return True

        k = m.group(1)
        v = autotype(context.replace_token(m.group(2)))
        if v == '[]':
            v = []
        context.set(k, v)

        # if verbose:
        #     sys.stderr.write('    set \"%s\" => %s\n' % (k,v))

        return True
    return False

regex_append = re.compile('^([A-Za-z_\.][A-Za-z0-9_\.]*)[ \t]*\+=[ \t]*(.*)$')
def appendop(context, line, verbose=False):
    m = regex_append.match(line)
    if m:
        if not context.active:
            return True

        k = m.group(1)
        vals = [autotype(x) for x in context.replace_token(m.group(2)).split(' ')]
        for v in vals:
            context.append(k, v)

        # if verbose:
        #     sys.stderr.write('    set \"%s\" => %s\n' % (k,v))

        return True
    return False

regex_unset = re.compile('^unset[ \t]+([A-Za-z_\.][A-Za-z0-9_\.]*)$')
def unsetop(context, line, verbose=False):
    m = regex_unset.match(line)
    if m:
        if not context.active:
            return True

        k = m.group(1)
        context.unset(k)

        # if verbose:
        #     sys.stderr.write('    set \"%s\" => %s\n' % (k,v))

        return True
    return False

regex_if = re.compile('^if[ \t]+(\$\{[a-zA-Z_][a-zA-Z0-9_\.]*\??\})[ \t]*([=<>!]+)[ \t]*(.*)$')
regex_if2 = re.compile('^if[ \t]+(\$\{[a-zA-Z_][a-zA-Z0-9_\.]*\??\})$')
regex_ifnot = re.compile('^if[ \t]+\![ \t]+(\$\{[a-zA-Z_][a-zA-Z0-9_\.]*\??\})$')

def ifop(context, line, verbose=False):
    m = regex_if.match(line)
    if m:
        l = autotype(context.replace_token(m.group(1)))
        op = m.group(2)
        r = autotype(m.group(3))

        test = False
        if op == '==':
            test = l == r
        elif op == '<':
            test = l < r
        elif op == '>':
            test = l > r
        elif op == '<=':
            test = l <= r
        elif op == '>=':
            test = l >= r
        elif op == '!=':
            test = l != r
        else:
            raise pilot.ParseError("Unknown test operator: %s" % op)

        # if verbose:
        #     sys.stderr.write('    if %s (%s) %s => %s\n' % (l,op,r, test))

        context.child = pilot.context.IfContext(context, test)

        return True

    m = regex_if2.match(line)
    if m:
        l = autotype(context.replace_token(m.group(1)))
        test = False
        if l:
            test = True

        # if verbose:
        #     sys.stderr.write('    if %s => %s\n' % (l, test))

        context.child = pilot.context.IfContext(context, test)
        return True

    m = regex_ifnot.match(line)
    if m:
        l = autotype(context.replace_token(m.group(1)))
        test = True
        if l:
            test = False

        # if verbose:
        #     sys.stderr.write('    if %s => %s\n' % (l, test))

        context.child = pilot.context.IfContext(context, test)
        return True
    return False

def elseop(context, line, verbose=False):
    if line.strip() == 'else':
        # if verbose:
        #     sys.stderr.write('    ELSE\n')

        context.parent.child = pilot.context.ElseContext(context)
        return True
    return False

def endifop(context, line, verbose=False):
    if line.strip() == 'endif':
        # if verbose:
        #     sys.stderr.write('    ENDIF\n')

        context.parent.child = None
        return True
    return False


regex_include = re.compile('^include[ \t]+(.*)$')
def includeop(context, line, verbose=False):
    m = regex_include.match(line)
    if m:
        if not context.active:
            return True

        fname = m.group(1)
        if fname and fname[0] == '"' and fname[-1] =='"':
            fname = fname[1:-1]

        # if verbose:
        #     sys.stderr.write('    loading file \"%s\"\n' % (fname))

        context.root.loader.load_file(fname)

        return True
    return False


regex_for = re.compile('^for[ \t]+([a-zA-Z_][a-zA-Z0-9_\.]*)[ \t]*in[ \t]*([^ \t]+)$')
def forop(context, line, verbose=False):
    m = regex_for.match(line)
    if m:
        var = m.group(1)
        varlist = None
        if '..' in m.group(2):
            spl = [x.strip() for x in m.group(2).split('..')]

            frm = autotype(context.replace_token(spl[0]))
            to = autotype(context.replace_token(spl[1]))

            if type(frm) == int and type(to) == int:
                varlist = range(frm, to+1)

                # if verbose:
                #     sys.stderr.write('    FOR %s IN %s\n' % (var, varlist))
        else:
            varlist = m.group(2).split()

        if varlist:
            context.child = pilot.context.ForContext(context, var, varlist)
            return True

        raise pilot.ParseError("Can't handle list: %s" % m.group(2))

    return False           

def doneop(context, line, verbose=False):
    if line.strip() == 'done':
        # if verbose:
        #     sys.stderr.write('    DONE\n')

        context.done()
        context.parent.child = None
        return True
    return False