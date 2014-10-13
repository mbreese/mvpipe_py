import re
import mvpipe
from mvpipe.support import autotype

regex_set = re.compile('^([A-Za-z_\.][A-Za-z0-9_\.]*)[ \t]*=[ \t]*(.*)$')
def setop(context, line):
    m = regex_set.match(line)
    if m:
        if not context.active:
            return True

        k = m.group(1)
        v = autotype(context.replace_token(m.group(2)))
        if v == '[]':
            v = []
        context.set(k, v)

        return True
    return False

regex_setine = re.compile('^([A-Za-z_\.][A-Za-z0-9_\.]*)[ \t]*\+=\?[ \t]*(.*)$')
def setineop(context, line):
    m = regex_setine.match(line)
    if m:
        if not context.active:
            return True

        k = m.group(1)
        v = autotype(context.replace_token(m.group(2)))
        if v == '[]':
            v = []
        context.set_ine(k, v)

        return True
    return False

regex_append = re.compile('^([A-Za-z_\.][A-Za-z0-9_\.]*)[ \t]*\+=[ \t]*(.*)$')
def appendop(context, line):
    m = regex_append.match(line)
    if m:
        if not context.active:
            return True

        k = m.group(1)
        vals = autotype(context.replace_token(m.group(2)))
        if type(vals) == list:
            for v in vals:
                context.append(k, v)
        else:
            context.append(k,vals)

        return True
    return False

regex_unset = re.compile('^unset[ \t]+([A-Za-z_\.][A-Za-z0-9_\.]*)$')
def unsetop(context, line):
    m = regex_unset.match(line)
    if m:
        if not context.active:
            return True

        k = m.group(1)
        context.unset(k)

        return True
    return False

regex_if = re.compile('^if[ \t]+(\$\{[^ \t]+\??\})[ \t]*([=<>!]+)[ \t]*(.*)$')
regex_ifset = re.compile('^if[ \t]+(\$\{[^ \t]+\??\})$')
regex_ifnot = re.compile('^if[ \t]+\![ \t]*(\$\{[^ \t]+\??\})$')

def ifop(context, line):
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
            raise mvpipe.ParseError("Unknown test operator: %s" % op)

        context.child = mvpipe.context.IfContext(context, test)

        return True

    m = regex_ifset.match(line)
    if m:
        l = autotype(context.replace_token(m.group(1), allow_missing=True))
        test = False
        if l:
            test = True

        context.child = mvpipe.context.IfContext(context, test)
        return True

    m = regex_ifnot.match(line)
    if m:
        l = autotype(context.replace_token(m.group(1), allow_missing=True))
        test = True
        if l:
            test = False

        context.child = mvpipe.context.IfContext(context, test)
        return True
    return False

def elseop(context, line):
    if line.strip() == 'else':
        context.parent.child = mvpipe.context.ElseContext(context)
        return True
    return False

def endifop(context, line):
    if line.strip() == 'endif':
        context.parent.child = None
        return True
    return False


regex_include = re.compile('^include[ \t]+(.*)$')
def includeop(context, line):
    m = regex_include.match(line)
    if m:
        if not context.active:
            return True

        fname = m.group(1)
        if fname and fname[0] == '"' and fname[-1] =='"':
            fname = fname[1:-1]

        context.root.loader.load_file(fname)

        return True
    return False


regex_for = re.compile('^for[ \t]+([a-zA-Z_][a-zA-Z0-9_\.]*)[ \t]*in[ \t]*([^ \t]+)$')
def forop(context, line):
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

        else:
            varlist = m.group(2).split()

        if varlist:
            context.child = mvpipe.context.ForContext(context, var, varlist)
            return True

        raise mvpipe.ParseError("Can't handle list: %s" % m.group(2))

    return False           

def doneop(context, line):
    if line.strip() == 'done':
        context.done()
        context.parent.child = None
        return True
    return False

def logop(context, line):
    if line.split(' ', 1)[0] == 'log':
        fname = context.replace_token(line.split(' ', 1)[1].strip())
        if fname[0] == '"' and fname[-1] == '"':
            fname = fname[1:-1]
        context.root.loader.set_log(fname)
        return True
    return False

