import os
import sys
import re

class Pilotfile(object):
    def __init__(self, filename=None, args=None):
        if not filename:
            filename = 'Pilotfile'

        if not os.path.exists(filename):
            sys.stderr.write("%s file is missing!\n" % filename)
            sys.exit(1)

        self.global_settings = {}
        self.jobdefs = []

        curjob = None

        with open(filename) as f:
            for i, line in enumerate(f):
                if line[0] == '#':
                    if line[1] == '$':
                        k,v = _parse_setting(line[2:])
                        self.global_settings[k]=v
                elif line[0] in ' \t':
                    if not curjob:
                        sys.stderr.write("Line %s: No current job for command line!\n" % i)
                        sys.exit(1)
                    curjob.add_line(line.strip())
                elif ':' in line[1:]:
                    spl = _remove_comments(line).strip().split(':')
                    if len(spl) != 2:
                        sys.stderr.write("Line %s: Error parsing outputs / inputs!\n" % i)
                        sys.exit(1)
                    
                    curjob = _JobDefinition([x for x in spl[0].split(' ') if x], [x for x in spl[1].split(' ') if x])
                    self.jobdefs.append(curjob)

        for k in args:
            self.global_settings[k] = args[k]

    def build(self, target, pipeline=None):
        if not pipeline:
            pre = None
            post = None
            for jd1 in self.jobdefs:
                if jd1.orig_outputs[0] == '__pre__':
                    pre = jd1
                elif jd1.orig_outputs[0] == '__post__':
                    post = jd1

            pipeline = _Pipeline(pre, post)

        # TODO: Make this support multiple possible targets? (.fastq and .fastq.gz)

        jd = None

        for jd1 in self.jobdefs:
            if target:    
                match, wildcards = jd1.match_output(target, self.global_settings)
                if match:
                    jd = jd1
                    break
            else:
                if jd1.orig_outputs[0] not in  ['__pre__', '__post__']:
                    match, wildcards = jd1.match_output(target, self.global_settings)
                    jd = jd1
                    break


        if match:
            args = {}
            for k in self.global_settings:
                args[k] = self.global_settings[k]
            for k in jd.settings:
                args[k] = jd.settings[k]

            inputs = []
            for inp in jd.inputs:
                inputs.append(_parse_input(inp, wildcards, args))

            for inp in inputs:
                if not os.path.exists(inp):
                    self.build(inp, pipeline)

            pipeline.add(jd, wildcards, args, inputs)

        else:
            sys.stderr.write("Unknown target: %s\n\n" % target)
            sys.exit(1)

        return pipeline

    def _process_line(self, line):
        pass


class _JobDefinition(object):
    def __init__(self, outputs, inputs):
        # sys.stderr.write("outputs: %s, inputs: %s\n" % (outputs, inputs))
        self.orig_outputs = outputs
        self.outputs = []

        for out in outputs:
            regex = '^%s$' % out.replace(".", "\.").replace("(", "\(").replace(")", "\)").replace('*', '(.*)')
            # print out, regex
            self.outputs.append(regex)

        self.inputs = inputs
        self.settings = {}
        self.commands = []

    def __repr__(self):
        if 'name' in self.settings:
            return 'job.%s' % (self.settings['name'].replace(' ', '.'))
        return 'job.%s' % self.orig_outputs[0]

    def add_line(self, line):
        line = line.strip()
        if line[0] == '#':
            if line[1] == '$':
                k,v = _parse_setting(line[2:])
                self.settings[k]=v
        else:
            self.commands.append(_remove_comments(line))

    def match_output(self, target, args):
        wildcards = []
        for out_templ in self.outputs:
            # print "checking %s vs %s" % (target, out_templ) ,

            out = re.compile(_replace_args(out_templ, args))


            if not target:
                wildcards.append('')
                # print "MATCH (null)"
            else:
                m = out.match(target)
                if m:
                    # print "MATCH (%s)" % m
                    if m.groups():
                        wildcards.append(m.group(1))
                    else:
                        wildcards.append('')
                else:
                    # print "NO MATCH"
                    return False, None

        return True, wildcards


class _Pipeline(object):
    def __init__(self, pre=None, post=None):
        self.jobs = []
        self.pre = pre
        self.post = post

    def add(self, job_def, wildcards, args, inputs):
        self.jobs.append((job_def, wildcards, args, inputs))

    def run(self, args):
        already_run = {}
        jid=1

        precmds = []
        postcmds = []

        if self.pre:
            for cmd in self.pre.commands:
                for k in args:
                    cmd = cmd.replace('${%s}' % k, args[k])

                precmds.append(cmd)

        if self.post:
            for cmd in self.post.commands:
                for k in args:
                    cmd = cmd.replace('${%s}' % k, args[k])
                
                postcmds.append(cmd)


        for job in self.jobs:
            myouts = []
            for templ, wildcard in zip(job[0].orig_outputs, job[1]):
                if templ and wildcard:
                    myouts.append(_replace_args(templ.replace('*', wildcard), job[2]))
                else:
                    myouts.append(_replace_args(templ, job[2]))

            needtorun = False
            for out in myouts:
                if out not in already_run:
                    needtorun = True
                    break


            if needtorun:
                # print "RUNNING JOB: %s" % job[0]
                # print "OUTPUTS: %s" % job[0].orig_outputs
                # print "WILDCARDS: %s" % job[1]
                # print "OUTPUTS: %s" % myouts
                # print "ARGS: %s" % job[2]
                # print "INPUTS: %s" % job[3]
                # print job[0].commands
                # print "---------------"

                cmds = []

                for cmd in job[0].commands:
                    for i, out in enumerate(myouts):
                        cmd = cmd.replace('$>%s' % (i+1), out)
                    for i, inp in enumerate(job[3]):
                        cmd = cmd.replace('$<%s' % (i+1), inp)
                    for k in job[2]:
                        cmd = cmd.replace('${%s}' % k, str(job[2][k]))

                    if cmd.strip():
                        cmds.append(cmd)

                if cmds:
                    #print '# %s' % _replace_args(str(job[0]),job[2])
                    print ''
                    print '# jobid: %s' % jid

                    print '# OUT: %s' % '\n# OUT: '.join(myouts)
                    print '# IN: %s' % '\n# IN: '.join(job[3])

                    for inp in job[3]:
                        if inp in already_run:
                            print '# JOBDEP: %s' % already_run[inp]


                    print '\n'.join(precmds)
                    print '\n'.join(cmds)
                    print '\n'.join(postcmds)

                    for out in myouts:
                        already_run[out] = jid
                    jid += 1


def _remove_comments(line):
    spl = line.split('#',1)

    if spl[0] and len(spl) == 2 and spl[0][-1] == '\\':
        return ('%s#%s' % (spl[0][:-1], spl[1])).strip()
    else:
        return spl[0].strip()

    

def _parse_setting(line):
    body = _remove_comments(line)
    if '=' in body:
        return body.split('=', 1)
    else:
        return (body, True)

def _replace_args(inp, args):
    s = inp
    for k in args:
        s=s.replace('${%s}' % k, str(args[k]))

    return s

def _parse_input(inp, wildcards, args):
    s = inp
    for i, val in enumerate(wildcards):
        if val:
            s=s.replace('${%s}' % (i+1), val)

    return _replace_args(s, args)
