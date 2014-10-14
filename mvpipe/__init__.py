import os
import sys
import subprocess

import context
import support
import logger
import runner
import config

def parse(fname, args, logfile=None, dryrun=False, verbose=False, **kwargs):
    config_args = config.load_config(args)
    log_inst = logger.FileLogger(logfile)

    runner_inst = config.get_runner(dryrun, verbose, log_inst)

    loader = PipelineLoader(config_args, runner_inst=runner_inst, logger=log_inst, dryrun=dryrun, verbose=verbose, **kwargs)
    loader.load_file(fname)
    return loader


class ParseError(Exception):
    def __init__(self, s, parent=None):
        Exception.__init__(self, s)
        self.parent = parent


class PipelineLoader(object):
    def __init__(self, args, runner_inst, logger=None, dryrun=False, verbose=False):
        self.context = context.RootContext(None, args, loader=self, verbose=verbose)
        self.verbose = verbose
        self.dryrun = dryrun
        self.paths = []
        self.logger = logger
        self.output_jobs = {}
        self.pending_jobs = {}
        self.runner_inst = runner_inst
        self.is_setup = False

    def close(self):
        self.runner_inst.done()
        if self.is_setup:
            self.teardown()

        if self.logger:
            self.logger.close()

    def abort(self):
        self.runner_inst.abort()
        if self.logger:
            self.logger.close()

    def set_log(self, fname):
        if self.logger:
            self.logger.set_fname(fname)

    def log(self, msg, stderr=False):
        if self.logger:
            self.logger.write(msg)
            if stderr:
                sys.stderr.write(msg)
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
                    self.log('ERROR: %s\n[%s:%s] %s\n\n' % (e, fname, i+1, line), True)
                    sys.exit(1)

            self.paths = self.paths[:-1]

        for line in self.context.out:
            self.log(line)


    def setup(self):
        self.is_setup = True
        for tgt in self.context._targets:
            if '__setup__' in tgt.outputs:
                cmd = tgt.eval_src()
                self.log("[setup]")
                for line in cmd:
                        self.log("setup: %s" % line.strip())
                
                if self.dryrun:
                    return

                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                out, err = proc.communicate()
                if out:
                    self.log("     : %s" % out)
                if err:
                    self.log("     : %s" % err, True)


    def teardown(self):
        for tgt in self.context._targets:
            if '__teardown__' in tgt.outputs:
                cmd = tgt.eval_src()
                self.log("[teardown]")
                for line in cmd:
                    if line and line.strip():
                        self.log("teardown: %s" % line.strip())

                if self.dryrun:
                    return

                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                out, err = proc.communicate()
                if out:
                    self.log("     : %s" % out)
                if err:
                    self.log("     : %s" % err, True)




    def build(self, target):
        if not self.is_setup:
            self.setup()

        self.log("***** STARTING BUILD *****")
        self.runner_inst.reset()
        self.missing = []
        self.pending_jobs = {}

        pre = []
        post = []

        for tgt in self.context._targets:
            if '__pre__' in tgt.outputs:
                pre = '\n'.join(tgt.eval_src())
            if '__post__' in tgt.outputs:
                post = '\n'.join(tgt.eval_src())
            if not target:
                for out in tgt.outputs:
                    if out not in ['__pre__', '__post__', '__setup__', '__teardown__']:
                        target = out
                        break

        self.log('Attempting to build target: %s' % target)
        self.log('Job runner: %s' % self.runner_inst.name)

        self.log('[State]')
        vals=self.context._clonevals()
        for k in sorted(vals):
            self.log('    %s => %s' % (k,vals[k]))

        valid, lastjob = self._build(target, pre, post)

        if valid:
            if type(lastjob) == str:
                return

            joblist = lastjob.flatten()
            added = True

            submitted = set()
            while added:
                added = False
                for job in joblist:
                    if job in submitted:
                        continue

                    if type(job) == str:
                        submitted.add(job)
                        continue

                    added = True
                    self.runner_inst.submit(job)
                    submitted.add(job)

                    if job.jobid:
                        self.log("Submitted job: %s %s" % (job.jobid, job.name))
                        if job.outputs:
                            self.log("      outputs: %s" % ' '.join(job.outputs))
                        if job.depids:
                            self.log("     requires: %s" % (','.join(job.depids)))

                        
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

                        for out in job.outputs:
                            self.output_jobs[out] = job.jobid


            if len(submitted) != len(joblist):
                self.log("WARNING: Didn't submit as many jobs as we had in the build-graph!", True)

        else:
            if self.missing:
                self.log("Missing files: %s\n" % ', '.join(self.missing), True)

            raise ParseError("ERROR: Can't build target: %s\n" % target)


    def _build(self, target, pre, post, indent=0):
        indentstr = ' ' * (indent * 4)
        self.log('%sTrying to build file: %s' % (indentstr, target))

#        if self.verbose:
#            sys.stderr.write('Target: %s\n' % target)

        if target:
            exists = support.target_exists(target)
            if exists:
                self.log('%s  - %s exists' % (indentstr, target))
                return True, None
            
            if target in self.output_jobs:
                self.log('%s  - %s already set to be built (%s)' % (indentstr, target, self.output_jobs[target]))
                return True, self.output_jobs[target]

            exists, jobid = self.runner_inst.check_file_exists(target)
            if exists:
                self.log('%s - %s already set to be built by existing job' % (indentstr, target))
                return True, jobid

        target_found = False
        
        for tgt in self.context._targets:
            if '__pre__' in tgt.outputs or '__post__' in tgt.outputs:
                continue

            match, numargs, outputs = tgt.match_target(target)
            if match:
                target_found = True
                good_input = True
                depends = []

                self.log('%s  - found build definition: %s' % (indentstr, tgt))

                inputs = [tgt.replace_token(inputstr, numargs) for inputstr in tgt.inputs]
                self.log('%s  - required inputs: %s' % (indentstr, inputs))

                try:
                    for inp in inputs:
                        if inp in self.pending_jobs:
                            self.log('%s  - %s pending' % (indentstr, inp))
                            depends.append(self.pending_jobs[inp])
                        else:
                            isvalid, dep = self._build(inp, pre, post, indent+1)

                            if not isvalid:
                                good_input = False
                                break

                            if dep:
                                depends.append(dep)

                except Exception, e:
                    self.log("%s  ***** Exception: %s" % (indentstr, str(e)))
                    good_input = False
                    break

                if good_input:
                    src = '\n'.join(tgt.eval_src(outputs, inputs, numargs))
                    kwargs = {}
                    target_vals = tgt._clonevals()
                    for k in target_vals:
                        if k[:4] == 'job.':
                            kwargs[k[4:]] = target_vals[k]

                    job = runner.Job(src, outputs, depends=depends, pre=pre, post=post, **kwargs)

                    self.log('%s  * submitting job' % (indentstr, ))


                    for out in outputs:
                        self.pending_jobs[out] = job

                    return True, job

                # look for an alternative target

        if not target_found:
            self.missing.append(target)

        return False, None
