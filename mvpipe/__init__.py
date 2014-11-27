import os
import sys
import subprocess

import context
import support
import logger
import runner
import config

def parse(fname, args, logfile=None, outfile=None, dryrun=False, verbose=False, **kwargs):
    config_args = config.load_config(args)
    log_inst = logger.FileLogger(logfile)

    runner_inst = config.get_runner(dryrun, verbose, log_inst)
    loader_config = config.config_prefix('mvpipe.loader.')

    for k in loader_config:
        if not k in kwargs:
            kwargs[k] = loader_config[k]

    loader = PipelineLoader(config_args, runner_inst=runner_inst, logger=log_inst, outfile=outfile, dryrun=dryrun, verbose=verbose, **kwargs)
    loader.load_file(fname)
    return loader


class ParseError(Exception):
    def __init__(self, s, parent=None):
        Exception.__init__(self, s)
        self.parent = parent


class PipelineLoader(object):
    def __init__(self, args, runner_inst, logger=None, dryrun=False, verbose=False, libpath=None, outfile=None):
        self.context = context.RootContext(None, args, loader=self, verbose=verbose)
        self.verbose = verbose
        self.dryrun = dryrun
        self.paths = []
        self.libpath = [os.path.expanduser(x) for x in libpath.split(':')] if libpath else []
        self.logger = logger
        self.output_jobs = {}
        self.pending_jobs = {}
        self.runner_inst = runner_inst
        self.is_setup = False

        self._outfile = None
        self.outfile_jobids = {}
        if outfile:
            self.set_outfile(outfile)

    def close(self):
        self.runner_inst.done()
        self.teardown()

        if self.logger:
            self.logger.close()

    def abort(self):
        self.runner_inst.abort()
        if self.logger:
            self.logger.close()

    def set_log(self, fname):
        if self.logger:
            if not os.path.exists(os.path.dirname(fname)):
                self.log('Creating directory: %s' % os.path.dirname(fname))
                os.makedirs(os.path.dirname(fname))

            self.logger.set_fname(fname)

    def write_outfile(self, outfile, jobid):
        if self._outfile:
            with open(self._outfile, 'a') as f:
                f.write('%s\t%s\n' % (outfile, jobid))

    def set_outfile(self, fname):
        self.log("Setting output-file: %s" % fname)
        self._outfile = fname
        self.outfile_jobids = {}
        if not os.path.exists(os.path.dirname(fname)):
            self.log('Creating directory: %s' % os.path.dirname(fname))
            os.makedirs(os.path.dirname(fname))
        if not os.path.exists(fname):
            f = open(fname, 'w')
            f.close()
            return

        with open(fname) as f:
            for line in f:
                cols = line.strip('\n').split('\t')
                self.outfile_jobids[cols[0]] = cols[1]

        # rewrite the outlog to remove duplicates...
        # 
        # ON SECOND THOUGHT - KEEP THE DUPLICATES FOR BETTER LOGGING
        #with open(fname, 'w') as f:
        #    for k in self.outfile_jobids:
        #        f.write('%s\t%s\n' % (k, self.outfile_jobids[k]))

    def log(self, msg, stderr=False):
        if self.logger:
            self.logger.write(msg)
            if stderr:
                sys.stderr.write('%s\n' % msg)
        elif self.verbose:
            sys.stderr.write('%s\n' % msg)

    def load_file(self, fname):
        srcfile = None
        if fname == '-':
            f = sys.stdin
        else:
            if os.path.exists(os.path.expanduser(fname)):
                # abs path (or current dir)
                srcfile = fname

            if not srcfile and self.paths:
                # dir of the current file
                if os.path.exists(os.path.join(self.paths[0], fname)):
                    srcfile = os.path.join(self.paths[0],fname)

            if not srcfile and self.libpath:
                for path in self.libpath:
                    if os.path.exists(os.path.join(path, fname)):
                        srcfile = os.path.join(path,fname)
                        break

            if not srcfile:
                raise ParseError("Error loading file: %s" % fname)

            self.log("Loading file: %s" % (os.path.relpath(srcfile)))
            f = open(srcfile)
            self.paths.append(os.path.dirname(os.path.abspath(srcfile)))

        for i, line in enumerate(f):
            if not line or not line.strip():
                continue

            line = line.strip('\n')

            if i == 0 and line[:2] == '#!':
                continue

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
                f.close()
                sys.exit(1)

        f.close()

        if fname != '-':
            self.paths = self.paths[:-1]

        for line in self.context.out:
            self.log(line)
            if line and line[0] == '#':
                sys.stderr.write('%s\n' % line)


    def setup(self):
        for tgt in self.context._targets:
            if '__setup__' in tgt.outputs:
                cmd = tgt.eval_src()
                self.log("[setup]")
                for line in cmd.out:
                        self.log("setup: %s" % line.strip())

                src = '\n'.join(cmd.out)
                kwargs = {}
                target_vals = cmd._clonevals()
                for k in target_vals:
                    if k[:4] == 'job.':
                        kwargs[k[4:]] = target_vals[k]

                job = runner.Job(src, **kwargs)
                return job
        return None
                # if self.dryrun:
                #     return

                # self.run_script('\n'.join(cmd.out))

    def teardown(self):
        for tgt in self.context._targets:
            if '__teardown__' in tgt.outputs:
                cmd = tgt.eval_src()
                self.log("[teardown]")
                for line in cmd.out:
                    if line and line.strip():
                        self.log("teardown: %s" % line.strip())

                src = '\n'.join(cmd.out)
                kwargs = {}
                target_vals = cmd._clonevals()
                for k in target_vals:
                    if k[:4] == 'job.':
                        kwargs[k[4:]] = target_vals[k]

                job = runner.Job(src, **kwargs)
                return job
        return None

                # if self.dryrun:
                #     return

                # self.run_script('\n'.join(cmd.out))

    def run_script(self, script):
        shell = config.get_shell()
        if not shell:
            self.log("ERROR: MISSING SHELL")
            raise ParseError("Valid shell can't be found! (%s)" % shell)

        proc = subprocess.Popen([shell], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        out, err = proc.communicate('#!%s\n%s' % (shell, script))
        if out:
            self.log("     : %s" % out)
        if err:
            self.log("     : %s" % err, True)

    def build(self, target):
        self.log("***** STARTING BUILD *****")
        self.runner_inst.reset()
        self.missing = []
        self.pending_jobs = {}

        pre = ''
        post = ''

        for tgt in self.context._targets:
            if '__pre__' in tgt.outputs:
                pre = '\n'.join(tgt.eval_src().out)
            if '__post__' in tgt.outputs:
                post = '\n'.join(tgt.eval_src().out)
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

            if not lastjob:
                # nothing to do...
                sys.stderr.write('Nothing to do...\n')
                self.log('Nothing to do...')
                return
            joblist = lastjob.flatten()
            added = True

            setup_job = self.setup()
            if setup_job:
                if setup_job.direct_exec:
                    self.run_script(setup_job.src)
                else:
                    self.runner_inst.submit(setup_job)

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

                    if job.direct_exec:
                        self.run_script(job.src)

                        for out in job.outputs:
                            self.output_jobs[out] = '__direct_exec__'

                    else:
                        if setup_job:
                            job.add_dep(setup_job)
                        self.runner_inst.submit(job)
                    
                    submitted.add(job)

                    if job.jobid:
                        self.log("Submitted job: %s %s" % (job.jobid, job.name))
                        if job.outputs:
                            self.log("      outputs: %s" % ' '.join(job.outputs))
                        if job.depids:
                            self.log("     requires: %s" % (','.join(job.depids)))

                        if job.pre: 
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
                        if job.post:
                            for i, line in enumerate(job.post.split('\n')):
                                if i == 0:
                                    self.log("         post: %s" % line)
                                else:
                                    self.log("             : %s" % line)

                        for out in job.outputs:
                            self.output_jobs[out] = job.jobid
                            self.write_outfile(out, job.jobid)

            teardown_job = self.teardown()
            if teardown_job:
                if teardown_job.direct_exec:
                    self.run_script(teardown_job.src)
                else:
                    for job in submitted:
                        teardown_job.add_dep(job)
                    self.runner_inst.submit(teardown_job)

            if len(submitted) != len(joblist):
                self.log("WARNING: Didn't submit as many jobs as we had in the build-graph!", True)
                self.log("Build-list: %s" % ','.join([str(x) for x in joblist]), True)
                self.log("Submitted : %s" % ','.join([str(x) for x in submitted]), True)

        else:
            if self.missing:
                self.log("Missing files: %s\n" % ', '.join([str(x) for x in self.missing]), True)

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

            if target in self.outfile_jobids:
                valid = self.runner_inst.check_jobid(self.outfile_jobids[target])
                if valid:
                    self.log('%s - %s already set to be built by existing job (%s)' % (indentstr, target, self.outfile_jobids[target]))
                    return True, self.outfile_jobids[target]
                else:
                    self.log('%s - %s already set to be built by existing job (%s), but it is no longer valid!' % (indentstr, target, self.outfile_jobids[target]))

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
                    tcxt = tgt.eval_src(outputs, inputs, numargs)
                    src = '\n'.join(tcxt.out)
                    kwargs = {}
                    target_vals = tcxt._clonevals()
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
