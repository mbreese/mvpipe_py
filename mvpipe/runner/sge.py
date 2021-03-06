import os
import string
import subprocess

import mvpipe.support
import mvpipe.config
from mvpipe.runner import Runner, Job

def_options = {'env': True, 'wd': os.path.abspath(os.curdir), 'mail': 'ea', 'hold': False}

'''
SGE options:

global_hold - start all pipelines with a "placeholder" job to synchronize
              starts (and ensure that all jobs are submitted correctly
              before releasing the pipeline

account     - a default account to use

parallelenv - the name of the parallel environment for multi-processor jobs
              default: 'shm'

hvmem_total - h_vmem should be specified as the total amount of memory, default
              is to specify it as the amount of memory per-processor
              (only used when procs > 1)

shell       - a shell to use for the script (default(s): /bin/bash, /usr/bin/bash, /usr/local/bin/bash, /bin/sh)

'''
class SGERunner(Runner):
    def __init__(self, dryrun, verbose, logger, global_hold=False, global_depends=None, account=None, parallelenv='shm', hvmem_total=False):
        Runner.__init__(self, dryrun, verbose, logger)
        self.global_hold = global_hold
        self._holding_job = None
        self.global_depends = global_depends if global_depends else []
        self.account = account
        self.parallelenv = parallelenv
        self.hvmem_total = hvmem_total

        self.jobids = []

        self.testjobcount = 1

    def reset(self):
        pass

    def abort(self):
        if self._holding_job:
            self.qdel(self._holding_job.jobid)
        else:
            for jobid in self.jobids:
                self.qdel(jobid)

    def done(self):
        if not self.dryrun:
            if self._holding_job:
                self.qrls(self._holding_job.jobid)

    def _setup_holding_job(self):
        self._holding_job = Job('sleep 1', name="holding", hold=True, stdout='/dev/null', stderr='/dev/null', walltime="00:00:30")
        self.submit(self._holding_job)
        self.global_depends.append(self._holding_job.jobid)

    def check_jobid(self, jobid):
        with open('/dev/null', 'w') as devnull:
            if subprocess.call(["qstat", "-j", jobid], stdout=devnull, stderr=devnull) == 0:
                return True
            return False

    def qrls(self, jobid):
        subprocess.call(["qrls", jobid])

    def qdel(self, jobid):
        subprocess.call(["qdel", jobid])

    def submit(self, job):
        if not job.src:
            return

        if self.global_hold and not self._holding_job:
            self._setup_holding_job()

        jobopts = dict(def_options)
        for k in job.args:
            jobopts[k] = job.args[k]

        body = ''

        if not 'nopre' in jobopts or not jobopts['nopre']:
            if job.pre:
                body = job.pre
                body += '\n'

        body += job.src

        if job.post:
            body += '\n'
            body += job.post


        if 'shell' in jobopts:
            shell = jobopts['shell']
        else:
            shell = mvpipe.config.get_shell()

        src = '#!%s\n' % shell
        src += '#$ -w e\n'
        src += '#$ -terse\n'
        src += '#$ -N %s\n' % (job.name if job.name[0] in string.ascii_letters else 'mvp_%s' % job.name)

        if 'hold' in jobopts and jobopts['hold']:
            src += '#$ -h\n'

        if 'env' in jobopts and jobopts['env']:
            src += '#$ -V\n'

        if 'walltime' in jobopts:
            src += '#$ -l h_rt=%s\n' % mvpipe.support.calc_time(jobopts['walltime'])

        if 'procs' in jobopts and int(jobopts['procs']) > 1:
            src += '#$ -pe %s %s\n' % (self.parallelenv, jobopts['procs'])

        if 'mem' in jobopts:
            if 'procs' in jobopts and not self.hvmem_total:
                procs = int(jobopts['procs'])

                #convert the mem option to a per-processor amount (save the units)
                mem = jobopts['mem']
                mem_num = ''
                while mem[0] in '0123456789.':
                    mem_num += mem[0]
                    mem = mem[1:]

                src += '#$ -l h_vmem=%s%s\n' % (float(mem_num) / procs, mem)

            else:
                src += '#$ -l h_vmem=%s\n' % jobopts['mem']

        if 'stack' in jobopts:
            src += '#$ -l h_stack=%s\n' % jobopts['stack']

        if job.depids or self.global_depends:
            depids = job.depids
            if self.global_depends:
                depids.extend(self.global_depends)

            if depids:
                src += '#$ -hold_jid %s\n' % ','.join(depids)

        # if 'priority' in jobopts:
        #     src += '#$ -p %s\n' % jobopts['priority']

        if 'qos' in jobopts:
            # this is actually the "Project" in SGE terms
            src += '#$ -P %s\n' % jobopts['qos']

        if 'queue' in jobopts:
            src += '#$ -q %s\n' % jobopts['queue']

        if 'mail' in jobopts:
            src += '#$ -m %s\n' % jobopts['mail']

        if 'wd' in jobopts:
            src += '#$ -wd %s\n' % jobopts['wd']

        if 'account' in jobopts:
            src += '#$ -A %s\n' % jobopts['account']
        elif self.account:
            src += '#$ -A %s\n' % self.account

        # if monitor:
        #     src += '#$ -o /dev/null\n'
        #     src += '#$ -e /dev/null\n'
        # else:
        if 'stdout' in jobopts:
            src += '#$ -o %s\n' % jobopts['stdout']

        if 'stderr' in jobopts:
            src += '#$ -e %s\n' % jobopts['stderr']

        src += '#$ -notify\n'
        src += 'FAILED=""\n'
        src += 'notify_stop() {\nkill_deps_signal "SIGSTOP"\n}\n'
        src += 'notify_kill() {\nkill_deps_signal "SIGKILL"\n}\n'
        src += 'kill_deps_signal() {\n'
        src += '  FAILED="1"\n'
        src += '  kill_deps\n'

        # if monitor:
        #     src += '  "%s" "%s" abort "%s.$JOB_ID" "$1"\n' % (clustrun.CLUSTRUN_MON_BIN, monitor, cluster)

        src += '}\n'

        src += 'kill_deps() {\n'
        src += '  DEPS="$(qstat -f -j $JOB_ID | grep jid_successor_list | awk \'{print $2}\' | sed -e \'s/,/ /g\')"\n'
        src += '  if [ "$DEPS" != "" ]; then\n'
        src += '    qdel $DEPS\n'
        src += '  fi\n'
        src += '}\n'

        src += 'trap notify_stop SIGUSR1\n'
        src += 'trap notify_kill SIGUSR2\n'
    
        src += 'set -o pipefail\nfunc () {\n  %s\n  return $?\n}\n' % body

        # if monitor:
        #     src += '"%s" "%s" start "%s.$JOB_ID" $HOSTNAME\n' % (clustrun.CLUSTRUN_MON_BIN, monitor, cluster)
        #     src += 'func 2>"$TMPDIR/$JOB_ID.clustrun.stderr" >"$TMPDIR/$JOB_ID.clustrun.stdout"\n'
        #     src += 'RETVAL=$?\n'
        #     src += 'if [ "$FAILED" == "" ]; then\n'
        #     src += '  "%s" "%s" stop "%s.$JOB_ID" $RETVAL "$TMPDIR/$JOB_ID.clustrun.stdout" "$TMPDIR/$JOB_ID.clustrun.stderr"\n' % (clustrun.CLUSTRUN_MON_BIN, monitor, cluster)
            
        #     if 'stdout' in jobopts:
        #         src += '  mv "$TMPDIR/$JOB_ID.clustrun.stdout" "%s"\n' % jobopts['stdout']
        #     else:
        #         src += '  rm "$TMPDIR/$JOB_ID.clustrun.stdout"\n'

        #     if 'stderr' in jobopts:
        #         src += '  mv "$TMPDIR/$JOB_ID.clustrun.stderr" "%s"\n' % jobopts['stderr']
        #     else:
        #         src += '  rm "$TMPDIR/$JOB_ID.clustrun.stderr"\n'
        # else:
        src += 'func\n'
        src += 'RETVAL=$?\n'
        src += 'if [ "$FAILED" == "" ]; then\n'

        src += '  if [ $RETVAL -ne 0 ]; then\n'
        src += '    kill_deps\n'
        for out in job.outputs:
            if not 'keepfailed' in jobopts or not jobopts['keepfailed']:
                if out[0] != '.':
                    src += '    if [ -e "%s" ]; then rm "%s"; fi\n' % (out, out)
        # if monitor:
        #     src += '    "%s" "%s" failed "%s.$JOB_ID"\n' % (clustrun.CLUSTRUN_MON_BIN, monitor, cluster)
        src += '  fi\n'

        src += '  exit $RETVAL\n'
        src += 'else\n'
        src += '  # wait for SGE to kill the job for accounting purposes (max 120 sec)\n'
        src += '  I=0\n'
        src += '  while [ $I -lt 120 ]; do\n'
        src += '    sleep 1\n'
        src += '    let "I=$I+1"\n'
        src += '  done\n'
        src += 'fi\n'

        if not self.dryrun:
            proc = subprocess.Popen(["qsub", ], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            output = proc.communicate(src)[0]
            retval = proc.wait()

            if retval != 0:
                self.log('Error submitting job %s: %s\n' % (job.name, output), True)
                raise RuntimeError(output)

            jobid = output.strip()
        else:
            jobid = 'testjob.%s' % self.testjobcount
            self.testjobcount += 1

        job.jobid = jobid

        # for out in job.outputs:
        #     self._output_jobs[out] = jobid

        print jobid
        self.jobids.append(jobid)

        self.log('job: %s' % jobid)
        for line in src.split('\n'):
            self.log('job: %s' % line.strip('\n'), self.verbose)

            # if jobid and monitor and self.postaccounting:
            #     acct_src = accounting_script % (jobid, jobid, jobid, clustrun.CLUSTRUN_MON_BIN, cluster, clustrun.CLUSTRUN_MON_BIN, cluster)
            #     proc = subprocess.Popen(["qsub", ], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            #     output = proc.communicate(acct_src)[0]
            #     retval = proc.wait()
            #     if retval != 0:
            #         sys.stderr.write('Error submitting accounting job for %s: %s\n' % (jobid, output))
            #         raise RuntimeError(output)
