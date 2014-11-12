import os
import os
import string
import subprocess

import mvpipe.support
from mvpipe.runner import Runner, Job

def_options = {'env': True, 'wd': os.path.abspath(os.curdir), 'mail': 'ea', 'hold': False}

'''
SLURM options:

global_hold - start all pipelines with a "placeholder" job to synchronize
              starts (and ensure that all jobs are submitted correctly
              before releasing the pipeline

account     - a default account to use

shell       - a shell to use for the script (default(s): /bin/bash, /usr/bin/bash, /usr/local/bin/bash, /bin/sh)

'''
class SlurmRunner(Runner):
    def __init__(self, dryrun, verbose, logger, global_hold=False, global_depends=None, account=None, interpreter=None):
        Runner.__init__(self, dryrun, verbose, logger)
        self.global_hold = global_hold
        self._holding_job = None
        self.global_depends = global_depends if global_depends else []
        self.account = account
        self.parallelenv = parallelenv
        self.hvmem_total = hvmem_total

        self.jobids = []

        self.testjobcount = 1

        if interpreter:
            self.interpreter = interpreter
        else:
            for intp in ['/bin/bash', '/usr/bin/bash', '/usr/local/bin/bash']:
                if os.path.exists(intp):
                    self.interpreter = intp
                    break
            if not self.interpreter:
                self.interpreter = '/bin/sh'


    def reset(self):
        pass

    def abort(self):
        if self._holding_job:
            self.cancel(self._holding_job.jobid)
        else:
            for jobid in self.jobids:
                self.cancel(jobid)

    def done(self):
        if not self.dryrun:
            if self._holding_job:
                self.release(self._holding_job.jobid)

    def _setup_holding_job(self):
        self._holding_job = Job('sleep 5', name="holding", hold=True, stdout='/dev/null', stderr='/dev/null', walltime="00:00:30")
        self.submit(self._holding_job)
        self.global_depends.append(self._holding_job.jobid)

    def check_jobid(self, jobid):
        with open('/dev/null', 'w') as devnull:
            proc = subprocess.Popen(["sacct", "-b", "-p", "-j", jobid], stdout=subprocess.PIPE, stderr=devnull)
            stdout, stderr = proc.communicate()
            for line in stdout.split('\n'):
                cols = line.strip().split('|')
                if cols[0] == jobid:
                    if cols[-1].split(':')[0] == '0':
                        return True
            return False

    def release(self, jobid):
        subprocess.call(["scontrol", "release", jobid])

    def cancel(self, jobid):
        subprocess.call(["scancel", jobid])

    def submit(self, job):
        if not job.src:
            return

        if self.global_hold and not self._holding_job:
            self._setup_holding_job()

        body = ''

        if job.pre:
            body = job.pre
            body += '\n'

        body += job.src

        if job.post:
            body += '\n'
            body += job.post

        jobopts = dict(def_options)
        for k in job.args:
            jobopts[k] = job.args[k]

        src = '#!%s\n' % self.interpreter
        src += '#SBATCH -J %s\n' % (job.name if job.name[0] in string.ascii_letters else 'mvp_%s' % job.name)

        if 'hold' in jobopts and jobopts['hold']:
            src += '#SBATCH -H\n'

        if 'env' in jobopts and jobopts['env']:
            src += '#SBATCH --export=ALL\n'

        if 'walltime' in jobopts:
            src += '#SBATCH -t %s\n' % mvpipe.support.calc_time(jobopts['walltime'])

        if 'procs' in jobopts and int(jobopts['procs']) > 1:
            src += '#SBATCH -n %s\n' % (jobopts['procs'])

        if 'nodes' in jobopts and int(jobopts['nodes']) > 1:
            src += '#SBATCH -N %s\n' % (jobopts['nodes'])

        if 'mem' in jobopts:
            if jobopts['mem'][-1] == 'M':
                mem = jobopts['mem'][:-1]
            elif jobopts['mem'][-1] == 'G':
                mem = int(jobopts['mem'][:-1]) * 1000
            else:
                mem = jobopts['mem']

            src += '#SBATCH --mem=%s\n' % mem

        # if 'stack' in jobopts:
        #     src += '#$ -l h_stack=%s\n' % jobopts['stack']

        if job.depids or self.global_depends:
            depids = job.depids
            if self.global_depends:
                depids.extend(self.global_depends)

            if depids:
                src += '#SBATCH -d afterok:%s\n' % ':'.join(depids)

        if 'qos' in jobopts:
            src += '#SBATCH --qos %s\n' % jobopts['qos']

        # if 'queue' in jobopts:
        #     src += '#$ -q %s\n' % jobopts['queue']

        if 'mail' in jobopts:
            src += '#SBATCH --mail-type %s\n' % jobopts['mail']

        if 'wd' in jobopts:
            src += '#SBATCH -D %s\n' % jobopts['wd']

        if 'account' in jobopts:
            src += '#SBATCH -A %s\n' % jobopts['account']
        elif self.account:
            src += '#SBATCH -A %s\n' % self.account

        if 'stdout' in jobopts:
            src += '#SBATCH -o %s\n' % jobopts['stdout']

        if 'stderr' in jobopts:
            src += '#SBATCH -e %s\n' % jobopts['stderr']

        src += 'set -o pipefail\nfunc () {\n  %s\n  return $?\n}\n' % body

        src += 'func\n'
        src += 'RETVAL=$?\n'
        src += 'exit $RETVAL\n'

        if not self.dryrun:
            proc = subprocess.Popen(["sbatch", ], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            output = proc.communicate(src)[0]
            retval = proc.wait()

            if retval != 0:
                self.log('Error submitting job %s: %s\n' % (job.name, output), True)
                raise RuntimeError(output)

            jobid = output.strip().split(' ')[-1]
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
