import os
import support
import runner
import runner.bash
import runner.sge
try:
    import runner.sjqrunner
    NOSJQ=False
except:
    NOSJQ=True
    pass
import runner.slurm
import socket

CONFIG_FILE=os.path.expanduser("~/.mvpiperc")
GLOBAL_CONFIG_FILE=os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", ".mvpiperc")

_config = None

_defconfig = {
    'mvpipe.runner': 'bash',
    'mvpipe.host': socket.gethostname()
}

def get_config():
    global _config
    if not _config:
        load_config()
    return _config


class ConfigError(Exception):
    def __init__(self, s):
        Exception.__init__(self, s)


def load_config(defaults=None):
    global _config

    if not _config:
        _config = dict(_defconfig)

    if os.path.exists(GLOBAL_CONFIG_FILE):
        with open(GLOBAL_CONFIG_FILE) as f:
            for line in f:
                if '=' in line:
                    spl = [x.strip() for x in line.strip().split('=',1)]
                    _config[spl[0]] = support.autotype(spl[1])

    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE) as f:
            for line in f:
                if '=' in line:
                    spl = [x.strip() for x in line.strip().split('=',1)]
                    _config[spl[0]] = support.autotype(spl[1])

    if defaults:
        for k in defaults:
            _config[k] = defaults[k]

    return _config


def config_prefix(prefix):
    out = {}
    cfg = get_config()
    for k in cfg:
        if k[:len(prefix)] == prefix:
            out[k[len(prefix):]] = cfg[k]
    return out


def get_shell():
    cfg = get_config()
    shell = None
    if 'mvpipe.shell' in cfg:
        shell = cfg['mvpipe.shell']
    
    if not shell or not os.path.exists(shell):
        for sh in ['/bin/bash', '/usr/bin/bash', '/usr/local/bin/bash', '/bin/sh']:
            if os.path.exists(sh):
                return sh

    return shell


def get_runner(dryrun=False, verbose=False, logger=None, global_hold=None):
    cfg = get_config()

    if cfg['mvpipe.runner'] == 'sge':
        runnercfg = config_prefix('mvpipe.runner.sge.')
        if global_hold is not None:
            runnercfg['global_hold'] = global_hold

        return runner.sge.SGERunner(dryrun=dryrun, verbose=verbose, logger=logger, **runnercfg)

    if cfg['mvpipe.runner'] == 'bash':
        return runner.bash.BashRunner(dryrun=dryrun, verbose=verbose, logger=logger, **config_prefix('mvpipe.runner.bash.'))

    if cfg['mvpipe.runner'] == 'sjq':
        if NOSJQ:
            raise ConfigError("Cannot load SJQ job runner")

        runnercfg = config_prefix('mvpipe.runner.sjq.')
        if global_hold is not None:
            runnercfg['global_hold'] = global_hold

        return runner.sjqrunner.SJQRunner(dryrun=dryrun, verbose=verbose, logger=logger, **runnercfg)

    if cfg['mvpipe.runner'] == 'slurm':
        runnercfg = config_prefix('mvpipe.runner.slurm.')
        if global_hold is not None:
            runnercfg['global_hold'] = global_hold

        return runner.slurm.SlurmRunner(dryrun=dryrun, verbose=verbose, logger=logger, **runnercfg)

    raise ConfigError("Cannot load job runner: %s" % cfg['mvpipe.runner'])
