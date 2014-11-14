import os
import support
import runner
import runner.bash
import runner.sge
import runner.sjqrunner
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


def get_runner(dryrun=False, verbose=False, logger=None):
    cfg = get_config()
    if cfg['mvpipe.runner'] == 'sge':
        return runner.sge.SGERunner(dryrun=dryrun, verbose=verbose, logger=logger, **config_prefix('mvpipe.runner.sge.'))

    if cfg['mvpipe.runner'] == 'bash':
        return runner.bash.BashRunner(dryrun=dryrun, verbose=verbose, logger=logger, **config_prefix('mvpipe.runner.bash.'))

    if cfg['mvpipe.runner'] == 'sjq':
        try:
            import sjq
            assert sjq
            return runner.sjqrunner.SJQRunner(dryrun=dryrun, verbose=verbose, logger=logger, **config_prefix('mvpipe.runner.sjq.'))
        except:
            raise ConfigError("Cannot load SJQ job runner")

    if cfg['mvpipe.runner'] == 'slurm':
        return runner.slurm.SlurmRunner(dryrun=dryrun, verbose=verbose, logger=logger, **config_prefix('mvpipe.runner.slurm.'))

    raise ConfigError("Cannot load job runner: %s" % cfg['mvpipe.runner'])
