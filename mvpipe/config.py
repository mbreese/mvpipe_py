import os
import support
import runner
import runner.bash
import runner.sge
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
        _config = _defconfig
    return _config


def load_config(defaults=None):
    global _config

    if not _config:
        _config = _defconfig

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
    for k in _config:
        if k[:len(prefix)] == prefix:
            out[k[len(prefix):]] = _config[k]
    return out


def get_runner(dryrun=False, verbose=False, logger=None):
    if _config['mvpipe.runner'] == 'sge':
        return runner.sge.SGERunner(dryrun=dryrun, verbose=verbose, logger=logger, **config_prefix('mvpipe.runner.sge.'))

    if _config['mvpipe.runner'] == 'bash':
        return runner.bash.BashRunner(dryrun=dryrun, verbose=verbose, logger=logger, **config_prefix('mvpipe.runner.bash.'))

    if _config['mvpipe.runner'] == 'sjq':
        return runner.sjqrunner.SJQRunner(dryrun=dryrun, verbose=verbose, logger=logger, **config_prefix('mvpipe.runner.sjq.'))

    return runner.get_runner()

