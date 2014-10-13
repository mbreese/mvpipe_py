import os
import support
import runner
import runner.bash
import runner.sge
import socket

CONFIG_FILE=os.path.expanduser("~/.mvpiperc")

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

    if defaults:
        for k in defaults:
            _config[k] = defaults[k]

    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE) as f:
            for line in f:
                if '=' in line:
                    spl = [x.strip() for x in line.strip().split('=',1)]
                    _config[spl[0]] = support.autotype(spl[1])
    return _config


def _config_prefix(prefix):
    out = {}
    for k in _config:
        if k[:len(prefix)] == prefix:
            out[k[len(prefix):]] = _config[k]
    return out


def get_runner(dryrun=False, verbose=False, logger=None):
    if _config['mvpipe.runner'] == 'sge':
        return runner.sge.SGERunner(dryrun=dryrun, verbose=verbose, logger=logger, **_config_prefix('mvpipe.runner.sge.'))

    if _config['mvpipe.runner'] == 'bash':
        return runner.bash.BashRunner(dryrun=dryrun, verbose=verbose, logger=logger, **_config_prefix('mvpipe.runner.bash.'))

    return runner.get_runner()

