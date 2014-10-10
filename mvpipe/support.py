import os

def autotype(val):
    if not val:
        return ''
    try:
        ret = int(val)
        return ret
    except:
        try:
            ret = float(val)
            return ret
        except:
            if val.upper() in ['T', 'TRUE', 'Y', 'YES']:
                return True
            if val.upper() in ['F', 'FALSE', 'N', 'NO']:
                return False

            if val[0] == '"' and val[-1] == '"':
                val = val[1:-1]
            return val


def target_exists(fname):
    if os.path.exists(fname):
        return True
    return False


def calc_time(self, val, multiplier=1):
    seconds = 0
    if ':' in val:
        cols = [int(x) for x in val.split(':')]
        if len(cols) == 3:
            h = cols[0]
            m = cols[1]
            s = cols[2]
        elif len(cols) == 2:
            h = 0
            m = cols[0]
            s = cols[1]

        seconds = s + (m * 60) + (h * 60 * 60)
    else:
        seconds = int(val)

    seconds = seconds * multiplier

    h = seconds / (60 * 60)
    seconds = seconds % (60 * 60)

    m = seconds / 60
    s = seconds % 60

    return '%d:%02d:%02d' % (h, m, s)
