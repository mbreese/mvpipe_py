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