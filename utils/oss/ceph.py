import os


def get_ceph_io_status():
    '''
    :return:
    '''
    result = os.popen('ceph -s')
    res = result.read()
    io_str = get_io_string(res)
    data = parse_io_str(io_str)
    return data


def parse_io_str(io_str):
    '''
    :param io_str:  '0 B/s rd, 7.7 KiB/s wr, 1 op/s rd, 1 op/s wr'
    :return: {
            'bw_rd': 0.0,   # Kb/s ,float
            'bw_wr': 0.0,   # Kb/s ,float
            'op_rd': 0,     # op/s, int
            'op_wr': 0      # op/s, int
        }
    '''
    data = {
        'bw_rd': 0.0,
        'bw_wr': 0.0,
        'op_rd': 0,
        'op_wr': 0
    }
    items = io_str.split(',')
    for item in items:
        item = item.strip()
        s = item.split(' ')
        try:
            sval = s[0]
            unit = s[1].lower()
            mode = s[2]
            value = float(sval)
        except:
            continue

        # IOPS
        if 'op/s' in unit:
            value = int(value)
            if mode == 'rd':
                data['op_rd'] = value
            elif mode == 'wr':
                data['op_wr'] = value
        #iobw
        else:
            u = unit[0]
            if u == 'b':
                value = value / 1024
            elif u == 'k':
                value = value
            elif u == 'm':
                value = value * 1024
            elif u == 'g':
                value = value * 1024 * 1024

            if mode == 'rd':
                data['bw_rd'] = value
            elif mode == 'wr':
                data['bw_wr'] = value

    return data


def get_io_string(res):
    lines = res.splitlines()
    lines.reverse()
    for l in lines:
        l = l.strip()
        if l.startswith('client:'):
            return l.lstrip('client:').strip()

    return ''

