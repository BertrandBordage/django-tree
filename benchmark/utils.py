from math import log10


class SkipTest(Exception):
    pass


SI_PREFIXES = (
    (1e9, 'G'),
    (1e6, 'M'),
    (1e3, 'k'),
    (1, ''),
    (1e-3, 'm'),
    (1e-6, 'µ'),
    (1e-9, 'n'),
)


def get_precision(n):
    return -int(log10(n))


def prefix_unit(v, unit, min_limit=None):
    if v is None:
        return

    prefixes = SI_PREFIXES
    if min_limit is not None:
        prefixes = prefixes[:min_limit]

    precision = get_precision(min([n for n, s in prefixes]))

    for exp, exp_str in prefixes:
        if v >= exp:
            break

    n = v / exp
    pat = ('%%.%df' % (precision - get_precision(exp)))
    res = '%s' % (pat % n)

    # We remove trailing zero and the dot if it's possible.
    if '.' in res:
        res = res.rstrip('0')
        if res[-1] == '.':
            res = res[:-1]

    if res == '0':
        return res

    return '%s %s%s' % (res, exp_str, unit)
