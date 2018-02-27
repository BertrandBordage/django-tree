ALPHANUM = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
ALPHANUM_LEN = len(ALPHANUM)
PAD_CHAR = ALPHANUM[0]
DEFAULT_MAX_SIBLINGS = ALPHANUM_LEN ** 4


def to_alphanum(i, size):
    label = ''
    while i > 0:
        i, remainder = divmod(i, ALPHANUM_LEN)
        label = ALPHANUM[remainder] + label
    return label.zfill(size)


def from_alphanum(label):
    i = 0
    for c in label:
        i = i*ALPHANUM_LEN + ALPHANUM.index(c)
    return i
