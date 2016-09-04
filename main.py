import sys

from common import config
from methods import amt, simple, single, stdm


def main(raw_args):
    return config.execute_with(
        raw_args, [amt.AMTMethod, simple.SimpleMethod, single.SingleMethod,
                   stdm.StdMMethod])

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
