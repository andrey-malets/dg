import argparse
import sys

from common import state
from methods import amt

def main(raw_args):
    methods = {'amt': lambda args: amt.AMTMethod(args.p, args.c, args.a)}

    parser = argparse.ArgumentParser(description='Deploy some machines')
    parser.add_argument('-g', metavar='GROUP', help='Group to deploy',
                        required=True)
    parser.add_argument('-m', choices=methods.keys(), help='Deploy method',
                        required=True)
    parser.add_argument('-a', metavar='AMTREDIRD', help='amtredird url',
                        default='https://urgu.org/amtredird')
    parser.add_argument('-c', metavar='CONFIG', help='config API url',
                        default='https://urgu.org/config')
    parser.add_argument(
        '-p', metavar='AMTPASSWD',
        help='passwd file for AMT deployment', default='amtpasswd')

    args = parser.parse_args(raw_args)
    return 0 if methods[args.m](args).run(state.State(args.g)) else 1

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
