import argparse
import os
import re


class RegFile(object):

    def __init__(self, version, keys):
        self.version = version
        self.keys = keys

    @staticmethod
    def parse(input_):
        def is_version(line):
            return line in ['Windows Registry Editor Version 5.00', 'REGEDIT4']

        def is_key(line):
            return len(line) > 2 and line[0] == '[' and line[-1] == ']'

        def get_key(line):
            assert is_key(line), line
            return line[1:-1]

        version = None
        keys = []

        while True:
            line = input_.readline()
            if not line:
                break
            line = line.strip()

            if is_version(line):
                assert not version or version == line
                version = line
                blank = input_.readline()
                assert blank and not len(blank.strip())
            else:
                key = get_key(line)
                content = []
                while True:
                    line = input_.readline()
                    assert line
                    if not len(line.strip()):
                        break
                    content.append(line.rstrip())
                keys.append((key, content))

        assert version
        return RegFile(version, keys)

    def dump(self, output):
        print(self.version, file=output)
        print(file=output)
        for key, content in self.keys:
            print(f'[{key}]', file=output)
            for line in content:
                print(line, file=output)
            print(file=output)


def filter_keys(reg_file, res):
    matchers = [re.compile(spec) for spec in res]
    keys = []
    for key, content in reg_file.keys:
        if any(matcher.match(key) for matcher in matchers):
            continue
        keys.append((key, content))
    return RegFile(reg_file.version, keys)


def main():
    parser = argparse.ArgumentParser('Filter keys in reg file')
    parser.add_argument(
        '-f', metavar='REGEX', action='append',
        help='Regular expression for key path')
    parser.add_argument(
        '-q', action='store_true',
        help='Exit quietly if source does not exist')
    parser.add_argument('SRC', help='Source file')
    parser.add_argument('DEST', help='Destination file')
    args = parser.parse_args()

    if args.q and not os.path.exists(args.SRC):
        return

    with open(args.SRC) as input_:
        reg_file = RegFile.parse(input_)

    with open(args.DEST, 'w') as output:
        filter_keys(reg_file, args.f).dump(output)


if __name__ == '__main__':
    main()
