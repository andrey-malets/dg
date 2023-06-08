import argparse
import json
import logging


def prepare_config_of(type_):
    def parser(value):
        with open(value) as config_input:
            return type_(**json.load(config_input))
    return parser


def parse_prepare_config(value):
    with open(value) as config_input:
        try:
            return json.load(config_input)
        except Exception:
            logging.exception('Failed to parse config %s', value)
            raise


def get_args(raw_args):
    config_parser = argparse.ArgumentParser(raw_args[0], add_help=False)
    config_parser.add_argument(
        '--config', type=parse_prepare_config,
        help='Path to config file with all the options in JSON'
    )
    known_args, unknown_args = config_parser.parse_known_args(raw_args[1:])
    if known_args.config is not None:
        if unknown_args:
            config_parser.error(
                '--config is not compatible with other options'
            )
        return known_args.config
    else:
        return raw_args[1:]
