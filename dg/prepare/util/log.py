import logging


def configure_logging(args):
    levels = {
        0: logging.WARN,
        1: logging.INFO,
    }
    logging.basicConfig(
        level=levels.get(args.verbose, logging.DEBUG),
        format='%(levelname)-8s ' '%(message)s',
    )
