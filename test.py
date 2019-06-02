#!/usr/bin/env python3
from firds import Firds
import logging
import json
import sys

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


def main():
    f = Firds()
    json.dump(f.links, fp=sys.stdout, indent=True)


if __name__ == '__main__':
    main()
