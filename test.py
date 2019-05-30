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
    #f.get_full_instrument_lists()
    #f.get_delta_instrument_lists()
    lst = f.get_list("2019-05-16T00:00:00Z","2019-05-30T23:59:59Z")
    json.dump(lst,fp=sys.stdout,indent=True)
    #get_specific_product(product=['C', 'F'])
    #print(json.dumps(lst,indent=True))


if __name__ == '__main__':
    main()
