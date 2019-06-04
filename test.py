#!/usr/bin/env python3
from firds import FirdsDB
from firds import FirdsQuery
import logging
import json
import sys

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


def main():
    query = FirdsQuery().get(criteria=dict(
        gnr_full_name="PSV",
        mic="NDEX",
        gnr_cfi_code="*")
    )
    for q in query:
    	print(q)
    # json.dump(query, fp=sys.stdout, indent=4)
    # json.dump(f.links, fp=sys.stdout, indent=True)


if __name__ == "__main__":
    main()
