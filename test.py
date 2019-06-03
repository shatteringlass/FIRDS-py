#!/usr/bin/env python3
from firds import FirdsDB
from firds import FirdsQuery
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

criteria = [
    {
        "name": "gnr_full_name",
                "value": "PSV",
                "type": "text",
                "isParent":
                    "true"
    },
    {
        "name": "mic",
                "value": "NDEX",
                "type": "text",
                "isParent": 'true'
    },
    {
        "name": "gnr_cfi_code",
                "value": "*",
                "type": "text",
                "isParent": 'true'
    },
    {
        "name": "firdsPublicationDateCustomSearchInputField",
                "value": "(latest_received_flag:1)AND(never_published_flag:0)",
                "type": "customSearchInputFieldQuery",
                "isParent": 'true'
    }
]




def main():
    query = FirdsQuery().get(criteria=criteria)
    json.dump(query, fp=sys.stdout, indent=4)
    # json.dump(f.links, fp=sys.stdout, indent=True)


if __name__ == '__main__':
    main()
