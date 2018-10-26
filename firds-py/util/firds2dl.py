import argparse
import json
import os
import requests
import sys
from datetime import datetime

ISOfmt = "%Y-%m-%dT%H:%M:%SZ"


def main():
    parser = argparse.ArgumentParser(
        description='This tool queries ESMA FIRDS system to obtain available financial instrument data. If no argument is provided, the tool downloads to the current working directory any file made available on the platform no earlier than the last run date (provided that a lastrun file containing such timestamp is present alongside this tool).')
    parser.add_argument('--cutoff', type=str,
                        help='Earliest data publication date to be searched, in ISO8601 format and Zulu time - i.e. YYYY-MM-DDTHH:MM:SSZ')
    parser.add_argument('--dest', type=str, default='./', help='Destination folder for downloaded data.')
    parser.add_argument('--prods', type=str, nargs='*',
                        help='The CFI initial letter for each product category to be downloaded (C D E F H I J K L M O R S T)',
                        default='C D E F H I J K L M O R S T')
    args = parser.parse_args()

    # Bypass the last run date if another ISO8601 timestamp is provided as argument
    if args.cutoff is not None:
        lastRun = datetime.strptime(args.cutoff, ISOfmt)
    else:
        fname = 'lastrun'
        lastRun = readDate(fname)

    list = getList(lastRun, args.prods, 0, 500)[0]
    downloadLinks(list, args.dest)


def readDate(fname):
    if os.path.isfile(fname):
        with open(fname, 'r') as f:
            line = f.read()
            assert (len(line) > 0)
            f.close()
            return datetime.strptime(line, ISOfmt)
    else:
        print("No file to read last run date from. Please retry and specify a last run date in ISO 8601 format.")
        sys.exit(1)


def writeDate(fname, date):
    if os.path.isfile(fname):
        with open(fname, 'r+') as f:
            f.seek(0)
            f.write(date)
            f.truncate()
            f.close()
    else:
        print("No such file.")
        sys.exit(1)


def getList(lastRun, prods, startRow, maxRows):
    if len(prods) <= 0:
        print("No products requested. Try again with more products.")
        sys.exit(1)

    endpoint = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/"
    nowISO = datetime.utcnow().replace(microsecond=0)
    endDate = nowISO.strftime(ISOfmt)

    startDate = lastRun.strftime(ISOfmt)
    query = "select?q=*&fq=publication_date:%5B{}+TO+{}%5D&wt=json&indent=true&start={}&rows={}".format(startDate,
                                                                                                        endDate,
                                                                                                        startRow,
                                                                                                        maxRows)

    destPage = endpoint + query
    response = json.loads(requests.get(destPage).content)
    response = response['response']

    leftRows = response['numFound'] - maxRows

    if leftRows <= 0:
        body = response['docs']
    else:
        print("Pagination needed.")
        body = response['docs'].update(getList(lastRun, maxRows + 1, leftRows))

    if len(body) == 0:
        print("No file available for required parameters.")
        sys.exit(1)

    FUL = [x for x in body if x['file_type'] == 'FULINS']


    ls = []

    newestFUL = None

    if len(FUL) > 0:
        for prod in prods:
            p_prods = [x for x in FUL if hasProduct(x, prod)]
            try:
                p_newestFUL = get_newest(p_prods)
            except(ValueError):
                p_newestFUL = lastRun
            newestFUL = p_newestFUL if not newestFUL or p_newestFUL > newestFUL else newestFUL
            ls.append([f for f in p_prods if datetime.strptime(f['publication_date'], ISOfmt) == p_newestFUL])

    DLT = [x for x in body if x['file_type'] == 'DLTINS' and isNewerThan(x, newestFUL)]
    ls.append(DLT)
    newestDLT = get_newest(DLT)
    return ls, newestFUL, newestDLT


def get_newest(list):
    return max([datetime.strptime(x['publication_date'], ISOfmt) for x in list])


def downloadLinks(list, destPath):
    for sublist in list:
        for file in sublist:
            link = file['download_link']
            downloadZip(link, destPath + getFilename(link))

def hasProduct(r, p):
    return r['file_name'].find('_{}_'.format(p)) != -1


def isNewerThan(r, dt):
    return datetime.strptime(r['publication_date'], ISOfmt) > dt


def getFilename(link):
    return link[link.rfind("/") + 1:]


def downloadZip(link, dest):
    response = requests.get(link, stream=True)
    # Throw an error for bad status codes
    response.raise_for_status()

    # Write chunks to file
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    with open(dest, 'wb') as handle:
        for block in response.iter_content(1024):
            handle.write(block)


if __name__ == '__main__':
    main()
