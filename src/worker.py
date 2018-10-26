def main():
    import argparse
    import os

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--cutoff', type=str, help='')
    parser.add_argument('--prd', type=str, nargs='*', default='S J F O',
                        help='The CFI initial letter for each product category to be downloaded (C D E F H I J K L M O R S T). Only options, swaps, futures and forwards are downloaded by default.')

    parser.add_argument('--cleanup', type=str, nargs='*', help='File extensions of downloaded files to be cleaned up after data ingestion.', default='')
    parser.add_argument('--sep', type=str, help='The CSV field separator. Semicolon by default.', default=';')

    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument('--wdir', type=str, help='Working directory path (used for downloads and conversions).',
                               required=True)
    requiredNamed.add_argument('--hst', type=str, help='The hostname for the pgSQL DBMS.', required=True)
    requiredNamed.add_argument('--dbn', type=str, help='The database name for which the data ingestion is desired.',
                               required=True)
    requiredNamed.add_argument('--uid', type=str, help='The username of the user peforming the ingestion.',
                               required=True)
    requiredNamed.add_argument('--pwd', type=str, help='The password of the user performing the ingestion.',
                               required=True)

    args = parser.parse_args()

    wdir = os.path.expanduser(args.wdir + '/')
    pth = os.path.dirname(os.path.realpath(__file__))
    cutoff = args.cutoff
    prd = args.prd.split(" ")
    hst = args.hst
    dbn = args.dbn
    uid = args.uid
    pwd = args.pwd
    sep = args.sep

    print("Fetching the available files from FIRDS web service...")
    n = list(map(lambda x: x.strftime("%Y%m%d"), download_files(cutoff, prd, wdir)))
    print("Most recent files downloaded. Now proceeding...")

    for file in os.listdir(wdir):
        dlt = isDLT(file)
        ful = isFUL(file)
        if file.endswith('.zip') and (dlt or ful):
            print(r"--> Unzipping and flattening file {}...".format(str(file)))
            z = os.path.join(wdir, file)
            unzip_files(z, os.path.dirname(z))

            no_ext = z[:-3]
            xml = "{}{}".format(no_ext, 'xml')
            xsl = pth + '/FUL.xsl' if ful else pth + '/DLT.xsl'
            csv = "{}{}".format(no_ext, 'csv')

            to_csv(xml, xsl, csv)
            print(r'--> CSV correctly generated for file {}'.format(file))

    m_ful = wdir + r'/merge_FULINS_' + n[0] + '.csv'
    m_dlt = wdir + r'/merge_DLTINS_' + n[1] + '.csv'

    dest = {'fulins': {'from': wdir + r'/FULINS*.csv', 'to': m_ful, 'hr': [0, 5]},
            'dltins': {'from': wdir + r'/DLTINS*.csv', 'to': m_dlt, 'hr': [1, 6]}}
    success = {}

    print("Merging available CSV files.")
    for key, value in dest.items():
        success[key] = merge_mult_csv(value['from'], value['to'])
        if success[key]:
            print(r"--> Merging {} files succeeded.".format(key))
            print("Now proceeding to hash table rows...")
            # Faccio l'hash delle righe utilizzando la 2-upla <ID,Venue>
            insert_hashes(value['to'], prd, sep, value['hr'])
            print(r"--> Hashing completed.")
            # Se la tabella esiste, tronco e inserisco dati, altrimenti la creo
            print("Ingesting {} data into pgSQL table.".format(key))
            ingest_db(hst, dbn, key, uid, pwd, value['to'], sep)

    if len(args.cleanup) > 0:
        print("Now removing leftover files.")
        cleanup(wdir, args.cleanup)


def download_files(cutoff, prods, dest_path):
    import firds2dl as f

    # Bypass the last run date if another ISO8601 timestamp is provided as argument
    if cutoff is not None:
        from datetime import datetime
        last_run = datetime.strptime(cutoff, f.ISOfmt)
    else:
        fname = 'lastrun'
        last_run = f.readDate(fname)  # Lancia errore se non esiste questo file

    result = f.getList(last_run, prods, 0, 500)
    f.downloadLinks(result[0], dest_path)
    return result[1], result[2]


def merge_mult_csv(pth, out):
    import shutil
    import glob
    allFiles = glob.glob(pth)
    if len(allFiles) > 0:
        with open(out, 'wb') as outfile:
            for i, fname in enumerate(allFiles):
                print("####### About to merge file {}".format(fname))
                with open(fname, 'rb') as infile:
                    if i != 0:
                        infile.readline()  # Throw away header on all but first file
                    # Block copy rest of file from input to output without parsing
                    shutil.copyfileobj(infile, outfile)
        return True
    else:
        print("No files to be merged.")
        return False


def unzip_files(zip_path, dest_path):
    import zipfile
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(dest_path)


def to_csv(xml, xsl, csv):
    import xml2csv as x
    x.transform(xml, xsl, csv)


def insert_hashes(file, prd, sep, rng):
    from tempfile import NamedTemporaryFile
    import shutil
    import csv
    import hashlib

    tempfile = NamedTemporaryFile(mode='w', delete=False)

    with open(file, 'r') as csvfile, tempfile:
        reader = csv.reader(csvfile, delimiter=sep)
        writer = csv.writer(tempfile, delimiter=sep)
        header = next(reader)
        nc = header.index('CFI_CODE')
        writer.writerow(header + ['hash'])

        for row in reader:
            if row[nc][0] not in prd:
                continue
            subset = ''
            for i in rng:
                subset += "{}".format(row[i])
            h = hashlib.md5(subset.encode('utf-8')).hexdigest()
            row.append(h)
            writer.writerow(row)

    shutil.move(tempfile.name, file)


def ingest_db(host, dbname, tbl, uid, pwd, csvfile, separator):
    import csv2pg as c
    dsn = "host = '{}' dbname = '{}' user = '{}' password = '{}'".format(host, dbname, uid, pwd)

    conn = c.connectDb(dsn)
    f = open(csvfile, 'r')

    has_table = c.existsTable(conn, tbl)

    if not has_table:
        h = c.readCSV(f)
        c.createTable(*h, conn, tbl)

    print("Tabella creata, ora inizio ingestion.")
    f.seek(0)
    c.loadCSV(conn, f, tbl, separator, has_table)
    print("Ingestion completata.")

    conn.close()
    f.close()


def cleanup(dir, ext):
    import os
    # Remove files with specified extension
    for file in os.listdir(dir):
        file = os.path.join(dir, file)
        for x in ext:
            if file.endswith('.' + str(x)):
                os.remove(file)


def isFUL(file):
    return string_contains(file, c='FULINS')


def isDLT(file):
    return string_contains(file, c='DLTINS')


def string_contains(s, c):
    return c in s


if __name__ == '__main__':
    main()
