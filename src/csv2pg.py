import os
import sys


def main():
    import argparse

    parser = argparse.ArgumentParser(description='This tool loads a CSV file to any pgSQL DBMS table.')
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument('--csv', type=str, help='The input CSV file to be loaded on DB.', required=True)
    requiredNamed.add_argument('--hst', type=str, help='The hostname for the pgSQL DBMS.', required=True)
    requiredNamed.add_argument('--dbn', type=str, help='The database name for which the data ingestion is desired.',
                               required=True)
    requiredNamed.add_argument('--uid', type=str, help='The username of the user peforming the ingestion.',
                               required=True)
    requiredNamed.add_argument('--pwd', type=str, help='The password of the user performing the ingestion.',
                               required=True)
    requiredNamed.add_argument('--tbl', type=str, help='The destination table name.', required=True)
    parser.add_argument('--sep', type=str, help='The CSV field separator. Semicolon by default.', default=';')
    args = parser.parse_args()

    csvfile = os.path.expanduser(args.csv)
    host = args.hst
    dbname = args.dbn
    uid = args.uid
    pwd = args.pwd
    dsn = "host = '{}' dbname = '{}' user = '{}' password = '{}'".format(host, dbname, uid, pwd)
    tbl = args.tbl
    separator = args.sep

    conn = connectDb(dsn)

    f = open(csvfile, 'r')

    if not existsTable(conn, tbl):
        createTable(readCSV(f), conn, tbl)

    print("Tabella creata, ora inizio ingestion.")
    f.seek(0)
    loadCSV(conn, f, tbl, separator, trunc=False)
    print("Ingestion completata.")

    conn.close()
    f.close()


def existsTable(conn, tbl):
    cur = conn.cursor()
    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (tbl,))
    return cur.fetchone()[0]


def readCSV(f):
    import csv

    reader = csv.reader(f, delimiter=';')

    longest, headers, type_list = [], [], []
    bools = ['t', 'true', 'y', 'yes', 'on', '1', 'f', 'false', 'n', 'no', 'off', '0']
    minlen = 10

    for row in reader:
        if len(headers) == 0:
            headers = row
            for _ in row:
                longest.append(minlen)
                type_list.append('')
        else:
            for i in range(len(row)):
                if type_list[i] == 'text' or row[i] == 'NA':
                    pass
                else:
                    type_list[i] = dataType(row[i], type_list[i],bools)
                if len(row[i]) > longest[i]:
                    longest[i] = len(row[i])

    return longest, headers, type_list


def dataType(val, current_type, bools):
    import ast
    try:
        t = ast.literal_eval(val)
    except ValueError:
        return 'text'
    except SyntaxError:
        return 'text'
    if type(t) in [int, float]:
        if type(t) is float:
            return 'real'
        if (type(t) is int) and (current_type is not 'real'):
            if (-32768 < t < 32767) and (current_type not in ['integer', 'bigint', 'real']):
                return 'smallint'
            elif (-2147483648 < t < 2147483647) and (current_type not in ['bigint']):
                return 'integer'
            else:
                return 'bigint'
        elif current_type is 'boolean':
            return 'text'
        else:
            return current_type
    else:
        if t in bools:
            return 'boolean'
        else:
            return 'text'


def createTable(longest, headers, type_list, conn, tbl):
    statement = 'create table ' + tbl + ' ('

    for i in range(len(headers)):
        if type_list[i] == 'varchar':
            statement = (statement + '\n{} varchar({}),').format(headers[i].lower(), str(longest[i]))
        else:
            statement = (statement + '\n' + '{} {}' + ',').format(headers[i].lower(), type_list[i])

    statement = statement[:-1] + ');'

    if conn is None:
        print("No connection available. Please retry.")
        sys.exit(1)

    cur = conn.cursor()

    cur.execute(statement)
    conn.commit()
    cur.close()


def connectDb(dsn):
    import psycopg2
    try:
        conn = psycopg2.connect(dsn)
    except:
        print("Connection failed.")
        return None

    return conn


def loadCSV(conn, file_object, table_name, separator, trunc):
    import psycopg2

    cursor = conn.cursor()

    if trunc:
        sql  = """
        TRUNCATE """+table_name+""";
        """
        cursor.execute(sql)
        print("Truncating table {}".format(table_name))
        conn.commit()

    sql = """
        COPY %s FROM STDIN WITH CSV HEADER
        DELIMITER AS '""" + separator + """'
        """

    cursor.copy_expert(sql=sql % table_name, file=file_object)
    conn.commit()
    cursor.close()


if __name__ == '__main__':
    main()
