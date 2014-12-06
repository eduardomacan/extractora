# encoding: utf-8

# This is a quick-and-dirty hack to recursively extract data from tables in an Oracle database
# It will build a query from command line (table, column and value) and from there it will
# get the foreign keys in that table and query for the referred values in other tables until done.
#
# This hack is brought to you by macan <eduardomacan@gmail.com>, out of the needs of my QA team
# in a project I'm leading :P


from __future__ import print_function
import sys
import types
import datetime
import os.path
import argparse
import ConfigParser as configparser
#import cx_Oracle





# TODO: get_dependencies and get_dependents should be refactored to become a singe function
def get_dependencies(cursor, table_name, owner = None):
    """query oracle metadata to obtain foreign keys for a given table"""

    if owner:
        query = '''select b.table_name,b.column_name,c.table_name,c.column_name, b.constraint_name from all_constraints a
            join all_cons_columns b on b.constraint_name=a.constraint_name
            join all_cons_columns c on c.constraint_name=a.r_constraint_name
            where a.CONSTRAINT_TYPE='R' and b.position = c.position and a.table_name=:TableName
            and a.owner=:Owner and b.owner=:Owner and c.owner=:Owner
            order by c.table_name'''
        cursor.execute(query, {'TableName': table_name, 'Owner': owner})
    else:
        query = '''select b.table_name,b.column_name,c.table_name,c.column_name, b.constraint_name from all_constraints a
            join all_cons_columns b on b.constraint_name=a.constraint_name
            join all_cons_columns c on c.constraint_name=a.r_constraint_name
            where a.CONSTRAINT_TYPE='R' and b.position = c.position and a.table_name=:TableName
            order by c.table_name'''
        cursor.execute(query, {'TableName': table_name})


    all_rows = cursor.fetchall()

    if all_rows:
        current_fk = all_rows[0][4]
        current_table = all_rows[0][0]
        cur_col = all_rows[0][2]
        grouped_from = []
        grouped_to = []
        grouped = []

        # group keys by referenced table  so that we know how to deal with composite foreign keys later
        for item in all_rows:
            if item[4] != current_fk:
                grouped.append((current_table, grouped_from, cur_col, grouped_to))
                current_fk = item[4]
                current_table = item[0]
                cur_col = item[2]
                grouped_from = []
                grouped_to = []
            grouped_from.append(item[1])
            grouped_to.append(item[3])

        return grouped
    else:
        return None


def get_dependants(cursor, table_name, owner=None):
    """query oracle metadata to obtain foreign keys for a given table"""

    if owner:
        query = '''select b.table_name,b.column_name,c.table_name,c.column_name, b.constraint_name from all_constraints a
            join all_cons_columns b on b.constraint_name=a.constraint_name
            join all_cons_columns c on c.constraint_name=a.r_constraint_name
            where a.CONSTRAINT_TYPE='R' and b.position = c.position and c.table_name=:TableName
            and a.owner=:Owner and b.owner=:Owner and c.owner=:Owner
            order by c.table_name'''
        cursor.execute(query, {'TableName': table_name, 'Owner': owner})
    else:
        query = '''select b.table_name,b.column_name,c.table_name,c.column_name, b.constraint_name from all_constraints a
            join all_cons_columns b on b.constraint_name=a.constraint_name
            join all_cons_columns c on c.constraint_name=a.r_constraint_name
            where a.CONSTRAINT_TYPE='R' and b.position = c.position and c.table_name=:TableName
            order by c.table_name'''
        cursor.execute(query, {'TableName': table_name})

    all_rows = cursor.fetchall()

    if all_rows:
        current_fk = all_rows[0][4]
        current_table = all_rows[0][0]
        cur_col = all_rows[0][1]
        grouped_from = []
        grouped_to = []
        grouped = []

        # group keys by referenced table  so that we know how to deal with composite foreign keys later
        for item in all_rows:
            if item[4] != current_fk:
                grouped.append((current_table, grouped_from, cur_col, grouped_to))
                current_fk = item[4]
                current_table = item[0]
                cur_col = item[1]
                grouped_from = []
                grouped_to = []
            grouped_from.append(item[1])
            grouped_to.append(item[3])
        return grouped
    else:
        return None


def get_columns(cursor, table_name, owner):
    """query oracle metadata for the columns of a given table
    :rtype : list of column names
    :param cursor: an open oracle cursor
    :param table_name: the name of the table
    :param owner: the schema owner
    """
    query = "select column_name from all_tab_columns where TABLE_NAME=:TableName and owner = :Owner order by column_id"
    cursor.execute(query, {'TableName': table_name, 'Owner': owner})
    tudo = cursor.fetchall()
    return [name[0] for name in tudo]


def sql_str(val):
    """convert python data types into more suitable oracle strings"""
    if type(val) == types.NoneType:
        return "null"
    elif type(val) == datetime.datetime:
        return "TO_DATE('" + str(val) + "', 'yyyy/mm/dd hh24:mi:ss')"
    elif type(val) == int or type(val) == float:
        return str(val)
    else:
        return "'" + str(val) + "'"


def get_rows(cursor, table_name, column_names, column_values):
    """fetches a row of data from a table, given arrays of column names and their values
    :param cursor: open oracle cursor
    :param table_name: the name of the table to extract a row from
    :param column_names: fields to query for
    :param column_values: values to query for (in the same order as fields)
    """
    query = "select * from " + table_name + " where " + column_names[0] + "=" + sql_str(column_values[0])
    sys.stdout.flush()
    if len(column_names) > 1:
        for idx in range(1, len(column_names)):
            query += " and " + column_names[idx] + "=" + sql_str(column_values[idx])

    cursor.execute(query)
    desc = [d[0] for d in cursor.description]
    result = [dict(zip(desc, line)) for line in cursor]
    return result

# Initialize stuff and parse command arguments

HOME = os.path.expanduser('~')
CONFIGFILE = os.path.join(HOME, ".extractoracfg")
CONFIG = None
DSN = ''
SCHEMA = ''
USER = ''
PASSWORD = ''

config = configparser.ConfigParser()
config.read(CONFIGFILE)

if config.has_option('DEFAULT','dsn'):
    DSN = config.get('DEFAULT','dsn', None)

if config.has_option('DEFAULT','user'):
    USER = config.get('DEFAULT','user', None)

if config.has_option('DEFAULT','password'):
    PASSWORD = config.get('DEFAULT','password', None)

if config.has_option('DEFAULT','schema'):
    SCHEMA = config.get('DEFAULT','schema', USER)
else:
    SCHEMA = USER

parser = argparse.ArgumentParser(description='recursively extract data from oracle')
parser.add_argument('--xml', '-x', dest='format', action='store_const',
                    const='xml', default='sql',
                    help='XML output (default=SQL)')
parser.add_argument('--file', '-f', dest='outputfile',
                    help='output filename (none for stdout)')

parser.add_argument('--dbuser', '-u', dest='dbuser',
                    help='Oracle username')

parser.add_argument('--dbpass', '-p', dest='dbpass',
                    help='Oracle password')

parser.add_argument('--dsn', '-d', dest='dsn',
                    help='Oracle DSN')

parser.add_argument('--reverse-deps', help='dependencies expressed by foreign keys only',
                    action='store_true', dest='revdeps')
parser.add_argument('--no-reverse-deps', help='dependencies expressed by foreign keys only',
                    action='store_false', dest='revdeps')
parser.set_defaults(revdeps=True)

parser.add_argument('--skip-tables', '-s', help='skip these tables', nargs='+', type=str)
parser.add_argument('table', help='table name')
parser.add_argument('column', help='column name')
parser.add_argument('value', help='value name')
args_ns = parser.parse_args()

ARGS = vars(args_ns)
target_table = ARGS['table'].upper()
target_column = ARGS['column'].upper()
target_value = ARGS['value']

if ARGS['skip_tables']:
    skip_tables = [x.upper() for x in ARGS['skip_tables']]
else:
    skip_tables = []

if ARGS['dsn']:
    DSN = ARGS['dsn']

if ARGS['dbuser']:
    USER = ARGS['dbuser']

if ARGS['dbpass']:
    PASSWORD = ARGS['dbpass']

print (DSN,USER,PASSWORD)
if not (DSN and USER and PASSWORD):
    sys.exit("User, password and dsn are needed.\nUse the command line options or edit {}".format(CONFIGFILE))

con = cx_Oracle.connect(USER, PASSWORD, DSN)
cur = con.cursor()

dependencies = {}
data = {}

row = get_rows(cur, target_table, [target_column], [target_value])
cache = {}
cache[str((target_table, [target_column], [target_value]))] = row
queue = [(target_table, x) for x in row]
processed = []

while queue:
    item = queue.pop(0)
    if item not in processed and item[0] not in skip_tables:
        tablename = item[0]
        rowdata = item[1]
        deps = get_dependencies(cur, tablename, owner=SCHEMA)

        if ARGS['revdeps']:
            revdeps = get_dependants(cur, tablename, owner=SCHEMA)
        else:
            revdeps = None
        processed.append(item)
        if deps:
            for dep in deps:
                vals = []
                for i in range(len(dep[1])):
                    vals.append(rowdata[dep[1][i]])
                if len(vals) and vals[0]:

                    if dep[2] not in skip_tables:
                        row = []
                        try:
                            row = cache[str((dep[2], dep[3], vals))]
                        except KeyError:
                            row = get_rows(cur, dep[2], dep[3], vals)
                            cache[str((dep[2], dep[3], vals))] = row
                    else:
                        row = []

                    for r in row:
                        if dep[2] not in skip_tables:
                            queue.append((dep[2], r))
        if revdeps:
            for dep in revdeps:
                if target_column in dep[1] or target_column in dep[3]:
                    vals = []
                    for i in range(len(dep[1])):
                        vals.append(rowdata[dep[3][i]])
                    if len(vals) and vals[0]:
                        if dep[0] not in skip_tables:
                            row = []
                            try:
                                row = cache[str((dep[0], dep[1], vals))]
                            except KeyError:
                                row = get_rows(cur, dep[0], dep[1], vals)
                                cache[str((dep[0], dep[1], vals))] = row
                        else:
                            row = []

                        for r in row:
                            # print(row,len(queue))
                            if dep[0] not in skip_tables:
                                queue.append((dep[0], r))

if ARGS['outputfile']:
    outputfile = open(ARGS['outputfile'], "w")
else:
    outputfile = sys.stdout


# TODO : this format selection and output code is awful, refactor and separate data, formatting and file output
# TODO : fix output encoding, make sure it's utf-8, or any encoding that may be user selected
# TODO : make output encoding user selectable :)

if ARGS['format'] == 'xml':
    outputfile.write("<?xml version='1.0' encoding='UTF-8'?>\n<dataset>\n")

processed.reverse()

for item in processed:
    tablename = item[0]
    table_data = item[1]

    fields = get_columns(cur, tablename, SCHEMA)
    if ARGS['format'] == 'sql':
        outputfile.write("insert into " + tablename + " (")
    else:
        outputfile.write("\t<" + tablename + " ")

    if ARGS['format'] == 'sql':
        for i in range(len(fields) - 1):
            outputfile.write(fields[i] + ",")
        outputfile.write(fields[-1] + ") values (")
        for i in range(len(fields) - 1):
            outputfile.write(sql_str(table_data[fields[i]]) + ",")
        outputfile.write(sql_str(table_data[fields[-1]]) + ");\n")
    else:
        for i in range(len(fields)):
            if table_data[fields[i]]:  # ommit null values from xml output TODO : cli arg? same for sql output?
                outputfile.write(fields[i] + "=\"" + str(table_data[0][fields[i]]) + "\" ")
        outputfile.write('/>\n')

if ARGS['format'] == 'xml':
    outputfile.write('</dataset>\n')

if ARGS['outputfile']:
    outputfile.close()