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
import json
import os.path
import argparse

import cx_Oracle


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
        current = all_rows[0][4]
        cur_col = all_rows[0][2]
        grouped_from = []
        grouped_to = []
        grouped = []

        # group keys by referenced table  so that we know how to deal with composite foreign keys later
        for item in all_rows:
            if item[4] != current:
                grouped.append((item[0], grouped_from, cur_col, grouped_to))
                current = item[4]
                cur_col = item[2]
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

# TODO: dotfile should be optional when parameters received via command line
try:
    with open(CONFIGFILE) as jsonfile:
        CONFIG = json.load(jsonfile)
        DSN = CONFIG['dsn']
        USER = CONFIG['user']
        SCHEMA = CONFIG['schema']  # TODO: SCHEMA should default to USER if not specified
        PASSWORD = CONFIG['password']
except IOError:
    print("Couldn't read config file: ", CONFIGFILE, file=sys.stderr)
    exit(1)

con = cx_Oracle.connect(USER, PASSWORD, DSN)
cur = con.cursor()

parser = argparse.ArgumentParser(description='recursively extract data from oracle')
parser.add_argument('--xml', '-x', dest='format', action='store_const',
                    const='xml', default='sql',
                    help='XML output (default=SQL)')
parser.add_argument('--file', '-f', dest='outputfile',
                    help='output filename (none for stdout)')
parser.add_argument('table', help='table name')
parser.add_argument('column', help='column name')
parser.add_argument('value', help='value name')
args_ns = parser.parse_args()

ARGS = vars(args_ns)
target_table = ARGS['table'].upper()
target_column = ARGS['column'].upper()
target_value = ARGS['value']

queue = [target_table]
processed = []

dependencies = {}
data = {}

row = get_rows(cur, target_table, [target_column], [target_value])
data[target_table] = row

while queue:
    tablename = queue.pop(0)
    if tablename not in processed:
        deps = get_dependencies(cur, tablename, owner=SCHEMA)
        dependencies[tablename] = deps
        tables = []
        if deps:
            tables = set([x[2] for x in deps])
            for dep in deps:
                try:
                    vals = []
                    for i in range(len(dep[1])):
                        vals.append(data[dep[0]][0][dep[1][i]])
                    if len(vals) and vals[0]:
                        row = get_rows(cur, dep[2], dep[3], vals)
                        data[dep[2]] = row
                        queue.append(dep[2])
                except KeyError:
                    data[dep[0]] = {}
                    data[dep[0]][dep[1]] = None
        processed.append(tablename)

if ARGS['outputfile']:
    outputfile = open(ARGS['outputfile'], "w")
else:
    outputfile = sys.stdout

processed.reverse()


# TODO : this format selection and output is awful, refactor and separate data, formatting and file output
# TODO : fix output encoding, make sure it's utf-8, or any encoding that may be user selected
# TODO : make output encoding user selectable :)

if ARGS['format'] == 'xml':
    outputfile.write("<?xml version='1.0' encoding='UTF-8'?>\n<dataset>\n")

for tablename in processed:
    for item in range(len(data[tablename])):
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
                outputfile.write(sql_str(data[tablename][item][fields[i]]) + ",")
            outputfile.write(sql_str(data[tablename][item][fields[-1]]) + ");\n")
        else:
            for i in range(len(fields)):
                if data[tablename][item][fields[i]]:  # ommit null values from xml output TODO : cli arg? same for sql output?
                    outputfile.write(fields[i] + "=\"" + str(data[tablename][0][fields[i]]) + "\" ")
            outputfile.write('/>\n')

if ARGS['format'] == 'xml':
    outputfile.write('</dataset>\n')

if ARGS['outputfile']:
    outputfile.close()