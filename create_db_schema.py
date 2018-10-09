import psycopg2
from normalize_data import ARRAY_HEADERS, SINGLE_HEADERS


column_types = {
    'WEAPSOC': "boolean"
}


def add_column_statement(col_name, col_type):
    return "{}  {}".format(col_name, col_type)


def resolve_column_type(col_name):
    if col_name in column_types:
        return column_types[col_name]
    else:
        return "text"


def get_column_statements():
    column_statements = []
    for header in SINGLE_HEADERS:
        column_statements.append(add_column_statement(header, resolve_column_type(header)))

    for header in ARRAY_HEADERS:
        col_type = resolve_column_type(header) + "[]"
        column_statements.append(add_column_statement(header, col_type))

    return column_statements


CREATE_TABLE_STATEMENT = """
CREATE TABLE ussc (
    id  SERIAL PRIMARY KEY,
    {}
);
""".format(',\n\t'.join(get_column_statements()))


conn = psycopg2.connect(dbname='ussc', user='postgres', password='postgres', host='localhost')
cur = conn.cursor()
cur.execute(CREATE_TABLE_STATEMENT)
cur.close()
conn.commit()



