import psycopg2
from normalize_data import unflatten_rows


def get_insert_statement(row):

    col_names = []
    data = []

    for k, v in row.items():
        col_names.append(k)
        value = v
        if isinstance(v, list):
            value = "array" + str(v)
        elif isinstance(v, bool):
            value = str(v)
        elif v is None:
            value = "null"
        elif isinstance(v, str):
            if v.strip() == '':
                value = "null"
            else:
                value = "'{}'".format(v)
        else:
            print(v)
            print("value is not a list or string")

        data.append(value)

    insert_statement = """
        INSERT INTO ussc ({}) VALUES ({})
        """.format(', '.join(col_names), ', '.join(data))

    return insert_statement


def add_data_to_database(start_row=0, commit_increment=1000):
    conn = psycopg2.connect(dbname='ussc', user='postgres', password='postgres', host='localhost')
    cur = conn.cursor()

    for count, row in enumerate(unflatten_rows(start_row)):
        cur.execute(get_insert_statement(row))
        if count > 0 and count % commit_increment == 0:
            print("Committing lines {} through {}".format(start_row + count - commit_increment + 1, start_row + count))
            cur.close()
            conn.commit()
            cur = conn.cursor()

    cur.close()
    conn.commit()


if __name__ == '__main__':
    add_data_to_database()
