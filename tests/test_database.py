import unittest
import psycopg2


class TestDatabase(unittest.TestCase):

    def test_connection(self):
        conn = psycopg2.connect(dbname='ussc', user='postgres', password='postgres', host='localhost')
        cur = conn.cursor()
        cur.execute("SELECT 1")
        row = cur.fetchone()
        self.assertEqual(row[0], 1)
