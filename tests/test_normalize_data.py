import unittest
from normalize_data import *


class TestNormalizeData(unittest.TestCase):

    def test_single_row_length(self):
        """
        Test that the first row of the CSV has the expected length.
        """

        EXPECTED_LENGTH = 20902

        row = next(generate_csv_dicts())

        count = 0
        for k in row:
            if row[k] is not None:
                count += 1

        self.assertEqual(count, EXPECTED_LENGTH)

    def test_unflatten(self):
        """
        Test that unflatten runs without any exceptions.
        """

        row = next(generate_csv_dicts())
        unflatten_row(row)

    def test_decode(self):
        """
        Test that unflatten runs without any exceptions.
        """

        row = next(generate_csv_dicts())
        row = unflatten_row(row)
        decode_row(row)
        self.assertEqual(row['MONRACE'], 'black')

    def test_read_file(self):
        for count, _ in enumerate(generate_lines()):
            if count % 1000 == 0:
                print(count)
