# coding=utf-8
"""
This file provides the needed functions for standardized CSV writing
"""

import csv


def write_to_csv(file_path, lines):
    """Write the given lines to the file with the given file path."""

    # write lines to file for current kind of artifact
    with open(file_path, 'wb') as csv_file:
        wr = csv.writer(csv_file, delimiter=';', lineterminator='\n', quoting=csv.QUOTE_NONNUMERIC)
        wr.writerows(lines)
