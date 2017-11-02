# coding=utf-8
"""
This file provides the needed functions for standardized CSV writing
"""

import csv


def __encode(line):
    """Encode the given line (a tuple of columns) properly in UTF-8."""

    lineres = ()  # re-encode column if it is unicode
    for column in line:
        if type(column) is unicode:
            lineres += (column.encode("utf-8"),)
        else:
            lineres += (column,)

    return lineres


def write_to_csv(file_path, lines):
    """Write the given lines to the file with the given file path."""

    # write lines to file for current kind of artifact
    with open(file_path, 'wb') as csv_file:
        wr = csv.writer(csv_file, delimiter=';', lineterminator='\n', quoting=csv.QUOTE_NONNUMERIC)
        # encode in proper UTF-8 before writing to file
        for line in lines:
            line_encoded = __encode(line)
            wr.writerow(line_encoded)
