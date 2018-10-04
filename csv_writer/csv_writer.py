# coding=utf-8
# This file is part of codeface-extraction, which is free software: you
# can redistribute it and/or modify it under the terms of the GNU General
# Public License as published by the Free Software Foundation, version 2.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#
# Copyright 2017 by Claus Hunsen <hunsen@fim.uni-passau.de>
# Copyright 2018 by Anselm Fehnker <fehnker@fim.uni-passau.de>
# All Rights Reserved.
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


def write_to_csv(file_path, lines, append=False):
    """
    Write the given lines to the file with the given file path.

    :param file_path: The path where the file shall be written
    :param lines: The lines that shall be written in the file
    :param append: Flag if lines shall be appended to file or overwrite file
    """

    open_mode = "a+b" if append else "wb"

    with open(file_path, open_mode) as csv_file:
        wr = csv.writer(csv_file, delimiter=';', lineterminator='\n', quoting=csv.QUOTE_NONNUMERIC)
        # encode in proper UTF-8 before writing to file
        for line in lines:
            line_encoded = __encode(line)
            wr.writerow(line_encoded)
