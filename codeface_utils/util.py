# coding=utf-8
# This file is part of codeface-extraction, which is free software: you
# can redistribute it and/or modify it under the terms of the GNU General
# Public License as published by the Free Software Foundation, version 2.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#
# Copyright 2013 by Siemens AG, Wolfgang Mauerer <wolfgang.mauerer@siemens.com>
# Copyright 2025 by Maximilian Löffler <s8maloef@stud.uni-saarland.de>
# Copyright 2026 by Thomas Bock <bockthom@cmu.edu>
# All Rights Reserved.
#
# The code in this file originates from:
# https://github.com/siemens/codeface/blob/master/codeface/util.py

from __future__ import absolute_import
import logging
import os
import os.path
import re
import sys
import traceback
import unicodedata
from threading import enumerate as threading_enumerate
from ftfy import fix_encoding

def setup_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
    )

log = logging.getLogger(__name__)

# Function to dump the stacks of all threads
def get_stack_dump():
    id2name = dict([(th.ident, th.name) for th in threading_enumerate()])
    code = ["Stack dump:"]
    for threadId, stack in sys._current_frames().items():
        code.append("")
        code.append("# Thread: %s(%d)" % (id2name.get(threadId,""), threadId))
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append('File: "%s", line %d, in %s' % (filename, lineno, name))
            if line:
                code.append("  %s" % (line.strip()))
    return code

def gen_range_path(base_path, i, start_rev, end_rev):
    if (len(start_rev) == 40):
        # Same logic as above, but construct a file system path
        start_rev = start_rev[0:6]
        end_rev = end_rev[0:6]
    return(os.path.join(base_path, "{0}--{1}-{2}".
                        format(str(i).zfill(3), start_rev, end_rev)))

def encode_as_utf8(string):
    """
    Encode the given string properly in UTF-8,
    independent from its internal representation (str or unicode).

    This function removes any control characters and four-byte-encoded unicode characters and replaces them
    with " ". (Four-byte-encoded unicode characters do not work with 'utf8' encoding of MySQL.)

    :param string: any string
    :return: the UTF-8 encoded string of type str
    """

    # Normalize to str first
    if isinstance(string, bytes):
        try:
            text = string.decode("utf-8")
        except UnicodeDecodeError:
            text = string.decode("utf-8", errors="replace")
    elif isinstance(string, str):
        text = string
    else:
        # not string-like, return as-is
        return string

    # convert to real unicode-utf8 encoded string, fix_text ensures proper encoding
    new_string = fix_encoding(text)

    # remove unicode characters from "Specials" block
    # see: https://www.compart.com/en/unicode/block/U+FFF0
    new_string = re.sub(r"[\ufff0-\uffff]", " ", new_string)

    # remove all kinds of control characters and emojis
    # see: https://www.fileformat.info/info/unicode/category/index.htm
    new_string = u"".join(ch if unicodedata.category(ch)[0] != "C" else " " for ch in new_string)

    new_string = new_string.encode("utf-8")

    # replace any 4-byte characters with a single space (previously: four_byte_replacement)
    try:
        # UCS-4 build
        four_byte_regex = re.compile(u"[\U00010000-\U0010ffff]")
    except re.error:
        # UCS-2 build
        four_byte_regex = re.compile(u"[\uD800-\uDBFF][\uDC00-\uDFFF]")

    four_byte_replacement = r" "  # r":4bytereplacement:"
    new_string = four_byte_regex.sub(four_byte_replacement, new_string.decode("utf-8")).encode("utf-8")

    return new_string.decode("utf-8")

