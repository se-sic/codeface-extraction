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
# Copyright 2010, 2011 by Wolfgang Mauerer <wm@linux-kernel.net>
# Copyright 2012, 2013 by Siemens AG, Wolfgang Mauerer <wolfgang.mauerer@siemens.com>
# Copyright 2025 by Maximilian Löffler <s8maloef@stud.uni-saarland.de>
# Copyright 2026 by Thomas Bock <bockthom@cmu.edu>
# All Rights Reserved.
#
# The code in this file originates from:
# https://github.com/siemens/codeface/blob/master/codeface/cluster/idManager.py
# We inherit the 'idManager' and 'dbIdManager' classes from codeface.
# The 'csvManager' class is original.

from __future__ import absolute_import
import re
from email.utils import parseaddr
from logging import getLogger
import http.client as http_client
import urllib.parse as urlparse
import json
import string
import random
import time
from abc import ABC, abstractmethod
import pandas

from ..util import encode_as_utf8


log = getLogger(__name__)

class idManager(ABC):

    def __init__(self):
        # Cache identical requests to the server
        self._cache = {}

        self.fixup_emailPattern = re.compile(r'([^<]+)\s+<([^>]+)>')
        self.commaNamePattern = re.compile(r'([^,\s]+),\s+(.+)')

    @abstractmethod
    def _query_user_id(self, name, email):
        pass

    @abstractmethod
    def getPersonFromDB(self, person_id):
        pass

    def getPersonID(self, addr):
        """Obtain a unique ID from contributor identity credentials."""

        (name, email) = self._decompose_addr(addr)
        if (name, email) not in self._cache:
            self._cache[(name, email)] = self._query_user_id(name, email)
        ID = self._cache[(name, email)]

        return ID

    def _cleanName(self, name):
        # Remove or replace characters in names that are known
        # to cause parsing problems in later stages
        name = name.replace('\"', "")
        name = name.replace("\'", "")
        name = name.strip()

        return name

    def _decompose_addr(self, addr):
        addr = addr.replace("[", "").replace("]", "")
        (name, email) = parseaddr(addr)

        # Handle cases where the name is unknown from commits that potentially
        # predate the era of git, where only an e-mail address was given.
        # In such a case, we set the name to the e-mail address. Otherwise,
        # all authors with unknown name would be matched to one person.
        if (name == "unknown" or name == "unknown (none)" or name == "none"):
            name = email

        # The eMail parser cannot handle Surname, Name <email@domain.tld> properly.
        # Provide a fixup hack for this case
        if (name == "" or email.count("@") == 0):
            m = re.search(self.fixup_emailPattern, addr)
            if m:
                name = m.group(1)
                email = m.group(2)
                m2 = re.search(self.commaNamePattern, name)
                if m2:
                    # Replace "Surname, Name" by "Name Surname"
                    name = "{0} {1}".format(m2.group(2), m2.group(1))

                # print "Fixup for addr {0} required -> ({1}/{2})".format(addr, name, email)
            else:
                # check for the following special format: email@domain.tld <>
                strangePattern = re.compile(r'(.*@.*)\s+(<>)')
                m3 = re.search(strangePattern, addr)
                if m3:
                    # Replace addr by "email <email@domain.tld>"
                    name = m3.group(1).split("@")[0] # get name before @ symbol
                    email = m3.group(1)
                    # print "Fixup for addr {0} required -> ({1}/{2})".format(addr, name, email)
                else:
                    # In this case, no eMail address was specified.
                    # print("Fixup for email required, but FAILED for {0}".format(addr))
                    name = addr
                    rand_str = "".join(random.choice(string.ascii_lowercase + string.digits)
                                       for _ in range(10))
                    email = "could.not.resolve@" + rand_str

        email = email.lower()

        name = self._cleanName(name)
        email = self._cleanName(email)

        return (name, email)


class dbIdManager(idManager):
    """Provide unique IDs for developers.

    This class provides an interface to the REST id server. Heuristics to
    detect developers who operate under multiple identities are included
    in the server."""

    def __init__(self, dbm, conf):
        super().__init__()

        self._idMgrServer = conf["idServiceHostname"]
        self._idMgrPort = conf["idServicePort"]
        self._conn = http_client.HTTPConnection(self._idMgrServer, self._idMgrPort)

        # Create a project ID
        self._dbm = dbm
        # TODO: Pass the analysis method to idManager via the configuration
        # file. However, the method should not influence the id scheme so
        # that the results are easily comparable.
        self._projectID = self._dbm.getProjectID(conf["project"],
                                                 conf["tagging"])

        # Construct request headers
        self.headers = {"Content-type":
                            "application/x-www-form-urlencoded; charset=utf-8",
                        "Accept": "text/plain"}

    def _query_user_id(self, name, email):
        """Query the ID database for a contributor ID"""

        name = encode_as_utf8(name)
        params = urlparse.urlencode({'projectID': self._projectID,
                                     'name': name,
                                     'email': email})

        try:
            self._conn.request("POST", "/post_user_id", params, self.headers)
            res = self._conn.getresponse()
        except:
            retryCount = 0
            successful = False
            while (retryCount <= 10 and not successful):
                log.warning("Could not reach ID service. Try to reconnect " \
                            "(attempt {}).".format(retryCount))
                self._conn.close()
                self._conn = http_client.HTTPConnection(self._idMgrServer, self._idMgrPort)
                time.sleep(60)
                #self._conn.ping(True)
                try:
                    self._conn.request("POST", "/post_user_id", params, self.headers)
                    res = self._conn.getresponse()
                    successful = True
                except:
                    if retryCount < 10:
                        retryCount += 1
                    else:
                        retryCount += 1
                        log.exception("Could not reach ID service. Is the server running?\n")
                        raise

        # TODO: We should handle errors by throwing an exception instead
        # of silently ignoring them
        result = res.read()
        jsond = json.loads(result)
        try:
            id = jsond["id"]
        except KeyError:
            raise Exception("Bad response from server: '{}'".format(jsond))

        return (id)

    def getPersonFromDB(self, person_id):
        """Query the ID database for a contributor and all corresponding data"""

        try:
            self._conn.request("GET", "/getUser/{}".format(person_id), headers=self.headers)
            res = self._conn.getresponse()
        except:
            self._conn.close()
            self._conn = http_client.HTTPConnection(self._idMgrServer, self._idMgrPort)
            retryCount = 0
            successful = False
            while (retryCount <= 10 and not successful):
                log.warning("Could not reach ID service. Try to reconnect " \
                            "(attempt {}).".format(retryCount))
                self._conn.close()
                self._conn = http_client.HTTPConnection(self._idMgrServer, self._idMgrPort)
                time.sleep(60)
                #self._conn.ping(True)
                try:
                    self._conn.request("GET", "/getUser/{}".format(person_id), headers=self.headers)
                    res = self._conn.getresponse()
                    successful = True
                except:
                    if retryCount < 10:
                        retryCount += 1
                    else:
                        retryCount += 1
                        log.exception("Could not reach ID service. Is the server running?\n")
                        raise

        result = res.read().decode("utf-8")
        jsond = json.loads(result)[0]

        return (jsond)


class csvIdManager(idManager):
    """Provide unique IDs for developers.

    This class provides an interface to CSV id files.
    """
    def __init__(self, conf):
        super().__init__()

        # CSV file containing the IDs
        self.csv_file = conf["csvFile"]
        self.csv_sep  = conf["csvSeparator"]
        self.df = self._verifyCsvFile()

    def _verifyCsvFile(self):
        with open(self.csv_file, "r") as file:
            df = pandas.read_csv(file, sep=self.csv_sep, names=['ID', 'name', 'email'])
            return df

    def _addRow(self, name, email):

        # determine next ID
        max_id = self.df['ID'].max()
        next_id = 0 if bool(pandas.isna(max_id)) else int(max_id) + 1

        # append new row
        self.df = self.df._append({
            'ID': next_id,
            'name': name,
            'email': email
        }, ignore_index=True)

        # dump df to file
        file = open(self.csv_file, "w")
        self.df.to_csv(file, sep=self.csv_sep, index=False, header=False)

        return next_id

    def _query_user_id(self, name, email):
        """Query the ID csv file for a contributor ID"""

        # no name is okay, but no email is not
        if not email:
            return -1

        # Match by name and email.
        # Disregard random string after "could.not.resolve@" in email
        # to avoid creating multiple entries for the same person.
        if email.startswith("could.not.resolve@"):
            rows = self.df[(self.df['name'] == name) &
                           (self.df['email'].str.startswith("could.not.resolve@"))]
        else:
            rows = self.df[(self.df['name'] == name) &
                           (self.df['email'] == email)]

        if len(rows) == 0:
            name = '' if not name else name
            return self._addRow(name, email)

        elif len(rows) == 1:
            return int(rows['ID'].values[0])

        else:
            raise Exception("Constructed author list is in invalid format. Duplicate entries found")

    def getPersonFromDB(self, person_id):
        rows = self.df[self.df['ID'] == person_id]
        if len(rows) == 1:
            return {
                'name': rows['name'].values[0],
                'email1': rows['email'].values[0],
                'id': person_id
            }
