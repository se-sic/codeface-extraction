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
# Copyright 2026 by Ritika Hiremath <rihi00002@stud.uni-saarland.de>
# Copyright 2026 by Thomas Bock <bockthom@cmu.edu>
# All Rights Reserved.
"""
This file merges different commits.list files and issues_github.list files from different projects.
"""
# coding=utf-8
import os
import re
import csv
import argparse
import subprocess
import sys
import json
from pathlib import Path
from logging import getLogger
from codeface_utils.util import setup_logging

# create logger
setup_logging()
log = getLogger(__name__)

# raise csv field size limit
csv.field_size_limit(sys.maxsize)

def run():
    parser = argparse.ArgumentParser(description="Merge issues-github.list files")
    parser.add_argument( "--resdir", required=True, help="Path to data/results/threemonth/" )
    parser.add_argument( "--projects", nargs="+", required=True, help="One or more project folder names, e.g. project1_proximity project2_proximity" )
    parser.add_argument( "--output", required= True, help="Custom output directory name" )
    parser.add_argument( "--gitauthority", required= True, help = "path to the cloned gitauthority")
    args = parser.parse_args()

    files = ["commits.list", "issues-github.list", "bots.list", "authors.list", "issues-jira.list", "issues-zulip.list", "emails.list", "usernames.list", "commitMessages.list"]
    for file in files:
        # extract data
        all_data = extract_data_per_project(args.projects, args.resdir, file)
        if not all_data:
            # if file not present in the project, skip to the next one
            log.warning(f"No project contained '{file}', skipping.")
            continue
        # merge and update the issue content
        merged_data = merge_data(all_data,file)
        if not merged_data:
            continue
        # save merged issues
        save_merged(merged_data, args.resdir, args.output, file)
        log.info(f"{file} data successfully merged!")
    # extracts all usernames.list and authors.list to a single user_data
    user_data = extract_user_data(args.projects, args.resdir)
    # save user_data to users.list in the output directory
    output_dir = os.path.join(os.path.abspath(args.resdir), args.output, "proximity")
    os.makedirs(output_dir, exist_ok=True)
    users_list_path = os.path.join(output_dir, "users.list")
    with open(users_list_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerows(user_data)
    log.info(f"Saved users data (users.list) to {users_list_path}")

    # run gitauthority and save the csv file
    run_gitauthority(args.gitauthority, output_dir, args.output)

    # update saved files and resave them
    update(output_dir, args.output)


def extract_data_per_project(project_list, dir,type_data):
    """
    Extracts each file's data from each project and appends to all data
    """
    all_data = {}
    for project in project_list:
        # Matches the actual path for data: threemonth/<project>/proximity/type_data(commits.list, issues-github.list)
        data_file = os.path.join(dir, project, "proximity", type_data)
        if not os.path.exists(data_file):
            log.warning(f"File not found: {data_file}")
            continue

        with open(data_file, newline="", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            rows = [row for row in reader]
        all_data[project] = rows
        log.info(f"Loaded {len(rows)} rows from {type_data} of '{project}'")
    return all_data

def extract_user_data(project_list, dir):
    """
    Extracts data from authors.list and usernames.list
    The data in all_data contains each row in the format [usernmae,name,email]
    """
    all_data = []
    author_data_files = ["authors.list","usernames.list"]
    for type_data in author_data_files:
        for project in project_list:
            # Matches the actual path for data: threemonth/<project>/proximity/type_data(commits.list, issues-github.list)
            user_file = os.path.join(dir, project, "proximity", type_data)
            if not os.path.exists(user_file):
                log.warning(f"File not found: {user_file}")
                continue

            with open(user_file, newline="", encoding="utf-8") as f:
                # [ "Hakim El Hattab", "hakim.elhattab@gmail.com", ""] for authors.list
                reader = csv.reader(f, delimiter=";")
                if type_data == "authors.list":
                    rows = [[row[1], row[2],""] for row in reader if row]
                else:
                    rows = [[row[1], row[2], row[0]] for row in reader if row and not row[0] == "None"]
            all_data.extend(rows)
            log.info(f"Loaded {len(rows)} rows from '{project}'")
    return all_data


def merge_data(all_data, file):
    """
    Merging data based on the file data
    """
    if file == "commits.list":
        return merge_commits(all_data)
    if file == "issues-github.list" or file == "issues-jira.list" or file == "issues-zulip.list":
        return merge_issues(all_data)
    # General case: just combine the rows line by line.
    return merge_generic(all_data)

def merge_generic(all_data):
    """
    Combines rows from all projects line by line, without any project-specific
    transformation. Used for files that don't need special handling.
    """
    merged = []
    seen = set()
    for rows in all_data.values():
        for row in rows:
            if not row:
                continue
            key = json.dumps(row, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            merged.append(row)
    log.info(f"Total merged rows: {len(merged)}")
    return merged

def run_gitauthority(script: str, dir: str, project_name: str):
    """
    Running the gitauthority script with all required files/data
    """
    script_path = Path(os.path.join(script, "gitAuthority.py"))
    input_file = os.path.join(dir, "users.list")
    Path(dir).mkdir(parents=True, exist_ok=True)
    clean_name = Path(project_name).stem
    cmd = [sys.executable, str(script_path),
           "--file", str(input_file),
           "--name", clean_name,
           "--output-dir", str(dir),
           "--username",
           "--drop-boolean-column"]
    log.info(f"[gitauthority] Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True, cwd=str(script_path.parent))


def merge_commits(all_commits):
    """
    All commit data is taken and updated to the required format
    """
    merged_commits = []
    for project, rows in all_commits.items():
        short_name = project.replace("_proximity","")
        for row in rows:
            if not row:
                continue
            new_row = row.copy()
            new_row[0] = f"{short_name}-{new_row[0]}"
            # update column 12 only if its not empty
            if new_row[12] != "":
                new_row[12] =f"{short_name}/{new_row[12]}"
            merged_commits.append(new_row)

    return merged_commits

def merge_issues(all_issues):
    """
    All issues are taken and corrected to the required format
    """
    merged = []
    for project, rows in all_issues.items():
        short_name = project.replace("_proximity","")
        for row in rows:
            if not row:
                continue
            new_row = row.copy()
            # Updating firts row: 1 -> project1-1
            new_row[0] = f"{short_name}-{new_row[0]}"

            # Checking last row is indeed """issue""" then updating the last but one row: 3885 -> project1-3885
            last_col = new_row[13].strip().strip('"')
            # checking if the 8th Column is "connected"
            connected_col = new_row[8].strip().strip('"')
            sub_issues = new_row[7].strip().strip('"')
            issue_num = new_row[12].strip().strip('"')

            if (last_col.lower() == "issue" or connected_col.lower() == "connected" ) and issue_num.isdigit():
                new_row[12] = f"{short_name}-{issue_num}"

            if sub_issues and sub_issues != '[]':
                inner = sub_issues.strip('[]')
                sub_issues_list = [s.strip() for s in inner.split(',')]
                new_row[7] = str([f"{short_name}-{issue}" for issue in sub_issues_list])
            merged.append(new_row)
    log.info(f"Total merged rows: {len(merged)}")
    return merged

def parse_name_email(value):
    """
    Parse a gitAuthority identity string like:
        'Firstname Lastname <email@domain.com>'
    Returns (name, email) or (value, "") if format is unexpected.
    """
    value = value.strip().strip('"')
    match = re.match(r'^(.*?)\s*<([^>]+)>\s*$', value)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return value, ""


def parse_gitauthority_csv(rows):
    """
    Parse the gitAuthority CSV with format:
        project ; original_author_id ; dealialized_author_id

    Returns:
        identity_map : dict[(str, str), (str, str)]
        (orig_name, orig_email) → (dealialized_name, dealialized_email)
        Only contains entries where original and dealialized differ.
    """
    identity_map = {}

    for row in rows:
        if len(row) < 3 or row[1].strip().strip('"') == "original_author_id":
            continue  # skip header or malformed rows

        original  = row[1].strip().strip('"')
        dealialized = row[2].strip().strip('"')

        orig_name,  orig_email  = parse_name_email(original)
        dealialized_name, dealialized_email = parse_name_email(dealialized)

        if orig_name != dealialized_name or orig_email != dealialized_email:
            identity_map[(orig_name, orig_email)] = (dealialized_name, dealialized_email)

    return identity_map


def extract_usernames(rows):
    """
    Build a deduplicated username;name;email list from the gitAuthority CSV.
    Column layout (with --username --drop-boolean-column):
        project ; original_author_id ; dealialized_author_id ; username
    Only rows with a non-empty username are kept.
    """
    seen = set()
    usernames = []

    for row in rows:
        if len(row) < 4 or row[1].strip().strip('"') == "original_author_id":
            continue  # skip header or malformed rows

        username = row[3].strip().strip('"')
        if not username or (username.lower() == "none"):
            continue

        name, email = parse_name_email(row[2].strip().strip('"'))
        entry = (username, name, email)
        if entry in seen:
            continue
        seen.add(entry)
        usernames.append(list(entry))

    return usernames


def update_issues_github(issues_github_rows, identity_map):
    """
    Update col 9 (name) and col 10 (email) in issues-github.list
    using dealialized identities from gitAuthority CSV.
    """

    updated_rows  = []
    updated_count = 0

    for row in issues_github_rows:
        if not row or len(row) < 11:
            updated_rows.append(row)
            continue

        new_row = row.copy()
        # dealialized: 0 -> name , 1 -> email
        dealialized = identity_map.get((row[9].strip().strip('"'), row[10].strip().strip('"')))
        if dealialized:
            new_row[9]  = dealialized[0]
            new_row[10] = dealialized[1]
            updated_count += 1

        updated_rows.append(new_row)

    log.info(f"update_issues_github: {updated_count}/{len(updated_rows)} rows updated")
    return updated_rows


def update_commits(commits_rows, identity_map):
    """
    Update the two set of user data (cols 2, 3), (cols 5, 6) in commits.list
    using dealialized identities from gitAuthority CSV.
    """

    updated_rows  = []
    updated_count = 0

    for row in commits_rows:
        if not row or len(row) < 7:
            updated_rows.append(row)
            continue

        new_row = row.copy()
        # dealialized: 0 -> name , 1 -> email
        dealialized = identity_map.get((row[2].strip().strip('"'), row[3].strip().strip('"')))
        if dealialized:
            new_row[2] = dealialized[0]
            new_row[3] = dealialized[1]
            updated_count += 1

        # dealialized: 0 -> name , 1 -> email
        dealialized = identity_map.get((row[5].strip().strip('"'), row[6].strip().strip('"')))
        if dealialized:
            new_row[5] = dealialized[0]
            new_row[6] = dealialized[1]

        updated_rows.append(new_row)

    log.info(f"update_commits: {updated_count}/{len(updated_rows)} rows updated")
    return updated_rows

def update_bots(bots_rows, identity_map):
    """
    Update the user data (cols 0, 1) in bots.list
    using dealialized identities from gitAuthority CSV.
    """

    updated_rows  = []
    updated_count = 0
    seen_rows = set()

    for row in bots_rows:
        if not row or len(row) < 2:
            updated_rows.append(row)
            continue

        new_row = row.copy()
        # dealialized: 0 -> name , 1 -> email
        dealialized = identity_map.get((row[0].strip().strip('"'), row[1].strip().strip('"')))
        if dealialized:
            new_row[0] = dealialized[0]
            new_row[1] = dealialized[1]
            updated_count += 1

        key = json.dumps(new_row, sort_keys=True)
        if key in seen_rows:
            continue

        seen_rows.add(key)
        updated_rows.append(new_row)

    log.info(f"update_bots: {updated_count}/{len(updated_rows)} rows updated")
    return updated_rows

def update_authors(authors_rows,identity_map):
    """
    Update the user data in authors.list
    using dealialized identities from gitAuthority CSV.
    """

    updated_rows  = []
    disambiguation_rows = []
    updated_count = 0
    seen_identities = set()

    for row in authors_rows:
        if not row or len(row) < 3:
            updated_rows.append(row)
            continue

        new_row = row.copy()
        # dealialized: 0 -> name , 1 -> email
        dealialized = identity_map.get((row[1].strip().strip('"'), row[2].strip().strip('"')))
        if dealialized:
            dealialized_name, dealialized_email = dealialized
            # find the id of the dealialized data in authors_rows.
            dealialized_row = next(
                (r for r in authors_rows if len(r) >= 3
                 and r[1].strip().strip('"') == dealialized_name
                 and r[2].strip().strip('"') == dealialized_email),
                None
            )
            # updating id of the dealized row.
            if dealialized_row:
                old_id = row[0]
                old_name = row[1]
                old_email = row[2]

                new_row[0] = dealialized_row[0]
                new_row[1] = dealialized_name
                new_row[2] = dealialized_email

                if old_id != new_row[0] or old_name != new_row[1] or old_email != new_row[2]:
                    disambiguation_rows.append([
                        new_row[0], new_row[1], new_row[2],
                        old_id, old_name, old_email
                    ])
                updated_count += 1

        # skip rows without an author name.
        if not new_row[1] or not new_row[1].strip().strip('"'):
            continue

        # the same user data can have different ids (e.g. merged from different projects), so dedupe by identity(name, email), not id.
        identity = (new_row[1].strip().strip('"'), new_row[2].strip().strip('"'))
        if identity in seen_identities:
            continue
        seen_identities.add(identity)
        updated_rows.append(new_row)

    log.info(f"update_authors: {updated_count}/{len(updated_rows)} rows updated")
    return updated_rows,disambiguation_rows

def update(output_dir, project_name):
    """
    Fetches the dealialized user data (merged_authors_{project_name}.csv).
    Checks and updates each exisiting files with this dealialized user data.
    """
    ga_filename = f"merged_authors_{project_name}.csv"
    ga_path = os.path.join(os.path.abspath(output_dir), ga_filename)
    if not os.path.exists(ga_path):
        log.error(f"gitAuthority CSV not found: {ga_path}")
        return

    with open(ga_path, newline="", encoding="utf-8") as f:
        git_authority_csv = list(csv.reader(f, delimiter=";"))
    identity_map = parse_gitauthority_csv(git_authority_csv)
    log.info(f"identity_map: {len(identity_map)} dealialized entries")

    # Save identity_map to a CSV for inspection
    identity_map_path = os.path.join(output_dir, "identity_map_debug.csv")
    with open(identity_map_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["orig_name", "orig_email", "dealialized_name", "dealialized_email"])
        for (orig_name, orig_email), (deal_name, deal_email) in identity_map.items():
            writer.writerow([orig_name, orig_email, deal_name, deal_email])
    log.info(f"identity_map saved to {identity_map_path}")

    # rebuild usernames.list from the gitAuthority output, deduplicated
    usernames_rows = extract_usernames(git_authority_csv)
    usernames_path = os.path.join(output_dir, "usernames.list")
    with open(usernames_path, "w", newline="", encoding="utf-8") as f:
        csv.writer(f, delimiter=";", quoting=csv.QUOTE_ALL).writerows(usernames_rows)
    log.info(f"usernames.list saved with {len(usernames_rows)} unique rows")

    def update_file(path, updater, label):
        """
        Checks if the file exists then runs the command to update the files with dealialized user data.
        """
        if os.path.exists(path):
            with open(path, newline="", encoding="utf-8") as f:
                rows = list(csv.reader(f, delimiter=";"))
            updated = updater(rows, identity_map)
            with open(path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f, delimiter=";", quoting=csv.QUOTE_ALL).writerows(updated)
            log.info(f"{label} saved")
        else:
            log.warning(f"{label} not found in {output_dir}")

    update_file(os.path.join(output_dir, "issues-github.list"), update_issues_github, "issues-github.list")
    update_file(os.path.join(output_dir, "commits.list"), update_commits, "commits.list")
    update_file(os.path.join(output_dir, "bots.list"), update_bots, "bots.list")
    # handle authors.list separately to also write disambiguation file
    authors_path = os.path.join(output_dir, "authors.list")
    if os.path.exists(authors_path):
        with open(authors_path, newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f, delimiter=";"))
        updated_rows, disambiguation_rows = update_authors(rows, identity_map)
        with open(authors_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f, delimiter=";", quoting=csv.QUOTE_ALL).writerows(updated_rows)
        log.info("authors.list saved")
        if disambiguation_rows:
            dis_path = os.path.join(output_dir, "disambiguation-after-db.list")
            with open(dis_path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f, delimiter=";", quoting=csv.QUOTE_ALL).writerows(disambiguation_rows)
            log.info("disambiguation-after-db.list saved")
    else:
        log.warning(f"authors.list not found in {output_dir}")

    log.info("update complete!")

def save_merged(merged_rows, resdir, custom_dir, file):
    """
    Saves the merged file to a new directory alongside the input directory.
    """
    # Same directory as input directory with custom name - given by user
    output_dir = os.path.join(os.path.abspath(resdir), custom_dir, "proximity")
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, file)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";", quoting=csv.QUOTE_ALL)
        writer.writerows(merged_rows)
    log.info(f"Saved to {output_path}")
