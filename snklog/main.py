#!/usr/bin/env python3
import os
import time
import glob
from pathlib import Path
import argparse
import sys
import subprocess
import re

SLURM_LOG_DIR = ".snakemake/slurm_logs/"
SNAKEMAKE_LOG_DIR = ".snakemake/log/"

def get_sorted_files(directory):
    files = []
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith('.log'):
                full_path = os.path.join(root, filename)
                files.append(full_path)
    return sorted(files, key=os.path.getmtime, reverse=True)

def list_logs(args):
    slurm_files = get_sorted_files(SLURM_LOG_DIR)
    snakemake_files = get_sorted_files(SNAKEMAKE_LOG_DIR)

    print(f"Total number of Slurm log files: {len(slurm_files)}")
    print(f"Total number of Snakemake log files: {len(snakemake_files)}")
    print(f"\n{args.num_files} most recent Slurm log files:")
    for i, file in enumerate(slurm_files[:args.num_files], 1):
        mtime = os.path.getmtime(file)
        print(f"S{i}. {time.ctime(mtime)} - {file}")

    print(f"\n{args.num_files} most recent Snakemake log files:")
    for i, file in enumerate(snakemake_files[:args.num_files], 1):
        mtime = os.path.getmtime(file)
        print(f"M{i}. {time.ctime(mtime)} - {file}")

def tail_file(file, num_lines=10, follow=False):
    with open(file, "r") as f:
        if follow:
            f.seek(0, 2)  # Go to the end of the file
            while True:
                line = f.readline()
                if not line:
                    time.sleep(0.1)  # Sleep briefly
                    continue
                yield line
        else:
            lines = f.readlines()
            for line in lines[-num_lines:]:
                yield line.strip()

def get_file_by_identifier(identifier):
    identifier = identifier.upper()  # Convert to upper case
    if identifier.startswith('S'):
        files = get_sorted_files(SLURM_LOG_DIR)
    elif identifier.startswith('M'):
        files = get_sorted_files(SNAKEMAKE_LOG_DIR)
    else:
        raise ValueError("Invalid identifier. Use 'S' for Slurm logs or 'M' for Snakemake logs.")
    
    try:
        index = int(identifier[1:]) - 1
        if index < 0 or index >= len(files):
            raise ValueError(f"Invalid file number. Please choose a number between 1 and {len(files)}")
        return files[index]
    except ValueError:
        raise ValueError("Invalid identifier format. Use 'S<number>' or 'M<number>'.")

def tail_log(args):
    try:
        file_to_tail = get_file_by_identifier(args.identifier)
    except ValueError as e:
        print(str(e))
        return

    print(f"Tailing the log file: {file_to_tail}")
    for line in tail_file(file_to_tail, num_lines=args.lines, follow=args.follow):
        print(line)

def less_log(args):
    try:
        file_to_open = get_file_by_identifier(args.identifier)
    except ValueError as e:
        print(str(e))
        return

    subprocess.run(["less", file_to_open])






def locate_failed_rules_in_log(log_file):
    failed_rules = []
    with open(log_file, 'r') as f:
        content = f.read()
        # Updated pattern to match rule name, time, and potential log files
        pattern = r'\[(.*?)\]\nError in rule (\w+):.*?log: (.*?) \(check log file\(s\) for error details\)'
        matches = re.findall(pattern, content, re.DOTALL)
        
        # if no match with log, try without log
        if not matches:
            pattern = r'\[(.*?)\]\nError in rule (\w+):'
            matches = re.findall(pattern, content, re.DOTALL)
        
        # Process the matches
        failed_rules = []
        for match in matches:
            time, rule_name, *log_file = match
            failed_rules.append({
                'time': time.strip()[0:24],
                'rule_name': rule_name,
                'log_file': log_file[0].strip() if log_file else None
            })
        
        return failed_rules

def print_rule_logs(failed_rules, verbose):
    for rule in failed_rules:
        print(f"\n{'='*60}")
        print(f"Rule: {rule['rule_name']}")
        print(f"Time: {rule['time']}")
        print(f"Log file: {rule['log_file']}")
        if rule['log_file']:
            if verbose:
                print("\nPrinting snakemake job logs. Ensure the latest snakemake log is used as specific job logs may have been overwritten.")
                print(f"{'='*60}\n")
                try:
                    with open(rule['log_file'], 'r') as f:
                        print(f"\n\n{f.read()}")
                except FileNotFoundError:
                    print(f"Log file not found: {rule['log_file']}")
        else:
            print("No specific log file assigned to this rule.")


def show_failed_rules(args, verbose):
    if args.identifier:
        try:
            log_file = get_file_by_identifier(args.identifier)
        except ValueError as e:
            print(str(e))
            return
    else:
        snakemake_files = get_sorted_files(SNAKEMAKE_LOG_DIR)
        if not snakemake_files:
            print("No Snakemake log files found.")
            return
        log_file = snakemake_files[0]
    
    print(f"Analyzing Snakemake log: {log_file}")
    
    failed_rules = locate_failed_rules_in_log(log_file)
    if not failed_rules:
        print("No failed rules found in the log.")
        return
    
    print(f"Found {len(failed_rules)} failed rule(s):")
    print_rule_logs(failed_rules, verbose=verbose)



def main():
    parser = argparse.ArgumentParser(description="Manage Snakemake and Slurm log files")
    subparsers = parser.add_subparsers(dest="command")

    # List command and its alias
    list_parser = subparsers.add_parser("list", aliases=["ls"], help="List recent log files")
    list_parser.add_argument("-n", "--num-files", type=int, default=5, help="Number of files to list for each log type")

    # Tail command and its alias
    tail_parser = subparsers.add_parser("tail", aliases=["t"], help="Tail a log file")
    tail_parser.add_argument("identifier", help="Identifier of the log file (e.g., S1, M2, s1, m2)")
    tail_parser.add_argument(
        "-n", "--lines", type=int, default=10, help="Number of lines to display"
    )
    tail_parser.add_argument(
        "-f", "--follow", action="store_true", help="Follow the file as it grows"
    )

    # Less command and its alias
    less_parser = subparsers.add_parser("less", aliases=["l"], help="Open a log file with less")
    less_parser.add_argument("identifier", help="Identifier of the log file (e.g., S1, M2, s1, m2)")
    
    # Failed command and its alias
    failed_parser = subparsers.add_parser("failed", aliases=["f"], help="Show failed rules in a log file")
    failed_parser.add_argument("identifier", nargs="?", help="Identifier of the log file (e.g., S1, M2, s1, m2). If not provided, the most recent log will be used.")

    failed_verbose_parser = subparsers.add_parser("failedv", aliases=['fv'], help="Show failed rules in a lo file and show specific rule logs if present")
    failed_verbose_parser.add_argument("identifier", nargs="?", help="Identifier of the log file (e.g., S1, M2, s1, m2). If not provided, the most recent log will be used.")

    args = parser.parse_args()

    if args.command in ["list", "ls"]:
        list_logs(args)
    elif args.command in ["tail", "t"]:
        tail_log(args)
    elif args.command in ["less", "l"]:
        less_log(args)
    elif args.command in ["failed", "f"]:
        show_failed_rules(args, verbose=False)
    elif args.command in ['failedv', "fv"]:
        show_failed_rules(args, verbose=True)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
