"""
cli/shell.py

Interactive and single-command CLI for the DFS.

Usage (single command):
    python main.py touch myfile.txt
    python main.py append myfile.txt ./local.txt
    python main.py read myfile.txt
    python main.py head myfile.txt 5
    python main.py tail myfile.txt 5
    python main.py ls
    python main.py stat myfile.txt
    python main.py delete myfile.txt
    python main.py sort input.csv output.csv
    python main.py verify output.csv

Usage (interactive shell):
    python main.py shell
"""

import json
import sys
import logging


def _fmt_stat(stat_dict: dict) -> str:
    lines = [
        f"  filename  : {stat_dict['filename']}",
        f"  size      : {stat_dict['size_bytes']} bytes",
        f"  pages     : {stat_dict['num_pages']}",
        f"  version   : {stat_dict['version']}",
        f"  created   : {stat_dict['created_at']}",
    ]
    for p in stat_dict.get("pages", []):
        lines.append(
            f"    page {p['page_no']:02d}: guid={p['guid']} "
            f"size={p['size']} replicas={p['replicas']}"
        )
    return "\n".join(lines)


def run_command(dfs, sort_fn, verify_fn, args: list[str]) -> int:
    """
    Dispatch one command.  Returns 0 on success, 1 on error.
    """
    if not args:
        print("No command given. Try: touch append read head tail ls stat delete sort verify shell")
        return 1

    cmd, *rest = args

    try:
        if cmd == "touch":
            if not rest:
                print("Usage: touch <filename>")
                return 1
            dfs.touch(rest[0])
            print(f"Created: {rest[0]}")

        elif cmd == "append":
            if len(rest) < 2:
                print("Usage: append <filename> <local_path>")
                return 1
            dfs.append(rest[0], rest[1])
            stat = dfs.stat(rest[0])
            print(f"Appended to {rest[0]}: now {stat['size_bytes']} bytes, "
                  f"{stat['num_pages']} page(s)")

        elif cmd == "read":
            if not rest:
                print("Usage: read <filename>")
                return 1
            data = dfs.read(rest[0])
            print(data.decode(errors="replace"), end="")

        elif cmd == "head":
            if not rest:
                print("Usage: head <filename> [n]")
                return 1
            n = int(rest[1]) if len(rest) > 1 else 10
            print(dfs.head(rest[0], n))

        elif cmd == "tail":
            if not rest:
                print("Usage: tail <filename> [n]")
                return 1
            n = int(rest[1]) if len(rest) > 1 else 10
            print(dfs.tail(rest[0], n))

        elif cmd == "ls":
            files = dfs.ls()
            if not files:
                print("(no files)")
            else:
                for f in files:
                    print(f"  {f}")

        elif cmd == "stat":
            if not rest:
                print("Usage: stat <filename>")
                return 1
            stat = dfs.stat(rest[0])
            print(_fmt_stat(stat))

        elif cmd == "delete":
            if not rest:
                print("Usage: delete <filename>")
                return 1
            dfs.delete_file(rest[0])
            print(f"Deleted: {rest[0]}")

        elif cmd == "sort":
            if len(rest) < 2:
                print("Usage: sort <input_filename> <output_filename>")
                return 1
            sort_fn(dfs, rest[0], rest[1])
            stat = dfs.stat(rest[1])
            print(f"Sorted {rest[0]} → {rest[1]} "
                  f"({stat['size_bytes']} bytes, {stat['num_pages']} page(s))")

        elif cmd == "verify":
            if not rest:
                print("Usage: verify <sorted_filename>")
                return 1
            ok = verify_fn(dfs, rest[0])
            print(f"Sort verification: {'PASSED' if ok else 'FAILED'}")
            return 0 if ok else 1

        elif cmd == "shell":
            return _interactive_shell(dfs, sort_fn, verify_fn)

        elif cmd in ("help", "--help", "-h"):
            _print_help()

        else:
            print(f"Unknown command: {cmd!r}")
            _print_help()
            return 1

    except Exception as e:
        print(f"Error: {e}")
        logging.getLogger(__name__).exception("Command %r failed", cmd)
        return 1

    return 0


def _interactive_shell(dfs, sort_fn, verify_fn) -> int:
    print("DFS interactive shell. Type 'help' for commands, 'exit' to quit.")
    while True:
        try:
            line = input("dfs> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not line:
            continue
        if line in ("exit", "quit"):
            break
        tokens = line.split()
        run_command(dfs, sort_fn, verify_fn, tokens)
    return 0


def _print_help():
    print("""
DFS commands:
  touch   <filename>              Create an empty file
  append  <filename> <path>       Append a local file into DFS
  read    <filename>              Print full file contents
  head    <filename> [n]          Print first n lines (default 10)
  tail    <filename> [n]          Print last n lines (default 10)
  ls                              List all files
  stat    <filename>              Show file metadata
  delete  <filename>              Delete a file
  sort    <input> <output>        Distributed sort key,value records
  verify  <filename>              Check that a file is sorted
  shell                           Start interactive shell
""")
