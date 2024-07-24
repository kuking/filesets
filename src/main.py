import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import mmh3
from colorama import Fore, Style, init
from tqdm import tqdm

from misc import load_config, load_fileset_data, save_fileset_data

init(autoreset=True)


def hash_file(filepath: Path, algo: str = "murmur128") -> str:
    if algo != "murmur128":
        raise ValueError(f"Unsupported algorithm: {algo}")

    with open(filepath, "rb") as f:
        return f"{mmh3.hash128(f.read()):032x}"


def sync(config_path: Path, full: bool = False):
    algo, paths = load_config(config_path)
    data_path = config_path.with_suffix(".fsd")
    data = load_fileset_data(data_path)

    files_found, files_added, files_deleted, files_existed, files_changed = 0, 0, 0, 0, 0

    print(f"{Fore.RESET}Scanning all files 🔎 ... {Fore.RESET}")
    all_files = []
    for real_path, virtual_path in paths.items():
        for root, _, files in os.walk(real_path):
            for file in files:
                filepath = Path(root) / file
                rel_path = filepath.relative_to(real_path)
                virt_path = Path(virtual_path) / rel_path
                all_files.append((filepath, str(virt_path)))

    aborted = False
    try:
        print(f"{Fore.RESET}fileset {config_path} syncing 🔄 ... ")
        print(f"{Fore.LIGHTWHITE_EX}sorting by last known ... {Fore.RESET}")
        all_files = sorted(all_files, key=lambda k: data[k[1]]['checked'] if k[1] in data else '1900-01-01T00:00:00')
        with tqdm(total=len(all_files), unit="files", desc="Scanning") as pbar:
            for filepath, virt_path in all_files:
                files_found += 1
                mtime = os.path.getmtime(filepath)
                size = os.path.getsize(filepath)
                current_time = datetime.now().isoformat()

                if virt_path in data:
                    if not full and data[virt_path]["mtime"] == f"{mtime:.6f}" and data[virt_path]["size"] == size:
                        files_existed += 1
                        data[virt_path]["checked"] = current_time
                        data[virt_path]["exist"] = True
                        pbar.update(1)
                        continue

                file_hash = hash_file(filepath, algo)

                if virt_path in data:
                    if data[virt_path]["hash"] != file_hash:
                        files_changed += 1
                    else:
                        files_existed += 1
                else:
                    files_added += 1

                data[virt_path] = {
                    "hash": file_hash,
                    "mtime": f"{mtime:.6f}",
                    "checked": current_time,
                    "hashed": current_time,
                    "exist": True,
                    "size": size
                }

                pbar.update(1)

    except KeyboardInterrupt:
        print("Interrupted, I will save the progress so you can continue later ...")
        for vir_path in data.keys():
            if 'exist' in data[vir_path]: del data[vir_path]['exist']
        aborted = True

    if not aborted:
        deleted_files = []
        for virt_path in data.keys():
            if not 'exist' in data[virt_path]:
                files_deleted += 1
                deleted_files.append(virt_path)
            else:
                del data[virt_path]["exist"]
        for file in deleted_files:
            del data[file]

    save_fileset_data(data_path, data)

    show_status(files_added, files_changed, files_deleted, files_existed, files_found)


def show_status(files_added: int, files_changed: int, files_deleted: int, files_existed: int, files_found: int):
    print(f"{Fore.RESET}{Style.BRIGHT}{files_found} Files found")
    print(f"{Fore.RESET}{files_added} Files added")
    print(f"{Fore.RED}{files_deleted} Files deleted")
    print(f"{Fore.YELLOW}{files_changed} Files changed")
    print(f"{Fore.CYAN}{files_existed} Files already known")


def status(config_path: Path):
    data_path = config_path.with_suffix(".fsd")
    data = load_fileset_data(data_path)

    total_files = len(data)
    deleted_files = 0
    added_files = 0
    modified_files = 0

    algo, paths = load_config(config_path)

    all_files = set()
    for real_path, virtual_path in paths.items():
        for root, _, files in os.walk(real_path):
            for file in files:
                filepath = Path(root) / file
                rel_path = filepath.relative_to(real_path)
                virt_path = str(Path(virtual_path) / rel_path)
                all_files.add(virt_path)

                if virt_path not in data:
                    added_files += 1
                else:
                    mtime = os.path.getmtime(filepath)
                    size = os.path.getsize(filepath)
                    if data[virt_path]["mtime"] != f"{mtime:.6f}" or data[virt_path]["size"] != size:
                        modified_files += 1

    deleted_files = total_files - (len(all_files) - added_files)

    print(f"{Fore.CYAN}Status report for {config_path}:")
    print(f"{Fore.GREEN}Total files in database: {total_files}")
    print(f"{Fore.GREEN}Total files in filesystem: {len(all_files)}")
    print(f"{Fore.RED}Deleted files: {deleted_files}")
    print(f"{Fore.YELLOW}Added files: {added_files}")
    print(f"{Fore.YELLOW}Modified files: {modified_files}")


def check(config_path: Path, percentage: float = 100):
    algo, paths = load_config(config_path)
    data_path = config_path.with_suffix(".fsd")
    data = load_fileset_data(data_path)

    status(config_path)

    files_to_check = sorted(data.items(), key=lambda x: float(x[1]["mtime"]))
    files_to_check = files_to_check[:int(len(files_to_check) * percentage / 100)]

    print(f"\n{Fore.CYAN}Checking {len(files_to_check)} files...")

    with tqdm(total=len(files_to_check), unit="files", desc="Checking files") as pbar:
        for virt_path, file_data in files_to_check:
            real_path = None
            for rp, vp in paths.items():
                if virt_path.startswith(vp):
                    real_path = Path(rp) / Path(virt_path).relative_to(vp)
                    break

            if real_path and real_path.exists():
                file_hash = hash_file(real_path, algo)
                if file_hash != file_data["hash"]:
                    print(f"{Fore.YELLOW}{file_hash} {virt_path} [CHANGED!]")
            else:
                print(f"{Fore.RED}{virt_path} [MISSING!]")

            pbar.update(1)


def diff(data_path1: Path, data_path2: Path):
    data1 = load_fileset_data(data_path1)
    data2 = load_fileset_data(data_path2)

    only_in_1 = set(data1.keys()) - set(data2.keys())
    only_in_2 = set(data2.keys()) - set(data1.keys())
    common = set(data1.keys()) & set(data2.keys())

    different = [f for f in common if data1[f]["hash"] != data2[f]["hash"]]

    print(f"{Fore.CYAN}Comparing {data_path1} and {data_path2}:")
    print(f"{Fore.GREEN}Files only in {data_path1}: {len(only_in_1)}")
    print(f"{Fore.GREEN}Files only in {data_path2}: {len(only_in_2)}")
    print(f"{Fore.YELLOW}Files with different hashes: {len(different)}")

    if only_in_1:
        print(f"\n{Fore.GREEN}Files only in {data_path1}:")
        for f in only_in_1:
            print(f"  {f}")

    if only_in_2:
        print(f"\n{Fore.GREEN}Files only in {data_path2}:")
        for f in only_in_2:
            print(f"  {f}")

    if different:
        print(f"\n{Fore.YELLOW}Files with different hashes:")
        for f in different:
            print(f"  {f}")


def print_help():
    print("Usage: filesets <file.fsc> {sync|status|check|diff [another.fsd]}")
    print("\nArguments:")
    print("  file.fsc          The main config file (mandatory)")
    print("  {sync|status|check|diff}")
    print("    sync            Synchronize the dataset, optional: [full] for a full sync")
    print("    status          Check the status of the file")
    print("    check           Perform a check on the file")
    print("    diff            Compare with another file (requires another.fsd)")


def main():
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Error: Incorrect number of arguments.")
        print_help()
        sys.exit(1)

    config = Path(sys.argv[1])
    action = sys.argv[2]
    extra = sys.argv[3] if len(sys.argv) > 3 else None

    if action not in ['sync', 'status', 'check', 'diff']:
        print(f"Error: Unknown action '{action}'.")
        print_help()
        sys.exit(1)

    if action == 'sync':
        sync(config, "full" == extra)
    elif action == 'status':
        status(config)
    elif action == 'check':
        percentage = float(extra.rstrip("%")) if extra else 100
        check(config, percentage)
    elif action == "diff":
        if extra is None:
            print(f"{Fore.RED}Error: 'diff' command requires one additional argument")
            sys.exit(1)
        diff(config, Path(extra))
    else:
        print_help()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Operation interrupted. Saving state and exiting...")
        sys.exit(0)
