import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

import mmh3
import zstandard
from colorama import Fore, Style, init
from tqdm import tqdm

init(autoreset=True)


def hash_file(filepath: Path, algo: str = "murmur128") -> str:
    if algo != "murmur128":
        raise ValueError(f"Unsupported algorithm: {algo}")

    with open(filepath, "rb") as f:
        return f"{mmh3.hash128(f.read()):032x}"


def load_config(config_path: Path) -> Tuple[str, Dict[str, str]]:
    with open(config_path, "r") as f:
        lines = f.readlines()

    algo = "murmur128"
    paths = {}

    for line in lines:
        line = line.strip()
        if line.startswith("algo="):
            algo = line.split("=")[1]
        elif "=>" in line:
            real_path, virtual_path = map(str.strip, line.split("=>"))
            real_path = real_path.strip('"')
            paths[real_path] = virtual_path

    return algo, paths


def load_data(data_path: Path) -> Dict:
    if not data_path.exists():
        return {}

    with open(data_path, "rb") as f:
        compressed_data = f.read()

    dctx = zstandard.ZstdDecompressor()
    json_data = dctx.decompress(compressed_data)
    return json.loads(json_data)


def save_data(data_path: Path, data: Dict):
    json_data = json.dumps(data).encode("utf-8")
    cctx = zstandard.ZstdCompressor(level=3)
    compressed_data = cctx.compress(json_data)

    with open(data_path, "wb") as f:
        f.write(compressed_data)


def sync_files(config_path: Path, fast: bool = False):
    algo, paths = load_config(config_path)
    data_path = config_path.with_suffix(".fsd")
    data = load_data(data_path)

    files_found = 0
    files_added = 0
    files_existed = 0
    files_changed = 0

    all_files = []
    for real_path, virtual_path in paths.items():
        for root, _, files in os.walk(real_path):
            for file in files:
                filepath = Path(root) / file
                rel_path = filepath.relative_to(real_path)
                virt_path = Path(virtual_path) / rel_path
                all_files.append((filepath, str(virt_path)))

    print(f"{Fore.CYAN}fileset {config_path} syncing ... 🔄")

    with tqdm(total=len(all_files), unit="files", desc="Sync'ing files") as pbar:
        for filepath, virt_path in all_files:
            files_found += 1
            mtime = os.path.getmtime(filepath)
            size = os.path.getsize(filepath)

            if virt_path in data:
                if fast and data[virt_path]["mtime"] == f"{mtime:.6f}" and data[virt_path]["size"] == size:
                    files_existed += 1
                    pbar.update(1)
                    continue

            file_hash = hash_file(filepath, algo)

            if virt_path in data:
                if data[virt_path]["hash"] != file_hash:
                    files_changed += 1
                    print(f"{Fore.YELLOW}{file_hash} {virt_path} [CHANGED!]")
                else:
                    files_existed += 1
            else:
                files_added += 1
                print(f"{Fore.GREEN}{file_hash} {virt_path} [ADDED]")

            data[virt_path] = {
                "hash": file_hash,
                "mtime": f"{mtime:.6f}",
                "size": size
            }

            pbar.update(1)

    save_data(data_path, data)

    print(f"\n{Fore.GREEN}{files_found} Files found")
    print(f"{Fore.GREEN}{files_added} Files added")
    print(f"{Fore.CYAN}{files_existed} Files already existed")
    print(f"{Fore.YELLOW}{files_changed} Files changed")


def status(config_path: Path):
    data_path = config_path.with_suffix(".fsd")
    data = load_data(data_path)

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
    data = load_data(data_path)

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
    data1 = load_data(data_path1)
    data2 = load_data(data_path2)

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


def main():
    parser = argparse.ArgumentParser(description="filesets - Tool to keep track of big datasets of files")
    parser.add_argument("config", type=Path, help="Path to the configuration file")
    parser.add_argument("command", choices=["sync", "status", "check", "diff", "help"], help="Command to execute")
    parser.add_argument("args", nargs="*", help="Additional arguments")

    args = parser.parse_args()

    if args.command == "sync":
        sync_files(args.config, "fast" in args.args)
    elif args.command == "status":
        status(args.config)
    elif args.command == "check":
        percentage = float(args.args[0].rstrip("%")) if args.args else 100
        check(args.config, percentage)
    elif args.command == "diff":
        if len(args.args) != 1:
            print(f"{Fore.RED}Error: 'diff' command requires one additional argument")
            sys.exit(1)
        diff(args.config, Path(args.args[0]))
    elif args.command == "help" or not args.command:
        parser.print_help()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Operation interrupted. Saving state and exiting...")
        sys.exit(0)