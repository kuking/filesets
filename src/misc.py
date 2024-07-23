import json
import lzma
from pathlib import Path
from typing import Tuple, Dict


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


def save_fileset_data(filename: Path, fsd: Dict):
    with lzma.open(filename, "wt", encoding="utf-8") as f:
        f.write(json.dumps(fsd))


def load_fileset_data(filename: Path) -> Dict:
    try:
        with lzma.open(filename, "rt", encoding="utf-8") as f:
            return json.loads(f.read())
    except FileNotFoundError:
        return {}
