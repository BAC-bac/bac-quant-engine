from pathlib import Path
from datetime import datetime
import yaml
import platform

def get_data_lake_root(paths: dict) -> Path:
    root_config = paths["data_lake_root"]

    if isinstance(root_config, dict):
        if platform.system() == "Windows":
            return Path(root_config["windows"])
        return Path(root_config["linux"])

    return Path(root_config)


def resolve_data_lake_path(data_lake_root: Path, value: str) -> Path:
    path = Path(value)

    if path.is_absolute() or ":" in value:
        return path

    return data_lake_root / path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


REQUIRED_GREYHOUND_FOLDERS = [
    "raw/results",
    "raw/tips",
    "raw/preoff_prices",
    "staging",
    "curated",
    "reports",
    "logs",
]


def load_paths() -> dict:
    if not PATHS_CONFIG.exists():
        raise FileNotFoundError(f"Missing paths config: {PATHS_CONFIG}")

    with open(PATHS_CONFIG, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def check_path(path: Path, label: str) -> dict:
    return {
        "label": label,
        "path": str(path),
        "exists": path.exists(),
        "is_dir": path.is_dir(),
    }


def test_write(folder: Path) -> bool:
    try:
        folder.mkdir(parents=True, exist_ok=True)
        test_file = folder / "_write_test.txt"
        test_file.write_text(
            f"Write test created at {datetime.now()}\n",
            encoding="utf-8",
        )
        test_file.unlink()
        return True
    except Exception as error:
        print(f"Write test failed for {folder}: {error}")
        return False


def main() -> None:
    print("Starting greyhound data lake path check...")

    paths = load_paths()
    greyhound_paths = paths["greyhounds"]

    data_lake_root = get_data_lake_root(paths)
    greyhound_root = data_lake_root / "greyhounds"

    print(f"\nProject root: {PROJECT_ROOT}")
    print(f"Paths config: {PATHS_CONFIG}")
    print(f"Data lake root: {data_lake_root}")
    print(f"Greyhound root: {greyhound_root}")

    checks = []

    checks.append(check_path(data_lake_root, "data_lake_root"))
    checks.append(check_path(greyhound_root, "greyhound_root"))

    print("\nChecking configured greyhound paths...")

    for key, value in greyhound_paths.items():
        if isinstance(value, str):
            checks.append(check_path(resolve_data_lake_path(data_lake_root, value), f"configured:{key}"))

    print("\nEnsuring required live folders exist...")

    for relative_folder in REQUIRED_GREYHOUND_FOLDERS:
        folder = greyhound_root / relative_folder
        folder.mkdir(parents=True, exist_ok=True)
        checks.append(check_path(folder, f"required:{relative_folder}"))

    logs_dir = greyhound_root / "logs"
    can_write = test_write(logs_dir)

    print("\nPath check results:")
    for item in checks:
        status = "OK" if item["exists"] else "MISSING"
        print(
            f"{status:8} | {item['label']:30} | "
            f"exists={item['exists']} | dir={item['is_dir']} | {item['path']}"
        )

    print("\nWrite test:")
    print(f"logs writable: {can_write}")

    if can_write and all(item["exists"] for item in checks):
        print("\nData lake path check PASSED.")
    else:
        print("\nData lake path check completed with warnings.")


if __name__ == "__main__":
    main()
