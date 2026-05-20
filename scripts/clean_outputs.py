from pathlib import Path
from shutil import rmtree


def main() -> None:
    for relative in ["data/raw", "data/bronze", "data/silver", "data/gold", "data/failed", "data/metrics", "logs"]:
        path = Path(relative)
        if path.exists():
            rmtree(path)
        path.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    main()
