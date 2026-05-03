from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from datacollection.cli import main


if __name__ == "__main__":
    sys.argv = [sys.argv[0], "yahoo-enrichment", *sys.argv[1:]]
    main()
