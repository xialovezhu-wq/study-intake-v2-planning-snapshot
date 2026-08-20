from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify applied Skill patches and detect later drift")
    parser.add_argument("--spec", default="config/skill-canary-patches.json")
    parser.add_argument("--stage")
    args = parser.parse_args()
    spec = json.loads(Path(args.spec).read_text(encoding="utf-8"))
    verified = []
    for patch in spec["patches"]:
        if args.stage and patch["stage"] != args.stage:
            continue
        path = Path(patch["path"])
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual == patch["after_sha256"]:
            verified.append({"skill": patch["skill"], "status": "applied", "sha256": actual})
            continue
        if actual == patch["before_sha256"]:
            raise SystemExit(f"NOT_APPLIED: {patch['skill']}")
        raise SystemExit(f"POST_APPLY_DRIFT: {patch['skill']}")
    print(json.dumps({"schema_version": spec["schema_version"], "verified": verified}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
