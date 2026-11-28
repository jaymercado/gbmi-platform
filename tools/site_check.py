"""make docs: no secret of the stack in the files of the documentation site.

    python3 tools/site_check.py <site folder> [--env .env] [--example .env.example]

The dbt documentation site is public (GitHub Pages; week 3 plan, WP-14). dbt writes the
compiled SQL of every model and the settings of the project into the site files, so a value of
the environment could travel with them. The script reads each secret of the stack from the
environment file: the generated secrets which the template names, and the declared identity
for the SEC. It searches every file of the site folder for each value and prints the name of
the file and the name of the variable for a hit, never a value.

The standard library only: the script runs with any Python 3 of a runner.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "compose"))

import make_env  # noqa: E402  (compose/make_env.py: the generator of .env)

#: Values of the environment which are no generated secret and still must not be published.
ALSO_PRIVATE = ("GBMI_SEC_USER_AGENT",)
#: A shorter value would match by chance; no secret of the template is that short.
_MIN_LENGTH = 8


def unquoted(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


def secrets(env_file: Path, example: Path) -> dict[str, str]:
    """Each private value of the environment file, by the name of its variable."""
    placeholders = (make_env.GENERATE, make_env.GENERATE_FERNET)
    template = make_env.parse(example.read_text("utf-8"))
    names = [name for name, value in template.items() if value in placeholders]
    values = make_env.parse(env_file.read_text("utf-8"))
    found = {}
    for name in (*names, *ALSO_PRIVATE):
        value = unquoted(values.get(name, ""))
        if len(value) >= _MIN_LENGTH:
            found[name] = value
    return found


def hits(site: Path, values: dict[str, str]) -> list[tuple[str, str]]:
    """Each (file, variable) whose value stands in a file of the site."""
    found = []
    for path in sorted(item for item in site.rglob("*") if item.is_file()):
        text = path.read_text("utf-8", errors="replace")
        found += [
            (str(path.relative_to(site)), name) for name, value in values.items() if value in text
        ]
    return found


def main(arguments: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("site", type=Path)
    parser.add_argument("--env", type=Path, default=REPO / ".env")
    parser.add_argument("--example", type=Path, default=REPO / ".env.example")
    options = parser.parse_args(arguments)
    files = [item for item in options.site.rglob("*") if item.is_file()]
    if not files:
        sys.stderr.write(f"site-check: the folder {options.site} holds no file\n")
        return 1
    if not options.env.is_file():
        sys.stderr.write("site-check: the environment file is missing; nothing to search for\n")
        return 1
    values = secrets(options.env, options.example)
    found = hits(options.site, values)
    for name, variable in found:
        sys.stdout.write(f"site-check: the file {name} holds the value of {variable}\n")
    if found:
        return 1
    sys.stdout.write(
        f"site-check: passed; {len(files)} files searched "
        f"for the values of {len(values)} variables\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
