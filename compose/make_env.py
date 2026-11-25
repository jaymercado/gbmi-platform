"""Create or complete the git-ignored .env file from .env.example.

Every placeholder in the template is replaced with a freshly generated secret. A value that
.env already holds is kept, so the command can be repeated after the template gains new keys.
Only the standard library is used, so it runs before any virtual environment exists.

Usage:
    python compose/make_env.py [--example .env.example] [--env .env]
"""

from __future__ import annotations

import argparse
import base64
import os
import secrets
from pathlib import Path

GENERATE = "__GENERATE__"
GENERATE_FERNET = "__GENERATE_FERNET__"


def generate(placeholder: str) -> str:
    """A URL-safe secret; hexadecimal so that it can sit inside a connection URL unescaped."""
    if placeholder == GENERATE_FERNET:
        # A Fernet key is 32 random bytes in URL-safe base64.
        return base64.urlsafe_b64encode(os.urandom(32)).decode("ascii")
    return secrets.token_hex(24)


def parse(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key, _, value = stripped.partition("=")
            values[key.strip()] = value.strip()
    return values


def render(example: str, existing: dict[str, str]) -> tuple[str, list[str], list[str]]:
    """Return the new file content, the keys generated and the keys kept."""
    generated: list[str] = []
    kept: list[str] = []
    lines: list[str] = []
    for line in example.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            lines.append(line)
            continue
        key, _, template_value = stripped.partition("=")
        key = key.strip()
        current = existing.get(key)
        if current is not None and current not in ("", GENERATE, GENERATE_FERNET):
            value = current
            kept.append(key)
        elif template_value in (GENERATE, GENERATE_FERNET):
            value = generate(template_value)
            generated.append(key)
        else:
            value = template_value
        lines.append(f"{key}={value}")
    # Keys the developer added by hand stay at the end of the file.
    template_keys = set(parse(example))
    extra = [f"{key}={value}" for key, value in existing.items() if key not in template_keys]
    if extra:
        lines += ["", "# --- Added locally ---", *extra]
    return "\n".join(lines) + "\n", generated, kept


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--example", type=Path, default=Path(".env.example"))
    parser.add_argument("--env", type=Path, default=Path(".env"))
    args = parser.parse_args()

    existing = parse(args.env.read_text(encoding="utf-8")) if args.env.exists() else {}
    content, generated, kept = render(args.example.read_text(encoding="utf-8"), existing)
    args.env.write_text(content, encoding="utf-8")
    args.env.chmod(0o600)
    print(f"{args.env}: {len(generated)} secrets generated, {len(kept)} existing values kept")


if __name__ == "__main__":
    main()
