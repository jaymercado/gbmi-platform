"""Text normalisation and the identifiers which are built on it (week 2 plan 3.3).

Two identifiers of the silver tables are hashes of normalised text: ``org_string_id`` for an
organisation string and ``story_id`` for the title of an article. Facts store the identifier
and are never rewritten (blueprint 8.8), so the rule is fixed here as version 1, and a list of
input and output pairs in the tests guards the rule. The functions are pure Python, shared by
every engine (blueprint 8.4).
"""

from __future__ import annotations

import hashlib
import html
import re
import unicodedata

#: Version of the normalisation rule. A change of the rule is a new version, never an edit.
TEXT_NORMALISATION_VERSION = 1
#: The identifiers are the first 16 hexadecimal characters (64 bits) of a SHA-256 hash.
ID_LENGTH = 16

_WHITESPACE = re.compile(r"\s+")
_PASSES = 8


def _normalise_once(text: str) -> str:
    # NFKC folds compatibility forms (full-width letters, ligatures); casefold removes case.
    folded = unicodedata.normalize("NFKC", text).casefold()
    # Decomposed, a letter with a diacritic is the letter followed by combining marks.
    decomposed = unicodedata.normalize("NFKD", folded)
    kept = []
    for character in decomposed:
        category = unicodedata.category(character)
        if category.startswith("L") or category == "Nd":
            kept.append(character)
        elif not category.startswith("M"):  # a combining mark vanishes; anything else separates
            kept.append(" ")
    return " ".join("".join(kept).split())


def normalise_text(text: str) -> str:
    """Version 1 of the normalisation.

    Unicode NFKC, case-fold, remove diacritics, replace every character which is not a letter
    or a digit with one space, collapse spaces, strip. The result holds only letters, digits
    and single spaces, and a second normalisation changes nothing: for a few rare characters
    one pass is not stable, so the pass is repeated until the text stops changing.
    """
    current = text
    for _ in range(_PASSES):
        following = _normalise_once(current)
        if following == current:
            return following
        current = following
    return current


def text_id(normalised: str) -> str:
    """The identifier of a normalised string."""
    return hashlib.sha256(normalised.encode("utf-8")).hexdigest()[:ID_LENGTH]


def org_string_id(normalised: str) -> str:
    """``org_string_id``: the identifier of a normalised organisation string."""
    return text_id(normalised)


def clean_title(raw: str | None) -> str | None:
    """The title of an article as text: character references resolved, white space collapsed.

    The title comes from the tag ``<PAGE_TITLE>`` of the GKG extras, which are XML: a title
    carries references such as ``&#xA3;``. An empty title is None.
    """
    if raw is None:
        return None
    title = _WHITESPACE.sub(" ", html.unescape(raw)).strip()
    return title or None


def story_id(title: str | None, document_url: str) -> str:
    """``story_id``: articles with the same normalised title are one story.

    A document without a usable title takes the hash of ``"url:"`` plus its URL, so the
    document is a story of its own.
    """
    normalised = normalise_text(title) if title else ""
    return text_id(normalised) if normalised else text_id(f"url:{document_url}")
