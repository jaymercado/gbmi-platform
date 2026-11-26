"""Text normalisation, version 1: properties, and pairs which pin the rule down."""

from __future__ import annotations

import string
import unicodedata

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from gbmi.core.text import (
    TEXT_NORMALISATION_VERSION,
    clean_title,
    normalise_text,
    org_string_id,
    story_id,
    text_id,
)

#: Input and output of version 1. A change of the rule breaks the list: facts store the
#: identifiers of the old rule and are never rewritten (blueprint 8.8).
VECTORS = [
    ("Opentable Inc", "opentable inc"),
    ("opentable inc", "opentable inc"),
    ("  Booking   Holdings,  Inc.  ", "booking holdings inc"),
    ("AT&T", "at t"),
    ("Moody's", "moody s"),
    ("Société Générale", "societe generale"),
    ("Nestlé S.A.", "nestle s a"),
    ("Müller-Thurgau GmbH", "muller thurgau gmbh"),
    ("STRASSE", "strasse"),
    ("Straße", "strasse"),
    ("\uff21\uff50\uff50\uff4c\uff45\u3000\uff29\uff4e\uff43", "apple inc"),  # full-width letters
    ("\ufb01rst \ufb02oor", "first floor"),  # the ligatures fi and fl
    ("İstanbul", "istanbul"),  # a capital I with a dot
    ("3M Co.", "3m co"),
    ("7-Eleven", "7 eleven"),
    ("x²", "x2"),
    ("Yahoo!", "yahoo"),
    ("Toys\u00a0\u201cR\u201d\u00a0Us", "toys r us"),  # no-break spaces and curly quotes
    ("line\tone\nline two", "line one line two"),
    ("株式会社トヨタ", "株式会社トヨタ"),
    ("Газпром", "газпром"),
    ("!!!", ""),
    ("", ""),
]


@pytest.mark.parametrize(("raw", "normalised"), VECTORS)
def test_version_1_gives_the_committed_output(raw: str, normalised: str) -> None:
    assert TEXT_NORMALISATION_VERSION == 1
    assert normalise_text(raw) == normalised


@settings(max_examples=500)
@given(st.text())
def test_a_second_normalisation_changes_nothing(text: str) -> None:
    once = normalise_text(text)

    assert normalise_text(once) == once


@settings(max_examples=500)
@given(st.text())
def test_the_output_holds_only_letters_digits_and_single_spaces(text: str) -> None:
    normalised = normalise_text(text)

    for character in normalised:
        category = unicodedata.category(character)
        assert character == " " or category.startswith("L") or category == "Nd"
    assert "  " not in normalised
    assert normalised == normalised.strip()


@given(st.text(alphabet=string.ascii_letters + string.digits, min_size=1), st.text())
def test_case_and_punctuation_around_a_name_do_not_change_its_normal_form(
    name: str, noise: str
) -> None:
    # ASCII letters only: for a few other letters, upper case and lower case are not a pair.
    punctuation = "".join(ch for ch in noise if unicodedata.category(ch).startswith("P"))

    assert normalise_text(punctuation + name.upper() + punctuation) == name.lower()


def test_an_identifier_is_64_bits_of_the_sha_256_hash() -> None:
    import hashlib

    identifier = org_string_id("opentable inc")

    assert identifier == hashlib.sha256(b"opentable inc").hexdigest()[:16]
    assert identifier == text_id("opentable inc")
    # The value itself is pinned down, like the normalisation.
    assert identifier == "3055e57fe6de47f0"


@pytest.mark.parametrize(
    ("raw", "title"),
    [
        ("OpenTable To Buy Me&amp;u", "OpenTable To Buy Me&u"),
        ("Budget adds &#xA3;2bn for the NHS", "Budget adds £2bn for the NHS"),
        ("Caf&#233; culture &#8211; a guide", "Caf\u00e9 culture \u2013 a guide"),
        ("  two\n lines\t and   spaces ", "two lines and spaces"),
        ("", None),
        ("   ", None),
        (None, None),
    ],
)
def test_a_title_is_cleaned_of_references_and_white_space(
    raw: str | None, title: str | None
) -> None:
    assert clean_title(raw) == title


def test_a_story_is_the_normalised_title_and_a_document_without_one_stands_alone() -> None:
    first = story_id("Budget adds £2bn for the NHS", "https://a.example/1")
    syndicated = story_id("BUDGET ADDS £2BN FOR THE NHS!", "https://b.example/2")

    assert first == syndicated == text_id("budget adds 2bn for the nhs")
    alone = story_id(None, "https://a.example/1")
    assert alone == text_id("url:https://a.example/1")
    assert story_id("???", "https://a.example/1") == alone  # nothing usable in the title
    assert story_id("", "https://a.example/3") != alone
