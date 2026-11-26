"""The positional schema of the GDELT 2.0 Global Knowledge Graph (``gkg``) feed.

A GKG file has exactly 27 tab-delimited columns, in the order of the GKG Codebook V2.1. One row
is one article: organisations, persons, themes, locations, tone, the source, the URL and a block
of extras in XML.

Two silver tables come from the feed (blueprint 7.4): ``gdelt_gkg_doc``, one row for each
article, and ``gdelt_gkg_org``, one row for each article and organisation string. The columns
``V2GCAM`` (about 70% of the bytes), the image and video columns and the quotations stay in
bronze only.
"""

from __future__ import annotations

from gbmi.core.positional import Kind, feed_schema

GKG_COLUMNS: tuple[str, ...] = (
    "GKGRECORDID", "V2.1DATE", "V2SOURCECOLLECTIONIDENTIFIER", "V2SOURCECOMMONNAME",
    "V2DOCUMENTIDENTIFIER", "V1COUNTS", "V2.1COUNTS", "V1THEMES",
    "V2ENHANCEDTHEMES", "V1LOCATIONS", "V2ENHANCEDLOCATIONS", "V1PERSONS",
    "V2ENHANCEDPERSONS", "V1ORGANIZATIONS", "V2ENHANCEDORGANIZATIONS", "V1.5TONE",
    "V2.1ENHANCEDDATES", "V2GCAM", "V2.1SHARINGIMAGE", "V2.1RELATEDIMAGES",
    "V2.1SOCIALIMAGEEMBEDS", "V2.1SOCIALVIDEOEMBEDS", "V2.1QUOTATIONS", "V2.1ALLNAMES",
    "V2.1AMOUNTS", "V2.1TRANSLATIONINFO", "V2EXTRASXML",
)  # fmt: skip

GKG_COLUMN_COUNT = 27
DOC_KEY = "gkg_record_id"
#: The logical key of ``gdelt_gkg_org``.
ORG_KEY = (DOC_KEY, "org_string_id")

#: ``GKGRECORDID``: the 14 digits of the batch time, a hyphen, an optional T for a translated
#: document, and the sequence number within the batch.
RECORD_ID_PATTERN = "[0-9]{14}-T?[0-9]+"
#: ``V1.5TONE`` holds exactly seven numbers, separated by commas, in this order.
TONE_FIELDS: tuple[str, ...] = (
    "tone",
    "positive_score",
    "negative_score",
    "polarity",
    "activity_reference_density",
    "self_group_reference_density",
    "word_count",
)

GKG = feed_schema(
    "gkg",
    GKG_COLUMNS,
    GKG_COLUMN_COUNT,
    (
        (DOC_KEY, "GKGRECORDID", Kind.REQUIRED_TEXT),
        ("doc_date", "V2.1DATE", Kind.REQUIRED_TIMESTAMP14),
        ("source_collection_identifier", "V2SOURCECOLLECTIONIDENTIFIER", Kind.INTEGER),
        ("source_common_name", "V2SOURCECOMMONNAME", Kind.TEXT),
        ("document_url", "V2DOCUMENTIDENTIFIER", Kind.REQUIRED_TEXT),
    ),
)

#: The columns of ``gdelt_gkg_doc`` which come straight from one GKG field.
DOC_COLUMNS = GKG.columns
