-- A concept belongs to one metric: no combination of taxonomy and concept appears twice in the
-- seed metric_concept_map. A concept with two metrics would give one fact to two metrics.
-- The test fails if this query returns a row.
select
    taxonomy,
    concept,
    count() as occurrences
from {{ ref('metric_concept_map') }}
group by
    taxonomy,
    concept
having occurrences > 1
