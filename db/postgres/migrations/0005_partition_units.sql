-- Units of work beyond the slot-hour and the slot-date (week 3 plan 3.3 and WP-02).
--
-- The SEC tables are replaced by other units: one date of a column, a set of key values, or
-- the whole table. ops.silver_partition keeps one row for each build of a unit, as before:
--   * 'hour' and 'day' name the unit with slot_date (and slot_hour);
--   * 'keys' and 'table' name the unit with unit_label, and slot_date stays NULL.
-- The rows of phase P1 keep their values; unit_label is NULL for the rows.

ALTER TABLE ops.silver_partition ALTER COLUMN slot_date DROP NOT NULL;
ALTER TABLE ops.silver_partition ADD COLUMN unit_label text;

-- The check keeps the name: the rules of phase P1 for 'hour' and 'day' stay inside.
ALTER TABLE ops.silver_partition DROP CONSTRAINT silver_partition_unit;
ALTER TABLE ops.silver_partition ADD CONSTRAINT silver_partition_unit CHECK (
    (unit_kind = 'hour' AND slot_date IS NOT NULL AND slot_hour IS NOT NULL
        AND slot_hour BETWEEN 0 AND 23)
    OR (unit_kind = 'day' AND slot_date IS NOT NULL AND slot_hour IS NULL)
    OR (unit_kind IN ('keys', 'table') AND slot_date IS NULL AND slot_hour IS NULL
        AND unit_label IS NOT NULL AND unit_label <> ''));

-- The current version of a unit which has a label.
CREATE INDEX silver_partition_by_label
    ON ops.silver_partition (table_name, unit_label, partition_version_id DESC)
    WHERE unit_label IS NOT NULL;
