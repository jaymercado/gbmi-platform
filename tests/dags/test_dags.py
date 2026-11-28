"""The DAG files as Airflow loads them (week 2 plan WP-07; blueprint 8.1 and 10).

The tests run with the interpreter of Airflow inside the image (``make test-dags``), with the
standard library module unittest: the Airflow environment has no pytest, and no package is
added to the Airflow environment without need.
"""

from __future__ import annotations

import os
import subprocess
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

import attrs
import jinja2

DAGS = Path(__file__).resolve().parents[2] / "dags"
GDELT_DAGS = ("gdelt_ingest", "gdelt_silver_hourly", "gdelt_silver_daily")
#: The three SEC DAGs of week 3, each with its one task which asks the SEC.
SEC_DAGS = {"sec_filings": "filings", "sec_reference_daily": "reference", "sec_xbrl_facts": "facts"}
TRACER = "tracer_gdelt_events"
GBMI_PYTHON = "/opt/venvs/gbmi/bin/python"
#: The tasks which commit to silver tables, for each DAG.
SILVER_WRITERS = {
    "gdelt_ingest": set(),
    "gdelt_silver_hourly": {"build_units"},
    "gdelt_silver_daily": {"build_export", "build_mentions", "build_gkg"},
    TRACER: {"build_silver"},
    # The SEC tasks commit to the five SEC tables only, and no other job writes the five
    # tables: the tasks take the pool sec_api and not the pool silver_writer (D49).
    **{dag_id: set() for dag_id in SEC_DAGS},
}
#: The task after each task, for the scheduled DAGs.
ORDER = {
    "gdelt_ingest": {
        "reconcile": {"ingest"},
        "ingest": {"report_overdue"},
        "report_overdue": set(),
    },
    "gdelt_silver_hourly": {"find_units": {"build_units"}, "build_units": set()},
    "gdelt_silver_daily": {
        "build_export": {"build_mentions"},
        "build_mentions": {"build_gkg"},
        "build_gkg": {"summary"},
        "summary": set(),
    },
    **{dag_id: {name: {"summary"}, "summary": set()} for dag_id, name in SEC_DAGS.items()},
}


def load(schedules: str = "on") -> tuple[Any, list[str]]:
    """Load the DAG files as the DAG processor does; also give the commands the import started."""
    from airflow.dag_processing.dagbag import BundleDagBag

    started: list[str] = []
    real = subprocess.Popen.__init__

    def recording(self: Any, args: Any, *rest: Any, **options: Any) -> None:
        started.append(args if isinstance(args, str) else " ".join(str(part) for part in args))
        real(self, args, *rest, **options)

    with (
        mock.patch.dict(os.environ, {"GBMI_DAG_SCHEDULES": schedules}),
        mock.patch.object(subprocess.Popen, "__init__", recording),
    ):
        bag = BundleDagBag(dag_folder=DAGS, bundle_path=DAGS, bundle_name="gbmi")
    return bag, started


def timetable_of(dag: Any) -> dict[str, Any]:
    """The declared timetable of a DAG, with the time zone by its name."""
    declared = attrs.asdict(dag.timetable)
    return {
        **declared,
        "type": type(dag.timetable).__name__,
        "timezone": getattr(declared["timezone"], "name", str(declared["timezone"])),
    }


def render(template: str, **context: Any) -> str:
    """Render a template as a task does: a name which the context lacks is an error."""
    environment = jinja2.Environment(undefined=jinja2.StrictUndefined, autoescape=True)
    return environment.from_string(template).render(**context)


class DagFilesTest(unittest.TestCase):
    bag: Any
    started: list[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.bag, cls.started = load()

    def test_every_dag_file_imports_without_an_error(self) -> None:
        self.assertEqual(self.bag.import_errors, {})
        self.assertEqual(sorted(self.bag.dags), sorted([*GDELT_DAGS, *SEC_DAGS, TRACER]))

    def test_the_import_starts_no_dbt_command(self) -> None:
        # Cosmos renders the dbt models from the prebuilt manifest (spike answer B3).
        self.assertEqual([command for command in self.started if "dbt" in command], [])

    def test_each_dag_runs_one_run_at_a_time_and_does_not_catch_up(self) -> None:
        for dag_id, dag in self.bag.dags.items():
            with self.subTest(dag=dag_id):
                self.assertEqual(dag.max_active_runs, 1)
                self.assertIs(dag.catchup, False)
                # New DAGs start paused: the stack sets the rule, and no DAG overrides the rule.
                self.assertIsNone(dag.is_paused_upon_creation)

    def test_each_task_has_an_execution_timeout(self) -> None:
        for dag_id, dag in self.bag.dags.items():
            for task in dag.tasks:
                with self.subTest(dag=dag_id, task=task.task_id):
                    self.assertIsInstance(task.execution_timeout, timedelta)
                    self.assertGreater(task.execution_timeout, timedelta(0))

    def test_each_task_which_writes_to_silver_uses_the_pool_silver_writer(self) -> None:
        for dag_id, writers in SILVER_WRITERS.items():
            with self.subTest(dag=dag_id):
                tasks = self.bag.dags[dag_id].tasks
                in_the_pool = {task.task_id for task in tasks if task.pool == "silver_writer"}
                self.assertEqual(in_the_pool, writers)

    def test_each_python_task_runs_in_the_gbmi_environment(self) -> None:
        for dag_id, dag in self.bag.dags.items():
            for task in dag.tasks:
                if task.task_id.startswith("dbt."):
                    continue  # dbt runs through Cosmos with the executable of the dbt environment
                with self.subTest(dag=dag_id, task=task.task_id):
                    self.assertEqual(task.python, GBMI_PYTHON)
                    self.assertIs(task.expect_airflow, False)

    def test_the_schedules_are_the_schedules_of_the_week_2_plan(self) -> None:
        cron = {"type": "CronTriggerTimetable", "timezone": "UTC", "run_immediately": False}
        self.assertEqual(
            timetable_of(self.bag.dags["gdelt_ingest"]),
            {**cron, "expression": "8,23,38,53 * * * *", "interval": timedelta(0)},
        )
        self.assertEqual(
            timetable_of(self.bag.dags["gdelt_silver_hourly"]),
            {**cron, "expression": "11,26,41,56 * * * *", "interval": timedelta(0)},
        )
        self.assertEqual(
            timetable_of(self.bag.dags["gdelt_silver_daily"]),
            {**cron, "expression": "25 8 * * *", "interval": timedelta(days=1)},
        )
        self.assertEqual(type(self.bag.dags[TRACER].timetable).__name__, "NullTimetable")

    def test_the_daily_run_of_0825_builds_the_day_before(self) -> None:
        # The scheduler class of the same timetable: the run which may start at 08:25 on
        # 26 November has the data interval which starts at 08:25 on 25 November. The start
        # of the interval is the logical date, and its date is the day which the run builds.
        from airflow.timetables.trigger import CronTriggerTimetable

        declared = timetable_of(self.bag.dags["gdelt_silver_daily"])
        timetable = CronTriggerTimetable(
            declared["expression"], timezone=declared["timezone"], interval=declared["interval"]
        )

        interval = timetable.infer_manual_data_interval(
            run_after=datetime(2026, 11, 26, 8, 25, tzinfo=UTC)
        )

        self.assertEqual(interval.start, datetime(2026, 11, 25, 8, 25, tzinfo=UTC))
        self.assertEqual(interval.end, datetime(2026, 11, 26, 8, 25, tzinfo=UTC))

    def test_no_scheduled_dag_has_a_start_date(self) -> None:
        # With a start date, an unpause would create a run for the most recent past time at
        # once, beside the runs of a backfill (week 2 plan 3.5, change C3).
        for dag_id in (*GDELT_DAGS, *SEC_DAGS):
            with self.subTest(dag=dag_id):
                self.assertIsNone(self.bag.dags[dag_id].start_date)

    def test_the_tasks_follow_each_other_as_the_plan_says(self) -> None:
        for dag_id, order in ORDER.items():
            dag = self.bag.dags[dag_id]
            with self.subTest(dag=dag_id):
                self.assertEqual(
                    {task.task_id: set(task.downstream_task_ids) for task in dag.tasks}, order
                )

    def test_a_failed_task_does_not_stop_the_tasks_which_have_to_run(self) -> None:
        rules = {
            dag_id: {
                task.task_id: str(getattr(task.trigger_rule, "value", task.trigger_rule))
                for task in self.bag.dags[dag_id].tasks
            }
            for dag_id in GDELT_DAGS
        }
        self.assertEqual(
            rules,
            {
                # The files keep landing when the daily reconciliation fails.
                "gdelt_ingest": {
                    "reconcile": "all_success",
                    "ingest": "all_done",
                    "report_overdue": "all_done",
                },
                "gdelt_silver_hourly": {"find_units": "all_success", "build_units": "all_success"},
                # One feed which fails does not stop the other feeds; the summary fails the run.
                "gdelt_silver_daily": {
                    "build_export": "all_success",
                    "build_mentions": "all_done",
                    "build_gkg": "all_done",
                    "summary": "all_done",
                },
            },
        )

    def test_the_jobs_which_commit_to_silver_have_two_retries_at_most(self) -> None:
        # The local stack sets GBMI_TASK_RETRIES=0; the rule of the blueprint is "at most 2".
        configured = int(os.environ.get("GBMI_TASK_RETRIES", "3"))
        for dag_id in ("gdelt_silver_hourly", "gdelt_silver_daily"):
            for task in self.bag.dags[dag_id].tasks:
                with self.subTest(dag=dag_id, task=task.task_id):
                    self.assertEqual(task.retries, min(configured, 2))
        for task in self.bag.dags["gdelt_ingest"].tasks:
            self.assertEqual(task.retries, configured)


class SecDagsTest(unittest.TestCase):
    """The wiring of the three SEC DAGs (week 3 plan, WP-07)."""

    bag: Any

    @classmethod
    def setUpClass(cls) -> None:
        cls.bag, _ = load()

    def test_the_schedules_are_the_schedules_of_the_week_3_plan(self) -> None:
        # One hour apart: the filings first, then the universe, then the facts.
        cron = {
            "type": "CronTriggerTimetable",
            "timezone": "UTC",
            "run_immediately": False,
            "interval": timedelta(0),
        }
        expressions = {dag_id: timetable_of(self.bag.dags[dag_id]) for dag_id in SEC_DAGS}
        self.assertEqual(
            expressions,
            {
                "sec_filings": {**cron, "expression": "30 8 * * *"},
                "sec_reference_daily": {**cron, "expression": "30 9 * * *"},
                "sec_xbrl_facts": {**cron, "expression": "30 10 * * *"},
            },
        )

    def test_one_task_of_each_dag_asks_the_sec_and_takes_the_pool_with_one_slot(self) -> None:
        for dag_id, name in SEC_DAGS.items():
            with self.subTest(dag=dag_id):
                tasks = self.bag.dags[dag_id].tasks
                self.assertEqual({task.task_id for task in tasks}, {name, "summary"})
                in_the_pool = {task.task_id for task in tasks if task.pool == "sec_api"}
                self.assertEqual(in_the_pool, {name})
        for dag_id in (*GDELT_DAGS, TRACER):
            pools = {task.pool for task in self.bag.dags[dag_id].tasks}
            self.assertNotIn("sec_api", pools)

    def test_the_time_limits_of_the_sec_tasks(self) -> None:
        limits = {
            dag_id: {task.task_id: task.execution_timeout for task in self.bag.dags[dag_id].tasks}
            for dag_id in SEC_DAGS
        }
        self.assertEqual(
            limits,
            {
                "sec_filings": {
                    "filings": timedelta(minutes=30),
                    "summary": timedelta(minutes=10),
                },
                # One download of 1.58 GB with a limit of 30 minutes, and 8,000 documents.
                "sec_reference_daily": {
                    "reference": timedelta(minutes=90),
                    "summary": timedelta(minutes=10),
                },
                "sec_xbrl_facts": {
                    "facts": timedelta(minutes=30),
                    "summary": timedelta(minutes=10),
                },
            },
        )

    def test_an_sec_task_has_one_retry_twenty_minutes_after_the_failure(self) -> None:
        # A block by the SEC lasts 15 minutes: the retry has to start after the block.
        configured = int(os.environ.get("GBMI_TASK_RETRIES", "3"))
        for dag_id in SEC_DAGS:
            for task in self.bag.dags[dag_id].tasks:
                with self.subTest(dag=dag_id, task=task.task_id):
                    self.assertEqual(task.retries, min(configured, 1))
                    self.assertEqual(task.retry_delay, timedelta(minutes=20))
                    self.assertIs(task.retry_exponential_backoff, False)

    def test_the_summary_runs_after_a_failed_task_too(self) -> None:
        for dag_id, name in SEC_DAGS.items():
            rules = {
                task.task_id: str(getattr(task.trigger_rule, "value", task.trigger_rule))
                for task in self.bag.dags[dag_id].tasks
            }
            self.assertEqual(rules, {name: "all_success", "summary": "all_done"})

    def test_no_sec_dag_has_a_parameter_for_the_source(self) -> None:
        # The stack decides the source of the SEC client (week 3 plan 3.3, change C22).
        parameters = {dag_id: set(self.bag.dags[dag_id].params) for dag_id in SEC_DAGS}
        self.assertEqual(
            parameters,
            {
                "sec_filings": {"first_day", "last_day", "accept_volume", "as_of"},
                "sec_reference_daily": {"bulk", "accept_volume", "as_of"},
                "sec_xbrl_facts": {"full", "accept_volume", "as_of"},
            },
        )

    def test_a_boolean_parameter_arrives_as_the_text_true_or_false(self) -> None:
        # The task compares the rendered text with "True".
        self.assertEqual(render("{{ params.bulk }}", params={"bulk": True}), "True")
        self.assertEqual(render("{{ params.full }}", params={"full": False}), "False")
        self.assertEqual(render("{{ params.first_day or '' }}", params={"first_day": None}), "")


class SchedulesOffTest(unittest.TestCase):
    def test_the_demo_stack_runs_its_dags_by_hand_only(self) -> None:
        bag, _ = load(schedules="off")

        self.assertEqual(bag.import_errors, {})
        for dag_id in (*GDELT_DAGS, *SEC_DAGS, TRACER):
            with self.subTest(dag=dag_id):
                self.assertEqual(type(bag.dags[dag_id].timetable).__name__, "NullTimetable")


class TemplatesTest(unittest.TestCase):
    """The clock and the day of a run, for a run with and without a logical date (R10)."""

    @classmethod
    def setUpClass(cls) -> None:
        load()  # puts the DAG folder on the import path
        import common

        cls.as_of = common.AS_OF
        cls.logical_date = common.LOGICAL_DATE

    def test_the_clock_of_a_run_is_the_parameter_or_the_time_of_the_run(self) -> None:
        # A run which is triggered by hand without a logical date: the context has no key
        # logical_date at all, and the run has run_after.
        manual = SimpleNamespace(
            logical_date=None, run_after=datetime(2026, 11, 26, 8, 9, 30, tzinfo=UTC)
        )

        self.assertEqual(
            render(self.as_of, params={"as_of": None}, dag_run=manual), "2026-11-26T08:09:30+00:00"
        )
        self.assertEqual(
            render(self.as_of, params={"as_of": "2026-11-25T09:00:00+00:00"}, dag_run=manual),
            "2026-11-25T09:00:00+00:00",
        )

    def test_the_logical_date_of_a_run_is_empty_for_a_run_without_one(self) -> None:
        manual = SimpleNamespace(
            logical_date=None, run_after=datetime(2026, 11, 26, 8, 9, tzinfo=UTC)
        )
        scheduled = SimpleNamespace(
            logical_date=datetime(2026, 11, 25, 8, 25, tzinfo=UTC),
            run_after=datetime(2026, 11, 26, 8, 25, tzinfo=UTC),
        )

        self.assertEqual(render(self.logical_date, dag_run=manual), "")
        self.assertEqual(render(self.logical_date, dag_run=scheduled), "2026-11-25T08:25:00+00:00")


if __name__ == "__main__":
    unittest.main()
