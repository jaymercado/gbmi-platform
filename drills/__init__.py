"""Scripted failure and recovery drills (blueprint 9.8).

Each drill injects a fault, runs the documented recovery and checks the outcome. A drill works
in a throwaway database and in a lake directory of its own: the database gbmi and the lake of a
stack are not touched (week 2 plan 3.3). Run one with ``make drill DR=<number>``.
"""
