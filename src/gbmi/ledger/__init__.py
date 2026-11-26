"""The operational database: the ledger of expected source files and the schema migrations.

Everything here talks to the PostgreSQL schema ``ops`` (blueprint 7.3). The ledger is the only
place which knows what exists and what is pending (blueprint 5.1, principle 4).
"""
