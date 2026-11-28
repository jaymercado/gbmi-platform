"""The SEC EDGAR source (blueprint 4.3; week 3 plan).

Every request to the SEC goes through ``gbmi.sources.sec.client.SecClient``: the client cannot
exceed the ceiling of 8 requests in any second, cannot hide its identity and cannot ignore a
block by the SEC.
"""
