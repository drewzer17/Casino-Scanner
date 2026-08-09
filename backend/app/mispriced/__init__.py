"""Standalone deep-ITM covered-call mispricing scanner.

Fully separate from the main scan pipeline (backend/app/scanner/). Stores
nothing — every sweep is a throwaway read: fetch live from Tradier sandbox,
compute, hold the latest result in memory, discard on the next sweep.
No new tables, no migrations, no writes to option_snapshots or anywhere else.
"""
