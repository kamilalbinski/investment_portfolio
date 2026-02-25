# Portfolio mode application updates (implemented)

This update starts the app-side migration after the SQL migration was applied.

## What was changed

- Data access now supports both filters:
  - legacy `ACCOUNT_OWNER_ID`
  - new `PORTFOLIO_ID`
- The app auto-detects portfolio views and uses them when available:
  - `PORTFOLIO_TRANSACTIONS_ALL`
  - `PORTFOLIO_CURRENT_HOLDINGS_ALL`
- GUI selector now works in either mode:
  - if portfolio views exist, selected value is treated as `PORTFOLIO_ID`
  - otherwise it remains owner-based
- Calculations and view transformations were made robust to either dimension column (`PORTFOLIO_ID` or `ACCOUNT_OWNER_ID`).

## Current limitation to address next

Historical area chart currently relies on `AGGREGATED_PORTFOLIO_VALUES`.
If your database still has only owner-based aggregation there, portfolio-level time-series will show empty results (instead of incorrect mixed data).

Recommended next DB step:
- add/refresh a portfolio-level historical aggregation source (e.g. `AGGREGATED_PORTFOLIO_VALUES` with `PORTFOLIO_ID`), then chart option #2 will work portfolio-by-portfolio.

