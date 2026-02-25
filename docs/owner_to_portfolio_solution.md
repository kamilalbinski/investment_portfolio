# Owner → Portfolio model proposal

## Why change
Your current model uses `ACCOUNT_OWNER_ID` as the main slicing dimension in views and UI. This makes it hard to build custom combined portfolios (cross-owner, cross-account, selective-instrument scope) without using special values like `None`.

## Target concept
Use **Portfolio** as the first-class analytics dimension.

A portfolio should be able to:
1. Include whole accounts, or only specific instruments from accounts.
2. Include assets from accounts owned by multiple users.
3. Define a target allocation by `ASSETS.PROFILE` (weights summing to 100).
4. Compare current profile mix vs target (drift monitoring).

## Proposed schema (SQL provided)
Migration SQL file:
- `ddl/migrations/20260225_owner_to_portfolio.sql`

Key objects introduced:
- `PORTFOLIOS`: portfolio master table (`PORTFOLIO_CODE`, `PORTFOLIO_NAME`, currency, status).
- `PORTFOLIO_MEMBERS`: membership rules from portfolio to account/account+asset with optional inclusion %.
- `PORTFOLIO_OWNERS`: optional owner↔portfolio relation (permissions/UI convenience).
- `PORTFOLIO_TARGET_PROFILE`: target weights by profile.

Key portfolio-centric views introduced:
- `PORTFOLIO_TRANSACTIONS_ALL`
- `PORTFOLIO_CURRENT_HOLDINGS_ALL`
- `PORTFOLIO_PROFILE_DRIFT`

## Backward compatibility strategy
The migration is additive and keeps old owner-centric objects untouched.

It also seeds:
- one portfolio per existing owner (`OWNER_<ACCOUNT_OWNER_ID>`), and
- one global portfolio (`ALL`) as a cleaner replacement of ad-hoc `'None'`.

This allows gradual app migration:
1. Add a new portfolio selector in GUI.
2. Switch chart/table queries from owner-based views to portfolio-based views.
3. Keep owner mode for validation until outputs match.
4. Remove owner-centric logic later.

## Application changes you should expect
Minimal code adaptation pattern:
- Replace `owner` parameter with `portfolio_id` in data access and calculation functions.
- Replace filters `WHERE ACCOUNT_OWNER_ID = ...` with `WHERE PORTFOLIO_ID = ...` on new views.
- Replace temporary owners list with `SELECT PORTFOLIO_ID, PORTFOLIO_NAME FROM PORTFOLIOS WHERE IS_ACTIVE = 1`.

## Drift comparison logic
`PORTFOLIO_PROFILE_DRIFT` computes:
- `CURRENT_PCT` by profile from live holdings value,
- `TARGET_PCT` from `PORTFOLIO_TARGET_PROFILE`,
- `DRIFT_PCT = CURRENT_PCT - TARGET_PCT`.

For a portfolio to be considered configured, ensure profile target rows sum exactly to 100.
(Trigger in migration enforces that sum does not exceed 100 to allow incremental editing.)

## Suggested rollout checklist
1. Backup DB.
2. Execute migration SQL file.
3. Create/adjust portfolios and memberships.
4. Populate target profile weights.
5. Validate with `PORTFOLIO_CURRENT_HOLDINGS_ALL` and `PORTFOLIO_PROFILE_DRIFT`.
6. Update GUI selector and visualizations to use portfolio.

