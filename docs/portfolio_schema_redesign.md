# Portfolio-Centric Schema Redesign Proposal

## Goal
Replace the current owner-centric filtering model with a portfolio-centric model so that:

1. One owner can create multiple portfolios.
2. A portfolio can include positions from multiple accounts owned by that owner.
3. A global admin owner (`ALL`) can create portfolios across all holdings.
4. All visualizations/tables can query by `PORTFOLIO_ID` as the primary selector.

## Key Design Decisions

- Keep **transactions** as the immutable source of truth.
- Treat holdings as a **derived view** from transactions.
- Use a **many-to-many** mapping between `PORTFOLIOS` and account/asset positions.
- Separate “who owns accounts” from “who owns portfolios” with clear foreign keys.
- Make `ALL` a first-class owner row, not `None` or hardcoded logic.

## Proposed Core Entities

### 1) OWNERS
Replaces `ACCOUNT_OWNERS` naming at the conceptual level (you can keep old table name if needed).

- `OWNER_ID` (PK)
- `OWNER_CODE` (`ALL`, `USR_001`, ...)
- `OWNER_NAME`
- `IS_ADMIN_SCOPE` (0/1)
- `IS_ACTIVE`

### 2) ACCOUNTS
Each broker account belongs to one owner.

- `ACCOUNT_ID` (PK)
- `ACCOUNT_NAME`
- `OWNER_ID` (FK -> OWNERS)
- `BROKER`
- `IS_ACTIVE`

### 3) PORTFOLIOS
User-defined containers used by UI, reports, and visualizations.

- `PORTFOLIO_ID` (PK)
- `OWNER_ID` (FK -> OWNERS)
- `PORTFOLIO_CODE` (business key)
- `PORTFOLIO_NAME`
- `BASE_CURRENCY`
- `TARGET_PROFILE_ID` (nullable FK -> PORTFOLIO_PROFILES)
- `IS_DEFAULT`
- `IS_ACTIVE`

### 4) PORTFOLIO_MEMBERSHIP
Defines what positions are included in a portfolio.

- `PORTFOLIO_ID` (FK -> PORTFOLIOS)
- `ACCOUNT_ID` (FK -> ACCOUNTS)
- `ASSET_ID` (FK -> ASSETS, nullable)

`ASSET_ID = NULL` means “include all assets from this account”.

This gives flexibility:
- include entire account,
- include specific instruments only,
- combine multiple accounts in one portfolio.

### 5) PORTFOLIO_PROFILES + PORTFOLIO_PROFILE_TARGETS
Optional target allocation model (for “distance from profile”).

- `PORTFOLIO_PROFILES`: profile header (e.g., Conservative 60/30/10)
- `PORTFOLIO_PROFILE_TARGETS`: target percentages by dimension (`CATEGORY`, `SUB_CATEGORY`, `ASSET_ID`)

This enables drift calculation between current and target structure.

## Ownership Rules

1. Non-admin owner portfolios may include only that owner’s accounts.
2. `ALL` owner portfolios may include any account.
3. Enforce via trigger validation in SQLite (or in application service layer + trigger backup).

## View Layer (Portfolio-First)

Create the following portfolio-level views:

- `V_HOLDINGS_ACCOUNT` — holdings by `(ACCOUNT_ID, ASSET_ID)` from transactions.
- `V_PORTFOLIO_HOLDINGS` — holdings expanded by portfolio membership.
- `V_PORTFOLIO_CURRENT_VALUE` — joins prices/fx for market value.
- `V_PORTFOLIO_ALLOCATION` — aggregate by category/subcategory/profile.
- `V_PORTFOLIO_DRIFT` — compare allocation vs target profile.

The GUI selector should use `PORTFOLIO_ID` / `PORTFOLIO_NAME` only.

### Implemented SQL Views (this repository)

The following view scripts were added under `ddl/view/` to support portfolio-level querying:

- `create_V_PORTFOLIO_HOLDINGS.sql`
- `create_V_PORTFOLIO_CURRENT_VALUES.sql`
- `create_V_PORTFOLIO_ALLOCATION.sql`
- `create_V_PORTFOLIO_DRIFT.sql`


## Migration Strategy (Clean Rebuild)

Since you want to abandon the old approach, use a clean migration path:

1. Backup DB.
2. Create new tables (`OWNERS`, `PORTFOLIOS`, `PORTFOLIO_MEMBERSHIP`, profile tables).
3. Insert owners from existing `ACCOUNT_OWNERS`.
4. Insert synthetic `ALL` owner.
5. Remap `ACCOUNTS.ACCOUNT_OWNER_ID -> OWNER_ID`.
6. Create at least one default portfolio per owner.
7. Populate `PORTFOLIO_MEMBERSHIP` with existing holdings/account scope.
8. Replace old owner-based views with portfolio-based views.
9. Update GUI filters and calc methods to accept `portfolio_id`.

## Minimal Application Refactor

1. `get_temporary_owners_list()` -> `get_portfolios_list(owner_scope=None)`.
2. All calculators should accept `portfolio_id` and query `V_PORTFOLIO_*` views.
3. Remove `owner=None` semantics and explicit "All" handling from GUI.
4. Add `ALL` owner portfolios to selector for admin users.

## Why This Works Better

- Clear separation: account ownership vs portfolio composition.
- No special `None` logic for global scopes.
- Naturally supports multiple portfolios per owner.
- Enables target profile drift features without schema hacks.
- Future-ready for rebalancing and strategy-level analytics.

## Suggested Rollout

- Phase 1: DB migration + compatibility views.
- Phase 2: GUI selector switched to portfolios.
- Phase 3: Drift/rebalancing widgets and alerts.

