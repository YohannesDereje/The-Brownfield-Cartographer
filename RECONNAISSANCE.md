# Phase 0: Manual Reconnaissance - dbt `jaffle_shop`

## Repository Context

- **Target Repository:** `dbt-labs/jaffle_shop`
- **Onboarding Persona:** Senior Forward Deployed Engineer (Day One)

## 1. The Five FDE Day-One Answers

### I. Primary Data Ingestion Path

**Entry Points**

Raw data enters the system primarily through dbt Seeds.

**Evidence**

- Configuration in `dbt_project.yml` (lines 15 and 27-31) points to the `seeds/` directory.
- Physical entry files include:
	- `seeds/raw_customers.csv`
	- `seeds/raw_orders.csv`
	- `seeds/raw_payments.csv`
- These are formally declared as sources in `models/staging/__sources.yml`, establishing the contract between raw CSVs and the SQL transformation layer.

### II. Critical Output Datasets (System of Record)

**The "Money" Models**

The final business value is concentrated in the `marts/` directory, materialized as permanent tables.

**Top 3 Outputs**

- `models/marts/orders.sql`: Definitive source for order-level facts and derived status flags.
- `models/marts/customers.sql`: "Customer 360" view with critical Lifetime Value (LTV) and behavioral rollups.
- `models/marts/order_items.sql`: Granular line-item enrichment used for unit-level analysis.

### III. Blast Radius Analysis: `stg_orders.sql`

**Direct Breakage**

If `stg_orders.sql` is deleted, `models/marts/orders.sql` and `models/marts/order_items.sql` fail immediately due to direct `ref()` dependencies.

**Transitive (Indirect) Breakage**

`models/marts/customers.sql` fails because it depends on `models/marts/orders.sql`.

**Impact Chain**

`stg_orders -> orders -> customers`

**Test Failures**

Multiple relationship tests in `models/staging/stg_order_items.yml` would also fail, halting the CI/CD pipeline.

### IV. Logic Concentration

**Architecture Pattern**

- `models/staging/`: Canonicalization only (column renaming and type casting). Low complexity.
- `models/marts/`: High complexity and business semantics (aggregations, joins, and window functions such as `row_number()`).
- `macros/`: Utility-level helpers (for example, `cents_to_dollars.sql`). Functional logic only, not business rules.

**Conclusion**

The Marts layer is the architectural "brain."

### V. Change Velocity (Last 90 Days)

**Top High-Activity Files**

- `.github/workflows/codeowners-check.yml` (Infrastructure/Governance)
- `packages.yml` (Dependency management)
- `.pre-commit-config.yaml` (Developer Experience/Linting)

**Insight**

Business logic is currently stable; recent changes are concentrated in governance and CI/CD stability.

## 2. Friction Analysis (The Brownfield Reality)

During this manual investigation, several cognitive bottlenecks were identified that justify the need for **The Brownfield Cartographer**:

- **Dependency Opacity:** While `ref()` tags expose direct links, visualizing the transitive blast radius (for example, how a staging change impacts the Customer Mart) requires deep mental mapping across multiple files.
- **Configuration Fragmentation:** Ingestion logic is split across `dbt_project.yml`, `seeds/*.csv`, and `models/staging/__sources.yml`. An FDE needs a unified source-to-sink view.
- **Semantic Drift:** No single artifact explains why `order_items` is joined in its current form. Engineers must read SQL and reverse-engineer intent.
