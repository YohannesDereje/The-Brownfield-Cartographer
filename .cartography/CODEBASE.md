# Codebase Cartography

## Day-One Brief

**Primary Business Mission**: Enable reliable understanding and operation of a brownfield codebase.

**Critical Path Clusters**:
- Core Operations
- Data Governance
- Platform Reliability

**Top Technical Risks**:
- Drift in destructive data modules can mislead maintainers.
- Cross-domain outliers increase regression risk and ownership ambiguity.
- Incomplete architecture boundaries slow safe onboarding and change planning.

**Mental Model**: The system is a map-making pipeline that turns scattered code signals into operational guidance.

## Domain Heatmap

### Core Operations

Supports core user and storage workflows.

- auth.py
- storage.py

### Data Governance

Manages data lifecycle and retention-sensitive operations.

- data_utils.py

### Integration Surface

Handles cross-domain integration and orchestration concerns.

- No modules assigned

### Platform Reliability

Maintains runtime consistency and resilience controls.

- No modules assigned

## Drift Audit

- data_utils.py: The docstring claims math helpers, but the implementation performs destructive database operations.
  Purpose: Deletes user database records for retention and compliance workflows.
