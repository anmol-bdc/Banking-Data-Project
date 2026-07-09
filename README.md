# Banking Data Engineering Simulation Platform

A production-inspired banking data engineering simulation built using Python, PySpark, Delta Lake, and Power BI.

The project demonstrates how banking data can move through a modern Medallion Architecture pipeline, from raw data generation to interactive business intelligence dashboards.

---
# Banking Data Engineering Simulation Platform

A production-inspired banking data engineering simulation built using Python, PySpark, Delta Lake, and Power BI.

---

## Project Team

### Interns
- Debshata Choudhury
- Anmol Garg

### Project Buddy
- Karan P. Singh

---

## Project Overview

The project demonstrates how banking data can move through a modern Medallion Architecture pipeline, from raw data generation to interactive business intelligence dashboards.

---

# Project Objective

The objective of this project was to:

- Simulate real-world banking datasets.
- Build a modular data engineering pipeline.
- Implement Medallion Architecture principles.
- Apply validation and data quality checks.
- Generate scenario-based stress testing datasets.
- Create interactive Power BI dashboards.
- Learn modern Big Data engineering concepts through practical implementation.

---

# Problem Statement

Banking datasets originate from multiple business domains and often require:

- Data standardization
- Data quality validation
- Transformation and enrichment
- Business rule implementation
- Analytics-ready outputs

Without a structured architecture, data pipelines become difficult to maintain, scale, and govern.

This project addresses those challenges by implementing a layered Medallion Architecture with validation, transformation, and analytics capabilities.

---

# Project Architecture

## End-to-End Flow

```text
Banking Data Sources
        │
        ▼
Synthetic Data Generation
        │
        ▼
Validation Framework
        │
        ▼
PySpark Ingestion
        │
        ▼
Bronze Layer
        │
        ▼
Transformation Engine
        │
        ▼
Silver Layer
        │
        ▼
Business Logic & Scenario Simulation
        │
        ▼
Gold Layer
        │
        ▼
Aggregates Layer
        │
        ▼
Power BI Dashboard
```

---

# Banking Data Domains

The simulation covers multiple banking domains:

- Loan Data
- Deposit Data
- Transaction Data
- Liquidity Data
- Cash Data
- Intraday Position Data
- Scenario Data

These datasets are connected and processed to generate business insights and scenario-based stress testing outputs.

---

# Technology Stack

## Programming

- Python
- PySpark

## Storage

- Delta Lake
- Parquet

## Analytics

- Power BI

## Data Engineering Concepts

- Medallion Architecture
- Data Validation Framework
- Scenario-Based Stress Testing
- Modular Pipeline Design

---

# Project Structure

```text
project/
│
├── data_generation/
│
├── ingestion/
│
├── transformation/
│
├── validation/
│
├── orchestration/
│
├── common/
│
├── configs/
│
├── analytics/
│
├── power_bi/
│
└── documentation/
```

Each layer has a clearly defined responsibility.

---

# Data Generation Layer

The generator creates synthetic banking datasets for:

- Loans
- Deposits
- Liquidity
- Transactions
- Intraday Positions

The generated data mimics realistic banking operations while allowing safe experimentation.

---

# Validation Framework

Before data enters the transformation pipeline, a validation framework is executed.

Checks include:

## Schema Validation

Ensures dataset structure matches expected definitions.

## Null Checks

Detects missing values in important business columns.

## Duplicate Checks

Identifies duplicate records.

## Business Rule Validation

Validates domain-specific banking rules.

---

# Medallion Architecture

A major project milestone was implementing Medallion Architecture after understanding the importance of proper layer separation.

---

## Bronze Layer

Purpose:

- Store raw ingested datasets.

Characteristics:

- Raw data preservation.
- Delta Lake storage.
- Initial landing zone.

Partition Strategy:

```text
Region
Date
```

---

## Silver Layer

Purpose:

- Store validated and standardized datasets.

Operations:

- Cleaning
- Standardization
- Validation
- Enrichment

Partition Strategy:

```text
Region
Date
```

---

## Gold Layer

Purpose:

- Business-ready datasets.

Operations:

- Dataset joins
- Business calculations
- Derived columns
- Scenario calculations

Partition Strategy:

```text
Region
Scenario
```

---

## Aggregate Layer

Purpose:

Generate reporting-ready datasets.

Contains:

- KPI Tables
- Scenario Metrics
- Trend Metrics
- Regional Analytics

---

# Key Transformations

The transformation layer performs:

## Dataset Joins

Combines banking domains.

## Currency Conversion

Uses exchange rates to standardize monetary values into common reporting currency.

## Derived Columns

Generates business metrics.

## Business Calculations

Creates analytics-ready measures.

---

# Scenario Simulation Engine

The project includes stress-testing simulations.

Supported scenarios:

- Base Scenario
- 1 Notch Downgrade
- 2 Notch Downgrade
- Severe Stress Scenario

This enables risk analysis across multiple banking conditions.

---

# Power BI Dashboard

The dashboard provides business visibility through:

## KPI Cards

- Total Exposure
- Total Liquidity
- Transaction Volume
- Net Intraday Position
- Stress Impact %

## Interactive Filters

- Scenario Selection
- Region Selection

## Analytics

- Regional Analysis
- Trend Analysis
- Scenario Comparison
- Stress Testing Insights
- Risk Visualization

---

# Challenges Faced

## Environment Setup

Initial project setup required learning multiple new technologies simultaneously.

## Modularization

The pipeline was intentionally split into separate generation, ingestion, transformation, validation, and orchestration layers.

Designing and maintaining clear separation increased development effort but improved maintainability.

## Layer Ownership Confusion

Initially, ingestion and transformation responsibilities overlapped.

Some ingestion functionality was accidentally implemented in transformation modules and vice versa.

This became the most important learning during the project.

The solution was implementing Medallion Architecture principles and assigning clear ownership to each layer.

---

# Key Learnings

## Big Data

- Data Engineering Fundamentals
- Distributed Data Processing Concepts

## PySpark

- Transformations
- Aggregations
- Joins
- Data Processing

## Delta Lake

- Transactional Data Lakes
- Layered Architecture

## Architecture Design

- Medallion Architecture
- Data Lake Concepts
- Pipeline Design

## Analytics

- KPI Design
- Dashboard Development
- Business Intelligence

## Professional Skills

- Problem Solving
- Documentation
- Communication
- Project Planning

---

# Project Outcomes

Successfully delivered:

- Modular Data Engineering Pipeline
- Validation Framework
- Banking Data Simulation
- Scenario Simulation Engine
- Medallion Architecture Implementation
- Aggregate Analytics Layer
- Power BI Dashboard

---

# Impact

This project demonstrates:

- Proper layer separation in data engineering.
- Importance of validation and data quality.
- End-to-end data flow design.
- Scenario-based analytical reporting.
- Scalable architectural thinking.

The project serves as a reusable learning framework for understanding modern banking data engineering systems.

---

# Future Roadmap

## Phase 1

- Enhanced business validations
- Additional KPI calculations
- More reporting metrics

## Phase 2

- Automated job scheduling
- Monitoring and alerting
- Expanded scenario testing

## Phase 3

- Cloud storage integration
- Near real-time data ingestion
- Advanced governance framework
- AI-powered analytics

## Long-Term Vision

Transform the simulation into a production-inspired banking analytics platform that supports:

- Real-time ingestion
- Monitoring
- Governance
- AI-assisted decision making
- Enterprise-scale reporting

---

# Core Takeaway

Problem:
Modern banking data pipelines are complex and require structured architecture.

Action:
Built a modular banking data engineering platform using Medallion Architecture.

Insight:
Clear layer separation improves maintainability, scalability, and governance.

Impact:
Created a reusable simulation that demonstrates modern data engineering and analytics concepts from source generation to business intelligence.
