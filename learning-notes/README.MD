# Learning Notes — Azure Databricks Fundamentals

Hands-on notebooks from working through the Databricks fundamentals before building the Formula 1 Lakehouse project. These cover the core building blocks used throughout the main project.

# Contents

- introduction-to-notebook/ — Databricks notebook basics: cells, magic commands (`%md`, `%sql`, `%fs`), widgets, and notebook workflow.
- introduction-to-delta-lake/ — Delta Lake fundamentals: creating Delta tables, `MERGE`/upsert operations, schema enforcement and evolution, time travel.
- introduction-to-unity-catalog/ — Unity Catalog basics: metastores, the three-level namespace (catalog → schema → table), managed vs. external tables, and access control.

# Why this is here
These notebooks were the learning foundation for the design decisions in the main Formula 1 project — the schema enforcement pattern used in Bronze, the `MERGE`-based upserts used in the incremental Silver/Gold layers, and the Unity Catalog governance setup (metastore, catalog, schemas) all build directly on what's practiced here.
