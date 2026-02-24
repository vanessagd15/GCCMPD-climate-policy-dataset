# GCCMPD Climate Policy Dataset -- Update Project (2026)

## Overview

This project represents a structured update and modernization of the
original GCCMPD Climate Policy Dataset repository.\
The work focuses on improving crawler robustness, restricting extraction
to recent policies (2021 onwards), cleaning large legacy datasets, and
preparing the infrastructure for systematic dataset merging.

This repository is now maintained independently and reflects ongoing
methodological updates.

------------------------------------------------------------------------

## Phase 1 -- Crawler Modernization

All major crawlers were updated to:

-   Extract policies from **2021 onwards** (MIN_YEAR filter)
-   Improve error handling and network robustness
-   Standardize output format (CSV-based pipeline)
-   Improve logging and execution transparency
-   Prepare outputs for structured dataset merging

Updated crawlers include:

-   APAP
-   CDR CCUS
-   CDR NETS
-   CRT
-   ECOLEX Legislation
-   ECOLEX Treaty
-   EEA
-   Gulf sources
-   ICAP ETS
-   MEE PRC
-   USA-related sources

Manual downloads were also incorporated from:

-   climatepolicytracker.org\
-   CCLW (Climate Change Laws of the World)

------------------------------------------------------------------------

## Phase 2 -- ECOLEX Dataset Restructuring

The original ECOLEX file (\~79,000 records) was reprocessed.

Changes:

-   Old dataset chunked into 40 segments
-   Updated dataset (\~4,000 records) chunked into 2 segments
-   Cleaned for duplication and structural consistency
-   Prepared for integration into master dataset

This restructuring significantly improves manageability and downstream
processing efficiency.

------------------------------------------------------------------------

## Phase 3 -- IEA, CP, and CCLW Integration

Work is ongoing in the:

    PolicyDB_IEA_CP_CCLW_Update/

This module:

-   Scrapes and processes IEA policy database entries
-   Standardizes outputs into CSV format
-   Tags data sources explicitly
-   Applies year filtering (2021+)
-   Prepares files for cross-dataset merging

------------------------------------------------------------------------

## Current Objectives

-   Harmonize column schemas across all crawlers
-   Standardize country naming conventions
-   Implement deduplication logic
-   Design unified merged dataset structure
-   Improve reproducibility of pipeline

------------------------------------------------------------------------

## Technical Direction

The updated architecture favors:

-   CSV-based storage over Excel
-   Optional database persistence
-   Source tagging for traceability
-   Modular crawler design
-   Clear separation between raw extraction and merging logic

------------------------------------------------------------------------

## Status

This repository reflects an active modernization effort of legacy
research infrastructure.\
It transitions the dataset from exploratory scripts to a more structured
data engineering pipeline.

------------------------------------------------------------------------

Maintainer: Vanessa Galeano\
Year: 2026
