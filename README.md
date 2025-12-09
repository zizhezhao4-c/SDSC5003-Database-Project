# SDSC5003 Storing and Retrieving Data - Alpha Data Management System

[![MongoDB](https://img.shields.io/badge/MongoDB-6.0-green.svg)](https://www.mongodb.com/)
[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)

A comprehensive database application for managing alpha strategy data, developed as the group project for SDSC5003 Storing and Retrieving Data (2025/26 Semester A) at City University of Hong Kong, Master of Science in Data Science.

## Project Overview

This system provides an end-to-end solution for alpha data lifecycle management, including data acquisition, storage, filtering, synchronization, and visualization. The architecture consists of five integrated subsystems:

### System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Alpha Data Management System                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐      │
│  │  fetch_and_      │    │  alpha_          │    │  alpha_          │      │
│  │  import_alphas   │───▶│  recordsets_     │───▶│  precision_      │      │
│  │                  │    │  update          │    │  sync_engine     │      │
│  │  Batch Import    │    │  Recordsets      │    │  Single Alpha    │      │
│  │  Pipeline        │    │  Pipeline        │    │  Sync            │      │
│  └──────────────────┘    └──────────────────┘    └──────────────────┘      │
│           │                       │                       │                │
│           ▼                       ▼                       ▼                │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │                         MongoDB Database                           │    │
│  │                    (regular_alphas / super_alphas)                 │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│           │                                                                │
│           ▼                                                                │
│  ┌──────────────────┐                            ┌──────────────────┐      │
│  │  alpha_filter    │                            │  alpha_          │      │
│  │                  │                            │  visualizer      │      │
│  │  Quality         │                            │  Chart           │      │
│  │  Filtering       │                            │  Generation      │      │
│  └──────────────────┘                            └──────────────────┘      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Subsystems

### 1. Batch Import Pipeline (`fetch_and_import_alphas/`)

High-performance batch import system for alpha metadata:

- Multi-user Support: Concurrent data fetching from multiple accounts
- Async Fetching: Asynchronous API requests with configurable concurrency
- Multiprocessing Import: 64+ worker processes for parallel MongoDB import
- Deep Merge Algorithm: Intelligent merging preserving historical data
- Multiple Import Modes: incremental, overwrite, rebuild, time_range, smart_merge
- SQLite State Tracking: Resume capability and duplicate prevention

**Workflow:**
```bash
# Step 1: Fetch alpha data from API
python 1_fetch_all_alpha_main_async.py

# Step 2: Package and organize data files
python 2_alpha_files_packaging_and_decomposition.py

# Step 3: Import to MongoDB
python 3_mongo_import_v4.py
```

### 2. Recordsets Update Pipeline (`alpha_recordsets_update/`)

Three-step pipeline for recordset data (PnL, Sharpe, Turnover, etc.):

- Quality-based Selection: Filter alphas by Sharpe, fitness, turnover, etc.
- Smart Fetching: Skip existing data, retry failed requests
- Incremental Merge: Preserve historical records while adding new data
- Multi-database Support: Write to multiple MongoDB instances

**Workflow:**
```bash
# Step 1: Select alphas based on quality criteria
python 1_select_alpha_range.py

# Step 2: Fetch recordsets from API
python 2_fetch_and_store_local.py

# Step 3: Import to MongoDB
python 3_import_to_database.py

# Or run all steps at once
python run_all.py --all
```

### 3. Precision Sync Engine (`alpha_precision_sync_engine/`)

Targeted synchronization for specific alphas:

- Single Alpha Sync: Update individual alphas on demand
- Batch ID Support: Process list of alpha IDs from JSON file
- Smart Merge: Deep merge without data loss
- State Tracking: Integration with main pipeline state manager

**Usage:**
```bash
# Sync specific alphas from command line
python sync_single_alphas.py <alpha_id_1> <alpha_id_2>

# Sync from JSON file
python sync_single_alphas.py --file target_alpha_ids_to_sync.json
```

### 4. Alpha Filter (`alpha_filter/`)

Flexible quality-based filtering system:

- JSON Configuration: Define filter criteria in config file
- Multiple Metrics: Filter by Sharpe, fitness, turnover, PnL, etc.
- Pyramid Matching: Filter by strategy pyramid categories
- Visualization: Generate distribution charts and statistics

**Usage:**
```bash
# Run filter with default config
python universal_alpha_filter.py

# Generate visualizations
python filter_sort_visualize.py
```

### 5. Alpha Visualizer (`alpha_visualizer/`)

Chart generation for alpha performance analysis:

- PnL Curves: Cumulative profit and loss visualization
- Sharpe Ratio: Rolling Sharpe ratio charts
- Turnover Analysis: Daily turnover visualization
- Interactive Mode: Real-time chart exploration

**Usage:**
```bash
# Generate charts for an alpha
python visualize_alpha.py <alpha_id>

# Interactive mode
python interactive.py
```

## Project Structure

```
SDSC5003-Database-Project/
├── README.md                         # This file
├── requirements.txt                  # Python dependencies
│
├── fetch_and_import_alphas/          # Subsystem 1: Batch Import
│   ├── 1_fetch_all_alpha_main_async.py
│   ├── 2_alpha_files_packaging_and_decomposition.py
│   ├── 3_mongo_import_v4.py
│   ├── efficient_state_manager.py
│   ├── async_config.json
│   ├── mongo_config.json
│   └── README_v4.md
│
├── alpha_recordsets_update/          # Subsystem 2: Recordsets
│   ├── 1_select_alpha_range.py
│   ├── 2_fetch_and_store_local.py
│   ├── 3_import_to_database.py
│   ├── run_all.py
│   ├── *_config.json
│   └── README.md
│
├── alpha_precision_sync_engine/      # Subsystem 3: Precision Sync
│   ├── sync_single_alphas.py
│   ├── efficient_state_manager.py
│   └── target_alpha_ids_to_sync.json
│
├── alpha_filter/                     # Subsystem 4: Filtering
│   ├── universal_alpha_filter.py
│   ├── filter_sort_visualize.py
│   └── alpha_filter_config.json
│
├── alpha_visualizer/                 # Subsystem 5: Visualization
│   ├── visualize_alpha.py
│   ├── interactive.py
│   └── README.md
│
└── wq_shared/                        # Shared Modules
    ├── wq_login.py                   # API authentication
    ├── wq_logger.py                  # Logging system
    ├── session_manager.py            # Multi-account sessions
    ├── session_proxy.py              # Request retry & throttling
    ├── timezone_utils.py             # Timezone conversion
    └── README.md
```

## Installation

### Prerequisites

- Python 3.10+
- MongoDB 6.0+
- pip

### Setup

```bash
# Clone the repository
git clone https://github.com/zizhezhao4-c/SDSC5003-Database-Project.git
cd SDSC5003-Database-Project

# Install dependencies
pip install -r requirements.txt

# Verify MongoDB connection
mongosh --eval "db.adminCommand('ping')"
```

## Configuration

### MongoDB Connection

Default configuration connects to `localhost:27017`. Modify `mongo_config.json` in each subsystem to customize:

```json
{
    "db_host": "localhost",
    "db_port": 27017,
    "db_name_regular": "regular_alphas",
    "db_name_super": "super_alphas",
    "workers": 64,
    "batch_size": 1000,
    "IMPORT_MODE": "incremental"
}
```

### Environment Variables

```bash
# Data storage paths (optional)
export WQ_DATA_ROOT=/path/to/data
export WQ_LOGS_ROOT=/path/to/logs

# API credentials
export BRAIN_CREDENTIAL_EMAIL=your_email
export BRAIN_CREDENTIAL_PASSWORD=your_password
```

## Database Schema

```javascript
// Alpha Document Structure
{
  "id": "xxxxxxxx",              // Unique alpha identifier
  "type": "REGULAR",             // REGULAR or SUPER
  "status": "ACTIVE",            // ACTIVE, DECOMMISSIONED, etc.
  "author": "user_id",
  "dateCreated": ISODate("..."),
  "dateModified": ISODate("..."),
  "settings": {
    "region": "USA",
    "universe": "TOP3000",
    "delay": 1,
    "decay": 0,
    "neutralization": "SUBINDUSTRY"
  },
  "is": {                        // In-sample statistics
    "sharpe": 1.85,
    "fitness": 0.92,
    "turnover": 0.45,
    "pnl": 2500000,
    "longCount": 150,
    "shortCount": 150
  },
  "recordsets": {                // Embedded time-series data
    "pnl": { "schema": {...}, "records": [...] },
    "sharpe": { "schema": {...}, "records": [...] },
    "turnover": { "schema": {...}, "records": [...] }
  }
}
```

## Performance

| Metric | Value |
|--------|-------|
| Batch Import Throughput | ~1,000+ files/second |
| Recordset Fetch Rate | ~100 alphas/minute |
| Concurrent Workers | Up to 64 processes |

## License

This project is developed for academic purposes as part of SDSC5003 at City University of Hong Kong.

---
*City University of Hong Kong - Master of Science in Data Science - 2025/26 Semester A*
