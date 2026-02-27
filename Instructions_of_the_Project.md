# Instructions of the Project

This document describes project setup, usage, and configuration. For a step-by-step guide to reproduce the paper's figures and tables, see the repository **README** (`README.md`).

This repository contains the implementation and comprehensive benchmarking framework for three spatiotemporal graph storage models implemented in Neo4j. The project evaluates different storage paradigms for representing spatiotemporal graph data with dynamic attributes and relationships.

## Overview

This project implements and benchmarks three fundamentally different storage paradigms for representing spatiotemporal graph data:

- **Model A (Attribute Metadata Approach - AMA)**: Stores dynamic attributes as JSON history arrays directly on entity nodes. Relationships are stored with spatiotemporal metadata (start/end times, geometry).
- **Model B (State Version Model - SVM)**: Creates state nodes for each state change, connected via `EVOLVED_TO` relationships. Each state node contains a complete snapshot of all attributes at that time point.
- **Model C (Process Atom Model - PAM)**: Uses independent process nodes to represent attribute and relationship evolution separately. Process nodes are linked via `NEXT_STATE` relationships, forming independent evolution chains.

### Benchmark Dimensions

The framework evaluates these models across multiple dimensions:

- **Ingestion Efficiency**: Time to load atomic facts into the database
- **Storage Scalability**: Database size under different data characteristics (scale, volatility, density)
- **Query Performance**: Latency for four query types:
  - **Snapshot Queries**: Retrieve entity state at a specific time point
  - **History Queries**: Retrieve complete attribute/relationship history
  - **Evolution Queries**: Track relationship changes over time
  - **Spatiotemporal Queries**: Combine spatial and temporal constraints

## Project Structure

```
.
├── main_etl_v2_REVISED.py          # Core ETL and query execution module
├── run_full_experiments.py         # Main experiment execution script
├── experiment_config.json          # Experiment configuration (Neo4j settings, experiment parameters)
├── queries.json                    # Synthetic query workload definitions
│
├── case_study/
│   ├── infection/                  # Epidemic dynamics case study (Primary School SocioPatterns)
│   │   ├── data/
│   │   │   ├── etl.py              # ETL script to convert contact data to atomic facts
│   │   │   ├── infection_facts.csv # Generated atomic facts
│   │   │   ├── metadata_primaryschool.txt
│   │   │   └── primaryschool.csv
│   │   ├── scripts/
│   │   │   ├── import_infection_data.py  # Import data to Neo4j
│   │   │   ├── run_experiment.py         # Run full case study experiments
│   │   │   └── create_figure5_infection_data.py  # Figure 5: Infection data visualization
│   │   ├── queries/
│   │   │   └── infection_queries.json     # Case study query definitions
│   │   └── results/
│   │       └── experiment_results.json
│   │
│   └── cadastral/                  # Cadastral evolution case study (NYC MapPLUTO)
│       ├── data/
│       │   ├── etl.py              # ETL script to convert cadastral data to atomic facts
│       │   ├── final_manhattan_facts.csv  # Full processed atomic facts (all years)
│       │   ├── MapPLUTO_Data/      # NYC MapPLUTO shapefile data (2011-2013 only, due to repository size limits)
│       │   └── nybb_20d/           # Manhattan boundary data
│       ├── scripts/
│       │   ├── import_cadastral_data.py
│       │   ├── run_experiment.py
│       │   └── create_figure6_cadastral_data.py  # Figure 6: Cadastral data visualization
│       ├── queries/
│       │   └── cadastral_queries.json
│       └── results/
│           └── experiment_results.json
│
└── scripts/                        # Figure generation scripts
    ├── create_figure2_ingestion_storage.py    # Figure 2: Ingestion efficiency and storage scalability
    ├── create_figure3_temporal_topological.py # Figure 3: Temporal and topological query performance
    ├── create_figure4_spatiotemporal_wall.py  # Figure 4: Spatiotemporal query performance
    └── figures/                    # Generated figures (PDF and PNG)
```

## Requirements

- **Python**: 3.12 or higher
- **Neo4j**: 5.26.11 (with APOC plugin enabled)
- **Python packages**:
  ```bash
  pip install neo4j pandas numpy matplotlib seaborn scipy geopandas shapely pyogrio
  ```

### Optional Dependencies

- `scienceplots` (for enhanced figure styling)
- `geopandas` and `pyogrio` (for cadastral case study shapefile processing)

## Installation

1. **Install Neo4j Desktop** and create a database instance
2. **Enable APOC plugin** in Neo4j Desktop:
   - Open Neo4j Desktop
   - Select your database
   - Go to "Plugins" tab
   - Install and enable "APOC Core"
3. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
   Or manually:
   ```bash
   pip install neo4j pandas numpy matplotlib seaborn scipy geopandas shapely pyogrio
   ```

## Configuration

Edit `experiment_config.json` to configure:

### Neo4j Connection Settings
```json
{
  "neo4j": {
    "uri": "bolt://localhost:7687",
    "user": "neo4j",
    "password": "your_password",
    "database": "bench-db",
    "isolated_db": true,
    "db_parent": "/path/to/neo4j/databases"
  }
}
```

### Experiment Parameters
- **G1_Scale**: Entity count (100 to 1M)
- **G2_Vol_Attr**: Attribute volatility (0.05 to 0.95)
- **G3_Vol_Rel**: Relation volatility (0.01 to 0.4)
- **G4_Density_Props**: Number of dynamic attributes (1 to 30)

### Benchmark Settings
- `samples_per_query`: Number of parameter samples per query type
- `query_warmups`: Number of warmup runs before measurement
- `query_runs`: Number of measurement runs
- `batch_size`: Batch size for ETL operations

## Usage

### Synthetic Experiments

Run comprehensive synthetic experiments to evaluate model performance under controlled conditions:

```bash
python run_full_experiments.py
```

#### Command-Line Options

```bash
python run_full_experiments.py \
  --experiment G1 \          # Experiment group: G1, G2, G3, G4, or "all"
  --skip-large \             # Skip large-scale experiments (10M entities)
  --models A B C             # Models to run: A, B, C, or combinations
```

#### Experiment Groups

- **G1 (Scale Impact)**: Evaluates scalability by varying entity count
  - Values: 100, 1K, 10K, 100K, 500K, 1M entities
  - Measures: Ingestion time, storage size, query latency

- **G2 (Attribute Volatility Impact)**: Evaluates performance under different attribute change frequencies
  - Values: 5%, 20%, 40%, 60%, 80%, 95% volatility
  - Fixed scale: 100K entities

- **G3 (Relation Volatility Impact)**: Evaluates performance under different relationship change frequencies
  - Values: 1%, 5%, 10%, 20%, 30%, 40% volatility
  - Fixed scale: 100K entities

- **G4 (Data Density Impact)**: Evaluates performance with varying numbers of dynamic attributes
  - Values: 1, 5, 10, 15, 20, 30 dynamic attributes
  - Fixed scale: 100K entities

### Case Study: Epidemic Dynamics (Infection)

This case study uses real-world contact network data from Primary School SocioPatterns to evaluate model performance on epidemic dynamics queries.

#### Step 1: Generate Atomic Facts

```bash
cd case_study/infection/data
python etl.py
```

This processes the contact network data and generates `infection_facts.csv` containing atomic facts.

#### Step 2: Import Data to Neo4j

```bash
cd case_study/infection/scripts

# Import for each model
python import_infection_data.py --model A --input ../data/infection_facts.csv
python import_infection_data.py --model B --input ../data/infection_facts.csv
python import_infection_data.py --model C --input ../data/infection_facts.csv
```

#### Step 3: Run Experiments

```bash
python run_experiment.py \
  --input ../data/infection_facts.csv \
  --models A B C \
  --queries ../queries/infection_queries.json \
  --output ../results/experiment_results.json
```

#### Step 4: Visualize Results

```bash
# Figure 5: Data visualization
python create_figure5_infection_data.py \
  --facts ../data/infection_facts.csv \
  --metadata ../data/metadata_primaryschool.txt \
  --output ../../figures/Figure5_Infection_Data
```

### Case Study: Cadastral Evolution (Cadastral)

This case study uses NYC MapPLUTO cadastral data to evaluate model performance on property and topology evolution queries.

**Note on Data Availability**: Due to repository size limitations, only shapefile data for years 2011, 2012, and 2013 are included in the repository. The `final_manhattan_facts.csv` file contains the full processed atomic facts for all available years. Running `etl.py` will regenerate and overwrite this file.

#### Step 1: Generate Atomic Facts

```bash
cd case_study/cadastral/data
python etl.py
```

This processes NYC MapPLUTO shapefiles and generates `final_manhattan_facts.csv` containing atomic facts for Manhattan parcels. **Note**: Running this script will overwrite the existing `final_manhattan_facts.csv` file.

#### Step 2: Import Data to Neo4j

```bash
cd case_study/cadastral/scripts

# Import for each model
python import_cadastral_data.py --model A --input ../data/final_manhattan_facts.csv
python import_cadastral_data.py --model B --input ../data/final_manhattan_facts.csv
python import_cadastral_data.py --model C --input ../data/final_manhattan_facts.csv
```

#### Step 3: Run Experiments

```bash
python run_experiment.py \
  --input ../data/final_manhattan_facts.csv \
  --models A B C \
  --queries queries/cadastral_queries.json \
  --output results/experiment_results.json
```

#### Step 4: Generate Figures

```bash
# Figure 6: Data visualization
python create_figure6_cadastral_data.py \
  --data ../data/final_manhattan_facts.csv \
  --shp ../data/MapPLUTO_Data/2013/MNMapPLUTO.shp \
  --boundary ../data \
  --output ../../figures
```

## Output Format

Experiment results are saved as JSON files with the following structure:

```json
{
  "model": "A",
  "config": "G1_Scale_100K",
  "ingest_time_s": 45.2,
  "store_mb": 125.8,
  "n_nodes": 150000,
  "n_edges": 300000,
  "query_results": {
    "Latency_Snapshot": {
      "latency_ms": {
        "values": [12.5, 13.1, 12.8, ...],
        "min": 12.3,
        "max": 15.2,
        "mean": 13.0,
        "median": 12.9,
        "std": 0.8
      }
    },
    ...
  }
}
```

### Output Locations

- **Synthetic experiments**: `experiment_results.json`
- **Infection case study**: `case_study/infection/results/experiment_results.json`
- **Cadastral case study**: `case_study/cadastral/results/experiment_results.json`

## Visualization

Generate publication-quality figures from experiment results:

### Synthetic Experiment Figures

```bash
# Figure 2: Ingestion Efficiency and Storage Scalability
python scripts/create_figure2_ingestion_storage.py

# Figure 3: Temporal and Topological Query Performance
python scripts/create_figure3_temporal_topological.py

# Figure 4: Spatiotemporal Query Performance
python scripts/create_figure4_spatiotemporal_wall.py
```

### Case Study Figures

```bash
# Figure 5: Infection case study data visualization
python case_study/infection/scripts/create_figure5_infection_data.py

# Figure 6: Cadastral case study data visualization
python case_study/cadastral/scripts/create_figure6_cadastral_data.py
```

All figures are generated in both PDF (vector) and PNG (raster) formats with 600 DPI resolution, suitable for publication.

## Key Components

### ETLRunner (`main_etl_v2_REVISED.py`)

The core ETL class provides three model implementations:

- **`run_etl_model_a()`**: Implements Model A ETL
  - Aggregates attribute histories into JSON arrays
  - Stores relationships with temporal metadata
  
- **`run_etl_model_b()`**: Implements Model B ETL
  - Creates state nodes for each state change
  - Connects states via `EVOLVED_TO` relationships
  - Attaches relationships to state nodes
  
- **`run_etl_model_c()`**: Implements Model C ETL
  - Creates independent process nodes for each attribute/relationship
  - Links process nodes via `NEXT_STATE` relationships
  - Maintains separate evolution chains

- **`clear_database_and_setup_indices()`**: Database setup with model-specific indexes

### DataSynthesizer

Generates synthetic atomic facts for benchmarking:

- Configurable parameters (entities, attributes, relationships, time span)
- Model-specific sorting support (for optimal ETL performance)
- Deterministic generation for reproducibility
- Supports all experiment group configurations

### BenchmarkSuite

Orchestrates end-to-end benchmarking:

- Data generation with specified parameters
- ETL execution for each model
- Query workload execution
- Storage measurement (database size)
- Result aggregation and statistics

### QueryWorkload

Manages query execution:

- Query warmup runs (to avoid cold start effects)
- Multiple execution runs for statistical significance
- Parameter sampling for representative queries
- Latency measurement and statistics collection

## Data Model

### Atomic Fact Format

All data is represented as atomic facts with the following structure:

```python
Fact(
    entity_id: str,      # Entity identifier
    attribute: str,      # Attribute name or relationship type
    value: Any,          # Attribute value or target entity ID
    start: int,          # Start time (timestamp)
    end: Optional[int],  # End time (None for open intervals)
    type: int            # 0 for attributes, 1 for relationships
)
```

### Model A Schema

```
(:Entity {id, attr1_history: [...], attr2_history: [...]})
(:Entity)-[:REL_TYPE {start, end}]->(:Entity)
```

### Model B Schema

```
(:Entity_State {id, sid, start, end, attr1, attr2, ...})
(:Entity_State)-[:EVOLVED_TO]->(:Entity_State)
(:Entity_State)-[:REL_TYPE {start, end}]->(:Entity_State)
```

### Model C Schema

```
(:Entity {id})
(:Entity)-[:HAS_PROCESS {attr}]->(:ProcessNode {value, start, end})
(:ProcessNode)-[:NEXT_STATE]->(:ProcessNode)
(:ProcessNode)-[:REL_TYPE]->(:Entity)
```

## Performance Optimizations

The implementation includes several performance optimizations:

- **Streaming Processing**: Processes data in batches to handle large datasets
- **External Sorting**: Uses disk-based sorting for Model B and C (which require temporal ordering)
- **Batch Loading**: Loads data in configurable batch sizes for optimal Neo4j performance
- **Index Optimization**: Creates model-specific indexes for query performance
- **Memory Management**: Uses spooling and buffering to control memory usage

## Troubleshooting

### Common Issues

1. **APOC not available**: Ensure APOC plugin is installed and enabled in Neo4j
2. **Database connection errors**: Check Neo4j URI, credentials, and database name in `experiment_config.json`
3. **Memory errors**: Reduce batch size in configuration or use external sorting
4. **Shapefile errors** (cadastral case study): Ensure `geopandas` and `pyogrio` are installed

### Performance Tips

- Use isolated databases for accurate storage measurements
- Adjust batch sizes based on available memory
- Use external sorting for large datasets (Model B and C)
- Enable Neo4j query logging for debugging

## Acknowledgments

- Primary School SocioPatterns dataset for the infection case study
- NYC Department of City Planning for MapPLUTO data used in the cadastral case study
