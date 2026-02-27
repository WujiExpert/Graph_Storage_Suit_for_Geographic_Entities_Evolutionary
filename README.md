# Guide to Reproducing Paper Results

This document provides a step-by-step guide to reproduce all figures and tables reported in the paper. For project setup, environment, and dependency installation, see **Instructions of the Project** (`Instructions_of_the_Project.md`).

## Prerequisites

Before you begin, please ensure you have installed all necessary Python dependencies as specified in **Instructions of the Project** (`Instructions_of_the_Project.md`).

**Running experiments** (optional): To regenerate all result files from scratch, you need a running **Neo4j** instance for synthetic experiments (Figures 2–4) and for case study experiments (Tables 2–3). Configure connection and database paths in `experiment_config.json` if needed.

**Generating figures and tables only**: If Neo4j is not installed or you do not wish to run experiments, you can still produce all figures and tables using the **pre-run result files** included in the project (e.g. `experiment_results.json` at project root for Figures 2–4; `case_study/infection/results/experiment_results.json` and `case_study/cadastral/results/experiment_results.json` for Tables 2–3). In that case, skip the "run experiment" steps and run only the figure/table generation steps; the scripts will read the existing result files. **Note:** Running the full experiment pipeline will **overwrite** these default result files. If you want to keep the provided results, do not run `run_full_experiments.py` or the case study `run_experiment.py` scripts.

All file paths referenced in this document (e.g., `data/...`, `results/...`, `scripts/...`) are **relative to the project root directory**. Unless otherwise noted, run commands from the **project root**.

---

## Figure 2: Ingestion Efficiency and Storage Scalability

### Source Data

  * `experiment_results.json` (at project root), produced by the synthetic experiment pipeline (see procedure step 1).  
  * **Without running experiments:** If this file is already present in the project (pre-run results), you can skip step 1 and run only step 2 to generate the figure.

### Tools Used

  * **Python** (with `matplotlib`, `seaborn`, `pandas`, `numpy`, `scipy`). Optional: `scienceplots` for style.

### Procedure

1.  **Generate synthetic experiment results**:
      * From the project root, run the full synthetic experiment script so that `experiment_results.json` is written to the project root. The script reads configuration from `experiment_config.json` and writes results to the path given by the `paths.results_output_file` key (default: `experiment_results.json`).
      * Command (from project root):
        ```bash
        python run_full_experiments.py
        ```
      * This runs all configured experiment groups (G1_Scale, G2_Vol_Attr, G3_Vol_Rel, G4_Density_Props) for models A (AMA), B (SVM), and C (PAM), and saves aggregated results to `experiment_results.json`.
2.  **Generate Figure 2**:
      * The figure script loads `../experiment_results.json` and writes outputs into a `figures` directory relative to the current working directory. To resolve paths correctly, run the script from the `scripts` directory:
        ```bash
        cd scripts
        python create_figure2_ingestion_storage.py
        ```
      * Outputs are saved under `scripts/figures/`:
        * `Figure2_Ingestion_Storage.pdf`
        * `Figure2_Ingestion_Storage.png`
      * The relevant code block in `create_figure2_ingestion_storage.py` is:
        ```python
        if __name__ == '__main__':
            data = load_all_experiment_data('../experiment_results.json')
            create_figure2_ingestion_storage(data, Path('figures'))
        ```
      * The figure layout: 4 rows × 2 columns; each row is an experiment group. Left column: build time and storage usage; right column: node and edge counts.

---

## Figure 3: Temporal and Topological Query Performance

### Source Data

  * `experiment_results.json` (at project root), same as for Figure 2.  
  * **Without running experiments:** Use the existing `experiment_results.json` in the project and run only the figure script (step 2).

### Tools Used

  * **Python** (with `matplotlib`, `seaborn`, `pandas`, `numpy`, `scipy`). Optional: `scienceplots`.

### Procedure

1.  **Generate synthetic experiment results** (if not already done):
      * From project root: `python run_full_experiments.py`, so that `experiment_results.json` exists at the project root.
2.  **Generate Figure 3 (main and supplement)**:
      * Run from the `scripts` directory so that `../experiment_results.json` points to the project root:
        ```bash
        cd scripts
        python create_figure3_temporal_topological.py
        ```
      * Outputs under `scripts/figures/`:
        * Main figure: `Figure3_Temporal_Topological_Main.pdf`, `Figure3_Temporal_Topological_Main.png`
        * Supplement figures: one per experiment group (G2, G3, G4), e.g. `Figure3_Temporal_Topological_G2_Vol_Attr.pdf` (and .png).
      * The relevant code block in `create_figure3_temporal_topological.py` is:
        ```python
        if __name__ == '__main__':
            file_path = '../experiment_results.json'
            output_dir = Path('figures')
            # ... create main figure and supplement figures per group
        ```
      * The main figure shows G1_Scale: Snapshot, History, and Evolution query latency; supplement figures show the same query types for G2, G3, and G4.

---

## Figure 4: Bottleneck Analysis (The Spatio-Temporal Wall)

### Source Data

  * `experiment_results.json` (at project root), same as for Figures 2 and 3.  
  * **Without running experiments:** Use the existing result file and run only the figure script (step 2).

### Tools Used

  * **Python** (with `matplotlib`, `seaborn`, `pandas`, `numpy`, `scipy`). Optional: `scienceplots`.

### Procedure

1.  **Generate synthetic experiment results** (if not already done):
      * From project root: `python run_full_experiments.py`.
2.  **Generate Figure 4**:
      * Run from the `scripts` directory:
        ```bash
        cd scripts
        python create_figure4_spatiotemporal_wall.py
        ```
      * Outputs under `scripts/figures/`:
        * `Figure4_SpatioTemporal_Wall.pdf`
        * `Figure4_SpatioTemporal_Wall.png`
      * The relevant code block in `create_figure4_spatiotemporal_wall.py` is:
        ```python
        if __name__ == '__main__':
            data = load_all_experiment_data('../experiment_results.json')
            # ... create 2×2 layout of spatiotemporal query performance per experiment group
            plt.savefig(output_dir / 'Figure4_SpatioTemporal_Wall.pdf', ...)
        ```
      * The figure shows spatiotemporal query performance for all four experiment groups (2×2 layout), including performance ratio markers.

---

## Figure 5: Infection Case Study Data Visualization

### Source Data

  * `case_study/infection/data/infection_facts.csv` (fact table produced by the infection case study ETL).
  * `case_study/infection/data/metadata_primaryschool.txt` (metadata: id, class, gender).  
  * **Using existing data:** If these files are already in the project, skip the ETL step and run only the figure script (step 2).

### Tools Used

  * **Python** (with `pandas`, `numpy`, `matplotlib`, `networkx`).

### Procedure

1.  **Prepare infection case study data** (if not already done):
      * Run the infection ETL so that `case_study/infection/data/infection_facts.csv` exists. This typically involves running the infection data pipeline (e.g., from raw Primary School contact data) and exporting facts in the format expected by the import/visualization scripts. See **Instructions of the Project** and `case_study/infection/data/etl.py` for details.
2.  **Generate Figure 5**:
      * From the project root, run:
        ```bash
        python case_study/infection/scripts/create_figure5_infection_data.py
        ```
      * The script resolves paths relative to its location: it expects `infection_facts.csv` and `metadata_primaryschool.txt` in `case_study/infection/data/`, and writes outputs to the project-level `figures` directory (one level above `case_study`).
      * Outputs:
        * `figures/Figure5_Infection_Data.pdf`
        * `figures/Figure5_Infection_Data.png`
      * The relevant code block in `create_figure5_infection_data.py` is:
        ```python
        if __name__ == '__main__':
            script_dir = Path(__file__).parent
            data_dir = script_dir.parent / 'data'
            facts_path = data_dir / 'infection_facts.csv'
            metadata_path = data_dir / 'metadata_primaryschool.txt'
            output_path = output_dir / 'Figure5_Infection_Data'  # output_dir = project figures/
            create_figure(facts_path, metadata_path, output_path)
        ```
      * The figure contains four subplots: (A) Contact network, (B) Contact activity over time, (C) SEIR epidemic progression, (D) Contact frequency distribution.

---

## Figure 6: Cadastral Case Study Data Visualization

### Source Data

  * `case_study/cadastral/data/final_manhattan_facts.csv` (cadastral fact table; default data path).
  * Optional: `case_study/cadastral/data/MapPLUTO_Data/2013/MapPLUTO.shp` (for Panel A map).
  * Optional: boundary data under `case_study/cadastral/data` (e.g., `nybb_20d` folder) if used.  
  * **Using existing data:** If the facts CSV (and optional shapefile) are already in the project, skip the ETL step and run only the figure script (step 2).

### Tools Used

  * **Python** (with `matplotlib`, `pandas`, `numpy`, `geopandas`, `shapely`, `scipy`). For shapefile reading: `pyogrio` is used when available.

### Procedure

1.  **Prepare cadastral data** (if not already done):
      * Run the cadastral ETL (`case_study/cadastral/data/etl.py`) to produce `final_manhattan_facts.csv`. The repository may only include a subset of years (e.g., 2011–2013); see **Instructions of the Project** for data scope. Running the ETL will overwrite or regenerate the facts file as configured.
2.  **Generate Figure 6**:
      * From the project root, run the cadastral figure script. Default arguments use paths relative to `case_study/cadastral`:
        ```bash
        python case_study/cadastral/scripts/create_figure6_cadastral_data.py
        ```
      * Optional arguments: `--data` / `-d` (facts CSV), `--shp` / `-s` (shapefile), `--boundary` / `-b` (boundary directory), `--output` / `-o` (output directory). Default output is `case_study/cadastral/../../figures` (project-level `figures` when run from root).
      * Outputs (default output directory):
        * `figures/Figure6_Cadastral_Data.pdf`
        * `figures/Figure6_Cadastral_Data.png`
      * The relevant code block in `create_figure6_cadastral_data.py` is:
        ```python
        # In create_figure_data_overview():
        output_path = output_dir / 'Figure6_Cadastral_Data'
        fig.savefig(f'{output_path}.pdf', dpi=300, bbox_inches='tight')
        fig.savefig(f'{output_path}.png', dpi=300, bbox_inches='tight')
        ```
      * The figure provides a data overview (e.g., attribute/relation change types, changes over time, and optional map panel).

---

## Table 2: Infection Case Study — Resource Efficiency and Query Latency

### Source Data

  * `case_study/infection/results/experiment_results.json`, produced by running the infection case study experiment for models A (AMA), B (SVM), and C (PAM).  
  * **Without running experiments:** The project may include this results file by default. You can compile Table 2 from it directly (procedure step 2). Running `run_experiment.py` will overwrite the file.

### Structure of the Table

  * Rows: Model (AMA, SVM, PAM).
  * Columns: **Resource Efficiency**: Ingest Time (s), Storage Cost (MB); **Query Latency**: Q1 (ms), Q2 (ms), Q3 (ms).  
  * Q1 = Query1_ContactFrequency, Q2 = Query2_InfectionSpreadPath, Q3 = Query3_HighRiskContact (see `case_study/infection/queries/infection_queries.json`).

### Procedure

1.  **Run the infection case study experiment**:
      * Ensure Neo4j is running and that `experiment_config.json` (if used) has correct Neo4j and `db_parent` settings. Run the experiment script from `case_study/infection/scripts` so that results are written to `case_study/infection/results/experiment_results.json` (default `--output ../results/experiment_results.json`):
        ```bash
        cd case_study/infection/scripts
        python run_experiment.py
        ```
      * Default input is `../data/infection_facts.csv`. To override paths, use `--input` and `--output` (paths are relative to the script directory).
      * The script runs import and queries for each model and writes a JSON object keyed by model (`"A"`, `"B"`, `"C"`). Each value contains `ingest_time_s`, `store_mb`, and `query_results` with one entry per query name.
2.  **Extract values for the table**:
      * Open `case_study/infection/results/experiment_results.json`.
      * For each model (`A` → AMA, `B` → SVM, `C` → PAM):
        * **Ingest Time (s)**: use `ingest_time_s` (round to 2 decimal places for the table).
        * **Storage Cost (MB)**: use `store_mb` (round to 2 decimal places).
        * **Q1 (ms)**: from `query_results["Query1_ContactFrequency"]["latency_ms"]["median"]` (round to 2 decimal places).
        * **Q2 (ms)**: from `query_results["Query2_InfectionSpreadPath"]["latency_ms"]["median"]`.
        * **Q3 (ms)**: from `query_results["Query3_HighRiskContact"]["latency_ms"]["median"]`.
3.  **Arrange the table**:
      * Build the table with header row "Model | Resource Efficiency | Query Latency" and subheaders "Ingest Time (s) | Storage Cost (MB) | Q1 (ms) | Q2 (ms) | Q3 (ms)". Fill rows with the values obtained in step 2.

---

## Table 3: Cadastral Case Study — Resource Efficiency and Query Latency

### Source Data

  * `case_study/cadastral/results/experiment_results.json`, produced by running the cadastral case study experiment for models A (AMA), B (SVM), and C (PAM).  
  * **Without running experiments:** The project may include this results file by default. You can compile Table 3 from it directly (procedure step 2). Running `run_experiment.py` will overwrite the file.

### Structure of the Table

  * Rows: Model (AMA, SVM, PAM).
  * Columns: **Resource Efficiency**: Ingest Time (s), Storage Cost (MB); **Query Latency**: Q1 (ms), Q2 (ms), Q3 (ms).  
  * Q1 = Query1_PropertyHistoryTracking, Q2 = Query2_TopologyEvolutionPath, Q3 = Query3_SpatioTemporalJointQuery (see `case_study/cadastral/queries/cadastral_queries.json`).

### Procedure

1.  **Run the cadastral case study experiment**:
      * Neo4j and (if used) `experiment_config.json` must be configured; ensure cadastral facts (e.g., `final_manhattan_facts.csv`) are available. Run the experiment from `case_study/cadastral/scripts` so that results are written to `case_study/cadastral/results/experiment_results.json` (default `--output ../results/experiment_results.json`):
        ```bash
        cd case_study/cadastral/scripts
        python run_experiment.py
        ```
      * Default input is `../data/final_manhattan_facts.csv`. Use `--input` and `--output` to override (paths relative to the script directory).
      * The script runs import and queries for each model and writes a JSON object keyed by model (`"A"`, `"B"`, `"C"`), with `ingest_time_s`, `store_mb`, and `query_results` per query.
2.  **Extract values for the table**:
      * Open `case_study/cadastral/results/experiment_results.json`.
      * For each model (`A` → AMA, `B` → SVM, `C` → PAM):
        * **Ingest Time (s)**: use `ingest_time_s` (round to 2 decimal places).
        * **Storage Cost (MB)**: use `store_mb` (round to 2 decimal places).
        * **Q1 (ms)**: from `query_results["Query1_PropertyHistoryTracking"]["latency_ms"]["median"]` (round to 2 decimal places).
        * **Q2 (ms)**: from `query_results["Query2_TopologyEvolutionPath"]["latency_ms"]["median"]`.
        * **Q3 (ms)**: from `query_results["Query3_SpatioTemporalJointQuery"]["latency_ms"]["median"]`.
3.  **Arrange the table**:
      * Build the table with the same structure as Table 2: Model, Resource Efficiency (Ingest Time, Storage Cost), Query Latency (Q1, Q2, Q3), and fill with the values from step 2.
