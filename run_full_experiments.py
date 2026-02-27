#!/usr/bin/env python3
"""
Complete Experiment Execution Script

Main script for running comprehensive synthetic experiments to evaluate
spatiotemporal graph storage models (A, B, C) across multiple dimensions:
- Scale impact (G1)
- Attribute volatility impact (G2)
- Relation volatility impact (G3)
- Data density impact (G4)

Features:
- Detailed progress logging and error handling
- Configurable experiment parameters
- Support for selective model execution
- Automatic result aggregation and statistics
"""

import os
import sys
import json
import time
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

# Import main module (only import necessary classes, do not execute main program logic)
from main_etl_v2_REVISED import (
    DataSynthesizer,
    ETLRunner,
    BenchmarkSuite,
    QueryWorkload,
    prepare_isolated_db,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOGGER = logging.getLogger(__name__)

# ============================================================================
# Experiment Configuration
# ============================================================================
# Configure experiment parameters here or use command-line arguments
# Command-line arguments override these default settings

# Default experiment configuration
EXPERIMENT_CONFIG = {
    "experiment": "G1",  # Options: "G1", "G2", "G3", "G4", "all"
    "skip_large": True,  # Whether to skip large-scale experiments (10M entities)
    "models": ["A", "B", "C"],  # Models to run: ["A"], ["B"], ["C"], ["A", "B"], ["A", "B", "C"]
}

# Configuration file path
CONFIG_FILE = "experiment_config.json"  # Path to experiment configuration file


def print_header(title: str, width: int = 80, char: str = "="):
    """Print header"""
    LOGGER.info("")
    LOGGER.info(char * width)
    LOGGER.info(f"  {title}")
    LOGGER.info(char * width)


def print_step(step: str, current: int = 0, total: int = 0):
    """Print step"""
    if total > 0:
        LOGGER.info("")
        LOGGER.info(f"[{current}/{total}] {step}")
        LOGGER.info("-" * 80)
    else:
        LOGGER.info("")
        LOGGER.info(f"{step}")
        LOGGER.info("-" * 80)


def load_experiment_config(config_path: Path) -> Dict[str, Any]:
    """Load experiment configuration file"""
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    
    LOGGER.info(f"✓ Configuration file loaded: {config_path}")
    return config


def build_experiment_configs(experiments_config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Build experiment configuration mapping from configuration file.
    
    Automatically generates all experiment configurations based on definitions in
    experiment_config.json. Each experiment group contains:
    - variable: Variable name
    - values: List of variable values
    
    Configuration names are automatically generated based on variable and values.
    
    Note: Experiment groups G2, G3, G4 use 0.1M entities (100K) instead of
    the default 1M to shorten experiment time and facilitate completion of all experiments.
    
    Returns: {config_name: {parameter configuration}}
    """
    # Get base configuration
    base = dict(DataSynthesizer.DEFAULTS)
    configs: Dict[str, Dict[str, Any]] = {}
    
    # Iterate through each experiment group
    for group_key, group_config in experiments_config.items():
        variable = group_config.get("variable")
        values = group_config.get("values", [])
        
        if not variable or not values:
            LOGGER.warning(f"Experiment group {group_key} missing variable or values, skipping")
            continue
        
        # For G2, G3, G4 experiment groups, use 0.1M entities (instead of default 1M)
        # This shortens experiment time and facilitates completion of all experiments
        # G1 experiment group is not affected as it has its own n_entities value
        group_base = dict(base)
        if group_key in ["G2_Vol_Attr", "G3_Vol_Rel", "G4_Density_Props"]:
            group_base["n_entities"] = 100_000  # 0.1M
            LOGGER.debug(f"Experiment group {group_key} uses 0.1M entities")
        
        # Generate configuration name and parameters for each value
        for value in values:
            # Automatically generate configuration name based on variable type
            if variable == "n_entities":
                if value >= 1_000_000:
                    config_name = f"{group_key}_{value/1_000_000:.1f}M"
                elif value >= 1_000:
                    # For 100K, display as 0.1M; for other K levels, display as K
                    if value == 100_000:
                        config_name = f"{group_key}_0.1M"
                    else:
                        config_name = f"{group_key}_{value/1_000:.0f}K"
                else:
                    config_name = f"{group_key}_{value}"
            elif variable == "v_attribute" or variable == "v_relation":
                config_name = f"{group_key}_{int(value*100)}pct"
            elif variable == "n_dynamic_props":
                config_name = f"{group_key}_{int(value)}"
            else:
                # Default: use variable value as suffix
                config_name = f"{group_key}_{value}"
            
            # Generate configuration parameters
            cfg = dict(group_base)
            cfg[variable] = value
            configs[config_name] = cfg
    
    return configs


def run_single_experiment(
    config_name: str,
    config_params: Dict[str, Any],
    runner: ETLRunner,
    query_workload: Optional[QueryWorkload],
    neo4j_config: Dict[str, Any],
    benchmark_config: Dict[str, Any],
    paths_config: Dict[str, Any],
    models: List[str],
    profile_dir: Path,
) -> Dict[str, Any]:
    """Run single experiment configuration"""
    
    print_step(f"Running configuration: {config_name}")
    
    LOGGER.info(f"Parameter configuration:")
    LOGGER.info(f"  n_entities: {config_params.get('n_entities', 'N/A'):,}")
    LOGGER.info(f"  v_attribute: {config_params.get('v_attribute', 'N/A')}")
    LOGGER.info(f"  v_relation: {config_params.get('v_relation', 'N/A')}")
    LOGGER.info(f"  n_dynamic_props: {config_params.get('n_dynamic_props', 'N/A')}")
    LOGGER.info(f"  Running models: {', '.join([m.upper() for m in models])}")
    
    # Set profile path
    profile_path = profile_dir / f"{config_name}.profile.json"
    
    # Create BenchmarkSuite
    suite = BenchmarkSuite(
        runner,
        query_workload=query_workload,
        store_path=neo4j_config.get("store_path"),
        db_parent=neo4j_config.get("db_parent"),
        db_name=neo4j_config["database"],
        reset_db_fn=None,
        profile_path=str(profile_path),
        samples_per_query=benchmark_config.get("samples_per_query", 200),
        uri=neo4j_config["uri"],
        user=neo4j_config["user"],
        password=neo4j_config["password"],
    )
    
    # Run experiment
    start_time = time.time()
    try:
        result = suite.run_configuration(
            config_name,
            config_params,
            models=tuple(models),
        )
        elapsed = time.time() - start_time
        result["elapsed_time_s"] = elapsed
        
        LOGGER.info("")
        LOGGER.info(f"✓ Configuration {config_name} completed")
        LOGGER.info(f"  Total elapsed time: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")
        
        # Output summary results
        if "results" in result:
            for model, model_results in result["results"].items():
                LOGGER.info(f"  Model {model.upper()}:")
                if "ingest_cost" in model_results:
                    LOGGER.info(f"    Ingest cost: {model_results['ingest_cost']:.2f} seconds")
                if "storage_cost" in model_results:
                    LOGGER.info(f"    Storage cost: {model_results['storage_cost']:.2f} MB")
                if "n_nodes" in model_results and model_results["n_nodes"] is not None:
                    LOGGER.info(f"    Number of nodes: {model_results['n_nodes']:,}")
                if "n_edges" in model_results and model_results["n_edges"] is not None:
                    LOGGER.info(f"    Number of edges: {model_results['n_edges']:,}")
                if "latency" in model_results:
                    for query_type, latency_data in model_results["latency"].items():
                        mean = latency_data.get("mean", 0)
                        LOGGER.info(f"    {query_type}: {mean:.3f}ms (average)")
        
        return result
        
    except Exception as e:
        elapsed = time.time() - start_time
        LOGGER.error("")
        LOGGER.error(f"✗ Configuration {config_name} failed (elapsed: {elapsed:.2f} seconds)")
        LOGGER.error(f"  Error: {e}")
        import traceback
        LOGGER.error(traceback.format_exc())
        raise


def run_experiment_group(
    group_key: str,
    group_config: Dict[str, Any],
    config_map: Dict[str, Dict[str, Any]],
    runner: ETLRunner,
    query_workload: Optional[QueryWorkload],
    neo4j_config: Dict[str, Any],
    benchmark_config: Dict[str, Any],
    paths_config: Dict[str, Any],
    models: List[str],
    profile_dir: Path,
    skip_large: bool = False,
) -> List[Dict[str, Any]]:
    """Run an experiment group"""
    
    print_header(f"Experiment Group: {group_key}", 80)
    LOGGER.info(f"Description: {group_config.get('description', 'N/A')}")
    
    # Extract all configurations for this experiment group from config_map (based on config name prefix)
    configs = [c for c in config_map.keys() if c.startswith(group_key)]
    
    # If skipping large-scale experiments, filter out 10M configurations
    if skip_large:
        original_count = len(configs)
        configs = [c for c in configs if "10.0M" not in c]
        if len(configs) < original_count:
            LOGGER.info(f"Skipped large-scale experiments (10M entities), {len(configs)} configurations remaining")
    
    if not configs:
        LOGGER.warning("No configurations to run, skipping this experiment group")
        return []
    
    LOGGER.info(f"Number of configurations: {len(configs)}")
    LOGGER.info(f"Configuration list: {', '.join(sorted(configs))}")
    
    results = []
    total_configs = len(configs)
    
    for config_idx, config_name in enumerate(configs, 1):
        if config_name not in config_map:
            LOGGER.warning(f"Configuration {config_name} does not exist, skipping")
            continue
        
        config_params = config_map[config_name]
        
        try:
            result = run_single_experiment(
                config_name,
                config_params,
                runner,
                query_workload,
                neo4j_config,
                benchmark_config,
                paths_config,
                models,
                profile_dir,
            )
            results.append(result)
        except Exception as e:
            LOGGER.error(f"Configuration {config_name} failed, continuing with other configurations...")
            # Continue running other configurations, don't interrupt the entire experiment group
            continue
    
    LOGGER.info("")
    LOGGER.info(f"Experiment group {group_key} completed: {len(results)}/{total_configs} configurations successful")
    
    return results


def main():
    """Main function"""
    
    # Read configuration from configuration area
    experiment = EXPERIMENT_CONFIG["experiment"]
    skip_large = EXPERIMENT_CONFIG["skip_large"]
    models = [m.strip().lower() for m in EXPERIMENT_CONFIG["models"]]
    
    # Print startup information
    print_header("Complete Experiment Execution Script", 80)
    LOGGER.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    LOGGER.info(f"Experiment group: {experiment}")
    LOGGER.info(f"Skip large-scale: {skip_large}")
    LOGGER.info(f"Running models: {', '.join([m.upper() for m in models])}")
    
    # Load configuration
    config_path = Path(CONFIG_FILE)
    try:
        config = load_experiment_config(config_path)
    except FileNotFoundError as e:
        LOGGER.error(f"Error: {e}")
        LOGGER.error(f"Please ensure configuration file {CONFIG_FILE} exists")
        sys.exit(1)
    
    neo4j_config = config.get("neo4j", {})
    benchmark_config = config.get("benchmark", {})
    paths_config = config.get("paths", {})
    experiments_config = config.get("experiments", {})
    
    # Build experiment configuration mapping from configuration file
    print_step("Building experiment configuration mapping")
    config_map = build_experiment_configs(experiments_config)
    LOGGER.info(f"✓ Experiment configurations built, total {len(config_map)} configurations")
    
    # Determine experiment groups to run
    if experiment == "all":
        experiment_groups = list(experiments_config.keys())
    else:
        experiment_groups = []
        exp_prefix = experiment.upper()
        for key in experiments_config.keys():
            if key.startswith(exp_prefix):
                experiment_groups.append(key)
                break
    
    if not experiment_groups:
        LOGGER.error(f"Error: Experiment group {experiment} not found")
        LOGGER.error(f"Available experiment groups: {', '.join(experiments_config.keys())}")
        sys.exit(1)
    
    LOGGER.info(f"Will run {len(experiment_groups)} experiment group(s): {', '.join(experiment_groups)}")
    
    # Prepare database
    db_name = neo4j_config.get("database", "bench-db")
    print_step(f"Preparing database: {db_name}")
    try:
        if neo4j_config.get("isolated_db", False):
            prepare_isolated_db(
                neo4j_config["uri"],
                neo4j_config["user"],
                neo4j_config["password"],
                db_name,
            )
            LOGGER.info(f"✓ Database {db_name} ready (isolated mode)")
        else:
            LOGGER.info(f"✓ Using existing database {db_name} (non-isolated mode)")
    except Exception as e:
        LOGGER.warning(f"Database preparation warning: {e}")
        LOGGER.warning("Will continue running, but may affect experiment results")
    
    # Initialize ETL Runner
    print_step("Initializing ETL Runner")
    runner = ETLRunner(
        neo4j_config["uri"],
        neo4j_config["user"],
        neo4j_config["password"],
        batch_size=benchmark_config.get("batch_size", 5000),
        database=db_name,
    )
    LOGGER.info("✓ ETL Runner initialized")
    
    # Load query workload
    print_step("Loading query workload")
    query_workload = None
    queries_path = Path(paths_config.get("queries_file", "queries.json"))
    if queries_path.exists():
        with open(queries_path, "r", encoding="utf-8") as f:
            queries_data = json.load(f)
        query_workload = QueryWorkload(
            queries_data,
            warmups=benchmark_config.get("query_warmups", 1),
            runs=benchmark_config.get("query_runs", 10),
            database=db_name,
        )
        LOGGER.info(f"✓ Query workload loaded: {queries_path}")
        LOGGER.info(f"  Warmup runs: {benchmark_config.get('query_warmups', 1)}")
        LOGGER.info(f"  Query runs: {benchmark_config.get('query_runs', 10)}")
    else:
        LOGGER.warning(f"Query file does not exist: {queries_path}, will skip query execution")
    
    # Create profile directory
    profile_dir = Path(paths_config.get("profile_output_dir", "experiment_profiles"))
    profile_dir.mkdir(parents=True, exist_ok=True)
    LOGGER.info(f"✓ Profile output directory: {profile_dir}")
    
    # Run experiment groups
    all_results = []
    total_groups = len(experiment_groups)
    overall_start_time = time.time()
    
    print_header("Starting experiments", 80)
    
    for group_idx, group_key in enumerate(experiment_groups, 1):
        if group_key not in experiments_config:
            LOGGER.warning(f"Experiment group {group_key} does not exist, skipping")
            continue
        
        group_config = experiments_config[group_key]
        
        try:
            results = run_experiment_group(
                group_key,
                group_config,
                config_map,
                runner,
                query_workload,
                neo4j_config,
                benchmark_config,
                paths_config,
                models,
                profile_dir,
                skip_large=skip_large,
            )
            all_results.extend(results)
        except Exception as e:
            LOGGER.error(f"Experiment group {group_key} failed: {e}")
            import traceback
            LOGGER.error(traceback.format_exc())
            # Continue running other experiment groups
            continue
    
    # Close runner
    runner.close()
    
    # Save results
    overall_elapsed = time.time() - overall_start_time
    
    print_header("Experiments completed", 80)
    LOGGER.info(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    LOGGER.info(f"Total elapsed time: {overall_elapsed:.2f} seconds ({overall_elapsed/60:.2f} minutes)")
    LOGGER.info(f"Successfully completed: {len(all_results)} experiment configurations")
    
    if all_results:
        results_path = Path(paths_config.get("results_output_file", "experiment_results.json"))
        results_path.parent.mkdir(parents=True, exist_ok=True)
        with open(results_path, "w", encoding="utf-8") as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        
        LOGGER.info("")
        LOGGER.info(f"✓ Experiment results saved to: {results_path}")
        LOGGER.info(f"  File size: {results_path.stat().st_size / 1024:.2f} KB")
    else:
        LOGGER.warning("No experiment results to save")
    
    LOGGER.info("=" * 80)


if __name__ == "__main__":
    main()

