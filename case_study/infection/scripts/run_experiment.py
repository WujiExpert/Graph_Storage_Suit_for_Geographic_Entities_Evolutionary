#!/usr/bin/env python3
"""
Case Study: Unified Experiment Script
Integrates data import, query execution, and storage measurement
"""
import statistics
import sys
import json
import time
import argparse
import os
from pathlib import Path
from typing import Dict, Any, Optional

# Add project root directory to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from neo4j import GraphDatabase
from main_etl_v2_REVISED import (
    ETLRunner,
    Fact,
    get_db_size,
    stop_database,
    start_database,
    prepare_isolated_db,
    wait_db_status
)


def load_facts_from_csv(csv_path: Path, model: str = 'A') -> list[Fact]:
    """
    Load facts from CSV file and automatically sort according to model type.

    Args:
        csv_path: CSV file path
        model: Model type ('A', 'B', 'C'), used to determine sorting method
    """
    import pandas as pd

    df = pd.read_csv(csv_path)

    # Handle End field: Convert 9999 to None (represents infinity)
    df['End'] = df['End'].replace(9999999999, None)
    df['End'] = df['End'].where(pd.notna(df['End']), None)

    # Sort according to model type
    model_upper = model.upper()
    if model_upper == 'A' or model_upper == 'B':
        # Model A/B: Sort by (entity_id, start)
        df = df.sort_values(by=['EntityID', 'Start'], kind='stable')
    elif model_upper == 'C':
        # Model C: Sort by (entity_id, attribute, start)
        df = df.sort_values(by=['EntityID', 'Attribute', 'Start'], kind='stable')

    facts = []
    for _, row in df.iterrows():
        end_val = row['End']
        if pd.isna(end_val) or end_val == 9999999999 or end_val == '9999999999' or end_val == 'None':
            end_val = None
        else:
            end_val = int(float(end_val))

        facts.append(Fact(
            entity_id=str(row['EntityID']),
            entity_label=str(row['Label']),
            attribute=str(row['Attribute']),
            value=row['Value'],
            value_type=int(row['Type']),
            start=int(row['Start']),
            end=end_val
        ))
    return facts


def measure_storage_size(db_parent: str, db_name: str) -> Optional[float]:
    """Measure database storage size (MB)"""
    if not db_parent:
        return None
    db_path = os.path.join(db_parent, db_name)
    return get_db_size(db_path)


def run_queries(
        model: str,
        queries_file: Path,
        uri: str,
        user: str,
        password: str,
        database: str
) -> Dict[str, Any]:
    """Run queries and measure performance"""
    driver = GraphDatabase.driver(uri, auth=(user, password))
    query_results = {}

    try:
        # Load query definitions
        with open(queries_file, 'r', encoding='utf-8') as f:
            queries = json.load(f)

        with driver.session(database=database) as session:
            for query_name, query_def in queries.items():
                cypher = query_def['cypher'].get(model)
                if not cypher:
                    query_results[query_name] = {"error": "Query not defined for this model"}
                    continue

                params = query_def.get('params', {})

                # Warmup run
                try:
                    session.run(cypher, params).consume()
                except Exception as e:
                    query_results[query_name] = {"error": str(e)}
                    continue

                # Run 100 times and take average
                latencies = []
                for _ in range(100):
                    start_time = time.time()
                    result = session.run(cypher, params)
                    result.consume()  # Ensure query fully executes
                    latency_ms = (time.time() - start_time) * 1000
                    latencies.append(latency_ms)

                query_results[query_name] = {
                    "latency_ms": {
                        "mean": sum(latencies) / len(latencies),
                        "min": min(latencies),
                        "max": max(latencies),
                        "median": statistics.median(latencies),
                        "values": latencies
                    }
                }

    finally:
        driver.close()

    return query_results


def run_experiment_for_model(
        model: str,
        facts: list[Fact],
        static_attributes: list[str],
        uri: str,
        user: str,
        password: str,
        database: str,
        db_parent: Optional[str],
        queries_file: Optional[Path]
) -> Dict[str, Any]:
    """Run complete experiment for a single model"""
    print(f"\n{'=' * 60}")
    print(f"Starting experiment - Model {model}")
    print(f"{'=' * 60}")

    # 1. Prepare database (clear and rebuild)
    print("\n[1/7] Preparing database...")
    prepare_isolated_db(uri, user, password, database)

    # 2. Measure empty database size
    print("\n[2/7] Measuring empty database size...")
    origin_size = measure_storage_size(db_parent, database)
    if origin_size is not None:
        print(f"  Empty database size: {origin_size:.2f} MB")
    else:
        print("  Warning: Cannot measure empty database size (db_parent not configured)")

    # 3. Import data
    print(f"\n[3/7] Importing data (Model {model})...")
    runner = ETLRunner(uri, user, password, database=database)

    # Set up indices
    labels = ["Person"]
    runner.clear_database_and_setup_indices(labels)

    # Run ETL
    start_time = time.time()
    method = getattr(runner, f"run_etl_model_{model.lower()}")
    ingest_cost = method(facts, static_attributes)
    total_ingest_time = time.time() - start_time

    print(f"  Import completed, elapsed time: {total_ingest_time:.2f} seconds")

    # Get graph statistics
    with runner.driver.session(database=database) as session:
        result = session.run("MATCH (n) RETURN count(n) as count")
        n_nodes = result.single()["count"]
        result = session.run("MATCH ()-[r]->() RETURN count(r) as count")
        n_edges = result.single()["count"]

    print(f"  Number of nodes: {n_nodes:,}, Number of edges: {n_edges:,}")
    runner.close()
    time.sleep(3)  # Wait for data to be written to disk

    # 6. Run queries
    query_results = {}
    if queries_file and queries_file.exists():
        print(f"\n[6/7] Running queries...")
        query_results = run_queries(model, queries_file, uri, user, password, database)

        print("  Query results:")
        for query_name, result in query_results.items():
            if "error" in result:
                print(f"    {query_name}: Error - {result['error']}")
            else:
                latency = result.get("latency_ms", {})
                print(
                    f"    {query_name}: {latency.get('mean', 0):.2f} ms (min: {latency.get('min', 0):.2f}, max: {latency.get('max', 0):.2f})")
    else:
        print(f"\n[6/7] Skipping queries (query file does not exist)")

    # 7. Stop database and measure final storage
    print("\n[7/7] Stopping database and measuring final storage...")
    stop_database(uri, user, password, database)
    time.sleep(1)

    final_size = measure_storage_size(db_parent, database)
    final_store_bytes = None
    if final_size is not None and origin_size is not None:
        final_store_bytes = final_size - origin_size
        print(f"  Final storage usage: {final_store_bytes:.2f} MB (baseline: {origin_size:.2f} MB, final: {final_size:.2f} MB)")

    return {
        "model": model,
        "ingest_time_s": total_ingest_time,
        "store_mb": final_store_bytes,
        "n_nodes": n_nodes,
        "n_edges": n_edges,
        "query_results": query_results
    }


def main():
    parser = argparse.ArgumentParser(
        description='Run complete case study experiments',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--input', '-i',
        type=str,
        default='../data/infection_facts.csv',
        help='Input CSV file path'
    )

    parser.add_argument(
        '--models', '-m',
        type=str,
        nargs='+',
        choices=['A', 'B', 'C'],
        default=['A', 'B', 'C'],
        help='Models to test (default: A B C)'
    )

    parser.add_argument(
        '--queries', '-q',
        type=str,
        default='../queries/infection_queries.json',
        help='Query definition file path'
    )

    parser.add_argument(
        '--uri',
        type=str,
        default='bolt://localhost:7687',
        help='Neo4j URI'
    )

    parser.add_argument(
        '--user', '-u',
        type=str,
        default='neo4j',
        help='Neo4j username'
    )

    parser.add_argument(
        '--password', '-p',
        type=str,
        default='wang1026',
        help='Neo4j password'
    )

    parser.add_argument(
        '--database', '-db',
        type=str,
        default='neo4j',
        help='Neo4j database name'
    )

    parser.add_argument(
        '--db-parent',
        type=str,
        default="/Users/wuji/Library/Application Support/neo4j-desktop/Application/Data/dbmss/dbms-b1116d6b-d328-417e-9309-5c18c687e212/data/databases",
        help='Neo4j database parent directory path (for storage measurement)'
    )

    parser.add_argument(
        '--output', '-o',
        type=str,
        default='../results/experiment_results.json',
        help='Output JSON file path'
    )

    args = parser.parse_args()

    # Load configuration
    config_path = project_root / "experiment_config.json"
    if config_path.exists():
        with open(config_path, 'r') as f:
            config = json.load(f)
            neo4j_config = config.get("neo4j", {})
            if not args.db_parent:
                args.db_parent = neo4j_config.get("db_parent")
            if not args.uri:
                args.uri = neo4j_config.get("uri", args.uri)
            if not args.user:
                args.user = neo4j_config.get("user", args.user)
            if not args.password:
                args.password = neo4j_config.get("password", args.password)

    # Determine static attributes
    static_attributes = ["gender", "class_name", "role"]

    # Load query file
    queries_file = Path(args.queries)
    if not queries_file.is_absolute():
        queries_file = Path(__file__).parent / queries_file

    # Run experiments (load and sort separately for each model)
    results = {}
    for model in args.models:
        try:
            # Load and sort separately for each model
            print(f"\nLoading and sorting data for Model {model}...")
            facts = load_facts_from_csv(Path(args.input), model=model)
            print(f"  Loaded {len(facts):,} facts in total")

            result = run_experiment_for_model(
                model=model,
                facts=facts,
                static_attributes=static_attributes,
                uri=args.uri,
                user=args.user,
                password=args.password,
                database=args.database,
                db_parent=args.db_parent,
                queries_file=queries_file
            )
            results[model] = result
        except Exception as e:
            print(f"\n❌ Model {model} experiment failed: {e}")
            import traceback
            traceback.print_exc()
            results[model] = {"error": str(e)}

    # Save results
    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = Path(__file__).parent / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\n{'=' * 60}")
    print("Experiments completed!")
    print(f"{'=' * 60}")
    print(f"\nResults saved to: {output_path}")

    # Print summary
    print("\nExperiment summary:")
    for model, result in results.items():
        if "error" in result:
            print(f"  Model {model}: ❌ {result['error']}")
        else:
            print(f"  Model {model}:")
            print(f"    Import time: {result.get('ingest_time_s', 0):.2f} seconds")
            print(f"    Storage usage: {result.get('store_mb', 0):.2f} MB")
            print(f"    Number of nodes: {result.get('n_nodes', 0):,}")
            print(f"    Number of edges: {result.get('n_edges', 0):,}")


if __name__ == '__main__':
    main()