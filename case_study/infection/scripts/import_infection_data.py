#!/usr/bin/env python3
"""
Case Study: Import epidemic transmission network data to Neo4j
Supports data import for three models (A, B, C)
"""

import sys
import argparse
import logging
from pathlib import Path

# Add project root directory to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from main_etl_v2_REVISED import ETLRunner, Fact

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
LOGGER = logging.getLogger(__name__)

# Default Neo4j connection configuration
DEFAULT_NEO4J_URI = "bolt://localhost:7687"
DEFAULT_NEO4J_USER = "neo4j"
DEFAULT_NEO4J_PASSWORD = "wang1026"
DEFAULT_NEO4J_DATABASE = "neo4j"

# Static attribute list
STATIC_ATTRIBUTES = {"gender", "class_name", "role"}


def load_facts_from_csv(csv_path: Path, model: str = 'A') -> list[Fact]:
    """
    Load fact data from CSV file and automatically sort according to model type.

    Args:
        csv_path: CSV file path
        model: Model type ('A', 'B', 'C'), used to determine sorting method
    """
    import pandas as pd

    LOGGER.info(f"Loading fact data: {csv_path}")
    df = pd.read_csv(csv_path)

    # Sort according to model type
    model_upper = model.upper()
    if model_upper == 'A' or model_upper == 'B':
        # Model A/B: Sort by (entity_id, start)
        LOGGER.info("  Sort method: (EntityID, Start)")
        df = df.sort_values(by=['EntityID', 'Start'], kind='stable')
    elif model_upper == 'C':
        # Model C: Sort by (entity_id, attribute, start)
        LOGGER.info("  Sort method: (EntityID, Attribute, Start)")
        df = df.sort_values(by=['EntityID', 'Attribute', 'Start'], kind='stable')

    facts = []
    for _, row in df.iterrows():
        fact = Fact(
            entity_id=str(row['EntityID']),
            entity_label=str(row['Label']),
            attribute=str(row['Attribute']),
            value=row['Value'],
            value_type=int(row['Type']),
            start=int(row['Start']),
            end=int(row['End']) if pd.notna(row['End']) else None
        )
        facts.append(fact)

    LOGGER.info(f"  Loaded {len(facts)} facts in total")
    return facts


def import_data(
        model: str,
        csv_path: Path,
        uri: str,
        user: str,
        password: str,
        database: str
):
    """
    Import data to Neo4j.

    Args:
        model: Model type ('A', 'B', 'C')
        csv_path: CSV file path
        uri: Neo4j URI
        user: Neo4j username
        password: Neo4j password
        database: Neo4j database name
    """
    LOGGER.info("=" * 60)
    LOGGER.info(f"Starting case study data import - Model {model}")
    LOGGER.info("=" * 60)

    # Load fact data (auto-sorted)
    facts = load_facts_from_csv(csv_path, model=model)

    # Create ETL Runner
    runner = ETLRunner(
        uri=uri,
        user=user,
        password=password,
        batch_size=5000,  # Use larger batch size for better performance
        database=database,
    )

    try:
        # Clear database and set up indices
        LOGGER.info("Clearing database and setting up indices...")
        labels = ["Person"]
        runner.clear_database_and_setup_indices(labels)

        # Import data (select based on model)
        LOGGER.info(f"Starting data import (Model {model})...")
        if model == 'A':
            total_time = runner.run_etl_model_a(facts, STATIC_ATTRIBUTES)
        elif model == 'B':
            total_time = runner.run_etl_model_b(facts, STATIC_ATTRIBUTES)
        elif model == 'C':
            total_time = runner.run_etl_model_c(facts, STATIC_ATTRIBUTES)
        else:
            raise ValueError(f"Unknown model type: {model}")

        LOGGER.info(f"Import completed, total elapsed time: {total_time:.2f} seconds")

        # Display statistics
        with runner.driver.session(database=database) as session:
            # Count nodes
            result = session.run("MATCH (n) RETURN count(n) as count")
            node_count = result.single()["count"]

            # Count relationships
            result = session.run("MATCH ()-[r]->() RETURN count(r) as count")
            rel_count = result.single()["count"]

            # Count nodes by type
            result = session.run("""
                MATCH (n)
                RETURN labels(n)[0] as label, count(n) as count
                ORDER BY label
            """)
            node_by_label = {record["label"]: record["count"] for record in result}

            # Count relationships by type
            result = session.run("""
                MATCH ()-[r]->()
                RETURN type(r) as type, count(r) as count
                ORDER BY type
            """)
            rel_by_type = {record["type"]: record["count"] for record in result}

            LOGGER.info("\n" + "=" * 60)
            LOGGER.info("Import Statistics:")
            LOGGER.info(f"  Total nodes: {node_count}")
            for label, count in node_by_label.items():
                LOGGER.info(f"    {label}: {count}")
            LOGGER.info(f"  Total relationships: {rel_count}")
            for rel_type, count in rel_by_type.items():
                LOGGER.info(f"    {rel_type}: {count}")
            LOGGER.info("=" * 60)

            # Display some example queries
            LOGGER.info("\nExample queries (can be executed in Neo4j browser):")
            LOGGER.info("1. View all nodes and relationships:")
            LOGGER.info("   MATCH (n)-[r]-(m) RETURN n, r, m LIMIT 50")
            LOGGER.info("\n2. View health status history of Person nodes:")
            if model == 'A':
                LOGGER.info("   MATCH (p:Person) RETURN p.id, p.health_status_history LIMIT 5")
            elif model == 'B':
                LOGGER.info("   MATCH (s:Person_State) RETURN s.id, s.health_status, s.start, s.end LIMIT 5")
            else:  # Model C
                LOGGER.info("   MATCH (p:Person)-[:HAS_PROCESS {attr:'health_status'}]->(head:ProcessNode)")
                LOGGER.info("   MATCH (head)-[:NEXT_STATE*0..]->(node:ProcessNode)")
                LOGGER.info("   RETURN p.id, node.value, node.start, node.end LIMIT 5")
            LOGGER.info("\n3. View contact relationships:")
            LOGGER.info("   MATCH (p1:Person)-[r:CONTACTS]->(p2:Person) RETURN p1.id, p2.id, r.start, r.end LIMIT 10")

    except Exception as e:
        LOGGER.error(f"Import failed: {e}", exc_info=True)
        raise
    finally:
        runner.close()
        LOGGER.info("Connection closed")


def main():
    parser = argparse.ArgumentParser(
        description='Import case study data to Neo4j',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Import Model A data
  python scripts/import_infection_data.py --model A --input data/infection_facts.csv

  # Import Model B data
  python scripts/import_infection_data.py --model B --input data/infection_facts.csv

  # Import Model C data
  python scripts/import_infection_data.py --model C --input data/infection_facts.csv
        """
    )

    parser.add_argument(
        '--model', '-m',
        type=str,
        choices=['A', 'B', 'C'],
        default='B',
        help='Model type (A/B/C)'
    )

    parser.add_argument(
        '--input', '-i',
        type=str,
        default='../data/infection_facts.csv',
        help='Input CSV file path'
    )

    parser.add_argument(
        '--uri',
        type=str,
        default=DEFAULT_NEO4J_URI,
        help=f'Neo4j URI (default: {DEFAULT_NEO4J_URI})'
    )

    parser.add_argument(
        '--user', '-u',
        type=str,
        default=DEFAULT_NEO4J_USER,
        help=f'Neo4j username (default: {DEFAULT_NEO4J_USER})'
    )

    parser.add_argument(
        '--password', '-p',
        type=str,
        default=DEFAULT_NEO4J_PASSWORD,
        help=f'Neo4j password (default: {DEFAULT_NEO4J_PASSWORD})'
    )

    parser.add_argument(
        '--database', '-db',
        type=str,
        default=DEFAULT_NEO4J_DATABASE,
        help=f'Neo4j database name (default: {DEFAULT_NEO4J_DATABASE})'
    )

    args = parser.parse_args()

    csv_path = Path(args.input)
    if not csv_path.exists():
        LOGGER.error(f"File not found: {csv_path}")
        sys.exit(1)

    import_data(
        args.model,
        csv_path,
        args.uri,
        args.user,
        args.password,
        args.database
    )


if __name__ == '__main__':
    main()

