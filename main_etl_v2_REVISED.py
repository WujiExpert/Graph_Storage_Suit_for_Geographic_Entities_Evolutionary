"""
Spatiotemporal Graph Storage ETL Runner

Core ETL and query execution module for three spatiotemporal graph storage models:
- Model A: Attribute Metadata Approach (AMA)
- Model B: State Version Model (SVM)
- Model C: Process Atom Model (PAM)

Features:
- Streaming and batching support for large-scale data processing
- Schema-agnostic design for flexible data modeling
- External sorting for memory-efficient processing
- Comprehensive benchmark framework
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import tempfile
import time
import heapq
import itertools
import threading
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from neo4j import GraphDatabase, basic_auth

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
# Performance optimization utilities
# ---------------------------------------------------------------------------

# Regular expression matching cache (for label inference)
_label_cache: Dict[str, str] = {}

def infer_label_from_id(entity_id: str) -> Optional[str]:
    """
    Infer label from entity_id using cache to avoid repeated matching.
    
    Examples: Region_0 -> Region, City_1 -> City
    
    Args:
        entity_id: Entity ID in format "Label_number"
        
    Returns:
        Inferred label, or None if inference fails
    """
    if entity_id in _label_cache:
        return _label_cache[entity_id]
    
    match = re.match(r"^([A-Za-z_]+)_\d+$", entity_id)
    if match:
        inferred_label = match.group(1)
        _label_cache[entity_id] = inferred_label
        return inferred_label
    return None


def batch_iter(iterator: Iterator[Any], batch_size: int) -> Iterator[List[Any]]:
    """
    Batch an iterator, returning a list for each batch.
    
    Used for streaming processing of large data, avoiding loading all data
    into memory at once.
    
    Args:
        iterator: Iterator to be batched
        batch_size: Size of each batch
        
    Yields:
        List of data for each batch
    """
    batch = []
    for item in iterator:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

DEFAULT_CONFIG_SEQUENCE = [
    "G1_Scale_0.1M",
    "G1_Scale_1.0M",
    "G1_Scale_5.0M",
    "G1_Scale_10.0M",
    "G2_Vol_Attr_5pct",
    "G2_Vol_Attr_20pct",
    "G2_Vol_Attr_40pct",
    "G2_Vol_Attr_60pct",
    "G2_Vol_Attr_80pct",
    "G2_Vol_Attr_95pct",
    "G3_Vol_Rel_1pct",
    "G3_Vol_Rel_5pct",
    "G3_Vol_Rel_10pct",
    "G3_Vol_Rel_20pct",
    "G3_Vol_Rel_40pct",
    "G4_Density_Props_1",
    "G4_Density_Props_5",
    "G4_Density_Props_10",
    "G4_Density_Props_15",
    "G4_Density_Props_20",
]


# ---------------------------------------------------------------------------
# Data structures & utilities
# ---------------------------------------------------------------------------


@dataclass
class Fact:
    """Atomic fact row."""

    entity_id: str
    entity_label: str
    attribute: str
    value: Any
    value_type: int  # 0 = attribute, 1 = relationship
    start: int
    end: Optional[int]

    @classmethod
    def from_tuple(cls, row: Tuple[Any, ...]) -> "Fact":
        return cls(
            entity_id=row[0],
            entity_label=row[1],
            attribute=row[2],
            value=row[3],
            value_type=row[4],
            start=row[5],
            end=row[6],
        )

    def to_tuple(self) -> Tuple[Any, ...]:
        return (
            self.entity_id,
            self.entity_label,
            self.attribute,
            self.value,
            self.value_type,
            self.start,
            self.end,
        )


class BatchBuffer:
    """Generic batch buffer with flush callback."""

    def __init__(self, batch_size: int, flush_cb: Callable[[List[Dict[str, Any]]], None]):
        self.batch_size = max(1, batch_size)
        self.flush_cb = flush_cb
        self._buffer: List[Dict[str, Any]] = []

    def add(self, item: Dict[str, Any]) -> None:
        self._buffer.append(item)
        if len(self._buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        self.flush_cb(self._buffer)
        self._buffer = []

    def __len__(self) -> int:  # pragma: no cover
        return len(self._buffer)


class JsonlSpool:
    """
    Lightweight JSONL spool file.

    Write phase uses write(record); read phase (iteration) guarantees consistent order.
    Optimization: Uses buffer to reduce disk I/O.
    """

    def __init__(self, buffer_size: int = 8192):
        self._file = tempfile.NamedTemporaryFile(
            mode="w+", delete=False, encoding="utf-8", newline=""
        )
        self._closed = False
        self._buffer_size = buffer_size
        self._write_buffer: List[str] = []
        self._buffer_bytes = 0

    def write(self, record: Dict[str, Any]) -> None:
        if self._closed:
            raise RuntimeError("Spool already closed")
        # Serialize record
        line = json.dumps(record) + "\n"
        self._write_buffer.append(line)
        self._buffer_bytes += len(line.encode('utf-8'))

        # Flush buffer when threshold is reached
        if self._buffer_bytes >= self._buffer_size:
            self._flush_buffer()

    def _flush_buffer(self) -> None:
        """Write buffer contents to file"""
        if self._write_buffer:
            self._file.write("".join(self._write_buffer))
            self._write_buffer.clear()
            self._buffer_bytes = 0

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        if self._closed:
            raise RuntimeError("Spool already iterated & closed")
        # Ensure buffer contents are written
        self._flush_buffer()
        self._file.flush()
        self._file.seek(0)
        for line in self._file:
            yield json.loads(line)
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        # Ensure buffer contents are written
        self._flush_buffer()
        self._file.close()
        os.unlink(self._file.name)
        self._closed = True


class ExternalSorter:
    """
    Chunk-based external sorter for Fact streams.

    Implements low-memory sorting by writing sorted runs to disk,
    suitable for ultra-large scale facts.
    """

    def __init__(self, key_fn: Callable[[Fact], Tuple[Any, ...]], chunk_size: int = 250_000):
        self.key_fn = key_fn
        self.chunk_size = max(1, chunk_size)

    def sort(self, iterator: Iterable[Fact]) -> Iterator[Fact]:
        runs: List[str] = []
        buffer: List[Fact] = []

        def flush_buffer() -> None:
            if not buffer:
                return
            buffer.sort(key=self.key_fn)
            tmp = tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
            for fact in buffer:
                record = {"key": self.key_fn(fact), "fact": fact.to_tuple()}
                tmp.write(json.dumps(record) + "\n")
            tmp.close()
            runs.append(tmp.name)
            buffer.clear()

        for fact in iterator:
            buffer.append(fact)
            if len(buffer) >= self.chunk_size:
                flush_buffer()
        flush_buffer()

        if not runs:
            # All sorted in memory, no merge needed
            for fact in buffer:
                yield fact
            return

        yield from self._merge_runs(runs)

    def _merge_runs(self, run_paths: List[str]) -> Iterator[Fact]:
        def run_iterator(path: str) -> Iterator[Tuple[Tuple[Any, ...], Fact]]:
            fh = open(path, "r", encoding="utf-8")
            try:
                for line in fh:
                    payload = json.loads(line)
                    key = tuple(payload["key"])
                    fact = Fact.from_tuple(tuple(payload["fact"]))
                    yield key, fact
            finally:
                fh.close()
                os.unlink(path)

        heap: List[Tuple[Tuple[Any, ...], int, Fact, Iterator[Tuple[Tuple[Any, ...], Fact]]]] = []
        iterators: List[Iterator[Tuple[Tuple[Any, ...], Fact]]] = []

        for idx, path in enumerate(run_paths):
            it = run_iterator(path)
            try:
                key, fact = next(it)
            except StopIteration:
                continue
            iterators.append(it)
            heap.append((key, idx, fact, it))

        heapq.heapify(heap)

        while heap:
            key, idx, fact, it = heapq.heappop(heap)
            yield fact
            try:
                next_key, next_fact = next(it)
                heapq.heappush(heap, (next_key, idx, next_fact, it))
            except StopIteration:
                continue


def _parse_date_int(value: int) -> Optional[datetime]:
    """
    Try to interpret an integer timestamp as YYYYMMDD.
    Returns datetime.date if possible, otherwise None.
    """

    s = str(value)
    if len(s) == 8:
        try:
            return datetime.strptime(s, "%Y%m%d")
        except ValueError:
            return None
    return None


def _tick_shift(value: int, delta_days: int) -> int:
    """
    Shift YYYYMMDD integer by delta_days.
    If parsing fails, fall back to simple integer addition.
    """

    dt = _parse_date_int(value)
    if dt is None:
        return value + delta_days
    new_dt = dt + timedelta(days=delta_days)
    return int(new_dt.strftime("%Y%m%d"))


def next_tick(value: int) -> int:
    return _tick_shift(value, 1)


def prev_tick(value: int) -> int:
    return _tick_shift(value, -1)


def advance_time(value: int, delta_days: int) -> int:
    return _tick_shift(value, delta_days)


def get_db_size(db_path: str) -> float:
    """
    Measure the total size of Neo4j database directory (MB).
    Applicable to Neo4j Desktop version, directly counting the entire database directory.
    """
    if not os.path.exists(db_path):
        LOGGER.warning("[Measure] Warning: Database path %s not found. Returning 0.", db_path)
        return 0.0

    total_bytes = 0
    try:
        for root, dirs, files in os.walk(db_path):
            for filename in files:
                file_path = os.path.join(root, filename)
                try:
                    total_bytes += os.path.getsize(file_path)
                except (OSError, FileNotFoundError):
                    continue
    except Exception as exc:  # pragma: no cover
        LOGGER.warning("[Measure] Error measuring disk size: %s", exc)
        return 0.0

    if total_bytes == 0:
        LOGGER.warning("[Measure] Warning: No files found in %s.", db_path)

    return total_bytes / (1024 * 1024.0)


def wait_db_status(
    admin_driver,
    db_name: str,
    desired: str = "online",
    timeout_s: int = 120,
    poll_s: float = 1.0,
) -> None:
    """
    Poll system database until database reaches desired status.
    Supported statuses: online, offline, absent
    """
    deadline = time.time() + timeout_s
    desired_lower = desired.lower()
    while time.time() < deadline:
        try:
            res = admin_driver.execute_query(
                "SHOW DATABASES YIELD name, currentStatus WHERE name = $n RETURN currentStatus",
                {"n": db_name},
                database_="system",
            )
            records = list(res.records)
            if records:
                status = str(records[0].get("currentStatus", "")).lower()
                if status == desired_lower:
                    return
            else:
                # If records is empty, database does not exist (absent)
                if desired_lower == "absent":
                    return
        except Exception:  # pragma: no cover - only for polling
            pass
        time.sleep(poll_s)
    raise TimeoutError(f"Timeout waiting for database {db_name} status='{desired}' ({timeout_s}s)")


def prepare_isolated_db(uri: str, user: str, password: str, db_name: str) -> None:
    """
    Delete and recreate specified database. Requires admin privileges.
    """
    driver = GraphDatabase.driver(uri, auth=basic_auth(user, password))
    try:
        # Use backticks to quote database name, supporting special characters
        db_name_quoted = f"`{db_name}`"
        driver.execute_query(f"DROP DATABASE {db_name_quoted} IF EXISTS", database_="system")
        try:
            wait_db_status(driver, db_name, desired="absent", timeout_s=30, poll_s=0.5)
        except TimeoutError:
            pass
        driver.execute_query(f"CREATE DATABASE {db_name_quoted}", database_="system")
        LOGGER.info("[Harness] Database %s created, waiting for online...", db_name)
        wait_db_status(driver, db_name, desired="online", timeout_s=180, poll_s=1.0)
        LOGGER.info("[Harness] Database %s is now online.", db_name)
    except Exception as exc:
        raise RuntimeError(f"Failed to create/reset database {db_name}: {exc}") from exc
    finally:
        driver.close()


def stop_database(uri: str, user: str, password: str, db_name: str) -> None:
    """
    Stop specified database. Requires admin privileges.
    """
    driver = GraphDatabase.driver(uri, auth=basic_auth(user, password))
    try:
        db_name_quoted = f"`{db_name}`"
        driver.execute_query(f"STOP DATABASE {db_name_quoted}", database_="system")
        LOGGER.info("[Harness] Stopping database %s, waiting for offline...", db_name)
        try:
            wait_db_status(driver, db_name, desired="offline", timeout_s=30, poll_s=0.5)
        except TimeoutError:
            LOGGER.warning("[Harness] Timeout waiting for database offline, continuing")
    except Exception as exc:
        raise RuntimeError(f"Failed to stop database {db_name}: {exc}") from exc
    finally:
        driver.close()


def start_database(uri: str, user: str, password: str, db_name: str) -> None:
    """
    Start specified database. Requires admin privileges.
    """
    driver = GraphDatabase.driver(uri, auth=basic_auth(user, password))
    try:
        db_name_quoted = f"`{db_name}`"
        driver.execute_query(f"START DATABASE {db_name_quoted}", database_="system")
        LOGGER.info("[Harness] Starting database %s, waiting for online...", db_name)
        try:
            wait_db_status(driver, db_name, desired="online", timeout_s=30, poll_s=0.5)
        except TimeoutError:
            LOGGER.warning("[Harness] Timeout waiting for database online, continuing")
    except Exception as exc:
        raise RuntimeError(f"Failed to start database {db_name}: {exc}") from exc
    finally:
        driver.close()


def ensure_sorted(iterator: Iterable[Fact], key_fn: Callable[[Fact], Any]) -> Iterator[Fact]:
    """
    Helper for demos/tests where fact volume is small and can be sorted directly.
    For real large-scale tasks, sorting should be guaranteed at the data source side
    (e.g., generator or external sorting).
    """

    materialized = list(iterator)
    materialized.sort(key=key_fn)
    for fact in materialized:
        yield fact


# ---------------------------------------------------------------------------
# Neo4j ETL Runner
# ---------------------------------------------------------------------------


class ETLRunner:
    def __init__(
        self,
        uri: str,
        user: str,
        password: str,
        batch_size: int = 5_000,
        database: str = "neo4j",
    ):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.batch_size = batch_size
        self.database = database
        LOGGER.info("ETL Runner Initialized (Streaming-enabled).")

    def close(self) -> None:
        self.driver.close()
        LOGGER.info("Connection closed.")

    # --------------------------- infrastructure ---------------------------

    def _run_batch_query(
        self,
        session,
        query: str,
        batches: Iterable[Dict[str, Any]],
        batch_key: str = "batch",
    ) -> None:
        """
        Execute UNWIND-style query via batches iterable.
        Accepts either list/iterator or JsonlSpool iterator.
        """

        buffer = BatchBuffer(
            self.batch_size,
            lambda chunk: session.run(query, {batch_key: chunk}),
        )
        for record in batches:
            buffer.add(record)
        buffer.flush()

    def _run_batch_query_with_progress(
        self,
        session,
        query: str,
        batches: Iterable[Dict[str, Any]],
        item_name: str,
        batch_size: int,
        batch_key: str = "batch",
        progress_interval: int = 10,
        model_tag: str = "A",
    ) -> None:
        """
        Execute batch query with progress display.
        """
        batch_count = 0
        start_time = time.time()
        buffer: List[Dict[str, Any]] = []

        def flush_batch():
            nonlocal batch_count
            if not buffer:
                return
            session.run(query, {batch_key: buffer.copy()}).consume()
            batch_count += 1
            if batch_count % progress_interval == 0:
                elapsed = time.time() - start_time
                LOGGER.info("[Model %s] %s loading progress: %d batches (%d records), elapsed %.1f seconds",
                           model_tag, item_name, batch_count, batch_count * batch_size, elapsed)
            buffer.clear()

        for record in batches:
            buffer.append(record)
            if len(buffer) >= batch_size:
                flush_batch()

        # Process last batch
        flush_batch()

        total_elapsed = time.time() - start_time
        LOGGER.info("[Model %s] %s loading completed, total %d batches, elapsed %.2f seconds", model_tag, item_name, batch_count, total_elapsed)

    # --------------------------- admin helpers ---------------------------

    def clear_database_and_setup_indices(self, labels: Iterable[str]) -> None:
        LOGGER.info("Clearing database and setting up indices...")
        with self.driver.session(database=self.database) as session:
            # First drop all constraints and indexes (avoid constraint conflicts when deleting data)
            try:
                for con in session.run("SHOW CONSTRAINTS"):
                    session.run(f"DROP CONSTRAINT {con['name']}")
            except Exception as exc:  # pragma: no cover
                LOGGER.warning("Error dropping constraints: %s", exc)
            try:
                for idx in session.run("SHOW INDEXES"):
                    if idx["type"] != "LOOKUP":
                        session.run(f"DROP INDEX {idx['name']}")
            except Exception as exc:  # pragma: no cover
                LOGGER.warning("Error dropping indexes: %s", exc)

            # Then delete all data
            session.run("MATCH (n) DETACH DELETE n")
            # Ensure data is completely deleted (commit transaction)
            session.run("RETURN 1")

            # Finally recreate constraints and indexes
            for label in labels:
                session.run(
                    f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE"
                )
                session.run(
                    f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}_State) REQUIRE n.sid IS UNIQUE"
                )
                session.run(
                    f"CREATE INDEX IF NOT EXISTS FOR (n:{label}_State) ON (n.id)"
                )
                session.run(
                    f"CREATE INDEX IF NOT EXISTS FOR (n:{label}_State) ON (n.start, n.end)"
                )
                # Composite index: optimize Snapshot and Evolution queries (precise location by id and time range)
                session.run(
                    f"CREATE INDEX IF NOT EXISTS FOR (n:{label}_State) ON (n.id, n.start, n.end)"
                )
            session.run(
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:ProcessNode) REQUIRE n.uid IS UNIQUE"
            )
            session.run("CREATE INDEX IF NOT EXISTS FOR (n:ProcessNode) ON (n.attr)")
            session.run(
                "CREATE INDEX IF NOT EXISTS FOR (n:ProcessNode) ON (n.start, n.end)"
            )
        LOGGER.info("Indices and constraints are set.")

    # ------------------------------------------------------------------
    # Model A – Attribute-embedded arrays
    # ------------------------------------------------------------------

    def run_etl_model_a(self, facts_iter: Iterable[Fact], static_attrs: Iterable[str]) -> float:
        """
        Streaming load:
            pass 1 -> Aggregate dynamic arrays for the same entity + write to entity_spool
            pass 1 -> Simultaneously write relationships to rel_spool
            pass 2 -> Read entity_spool and write nodes
            pass 3 -> Read rel_spool and write relationships
        """

        static_attrs = set(static_attrs)
        entity_spool = JsonlSpool()
        rel_spool = JsonlSpool()

        start_transform = time.time()
        current_entity: Optional[Dict[str, Any]] = None
        last_entity_id: Optional[str] = None

        # Optimization: Maintain node id to label mapping to determine target node labels
        # This ensures that both source and target nodes can use indexes in the Load phase
        node_id_to_label: Dict[str, str] = {}

        def flush_entity(entity_payload: Optional[Dict[str, Any]]) -> None:
            if not entity_payload:
                return
            # Optimization: Lazy serialization, serialize only once when writing
            # Original implementation: Called json.dumps for each history item, causing excessive serialization
            # New implementation: Store dictionary list directly, serialize uniformly with json.dumps when writing to spool
            # This reduces serialization count: from N times (each history item) to 1 time (entire record)
            # Merge static_props and dynamic_props, write in one go
            all_props = {**entity_payload["static_props"], **entity_payload["dynamic_props"]}
            entity_spool.write({
                "id": entity_payload["id"],
                "label": entity_payload["label"],
                "props": all_props,
            })
            # Record node id to label mapping
            node_id_to_label[entity_payload["id"]] = entity_payload["label"]

        for fact in facts_iter:
            if fact.entity_id != last_entity_id:
                flush_entity(current_entity)
                current_entity = {
                    "id": fact.entity_id,
                    "label": fact.entity_label,
                    "static_props": {},
                    "dynamic_props": defaultdict(list),
                }
                last_entity_id = fact.entity_id

            assert current_entity is not None  # for type checkers

            if fact.attribute in static_attrs:
                if fact.attribute == "location" and isinstance(fact.value, str):
                    current_entity["static_props"][fact.attribute] = fact.value
                else:
                    current_entity["static_props"][fact.attribute] = fact.value
                continue

            if fact.value_type == 0:
                hist_key = f"{fact.attribute}_history"
                current_entity["dynamic_props"][hist_key].append(
                    {"start": fact.start, "end": fact.end, "value": fact.value}
                )
            else:
                # Optimization: Determine target node label
                # Prefer node_id_to_label mapping, if not exists infer from target_id
                # If inference fails, keep as None, Load phase will use optimized fallback query (ensures correctness)
                target_id = fact.value
                label_to = node_id_to_label.get(target_id)
                if label_to is None:
                    # If target node not in mapping, try to infer label from target_id (using cache)
                    # Example: Region_0 -> Region, City_1 -> City
                    inferred = infer_label_from_id(target_id)
                    if inferred:
                        label_to = inferred
                        LOGGER.debug("[Model A] Infer label from target_id: %s -> %s", target_id, label_to)
                        # Cache inference result to avoid repeated inference
                        node_id_to_label[target_id] = label_to
                    else:
                        # If inference fails, keep as None, Load phase will use optimized fallback query
                        # This ensures correctness: won't create incorrect relationships
                        label_to = None
                        LOGGER.debug("[Model A] Cannot infer label from target_id: %s, will query in Load phase", target_id)

                rel_spool.write(
                    {
                        "id_from": fact.entity_id,
                        "label_from": fact.entity_label,
                        "rel_type": fact.attribute.upper(),
                        "id_to": target_id,
                        "label_to": label_to,  # May be None, Load phase will use fallback query
                        "props": {"start": fact.start, "end": fact.end},
                    }
                )

        flush_entity(current_entity)
        transform_cost = time.time() - start_transform
        LOGGER.info("[Model A] Transform Cost: %.4f s", transform_cost)

        start_load = time.time()
        with self.driver.session(database=self.database) as session:
            # Optimization: All properties are already merged in Transform phase, use props directly here
            # Avoids FOREACH loop, significantly improves performance
            # Note: History items of dynamic attributes need to be serialized to JSON strings
            #       (because queries use apoc.convert.fromJsonMap)
            # We serialize in batches in Load phase, not serialize each history item in Transform phase
            LOGGER.info("[Model A] Starting node loading...")
            # Optimization: Batch serialize history items of dynamic attributes in Load phase,
            #               rather than serializing one by one in Transform phase
            # This reduces serialization overhead
            def preprocess_entity_batch(entities):
                """Preprocess node data, serialize history items of dynamic attributes"""
                batch = []
                for e in entities:
                    props = e["props"].copy()
                    # Serialize all attributes ending with _history (dict list -> JSON string list)
                    for key in list(props.keys()):
                        if key.endswith("_history") and isinstance(props[key], list):
                            props[key] = [json.dumps(item) if isinstance(item, dict) else item
                                         for item in props[key]]
                    batch.append({
                        "id": e["id"],
                        "label": e["label"],
                        "props": props
                    })
                return batch

            # Group by label, build dynamic query for each label
            entities_by_label: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
            for record in iter(entity_spool):
                label = record["label"]
                entities_by_label[label].append(record)

            batch_count = 0
            node_start = time.time()

            for label, entities in entities_by_label.items():
                # Build dynamic query for each label, use MERGE and SET to ensure properties are correctly set
                # Note: SET n += e.props sets all key-value pairs in e.props dictionary as node properties
                query_nodes_labeled = f"""
                UNWIND $batch AS e
                MERGE (n:{label} {{id: e.id}})
                SET n += e.props
                RETURN count(n) AS count
                """

                # Preprocess and batch process
                buffer: List[Dict[str, Any]] = []
                for entity in entities:
                    buffer.append(entity)
                    if len(buffer) >= self.batch_size:
                        preprocessed = preprocess_entity_batch(buffer)
                        result = session.run(query_nodes_labeled, {"batch": preprocessed})
                        count = result.single()["count"]
                        if count != len(preprocessed):
                            LOGGER.warning("[Model A] Batch processing result inconsistent: expected %d, actual %d", len(preprocessed), count)
                        batch_count += 1
                        if batch_count % 10 == 0:
                            elapsed = time.time() - node_start
                            LOGGER.info("[Model A] Node loading progress: %d batches (%d records), elapsed %.1f seconds",
                                       batch_count, batch_count * self.batch_size, elapsed)
                        buffer.clear()

                # Process last batch
                if buffer:
                    preprocessed = preprocess_entity_batch(buffer)
                    result = session.run(query_nodes_labeled, {"batch": preprocessed})
                    count = result.single()["count"]
                    if count != len(preprocessed):
                        LOGGER.warning("[Model A] Last batch processing result inconsistent: expected %d, actual %d", len(preprocessed), count)
                    batch_count += 1

            node_elapsed = time.time() - node_start
            LOGGER.info("[Model A] Node loading completed, total %d batches, elapsed %.2f seconds", batch_count, node_elapsed)

            LOGGER.info("[Model A] Starting relationship loading...")

            # Optimization strategy: Stream process relationships, group by relationship type, source label and target label,
            #                       use direct CREATE statements
            # This ensures both source and target nodes can use indexes, optimal performance
            # Since we need to query target node labels first, use single-pass traversal strategy (merge two traversals)
            
            LOGGER.info("[Model A] Starting stream processing relationships...")
            # First pass: Collect target node IDs that need label querying, temporarily store relationships (if label_to is None)
            target_ids_without_label = set()
            rels_without_label: List[Dict[str, Any]] = []  # Temporarily store relationships with label_to=None
            rel_by_type_and_labels: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
            stream_batch_size = 50000
            rel_batch_size = min(self.batch_size * 5, 20000)
            total_rels = 0
            
            def process_relationship_group(key: tuple, rels: List[Dict[str, Any]]):
                """Process a group of relationships"""
                rel_type, label_from, label_to = key
                if label_to == "__UNKNOWN__":
                    # Use generic query (label not specified)
                    query_rels = f"""
                    UNWIND $batch AS r
                    MATCH (a:{label_from} {{id: r.id_from}})
                    WITH a, r
                    MATCH (b {{id: r.id_to}})
                    CREATE (a)-[:{rel_type} {{start: r.start, end: r.end}}]->(b)
                    RETURN count(*) AS count
                    """
                else:
                    # Explicitly specify label, use index
                    query_rels = f"""
                    UNWIND $batch AS r
                    MATCH (a:{label_from} {{id: r.id_from}})
                    WITH a, r
                    MATCH (b:{label_to} {{id: r.id_to}})
                    CREATE (a)-[:{rel_type} {{start: r.start, end: r.end}}]->(b)
                    RETURN count(*) AS count
                    """
                
                batch_data = []
                for rel in rels:
                    batch_data.append({
                        "id_from": rel["id_from"],
                        "id_to": rel["id_to"],
                        "start": rel["props"]["start"],
                        "end": rel["props"]["end"]
                    })
                
                batch_count = 0
                for i in range(0, len(batch_data), rel_batch_size):
                    batch = batch_data[i:i + rel_batch_size]
                    session.run(query_rels, {"batch": batch}).consume()
                    batch_count += 1
                    if batch_count % 10 == 0:
                        LOGGER.info("[Model A] Relationship (type: %s, from: %s, to: %s) loading progress: %d batches",
                                   rel_type, label_from, label_to, batch_count)
            
            # Single-pass traversal: Collect node IDs that need querying, simultaneously process relationships with existing labels
            for batch in batch_iter(iter(rel_spool), stream_batch_size):
                for rel_record in batch:
                    if rel_record.get("label_to") is None and node_id_to_label.get(rel_record.get("id_to")) is None:
                        # Relationships that need label querying, temporarily store
                        target_ids_without_label.add(rel_record["id_to"])
                        rels_without_label.append(rel_record)
                    else:
                        # Relationships with existing labels, directly group and process
                        rel_type = rel_record["rel_type"]
                        label_from = rel_record["label_from"]
                        label_to = node_id_to_label.get(rel_record.get("id_to")) if rel_record.get("label_to") is None else rel_record["label_to"]
                        rel_by_type_and_labels[(rel_type, label_from, label_to)].append(rel_record)
                
                # If a group reaches threshold, process a batch (only process relationships with existing labels)
                for key, rels in list(rel_by_type_and_labels.items()):
                    if len(rels) >= stream_batch_size:
                        process_relationship_group(key, rels)
                        total_rels += len(rels)
                        rel_by_type_and_labels[key] = []  # Clear, free memory

            # Query target node labels (if needed)
            # Optimization: First try to get all existing labels from database, then try these labels
            # This adapts to real data, no hardcoded labels
            target_label_map = {}
            if target_ids_without_label:
                LOGGER.info("[Model A] Querying labels for %d target nodes...", len(target_ids_without_label))
                
                # Step 1: Get all existing labels from database (for query optimization)
                # This adapts to real data, no hardcoded labels
                query_all_labels = """
                CALL db.labels() YIELD label
                RETURN collect(label) AS labels
                """
                result = session.run(query_all_labels)
                all_labels = result.single()["labels"] if result.single() else []
                
                if all_labels:
                    LOGGER.debug("[Model A] Found labels in database: %s", all_labels)
                    # Query for each existing label (optimization: use index)
                    for label in all_labels:
                        query_target_labels = f"""
                        UNWIND $ids AS id
                        MATCH (n:{label} {{id: id}})
                        RETURN n.id AS id, labels(n) AS labels
                        """
                        result = session.run(query_target_labels, {"ids": list(target_ids_without_label)})
                        for record in result:
                            node_id = record["id"]
                            if node_id not in target_label_map:  # Avoid overwriting already found labels
                                node_labels = record["labels"]
                                if node_labels:
                                    # Use first label (usually only one)
                                    target_label_map[node_id] = node_labels[0]
                
                # If there are still unfound nodes, use generic query (fallback)
                remaining_ids = [id for id in target_ids_without_label if id not in target_label_map]
                if remaining_ids:
                    LOGGER.debug("[Model A] %d target nodes cannot be found via label query, using generic query", len(remaining_ids))
                    query_target_labels_fallback = """
                    UNWIND $ids AS id
                    MATCH (n {id: id})
                    RETURN n.id AS id, labels(n) AS labels
                    """
                    result = session.run(query_target_labels_fallback, {"ids": remaining_ids})
                    for record in result:
                        node_id = record["id"]
                        node_labels = record["labels"]
                        if node_labels:
                            target_label_map[node_id] = node_labels[0]
                
                # Update label_to for temporarily stored relationships
                for rel_record in rels_without_label:
                    rel_record["label_to"] = target_label_map.get(rel_record["id_to"])
                    if rel_record["label_to"] is None:
                        LOGGER.warning("[Model A] Warning: Cannot determine label for target node %s, will use generic query", rel_record["id_to"])
                    
                    rel_type = rel_record["rel_type"]
                    label_from = rel_record["label_from"]
                    label_to = rel_record.get("label_to")
                    if label_to is None:
                        label_to = "__UNKNOWN__"
                    rel_by_type_and_labels[(rel_type, label_from, label_to)].append(rel_record)
                
                # Clear temporary storage, free memory
                rels_without_label = []
            
            # Process all relationships (including those with existing labels and those with queried labels)
            for key, rels in list(rel_by_type_and_labels.items()):
                if len(rels) >= stream_batch_size:
                    process_relationship_group(key, rels)
                    total_rels += len(rels)
                    rel_by_type_and_labels[key] = []  # Clear, free memory
            
            # Process remaining relationships
            for key, rels in rel_by_type_and_labels.items():
                if rels:
                    process_relationship_group(key, rels)
                    total_rels += len(rels)
            
            LOGGER.info("[Model A] Total relationships: %d, groups: %d", total_rels, len(rel_by_type_and_labels))

        load_cost = time.time() - start_load
        LOGGER.info("[Model A] Load Cost: %.4f s", load_cost)
        return transform_cost + load_cost

    # ------------------------------------------------------------------
    # Model B – State/version model
    # ------------------------------------------------------------------

    def run_etl_model_b(self, facts_iter: Iterable[Fact], static_attrs: Iterable[str]) -> float:
        """
        Model B Transform phase: State Version Model

        Design Philosophy:
        - Create independent state nodes for each state change
        - State nodes contain all attributes at that time point (static attributes + dynamic attributes)
        - Connect state nodes via EVOLVED_TO relationships, forming state evolution chains
        - Relationships are also attached to state nodes

        Processing Flow:
        1. Collect static attributes to entity_static
        2. Group dynamic facts by entity
        3. For each entity, generate state node sequence
        4. Create EVOLVED_TO relationships between state nodes
        5. Create relationships on state nodes (pointing to other entities)
        """
        static_attrs = set(static_attrs)
        states_spool = JsonlSpool()
        evo_links_spool = JsonlSpool()
        rel_spool = JsonlSpool()

        start_transform = time.time()

        # Step 1: Collect static attributes (grouped by entity)
        entity_static: Dict[str, Dict[str, Any]] = defaultdict(dict)
        entity_id_to_label: Dict[str, str] = {}
        entity_facts_map: Dict[str, List[Fact]] = defaultdict(list)

        # Iterate through all facts, separate static attributes and dynamic facts
        for fact in facts_iter:
            if fact.attribute in static_attrs:
                # Static attributes: collect to entity_static
                bucket = entity_static[fact.entity_id]
                bucket["id"] = fact.entity_id
                bucket["label"] = fact.entity_label
                bucket[fact.attribute] = fact.value
                # Record entity_id to label mapping
                if fact.entity_label:
                    entity_id_to_label[fact.entity_id] = fact.entity_label
            else:
                # Dynamic facts: group by entity
                entity_facts_map[fact.entity_id].append(fact)
                # Record entity_id to label mapping (if not already present)
                if fact.entity_id not in entity_id_to_label and fact.entity_label:
                    entity_id_to_label[fact.entity_id] = fact.entity_label

        # Step 2: For each entity, generate state node sequence
        states_exist: set[str] = set()
        for entity_id, facts in entity_facts_map.items():
            # Get static properties
            static_props = entity_static.get(entity_id, {})
            entity_label = static_props.get("label", "") or entity_id_to_label.get(entity_id, "")

            # Process state evolution for this entity
            produced = self._process_state_entity(
                entity_id=entity_id,
                label=entity_label,
                facts=facts,
                static_props=static_props,
                states_spool=states_spool,
                evo_links_spool=evo_links_spool,
                rel_spool=rel_spool,
                entity_id_to_label=entity_id_to_label,
            )
            if produced:
                states_exist.add(entity_id)

        # Step 3: Process entities with only static attributes but no dynamic facts (sentinel state nodes)
        sentinel_end = 9_9999_9999
        for entity_id, static_props in entity_static.items():
            if entity_id in states_exist:
                continue
            label = static_props.get("label", "")
            if not label:
                continue
            sentinel_state = {
                "id": entity_id,
                "label": f"{label}_State",
                "sid": f"{entity_id}_static",
                "start": 0,
                "end": sentinel_end,
            }
            # Add static attributes (exclude label and id)
            sentinel_state.update({k: v for k, v in static_props.items() if k not in {"label", "id"}})
            states_spool.write(sentinel_state)
            states_exist.add(entity_id)

        transform_cost = time.time() - start_transform
        LOGGER.info("[Model B] Transform Cost: %.4f s", transform_cost)

        start_load = time.time()
        with self.driver.session(database=self.database) as session:
            # Optimization 1: Use direct CREATE, faster than apoc.create.node
            # Optimization 2: Add progress display
            # Optimization 3: Group by label, ensure index usage
            LOGGER.info("[Model B] Starting state node loading...")

            # Optimization: Stream process state nodes, avoid loading all data into memory at once
            # Since labels are dynamic, we need to process by label groups
            # Strategy: Stream process, when encountering new label, finish processing old label batch,
            #           release memory after each batch
            LOGGER.info("[Model B] Starting stream loading state nodes...")
            
            def process_label_batch(label: str, batch: List[Dict[str, Any]], seen_sids: Dict[str, Dict[str, Any]], 
                                   duplicate_sids: List[str]) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
                """Process a label batch, perform deduplication"""
                for state in batch:
                    sid = state.get("sid")
                    if sid and sid not in seen_sids:
                        seen_sids[sid] = state
                    elif sid:
                        # Check if duplicate sid corresponds to same state node
                        existing_state = seen_sids[sid]
                        if existing_state != state:
                            LOGGER.error("[Model B] Found duplicate sid but states differ: %s", sid)
                            LOGGER.error("[Model B]   First state: %s", existing_state)
                            LOGGER.error("[Model B]   Duplicate state: %s", state)
                        else:
                            LOGGER.debug("[Model B] Found duplicate sid but states same: %s (normal, skip)", sid)
                        duplicate_sids.append(sid)
                return seen_sids, duplicate_sids
            
            def load_label_batch(label: str, unique_states: List[Dict[str, Any]], states_batch_size: int):
                """Load deduplicated state nodes to database"""
                if not unique_states:
                    return
                
                query_states_labeled = f"""
                UNWIND $batch AS s
                CREATE (n:{label})
                SET n = s
                RETURN count(n) AS count
                """
                
                batch_count = 0
                label_start = time.time()
                for i in range(0, len(unique_states), states_batch_size):
                    batch = unique_states[i:i + states_batch_size]
                    session.run(query_states_labeled, {"batch": batch}).consume()
                    batch_count += 1
                    if batch_count % 10 == 0:
                        elapsed = time.time() - label_start
                        LOGGER.info("[Model B] State nodes (label: %s) loading progress: %d batches (%d nodes), elapsed %.1f seconds",
                                   label, batch_count, batch_count * states_batch_size, elapsed)
                
                label_elapsed = time.time() - label_start
                LOGGER.info("[Model B] State nodes (label: %s) loading completed, total %d batches, elapsed %.2f seconds",
                           label, batch_count, label_elapsed)
            
            states_start = time.time()
            states_batch_size = min(self.batch_size * 2, 15000)
            stream_batch_size = 50000  # Stream processing batch size
            
            # Stream processing: Group by label, process while reading
            current_label = None
            current_batch: List[Dict[str, Any]] = []
            all_state_labels = set()
            seen_sids_by_label: Dict[str, Dict[str, Dict[str, Any]]] = {}  # label -> {sid -> state}
            duplicate_sids_by_label: Dict[str, List[str]] = {}  # label -> [sid, ...]
            total_states = 0
            
            for state_record in iter(states_spool):
                label = state_record.get("label", "")
                if label:
                    all_state_labels.add(label)
                
                # If encountering new label, finish processing old label batch
                if label != current_label and current_batch:
                    # Process current label batch
                    if current_label not in seen_sids_by_label:
                        seen_sids_by_label[current_label] = {}
                        duplicate_sids_by_label[current_label] = []
                    
                    seen_sids, duplicate_sids = process_label_batch(
                        current_label, current_batch, 
                        seen_sids_by_label[current_label], 
                        duplicate_sids_by_label[current_label]
                    )
                    seen_sids_by_label[current_label] = seen_sids
                    duplicate_sids_by_label[current_label] = duplicate_sids
                    
                    # If current label batch reaches threshold, load to database and clear
                    unique_states = list(seen_sids.values())
                    if len(unique_states) >= stream_batch_size:
                        load_label_batch(current_label, unique_states, states_batch_size)
                        total_states += len(unique_states)
                        # Clear, free memory (but keep seen_sids for subsequent deduplication)
                        seen_sids_by_label[current_label] = {}
                    
                    current_batch = []  # Free memory
                
                current_label = label
                current_batch.append(state_record)
                
                # If batch reaches threshold, process a batch
                if len(current_batch) >= stream_batch_size:
                    if current_label not in seen_sids_by_label:
                        seen_sids_by_label[current_label] = {}
                        duplicate_sids_by_label[current_label] = []
                    
                    seen_sids, duplicate_sids = process_label_batch(
                        current_label, current_batch,
                        seen_sids_by_label[current_label],
                        duplicate_sids_by_label[current_label]
                    )
                    seen_sids_by_label[current_label] = seen_sids
                    duplicate_sids_by_label[current_label] = duplicate_sids
                    
                    # Load to database
                    unique_states = list(seen_sids.values())
                    if len(unique_states) >= stream_batch_size:
                        load_label_batch(current_label, unique_states, states_batch_size)
                        total_states += len(unique_states)
                        seen_sids_by_label[current_label] = {}
                    
                    current_batch = []  # Free memory
            
            # Process last batch
            if current_batch:
                if current_label not in seen_sids_by_label:
                    seen_sids_by_label[current_label] = {}
                    duplicate_sids_by_label[current_label] = []
                
                seen_sids, duplicate_sids = process_label_batch(
                    current_label, current_batch,
                    seen_sids_by_label[current_label],
                    duplicate_sids_by_label[current_label]
                )
                seen_sids_by_label[current_label] = seen_sids
                duplicate_sids_by_label[current_label] = duplicate_sids
            
            # Process all remaining label batches
            for label in all_state_labels:
                if label in seen_sids_by_label and seen_sids_by_label[label]:
                    seen_sids = seen_sids_by_label[label]
                    unique_states = list(seen_sids.values())
                    if unique_states:
                        load_label_batch(label, unique_states, states_batch_size)
                        total_states += len(unique_states)
                    
                    # Record deduplication info
                    if label in duplicate_sids_by_label and duplicate_sids_by_label[label]:
                        duplicate_sids = duplicate_sids_by_label[label]
                        LOGGER.warning("[Model B] State nodes (label: %s) deduplication: removed %d duplicate nodes",
                                     label, len(duplicate_sids))
            
            states_elapsed = time.time() - states_start
            LOGGER.info("[Model B] All state nodes loaded, total state nodes: %d, labels: %d, total elapsed %.2f seconds", 
                       total_states, len(all_state_labels), states_elapsed)

            # Optimization: Stream process evolution relationships, avoid loading all data into memory at once
            LOGGER.info("[Model B] Starting evolution relationship loading...")

            # Stream processing: Batch read, group and process while reading
            evo_links_by_labels: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
            stream_batch_size = 50000
            links_batch_size = min(self.batch_size * 3, 20000)
            total_links = 0
            
            def process_evolution_links_batch(key: tuple, links: List[Dict[str, Any]]):
                """Process a batch of evolution relationships"""
                from_label, to_label = key
                if from_label == "__UNKNOWN__" or to_label == "__UNKNOWN__":
                    # Fallback: Use generic query (should not happen)
                    LOGGER.warning("[Model B] Using fallback query to load evolution relationships (missing label info)")
                    query_links = """
                    UNWIND $batch AS l
                    MATCH (s1)
                    WHERE s1.sid = l.from_sid
                    WITH s1, l
                    MATCH (s2)
                    WHERE s2.sid = l.to_sid
                    CREATE (s1)-[:EVOLVED_TO {time: l.time}]->(s2)
                    RETURN count(*) AS count
                    """
                else:
                    # Optimization: Explicitly specify label, use unique constraint index
                    query_links = f"""
                    UNWIND $batch AS l
                    MATCH (s1:{from_label} {{sid: l.from_sid}})
                    WITH s1, l
                    MATCH (s2:{to_label} {{sid: l.to_sid}})
                    CREATE (s1)-[:EVOLVED_TO {{time: l.time}}]->(s2)
                    RETURN count(*) AS count
                    """
                
                batch_count = 0
                for i in range(0, len(links), links_batch_size):
                    batch = links[i:i + links_batch_size]
                    session.run(query_links, {"batch": batch}).consume()
                    batch_count += 1
                    if batch_count % 10 == 0:
                        LOGGER.info("[Model B] Evolution relationships (from: %s, to: %s) loading progress: %d batches",
                                   from_label, to_label, batch_count)
            
            # Read and process in batches
            for batch in batch_iter(iter(evo_links_spool), stream_batch_size):
                for link in batch:
                    from_label = link.get("from_label", "")
                    to_label = link.get("to_label", "")
                    if from_label and to_label:
                        evo_links_by_labels[(from_label, to_label)].append(link)
                    else:
                        # If label is missing (should not happen), use fallback
                        LOGGER.warning("[Model B] Evolution relationship missing label info: %s", link)
                        evo_links_by_labels[("__UNKNOWN__", "__UNKNOWN__")].append(link)
                
                # If a group reaches threshold, process a batch
                for key, links in list(evo_links_by_labels.items()):
                    if len(links) >= stream_batch_size:
                        process_evolution_links_batch(key, links)
                        total_links += len(links)
                        evo_links_by_labels[key] = []  # Clear, free memory
            
            # Process remaining relationships
            for key, links in evo_links_by_labels.items():
                if links:
                    process_evolution_links_batch(key, links)
                    total_links += len(links)
            
            LOGGER.info("[Model B] Total evolution relationships: %d", total_links)

            # Optimization: Stream process relationships to avoid loading all data into memory at once
            # 1. Source node explicitly specifies label, uses sid index
            # 2. Target node explicitly specifies label, uses id index and time range filter
            # 3. Use direct CREATE, faster than apoc.create.relationship
            # 4. Target node label already determined in Transform phase, no need to query database
            LOGGER.info("[Model B] Starting to load relationships...")

            # Stream processing: Read in batches, group and process while reading
            rel_by_labels: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
            stream_batch_size = 50000
            rel_batch_size = min(self.batch_size * 5, 20000)
            total_rels = 0
            missing_labels_count = 0
            
            def process_relationship_batch(key: tuple, rels: List[Dict[str, Any]]):
                """Process a batch of relationships"""
                from_label, target_label = key
                # Group by relationship type
                rel_by_type: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
                for rel in rels:
                    rel_by_type[rel["rel_type"]].append(rel)
                
                for rel_type, typed_rels in rel_by_type.items():
                    LOGGER.info("[Model B] Starting to load relationship type %s (from: %s, to: %s), total %d",
                               rel_type, from_label, target_label, len(typed_rels))
                    
                    # Optimization: Use explicit label matching, fully utilize unique constraint index
                    if from_label == "__UNKNOWN__" or target_label == "__UNKNOWN__":
                        # Fallback: Use generic query (should not happen)
                        LOGGER.warning("[Model B] Using fallback query to load relationships (missing label info)")
                        query_rels_typed = f"""
                        UNWIND $batch AS r
                        MATCH (a)
                        WHERE a.sid = r.from_sid
                        WITH a, r
                        MATCH (b)
                        WHERE b.id = r.to_target_id
                          AND b.start IS NOT NULL
                          AND b.start <= r.rel_time
                          AND (b.end IS NULL OR b.end >= r.rel_time)
                        CREATE (a)-[:{rel_type}]->(b)
                        RETURN count(*) AS count
                        """
                    else:
                        # Optimization: Explicitly specify from_label and target_label, use unique constraint index
                        query_rels_typed = f"""
                        UNWIND $batch AS r
                        MATCH (a:{from_label} {{sid: r.from_sid}})
                        WITH a, r
                        MATCH (b:{target_label} {{id: r.to_target_id}})
                        WHERE b.start IS NOT NULL
                          AND b.start <= r.rel_time
                          AND (b.end IS NULL OR b.end >= r.rel_time)
                        CREATE (a)-[:{rel_type}]->(b)
                        RETURN count(*) AS count
                        """
                    
                    batch_data = []
                    for rel in typed_rels:
                        batch_data.append({
                            "from_sid": rel["from_sid"],
                            "to_target_id": rel["to_target_id"],
                            "rel_time": rel["rel_time"]
                        })
                    
                    batch_count = 0
                    for i in range(0, len(batch_data), rel_batch_size):
                        batch = batch_data[i:i + rel_batch_size]
                        session.run(query_rels_typed, {"batch": batch}).consume()
                        batch_count += 1
                        if batch_count % 5 == 0:
                            LOGGER.info("[Model B] Relationship type %s (from: %s, to: %s) loading progress: %d batches",
                                       rel_type, from_label, target_label, batch_count)
            
            # Read and process in batches
            for batch in batch_iter(iter(rel_spool), stream_batch_size):
                for rel in batch:
                    from_label = rel.get("from_label", "")
                    target_label = rel.get("to_target_label", "")
                    if from_label and target_label:
                        rel_by_labels[(from_label, target_label)].append(rel)
                    else:
                        # If label is missing (should not happen), log warning and use fallback
                        missing_labels_count += 1
                        if missing_labels_count <= 5:  # Only log first 5 warnings
                            LOGGER.warning("[Model B] Relationship missing label info: from_label=%s, to_target_label=%s", 
                                         from_label, target_label)
                        rel_by_labels[("__UNKNOWN__", target_label or "__UNKNOWN__")].append(rel)
                
                # If a group reaches threshold, process a batch
                for key, rels in list(rel_by_labels.items()):
                    if len(rels) >= stream_batch_size:
                        process_relationship_batch(key, rels)
                        total_rels += len(rels)
                        rel_by_labels[key] = []  # Clear, free memory
            
            # Process remaining relationships
            for key, rels in rel_by_labels.items():
                if rels:
                    process_relationship_batch(key, rels)
                    total_rels += len(rels)
            
            if missing_labels_count > 0:
                LOGGER.warning("[Model B] Total %d relationships missing label info, will use fallback query", missing_labels_count)
            
            LOGGER.info("[Model B] Total relationships: %d", total_rels)

        load_cost = time.time() - start_load
        LOGGER.info("[Model B] Load Cost: %.4f s", load_cost)
        return transform_cost + load_cost

    def _process_state_entity(
        self,
        entity_id: str,
        label: str,
        facts: List[Fact],
        static_props: Dict[str, Any],
        states_spool: JsonlSpool,
        evo_links_spool: JsonlSpool,
        rel_spool: JsonlSpool,
        entity_id_to_label: Dict[str, str],  # Used to determine target state node label
    ) -> bool:
        """
        Process state evolution for a single entity.
        
        Design Philosophy: Write-time aggregation, full snapshot
        - When attributes or relationships change, create new state nodes
        - State nodes contain all information at that time point (static attributes + dynamic attributes)
        - Relationships point from state nodes to target entity state nodes
        
        Core Principles:
        1. Process facts in temporal order
        2. Maintain currently active attributes (active_attrs) and relationships (active_rels)
        3. When encountering new time points or attribute/relationship changes, emit current state node
        4. Create EVOLVED_TO relationships between state nodes
        5. Create relationships on state nodes (pointing to target entity state nodes)
        
        Important: Do not skip any state node creation, if there are any attributes or relationships,
                   a state node must be created.
        """
        if not facts:
            return False

        # Extract static attributes (exclude label and id)
        base_static = {k: v for k, v in static_props.items() if k not in {"label", "id"}}
        
        # Sort facts by time (ensure processing in temporal order)
        sorted_facts = sorted(facts, key=lambda f: (f.start, f.end or 9_9999_9999))
        
        # Initialize state tracking
        sentinel_end = 9_9999_9999
        counter = itertools.count()
        end_heap: List[Tuple[int, str, Any, int]] = []  # (end_time, kind, key, token)
        active_attrs: Dict[str, Any] = {}  # Currently active dynamic attributes
        attr_tokens: Dict[str, int] = {}  # Attribute token mapping (for handling overlapping time ranges)
        active_rels: Dict[str, Dict[int, Dict[str, Any]]] = defaultdict(dict)  # Currently active relationships

        produced = False
        current_start: Optional[int] = None  # Start time of current state
        last_sid: Optional[str] = None  # sid of previous state node
        idx = 0
        facts_len = len(sorted_facts)

        # Track emitted sids to avoid duplicates
        emitted_sids: set[str] = set()
        
        def emit_state(end_time: int) -> None:
            """
            Emit a state node.
            
            Core Principles:
            - State nodes must contain all information at that time point (static attributes + dynamic attributes)
            - Relationships must be emitted when state nodes are created
            - Create EVOLVED_TO relationships between state nodes
            """
            nonlocal current_start, last_sid, produced
            if current_start is None or current_start > end_time:
                return
            
            sid = f"{entity_id}_{current_start}"
            
            # Check if this sid has already been generated (avoid duplicates)
            if sid in emitted_sids:
                LOGGER.warning("[Model B] Attempting to generate duplicate sid: %s (entity: %s, time: %d), skipping", 
                             sid, entity_id, current_start)
                return
            
            emitted_sids.add(sid)
            
            # Build state node properties
            # Core principle: State node must contain all information at that time point
            props = {
                "id": entity_id,
                "label": f"{label}_State",
                "sid": sid,
                "start": current_start,
                "end": end_time,
            }
            # Add static attributes (all state nodes should have static attributes)
            if base_static:
                props.update(base_static)
            # Add dynamic attributes (attributes active at current time point)
            if active_attrs:
                props.update(active_attrs)
            
            # Write state node
            states_spool.write(props)
            
            # Create EVOLVED_TO relationship (connect to previous state node)
            # Optimization: Record from_label and to_label for Load phase to group by label and use index
            if last_sid:
                # Determine from_label and to_label (both are current entity's state node label)
                state_label = props["label"]  # e.g., "City_State"
                evo_links_spool.write(
                    {
                        "from_sid": last_sid,
                        "from_label": state_label,  # Source state node label
                        "to_sid": props["sid"],
                        "to_label": state_label,  # Target state node label (same entity)
                        "time": current_start
                    }
                )
            
            # Core principle: Relationships must be emitted when state nodes are created
            # Relationships point from current state node to target entity state nodes
            for rel_type, instances in active_rels.items():
                for inst in instances.values():
                    rel_time = max(current_start, inst["start"])
                    target_id = inst["target_id"]
                    # Determine target state node label
                    # Optimization: Prefer entity_id_to_label mapping, if not exists infer from target_id
                    # If cannot infer, keep as None, Load phase will use optimized fallback query (ensures correctness)
                    target_entity_label = entity_id_to_label.get(target_id)
                    if target_entity_label:
                        target_state_label = f"{target_entity_label}_State"
                    else:
                        # If target entity not in mapping, try to infer label from target_id (use cache)
                        # e.g., Region_0 -> Region, City_1 -> City
                        inferred = infer_label_from_id(target_id)
                        if inferred:
                            target_state_label = f"{inferred}_State"
                            LOGGER.debug("[Model B] Inferring label from target_id: %s -> %s", target_id, target_state_label)
                            # Cache inference result to avoid repeated inference
                            entity_id_to_label[target_id] = inferred
                        else:
                            # If cannot infer, keep as None, Load phase will use optimized fallback query
                            # This ensures correctness: won't create incorrect relationships
                            target_state_label = None
                            LOGGER.debug("[Model B] Unable to infer label from target_id: %s, will query in Load phase", target_id)
                    
                    # Write relationship record
                    # Optimization: Record from_label for Load phase to group by label and use index
                    rel_spool.write(
                        {
                            "from_sid": props["sid"],
                            "from_label": props["label"],  # Source state node label (e.g., "City_State")
                            "to_target_id": target_id,  # Target entity id
                            "to_target_label": target_state_label,  # Target state node label
                            "rel_type": rel_type,
                            "rel_time": rel_time,
                        }
                    )
            
            last_sid = props["sid"]
            produced = True

        # Event-driven processing: Process facts and expiration events in temporal order
        while idx < facts_len or end_heap:
            # Determine next event (new fact start time or existing fact end time)
            next_start = sorted_facts[idx].start if idx < facts_len else None
            next_end = end_heap[0][0] if end_heap else None

            # Process expiration events (attribute or relationship ends)
            if next_end is not None and (next_start is None or next_end <= next_start):
                boundary = next_end
                
                # Emit current state node before clearing attributes
                # Core principle: As long as there are any attributes or relationships, must create state node
                if current_start is not None and current_start < boundary:
                    emit_state(prev_tick(boundary))
                    current_start = boundary
                
                # Clear expired attributes or relationships
                while end_heap and end_heap[0][0] == boundary:
                    _, kind, key, token = heapq.heappop(end_heap)
                    if kind == "attr":
                        # Clear expired attribute
                        if attr_tokens.get(key) == token:
                            attr_tokens.pop(key, None)
                            active_attrs.pop(key, None)
                    else:
                        # Clear expired relationship
                        rel_dict = active_rels.get(key)
                        if rel_dict and token in rel_dict:
                            del rel_dict[token]
                            if not rel_dict:
                                active_rels.pop(key, None)
                continue

            # Process new fact
            if idx >= facts_len:
                break

            fact = sorted_facts[idx]
            idx += 1
            t = fact.start

            # If encountering new time point, emit current state node
            if current_start is None:
                current_start = t
            elif current_start < t:
                # Time point changed, emit current state node
                # Core principle: As long as there are any attributes or relationships, must create state node
                emit_state(t - 1)
                current_start = t

            # Process attribute or relationship
            if fact.value_type == 0:
                # Dynamic attribute
                token = next(counter)
                active_attrs[fact.attribute] = fact.value
                attr_tokens[fact.attribute] = token
                if fact.end is not None:
                    heapq.heappush(
                        end_heap, (next_tick(fact.end), "attr", fact.attribute, token)
                    )
            else:
                # Relationship
                rel_type = fact.attribute.upper()
                token = next(counter)
                active_rels[rel_type][token] = {
                    "target_id": fact.value,
                    "start": fact.start,
                }
                if fact.end is not None:
                    heapq.heappush(
                        end_heap,
                        (next_tick(fact.end), "rel", rel_type, token),
                    )

        # Process last state node
        # Core principle: As long as there are any attributes or relationships, must create state node
        if current_start is not None:
            emit_state(sentinel_end)

        return produced

    # ------------------------------------------------------------------
    # Model C – Process/anchor model
    # ------------------------------------------------------------------

    def run_etl_model_c(self, facts_iter: Iterable[Fact], static_attrs: Iterable[str]) -> float:
        static_attrs = set(static_attrs)
        anchor_spool = JsonlSpool()
        process_node_spool = JsonlSpool()
        next_state_spool = JsonlSpool()
        anchor_link_spool = JsonlSpool()
        reified_rel_spool = JsonlSpool()

        start_transform = time.time()
        anchor_cache: Dict[str, Dict[str, Any]] = {}
        seen_has_process: set = set()
        last_process_uid: Dict[Tuple[str, str], str] = {}
        # Optimization: Maintain target_id to label mapping in Transform phase to avoid Load phase queries
        target_id_to_label: Dict[str, str] = {}

        def flush_anchor(anchor_id: str) -> None:
            if anchor_id in anchor_cache:
                anchor_spool.write(anchor_cache[anchor_id])
                del anchor_cache[anchor_id]

        for fact in facts_iter:
            anchor = anchor_cache.setdefault(
                fact.entity_id,
                {"id": fact.entity_id, "label": fact.entity_label, "static_props": {}},
            )
            if fact.attribute in static_attrs:
                anchor["static_props"][fact.attribute] = fact.value
                continue

            # For relationship type facts (value_type == 1), need to include value in uid to ensure uniqueness
            # Because same entity may have multiple relationships at same time point (e.g., one person contacts multiple people simultaneously)
            if fact.value_type == 1:
                uid = f"{fact.entity_id}_{fact.attribute}_{fact.start}_{fact.value}"
            else:
                uid = f"{fact.entity_id}_{fact.attribute}_{fact.start}"
            
            process_node_spool.write(
                {
                    "uid": uid,
                    "attr": fact.attribute,
                    "start": fact.start,
                    "end": fact.end,
                    "value": fact.value,
                }
            )

            chain_key = (fact.entity_id, fact.attribute)
            
            # For all types of facts, form chain connections
            # Only create HAS_PROCESS relationship once per (entity_id, attribute) pair (pointing to chain head)
            if chain_key not in seen_has_process:
                anchor_link_spool.write(
                    {"label": fact.entity_label, "id": fact.entity_id, "attr": fact.attribute, "uid": uid}
                )
                seen_has_process.add(chain_key)
                flush_anchor(fact.entity_id)
            
            # All types of facts form chain connections (in temporal order)
            if chain_key in last_process_uid:
                prev_uid = last_process_uid[chain_key]
                next_state_spool.write({"from_uid": prev_uid, "to_uid": uid})
            last_process_uid[chain_key] = uid

            if fact.value_type == 1:
                # Optimization: Determine target node label in Transform phase
                # If cannot infer, keep as None, Load phase will use optimized fallback query (ensures correctness)
                target_id = fact.value
                target_label = target_id_to_label.get(target_id)

                if target_label is None:
                    # relation_targets are already created during data generation, their labels can be inferred from target_id (use cache)
                    # e.g., Region_0 -> Region, Region_1 -> Region
                    inferred = infer_label_from_id(target_id)
                    if inferred:
                        target_label = inferred
                        target_id_to_label[target_id] = target_label
                    else:
                        # If cannot infer from target_id (doesn't match "Label_number" pattern), keep as None
                        # Load phase will use optimized fallback query to ensure correctness
                        target_label = None
                        LOGGER.debug("[Model C] Unable to infer label from target_id: %s, will query in Load phase", target_id)

                reified_rel_spool.write(
                    {"from_uid": uid, "to_id": target_id, "to_label": target_label, "rel_type": fact.attribute.upper()}
                )
            else:
                target_id = fact.entity_id
                target_label = fact.entity_label
                target_id_to_label[target_id] = target_label

        for anchor_id in list(anchor_cache.keys()):
            flush_anchor(anchor_id)

        transform_cost = time.time() - start_transform
        LOGGER.info("[Model C] Transform Cost: %.4f s", transform_cost)

        start_load = time.time()
        with self.driver.session(database=self.database) as session:
            # 1. Anchor node loading (requires MERGE, keep batch_size)
            query_anchors = """
            UNWIND $batch AS a
            CALL apoc.merge.node([a.label], {id: a.id}, a.static_props, {}) YIELD node
            RETURN count(node)
            """
            anchor_batch_size = self.batch_size
            self._run_batch_query_with_progress(
                session, query_anchors, iter(anchor_spool),
                "Anchor nodes", anchor_batch_size, progress_interval=10, model_tag="C"
            )

            # 2. ProcessNode loading (CREATE operation, can use larger batch size)
            query_process_nodes = """
            UNWIND $batch AS p
            CREATE (pn:ProcessNode {
                uid: p.uid,
                attr: p.attr,
                start: p.start,
                end: p.end,
                value: p.value
            })
            """
            process_batch_size = min(self.batch_size * 2, 10000)
            self._run_batch_query_with_progress(
                session, query_process_nodes, iter(process_node_spool),
                "ProcessNode", process_batch_size, progress_interval=10, model_tag="C"
            )

            # 3. NEXT_STATE relationship loading (CREATE operation, can use larger batch size)
            query_next_links = """
            UNWIND $batch AS l
            MATCH (p1:ProcessNode {uid: l.from_uid})
            WITH p1, l
            MATCH (p2:ProcessNode {uid: l.to_uid})
            CREATE (p1)-[:NEXT_STATE]->(p2)
            """
            next_state_batch_size = min(self.batch_size * 3, 15000)
            self._run_batch_query_with_progress(
                session, query_next_links, iter(next_state_spool),
                "NEXT_STATE relationships", next_state_batch_size, progress_interval=5, model_tag="C"
            )

            # 4. HAS_PROCESS relationship loading (optimization: stream processing, group by label, explicitly specify label, use index)
            # Anchor nodes already created in step 1, no need to MERGE again
            # Optimization: Stream processing to avoid loading all data into memory at once
            stream_batch_size = 50000
            anchor_link_batch_size = self.batch_size
            links_by_label: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
            
            # Stream processing: Read in batches, group and process while reading
            for batch in batch_iter(iter(anchor_link_spool), stream_batch_size):
                for link in batch:
                    label = link.get("label", "")
                    if label:
                        links_by_label[label].append(link)
                
                # If a group reaches threshold, process a batch
                for label, links in list(links_by_label.items()):
                    if len(links) >= stream_batch_size:
                        # Process this batch
                        query_anchor_links = f"""
                        UNWIND $batch AS l
                        MATCH (node:{label} {{id: l.id}})
                        WITH node, l
                        MATCH (p:ProcessNode {{uid: l.uid}})
                        CREATE (node)-[:HAS_PROCESS {{attr: l.attr}}]->(p)
                        RETURN count(*)
                        """
                        self._run_batch_query_with_progress(
                            session, query_anchor_links, iter(links),
                            f"HAS_PROCESS relationship ({label})", anchor_link_batch_size, progress_interval=5, model_tag="C"
                        )
                        links_by_label[label] = []  # Clear, free memory
            
            if links_by_label:

                LOGGER.info("[Model C] HAS_PROCESS relationships grouped by label: %d label types", len(links_by_label))

                anchor_link_batch_size = self.batch_size
                for label, links in links_by_label.items():
                    # Build specialized query for each label, explicitly specify label to use unique constraint index
                    query_anchor_links = f"""
                    UNWIND $batch AS l
                    MATCH (node:{label} {{id: l.id}})
                    WITH node, l
                    MATCH (p:ProcessNode {{uid: l.uid}})
                    CREATE (node)-[:HAS_PROCESS {{attr: l.attr}}]->(p)
                    RETURN count(*)
                    """
                    self._run_batch_query_with_progress(
                        session, query_anchor_links, iter(links),
                        f"HAS_PROCESS relationship ({label})", anchor_link_batch_size, progress_interval=5, model_tag="C"
                    )

            # 5. Reified relationship loading (optimization: stream processing, use to_label recorded in Transform phase, avoid database queries)
            # Target node label already determined in Transform phase and written to reified_rel_spool
            # Optimization: Stream processing to avoid loading all data into memory at once
            rels_by_type_and_label: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
            reified_batch_size = min(self.batch_size * 3, 15000)
            
            # Stream processing: Read in batches, group and process while reading
            for batch in batch_iter(iter(reified_rel_spool), stream_batch_size):
                for rel in batch:
                    rel_type = rel["rel_type"]
                    target_label = rel.get("to_label")
                    # If target_label is None, use "__UNKNOWN__" marker, Load phase will use fallback query
                    if target_label is None:
                        target_label = target_id_to_label.get(rel.get("to_id"), "__UNKNOWN__")
                    rels_by_type_and_label[(rel_type, target_label)].append(rel)
                
                # If a group reaches threshold, process a batch
                for key, rels in list(rels_by_type_and_label.items()):
                    if len(rels) >= stream_batch_size:
                        rel_type, target_label = key
                        # Process this batch
                        if target_label == "__UNKNOWN__":
                            query_reified = f"""
                            UNWIND $batch AS r
                            MATCH (p:ProcessNode {{uid: r.from_uid}})
                            WITH p, r
                            MATCH (target) WHERE target.id = r.to_id
                            CREATE (p)-[:{rel_type}]->(target)
                            RETURN count(*)
                            """
                        else:
                            query_reified = f"""
                            UNWIND $batch AS r
                            MATCH (p:ProcessNode {{uid: r.from_uid}})
                            WITH p, r
                            MATCH (target:{target_label} {{id: r.to_id}})
                            CREATE (p)-[:{rel_type}]->(target)
                            RETURN count(*)
                            """
                        self._run_batch_query_with_progress(
                            session, query_reified, iter(rels),
                            f"Reified relationship ({rel_type}, target:{target_label})", reified_batch_size, progress_interval=5, model_tag="C"
                        )
                        rels_by_type_and_label[key] = []  # Clear, free memory
            
            if rels_by_type_and_label:

                LOGGER.info("[Model C] Reified relationships grouped by type and target label: %d groups", len(rels_by_type_and_label))

                reified_batch_size = min(self.batch_size * 3, 15000)
                for (rel_type, target_label), rels in rels_by_type_and_label.items():
                    if target_label == "__UNKNOWN__":
                        # If target label is unknown (theoretically should not happen), use generic query
                        LOGGER.warning("[Model C] Reified relationship (%s) target label unknown, using generic query", rel_type)
                        query_reified = f"""
                        UNWIND $batch AS r
                        MATCH (p:ProcessNode {{uid: r.from_uid}})
                        WITH p, r
                        MATCH (target) WHERE target.id = r.to_id
                        CREATE (p)-[:{rel_type}]->(target)
                        RETURN count(*)
                        """
                    else:
                        # Explicitly specify target label, use unique constraint index
                        query_reified = f"""
                        UNWIND $batch AS r
                        MATCH (p:ProcessNode {{uid: r.from_uid}})
                        WITH p, r
                        MATCH (target:{target_label} {{id: r.to_id}})
                        CREATE (p)-[:{rel_type}]->(target)
                        RETURN count(*)
                        """
                    self._run_batch_query_with_progress(
                        session, query_reified, iter(rels),
                        f"Reified relationship ({rel_type}, target:{target_label})", reified_batch_size, progress_interval=5, model_tag="C"
                    )

        load_cost = time.time() - start_load
        LOGGER.info("[Model C] Load Cost: %.4f s", load_cost)
        return transform_cost + load_cost


# ---------------------------------------------------------------------------
# Data synthesizer & experiment orchestration
# ---------------------------------------------------------------------------


class DataSynthesizer:
    """
    Generate controllable "atomic fact streams" based on paper experiment parameters.

    - Independently simulate evolution for each entity, ensuring output is naturally
      sorted by (entity_id, start);
    - Provide sorting interface according to model requirements (e.g., Model C requires
      sorting by (id, attr, start));
    - Default to deterministic seed, ensuring G1-G5 scenarios are reproducible.
    """

    DEFAULTS = {
        "n_entities": 1_000_000,
        "t_start": 19900101,
        "t_end": 20201231,
        "t_step_days": 365,
        "v_attribute": 0.3,
        "v_relation": 0.05,
        "n_static_props": 2,
        "n_dynamic_props": 5,
        "n_dynamic_rels": 1,
        "entity_labels": ["City"],
        "relation_label": "Region",
        "seed": 42,
        "sort_chunk_size": 250_000,
    }

    def __init__(self, **params: Any):
        config = dict(self.DEFAULTS)
        config.update(params)
        self.params = config
        self.logger = logging.getLogger(self.__class__.__name__)

        # Dynamically adjust Region count to make data distribution more realistic
        # If user doesn't explicitly specify relation_targets, dynamically generate based on n_entities
        # Formula: n_regions = max(10, min(1000, n_entities // 100))
        # This way: 1K City → 10 Regions, 1M City → 1000 Regions
        # Average each Region contains about 100 Cities (more realistic: tens to hundreds of Cities/Region)
        if "relation_targets" not in params or params.get("relation_targets") is None:
            n_entities = self.params["n_entities"]
            n_regions = max(10, min(10000, n_entities // 100))
            self.params["relation_targets"] = [f"Region_{i}" for i in range(n_regions)]
            self.logger.info(
                "Auto-calculated Region count: %d Regions (based on %d entities, average each Region contains about %d entities)",
                n_regions, n_entities, n_entities // n_regions if n_regions > 0 else 0
            )

        extra_static = max(0, self.params["n_static_props"] - 2)
        self.static_prop_names = ["name", "location"] + [
            f"static_p_{i}" for i in range(extra_static)
        ]

        extra_dyn = max(0, self.params["n_dynamic_props"] - 1)
        self.dynamic_prop_names = ["population"] + [
            f"dyn_p_{i}" for i in range(extra_dyn)
        ]

        self.dynamic_rel_names = [
            f"rel_{i}" for i in range(max(1, self.params["n_dynamic_rels"]))
        ]

        self.logger.info("Synthesizer configured with %s", self.params)

    # ---------------------------- public API ----------------------------

    def get_static_attributes(self) -> Set[str]:
        return set(self.static_prop_names)

    def get_entity_labels(self) -> Set[str]:
        labels = set(self.params["entity_labels"])
        labels.add(self.params["relation_label"])
        return labels

    def build_profile(self) -> SynthProfile:
        time_points = []
        current = self.params["t_start"]
        while current <= self.params["t_end"]:
            time_points.append(current)
            current = advance_time(current, self.params["t_step_days"])

        dynamic_attrs = {
            attr: {"entities": self.params["n_entities"]}
            for attr in self.dynamic_prop_names
        }
        relations = {
            rel: {
                "entities": self.params["n_entities"],
                "targets": self.params["relation_targets"],
            }
            for rel in self.dynamic_rel_names
        }

        # Dynamically adjust entity sample count in profile based on entity count
        # Goal: Ensure query parameter sampling has sufficient representativeness
        n_entities = self.params["n_entities"]
        if n_entities <= 1000:
            # Small scale: Include all entities
            max_profile_entities = n_entities
            entities = [f"e{i}" for i in range(max_profile_entities)]
        elif n_entities <= 100000:
            # Medium scale: 10000 entity samples (10% coverage)
            max_profile_entities = 10000
            rng = self._new_rng()
            entity_indices = rng.sample(range(n_entities), max_profile_entities)
            entities = [f"e{i}" for i in sorted(entity_indices)]
        else:
            # Large scale: At least 1% entity samples, but not exceeding 100000
            # Ensure sampling coverage reaches at least 1%, but not exceeding 100000 entities (avoid profile file being too large)
            max_profile_entities = min(
                max(10000, int(n_entities * 0.01)),
                100000
            )
            rng = self._new_rng()
            entity_indices = rng.sample(range(n_entities), max_profile_entities)
            entities = [f"e{i}" for i in sorted(entity_indices)]

        return SynthProfile(
            entities=entities,
            dynamic_attrs=dynamic_attrs,
            relations=relations,
            time_span=(self.params["t_start"], self.params["t_end"]),
            time_points=time_points,
        )

    def iter_for_model(
        self, model_tag: str, profile: Optional[SynthProfile] = None
    ) -> Iterator[Fact]:
        """
        Provide a generator tailored for model-specific sorting requirements.
        Model A/B: Natural order already satisfies (id, start).
        Model C: Requires (id, attr, start), use ExternalSorter for low-memory reordering.
        """

        stream = self.generate_facts(profile)
        model = model_tag.lower()
        if model in ("a", "b"):
            return stream
        if model == "c":
            sorter = ExternalSorter(
                key_fn=lambda f: (f.entity_id, f.attribute, f.start),
                chunk_size=self.params["sort_chunk_size"],
            )
            return sorter.sort(stream)
        raise ValueError(f"Unsupported model tag: {model_tag}")

    def generate_facts(self, profile: Optional[SynthProfile] = None) -> Iterator[Fact]:
        rng = self._new_rng()
        t_start = self.params["t_start"]
        relation_label = self.params["relation_label"]

        # Pre-output static anchors for target regions to ensure relationships can be created
        for target in self.params["relation_targets"]:
            name_fact = Fact(
                entity_id=target,
                entity_label=relation_label,
                attribute="name",
                value=target,
                value_type=0,
                start=t_start,
                end=None,
            )
            location_fact = Fact(
                entity_id=target,
                entity_label=relation_label,
                attribute="location",
                value=self._generate_wkt(rng),
                value_type=0,
                start=t_start,
                end=None,
            )
            yield name_fact
            yield location_fact

        for idx in range(self.params["n_entities"]):
            entity_id = f"e{idx}"
            label = rng.choice(self.params["entity_labels"])
            yield from self._generate_entity_facts(entity_id, label, rng)

    @classmethod
    def get_experiment_configs(cls) -> Dict[str, Dict[str, Any]]:
        base = dict(cls.DEFAULTS)
        configs: Dict[str, Dict[str, Any]] = {}

        # G1: Scale
        for n in [100_000, 1_000_000, 5_000_000, 10_000_000]:
            cfg = dict(base)
            cfg["n_entities"] = n
            configs[f"G1_Scale_{n/1_000_000:.1f}M"] = cfg

        # G2: Attribute volatility
        for v in [0.05, 0.2, 0.4, 0.6, 0.8, 0.95]:
            cfg = dict(base)
            cfg["v_attribute"] = v
            configs[f"G2_Vol_Attr_{int(v*100)}pct"] = cfg

        # G3: Relation volatility
        for v in [0.01, 0.05, 0.1, 0.2, 0.4]:
            cfg = dict(base)
            cfg["v_relation"] = v
            configs[f"G3_Vol_Rel_{int(v*100)}pct"] = cfg

        # G4: Density (dynamic props)
        for n_prop in [1, 5, 10, 15, 20]:
            cfg = dict(base)
            cfg["n_dynamic_props"] = n_prop
            configs[f"G4_Density_Props_{n_prop}"] = cfg

        return configs

    # ---------------------------- internals ----------------------------

    def _new_rng(self) -> random.Random:
        return random.Random(self.params.get("seed", 42))

    def _generate_wkt(self, rng: random.Random) -> str:
        lon = rng.uniform(-180, 180)
        lat = rng.uniform(-90, 90)
        return f"POINT({lon:.5f} {lat:.5f})"

    def _generate_value(self, attr: str, current: Optional[float], rng: random.Random) -> Any:
        if "population" in attr:
            if current:
                return int(current * (1 + rng.uniform(-0.01, 0.05)))
            return rng.randint(10_000, 1_000_000)
        return rng.randint(100, 1_000)

    def _generate_entity_facts(
        self, entity_id: str, label: str, rng: random.Random
    ) -> Iterator[Fact]:
        t_start = self.params["t_start"]
        t_end = self.params["t_end"]
        step = self.params["t_step_days"]

        facts: List[Fact] = []

        # Static attributes
        for attr in self.static_prop_names:
            if attr == "name":
                value = f"{label}_{entity_id}"
            elif attr == "location":
                value = self._generate_wkt(rng)
            else:
                value = f"{attr}_{entity_id}"
            facts.append(
                Fact(
                    entity_id=entity_id,
                    entity_label=label,
                    attribute=attr,
                    value=value,
                    value_type=0,
                    start=t_start,
                    end=None,
                )
            )

        attr_state: Dict[str, Dict[str, Any]] = {}
        rel_state: Dict[str, Dict[str, Any]] = {}

        for attr in self.dynamic_prop_names:
            attr_state[attr] = {
                "value": self._generate_value(attr, None, rng),
                "start": t_start,
            }

        for rel in self.dynamic_rel_names:
            target = rng.choice(self.params["relation_targets"])
            rel_state[rel] = {"target": target, "start": t_start}

        current_time = advance_time(t_start, step)

        while current_time <= t_end:
            # Attribute evolution
            for attr in self.dynamic_prop_names:
                if rng.random() < self.params["v_attribute"]:
                    state = attr_state[attr]
                    facts.append(
                        Fact(
                            entity_id=entity_id,
                            entity_label=label,
                            attribute=attr,
                            value=state["value"],
                            value_type=0,
                            start=state["start"],
                            end=prev_tick(current_time),
                        )
                    )
                    attr_state[attr] = {
                        "value": self._generate_value(attr, state["value"], rng),
                        "start": current_time,
                    }

            # Relationship evolution
            for rel in self.dynamic_rel_names:
                if rng.random() < self.params["v_relation"]:
                    state = rel_state[rel]
                    facts.append(
                        Fact(
                            entity_id=entity_id,
                            entity_label=label,
                            attribute=rel,
                            value=state["target"],
                            value_type=1,
                            start=state["start"],
                            end=prev_tick(current_time),
                        )
                    )
                    new_target = self._pick_new_target(state["target"], rng)
                    rel_state[rel] = {"target": new_target, "start": current_time}

            current_time = advance_time(current_time, step)

        # Finalize: Output unclosed intervals
        for attr, state in attr_state.items():
            facts.append(
                Fact(
                    entity_id=entity_id,
                    entity_label=label,
                    attribute=attr,
                    value=state["value"],
                    value_type=0,
                    start=state["start"],
                    end=None,
                )
            )

        for rel, state in rel_state.items():
            facts.append(
                Fact(
                    entity_id=entity_id,
                    entity_label=label,
                    attribute=rel,
                    value=state["target"],
                    value_type=1,
                    start=state["start"],
                    end=None,
                )
            )

        facts.sort(key=lambda f: (f.entity_id, f.start, f.attribute))
        for fact in facts:
            yield fact

    def _pick_new_target(self, current: str, rng: random.Random) -> str:
        candidates = [t for t in self.params["relation_targets"] if t != current]
        if not candidates:
            return current
        return rng.choice(candidates)


QuerySpec = Union[str, Dict[str, Any]]


class QueryWorkload:
    """
    Unified management of latency measurement for four query types. Supports templated parameters and multiple sampling.
    """

    def __init__(
        self,
        queries: Dict[str, QuerySpec],
        param_builder: Optional[Callable[[str, str], Dict[str, Any]]] = None,
        warmups: int = 1,
        runs: int = 3,
        database: str = "neo4j",
    ):
        self.queries = queries
        self.param_builder = param_builder or (lambda name, model: {})
        self.warmups = warmups
        self.runs = runs
        self.database = database

    def measure(
        self,
        driver,
        model_tag: str,
        param_sampler: Callable[[str, Dict[str, Any]], Dict[str, Any]],
        samples: int,
        query_timeout: Optional[float] = None,
    ) -> Dict[str, Dict[str, float]]:
        """
        Execute query performance testing

        Args:
            driver: Neo4j driver
            model_tag: Model tag (A/B/C)
            param_sampler: Parameter sampler
            samples: Number of samples per query
            query_timeout: Query timeout in seconds, if None then no timeout is set
        """
        metrics: Dict[str, Dict[str, float]] = {}
        total_queries = len(self.queries)
        skip_remaining_queries = False

        for query_idx, (name, spec) in enumerate(self.queries.items(), 1):
            # If previous query timed out, skip subsequent queries
            if skip_remaining_queries:
                LOGGER.warning("[Query] Skipping query %s (%d/%d) - model %s (due to previous query timeout)",
                             name, query_idx, total_queries, model_tag)
                metrics[name] = {
                    "mean_ms": float('inf'),
                    "p50_ms": float('inf'),
                    "p95_ms": float('inf'),
                    "samples": 0,
                    "timeout": True,
                }
                continue

            LOGGER.info("[Query] Executing query %s (%d/%d) - model %s", name, query_idx, total_queries, model_tag)
            query_text, default_params = self._resolve_query(spec, model_tag)
            latencies: List[float] = []

            session = driver.session(database=self.database)
            try:
                for sample_idx in range(samples):
                    params = dict(default_params)
                    params.update(self.param_builder(name, model_tag))
                    params = param_sampler(name, params)
                    # Optimization: For Model B, replace dynamic property access with static property name
                    # This avoids Neo4j dynamic property warnings and may enable index usage
                    optimized_query = self._optimize_query_for_model_b(query_text, model_tag, params)

                    # Warmup phase
                    for _ in range(self.warmups):
                        try:
                            if query_timeout is not None:
                                # Set timeout for all queries
                                self._run_query_with_timeout(
                                    session, optimized_query, params, query_timeout, name, "warmup"
                                )
                            else:
                                session.run(optimized_query, params).consume()
                        except TimeoutError:
                            LOGGER.error("[Query] Query %s timed out in warmup phase", name)
                            skip_remaining_queries = True
                            # After timeout, don't continue execution, but also don't close session to avoid breaking connection
                            break
                        except Exception as e:
                            if "timeout" in str(e).lower() or "timed out" in str(e).lower():
                                LOGGER.error("[Query] Query %s timed out in warmup phase: %s", name, e)
                                skip_remaining_queries = True
                                break
                            elif "defunct connection" in str(e).lower() or "connection" in str(e).lower():
                                # Connection error, possibly caused by timeout breaking connection
                                LOGGER.error("[Query] Query %s connection error in warmup phase: %s", name, e)
                                skip_remaining_queries = True
                                break
                            else:
                                raise

                    if skip_remaining_queries:
                        break

                    # Actual testing phase
                    for run_idx in range(self.runs):
                        try:
                            start = time.perf_counter()
                            if query_timeout is not None:
                                # Set timeout for all queries
                                self._run_query_with_timeout(
                                    session, optimized_query, params, query_timeout, name, f"run {run_idx + 1}"
                                )
                            else:
                                session.run(optimized_query, params).consume()
                            elapsed = (time.perf_counter() - start) * 1000
                            latencies.append(elapsed)

                            # Check if exceeds timeout (even if query succeeds, record if takes too long)
                            if query_timeout is not None and elapsed > query_timeout * 1000:
                                LOGGER.warning("[Query] Query %s run %d took %.2f seconds, exceeding timeout %.2f seconds",
                                             name, run_idx + 1, elapsed / 1000, query_timeout)
                        except TimeoutError:
                            elapsed = (time.perf_counter() - start) * 1000
                            LOGGER.error("[Query] Query %s run %d timed out (took %.2f seconds)",
                                       name, run_idx + 1, elapsed / 1000)
                            # Mark timeout, skip subsequent queries
                            skip_remaining_queries = True
                            break
                        except Exception as e:
                            if "timeout" in str(e).lower() or "timed out" in str(e).lower():
                                elapsed = (time.perf_counter() - start) * 1000
                                LOGGER.error("[Query] Query %s run %d timed out (took %.2f seconds): %s",
                                           name, run_idx + 1, elapsed / 1000, e)
                                # Mark timeout, skip subsequent queries
                                skip_remaining_queries = True
                                break
                            elif "defunct connection" in str(e).lower() or "connection" in str(e).lower():
                                # Connection error, possibly caused by timeout breaking connection
                                elapsed = (time.perf_counter() - start) * 1000
                                LOGGER.error("[Query] Query %s run %d connection error (took %.2f seconds): %s",
                                           name, run_idx + 1, elapsed / 1000, e)
                                # Mark timeout, skip subsequent queries
                                skip_remaining_queries = True
                                break
                            else:
                                raise

                    if skip_remaining_queries:
                        break

                # If timeout, record and skip subsequent queries
                if skip_remaining_queries:
                    LOGGER.error("[Query] Query %s timed out, skipping subsequent queries for model %s", name, model_tag)
                    metrics[name] = {
                        "mean_ms": float('inf'),
                        "p50_ms": float('inf'),
                        "p95_ms": float('inf'),
                        "samples": len(latencies),
                        "timeout": True,
                    }
                    # Don't immediately close session after timeout to avoid breaking connection
                    # Let database side complete query naturally or be terminated by server-side timeout
                    # Will be closed uniformly in finally block
                    continue
            finally:
                # Ensure session is always closed (even if no timeout)
                # Use try-except to avoid errors during close affecting subsequent flow
                try:
                    if not session.closed():
                        # Try graceful close, if fails ignore (connection may be disconnected)
                        try:
                            session.close()
                        except Exception as close_err:
                            # Connection may already be disconnected, ignore close error
                            LOGGER.debug("[Query] Error closing session (connection may be disconnected): %s", close_err)
                except Exception:
                    pass  # Ignore errors when checking session status

            if skip_remaining_queries:
                # If loop exited due to timeout, continue to next query
                continue

            if latencies:
                # Calculate complete statistics
                sorted_latencies = sorted(latencies)
                n = len(latencies)
                mean_val = sum(latencies) / n
                
                # Calculate standard deviation
                variance = sum((x - mean_val) ** 2 for x in latencies) / n
                stddev = variance ** 0.5
                
                # Calculate percentiles
                p25 = float(self._percentile(sorted_latencies, 0.25))
                p50 = float(self._percentile(sorted_latencies, 0.5))
                p75 = float(self._percentile(sorted_latencies, 0.75))
                p95 = float(self._percentile(sorted_latencies, 0.95))
                
                # Calculate minimum and maximum values
                min_val = float(sorted_latencies[0])
                max_val = float(sorted_latencies[-1])
                
                metrics[name] = {
                    # Keep original fields (backward compatibility)
                    "mean_ms": mean_val,
                    "p50_ms": p50,
                    "p95_ms": p95,
                    "samples": n,
                    # New statistical metrics
                    "stddev_ms": stddev,  # Standard deviation (for error bars)
                    "p25_ms": p25,        # Lower quartile (for box plots)
                    "p75_ms": p75,        # Upper quartile (for box plots)
                    "min_ms": min_val,    # Minimum value (for box plots)
                    "max_ms": max_val,    # Maximum value (for box plots)
                    # Raw sample list (for advanced charts like CDF)
                    "raw_samples_ms": latencies,  # Complete raw sample list
                }
                LOGGER.info("[Query] Query %s completed - Mean latency: %.2f ms, P50: %.2f ms, P95: %.2f ms, StdDev: %.2f ms",
                           name, mean_val, p50, p95, stddev)
            else:
                # If no successful results, record as timeout
                metrics[name] = {
                    "mean_ms": float('inf'),
                    "p50_ms": float('inf'),
                    "p95_ms": float('inf'),
                    "samples": 0,
                    "timeout": True,
                }

        return metrics

    @staticmethod
    def _run_query_with_timeout(
        session, query: str, params: Dict[str, Any], timeout: float, query_name: str, stage: str
    ) -> None:
        """
        Query execution with timeout

        Use threading to implement timeout mechanism, as Neo4j Python driver may not support timeout parameter

        Note: After timeout, the query on the database side may still be executing, but Python side will immediately return TimeoutError
        Do not close session after timeout to avoid breaking connection, let database side complete naturally or timeout
        """
        result_container = {"result": None, "exception": None, "completed": False}

        def run_query():
            try:
                result_container["result"] = session.run(query, params)
                result_container["result"].consume()
                result_container["completed"] = True
            except Exception as e:
                result_container["exception"] = e
                result_container["completed"] = True

        thread = threading.Thread(target=run_query)
        thread.daemon = True
        thread.start()
        thread.join(timeout=timeout)

        if thread.is_alive():
            # Query timeout - return immediately, don't wait for thread to finish
            # Note: Do not close session or result to avoid breaking connection
            # Query on database side may still be executing, but Python side will return immediately
            # Since thread is daemon=True, it will automatically terminate when main program exits
            LOGGER.error("[Query] Query %s (%s) timed out (%.0f seconds), returning immediately (query on database side may still be executing)",
                        query_name, stage, timeout)
            raise TimeoutError(f"Query {query_name} ({stage}) timed out after {timeout} seconds")

        if result_container["exception"]:
            raise result_container["exception"]

        if not result_container["completed"]:
            raise RuntimeError(f"Query {query_name} ({stage}) did not complete")

    @staticmethod
    def _percentile(values: List[float], q: float) -> float:
        """
        Calculate percentile
        
        Args:
            values: List of values (can be unsorted, method will sort internally)
            q: Percentile (0.0-1.0), e.g., 0.5 represents median
        
        Returns:
            Percentile value
        """
        if not values:
            return 0.0
        sorted_vals = sorted(values)
        idx = int(max(0, min(len(sorted_vals) - 1, round(q * (len(sorted_vals) - 1)))))
        return sorted_vals[idx]

    @staticmethod
    def _optimize_query_for_model_b(query_text: str, model_tag: str, params: Dict[str, Any]) -> str:
        """
        Optimize Model B queries: Replace dynamic property access s[$attr] with static property name s.attr_name
        This avoids Neo4j dynamic property warnings and may enable index usage

        Note: This optimization only applies to Model B, because Model B state node property names are known
        """
        if model_tag.upper() != "B":
            return query_text

        # If query contains dynamic property access and params has attr, replace it
        # Note: Key in params dict is "attr" (without $ symbol), not "$attr"
        if "attr" in params and "s[$attr]" in query_text:
            attr_name = params["attr"]
            # Replace all s[$attr] with s.attr_name
            # Note: Need to ensure property name is safe (does not contain special characters)
            if attr_name and isinstance(attr_name, str) and attr_name.replace("_", "").replace("-", "").isalnum():
                # Use regex replacement to ensure only property access is replaced, not other content
                # Replace s[$attr] with s.attr_name (use backticks to wrap property name for safety)
                query_text = re.sub(r's\[\$attr\]', f's.`{attr_name}`', query_text)
                # Note: Neo4j property names containing special characters need to be wrapped in backticks
                # But usually property names are safe (e.g., population, dyn_p_0, etc.)

        return query_text

    def _resolve_query(
        self, spec: QuerySpec, model_tag: str
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Support three forms:
            1. Pure string: Can contain {model} placeholder;
            2. { "A": "...", "B": "...", ... } Differentiated by model;
            3. { "cypher": <string or dict>, "params": {...} }, for specifying default parameters simultaneously.
        """

        params: Dict[str, Any] = {}
        cypher_spec: Union[str, Dict[str, str]]

        if isinstance(spec, dict) and "cypher" in spec:
            cypher_spec = spec["cypher"]
            params = dict(spec.get("params", {}))
        else:
            cypher_spec = spec  # type: ignore[assignment]

        if isinstance(cypher_spec, str):
            return cypher_spec.format(model=model_tag), params

        model_upper = model_tag.upper()
        if model_upper not in cypher_spec:
            raise KeyError(f"Query not defined for model {model_upper}")
        return cypher_spec[model_upper], params


@dataclass
class SynthProfile:
    """Describes metadata for a synthetic data run, for use in queries and analysis."""

    entities: List[str]
    dynamic_attrs: Dict[str, Dict[str, Any]]
    relations: Dict[str, Dict[str, Any]]
    time_span: Tuple[int, int]
    time_points: List[int]

    def sample_entities(self, k: int) -> List[str]:
        if not self.entities:
            return []
        k = min(k, len(self.entities))
        return random.sample(self.entities, k)

    def pick_entity_with_attr(self, attr: str) -> Optional[str]:
        if attr not in self.dynamic_attrs or not self.entities:
            return None
        return random.choice(self.entities)

    def pick_relation_entity(self, rel: str) -> Optional[str]:
        if rel not in self.relations or not self.entities:
            return None
        return random.choice(self.entities)


class BenchmarkSuite:
    """
    Chains DataSynthesizer + ETLRunner + QueryWorkload together to produce metrics required for the paper.
    """

    def __init__(
        self,
        runner: ETLRunner,
        query_workload: Optional[QueryWorkload] = None,
        store_path: Optional[Path] = None,
        db_parent: Optional[str] = None,
        db_name: str = "neo4j",
        reset_db_fn: Optional[Callable[[], None]] = None,
        profile_path: Optional[Path] = None,
        samples_per_query: int = 50,
        uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.profile_path = Path(profile_path) if profile_path else None
        self.samples_per_query = samples_per_query
        self.runner = runner
        self.query_workload = query_workload
        self.store_path = Path(store_path) if store_path else None
        self.db_parent = db_parent
        self.db_name = db_name
        self.reset_db_fn = reset_db_fn
        self.uri = uri
        self.user = user
        self.password = password

    def run_configuration(
        self,
        config_name: str,
        config: Dict[str, Any],
        models: Tuple[str, ...] = ("a", "b", "c"),
    ) -> Dict[str, Any]:
        synth = DataSynthesizer(**config)
        static_attrs = synth.get_static_attributes()
        labels = synth.get_entity_labels()

        results: Dict[str, Any] = {}

        profile = self._generate_profile(synth)

        # Create counting iterator wrapper to count facts during consumption
        class CountingIterator:
            """Wrap iterator to count elements during iteration"""
            def __init__(self, iterator: Iterable[Fact], name: str = "facts"):
                self.iterator = iter(iterator)
                self.count = 0
                self.name = name
                
            def __iter__(self):
                return self
                
            def __next__(self):
                try:
                    item = next(self.iterator)
                    self.count += 1
                    if self.count % 100000 == 0:
                        LOGGER.info("[Progress] Processed %d %s...", self.count, self.name)
                    return item
                except StopIteration:
                    if self.count > 0:
                        LOGGER.info("[Progress] %s processing completed, total %d %s", self.name, self.count, self.name)
                    raise

        total_models = len(models)
        facts_count = None  # Will be determined when processing the first model
        
        for model_idx, model_tag in enumerate(models, 1):
            tag = model_tag.lower()
            LOGGER.info("[Progress] Running model %s (%d/%d)", tag.upper(), model_idx, total_models)
            facts_iter_raw = synth.iter_for_model(tag, profile)
            
            # Wrap with counting iterator to count during consumption
            counting_iter = CountingIterator(facts_iter_raw, "facts")
            facts_iter = counting_iter

            # Optimization: Completely rebuild database before each model to ensure each model starts from a completely clean state
            # Use prepare_isolated_db instead of clear_database_and_setup_indices to ensure complete deletion of all data
            if self.uri and self.user and self.password:
                LOGGER.info("[Progress] Rebuilding database for model %s...", tag.upper())
                prepare_isolated_db(self.uri, self.user, self.password, self.db_name)
                # Recreate runner connection because database has been rebuilt
                saved_batch_size = self.runner.batch_size
                self.runner.close()
                self.runner = ETLRunner(
                    self.uri,
                    self.user,
                    self.password,
                    batch_size=saved_batch_size,
                    database=self.db_name,
                )
            elif self.reset_db_fn:
                # If no uri/user/password, use provided reset_db_fn
                self.reset_db_fn()

            # Set up indices (database already rebuilt, only need to set up indices)
            self.runner.clear_database_and_setup_indices(labels)

            # Measure baseline size (empty database size after clearing)
            # Note: Need to measure after clearing database to ensure baseline is clean
            origin_size = self._measure_store_size()
            LOGGER.info("[Measure] Baseline database size: %.2f MB", origin_size or 0.0)

            # Important: After recreating runner, need to re-get method because method is bound to runner instance
            method = getattr(self.runner, f"run_etl_model_{tag}")
            LOGGER.info("[Progress] Starting ETL ingestion (model %s)", tag.upper())
            ingest_cost = method(facts_iter, static_attrs)
            
            # Get fact count from counting iterator (determined when processing first model, subsequent models should be the same)
            if facts_count is None:
                facts_count = counting_iter.count
            elif counting_iter.count != facts_count:
                LOGGER.warning("[Progress] Model %s fact count (%d) does not match first model (%d)", 
                             tag.upper(), counting_iter.count, facts_count)
            
            LOGGER.info("[Progress] ETL completed (model %s), elapsed time: %.2f seconds, processed %d facts", 
                       tag.upper(), ingest_cost, counting_iter.count)

            # Count nodes and edges in graph database
            graph_stats = self._get_graph_statistics()
            if graph_stats:
                LOGGER.info("[Progress] Graph database statistics (model %s): nodes=%d, edges=%d", 
                           tag.upper(), graph_stats.get("n_nodes", 0), graph_stats.get("n_edges", 0))
            else:
                LOGGER.warning("[Progress] Unable to get graph database statistics (model %s)", tag.upper())
                graph_stats = {"n_nodes": None, "n_edges": None}

            # Execute queries (if configured)
            query_metrics = {}
            if self.query_workload:
                LOGGER.info("[Progress] Starting query execution (model %s)", tag.upper())
                # Set 30 second timeout for all queries (any query exceeding 30 seconds is unacceptable)
                query_timeout = 30.0
                LOGGER.info("[Progress] All queries set timeout: %.0f seconds", query_timeout)
                try:
                    query_metrics = self.query_workload.measure(
                        self.runner.driver,
                        tag.upper(),
                        self._build_param_sampler(profile, tag.upper()),
                        self.samples_per_query,
                        query_timeout=query_timeout,
                    )
                except Exception as e:
                    # If connection error occurs during query, try to reconnect
                    if "connection" in str(e).lower() or "defunct" in str(e).lower():
                        LOGGER.warning("[Progress] Connection error during query, attempting to reconnect: %s", e)
                        try:
                            # Close old runner
                            self.runner.close()
                            # Recreate runner
                            saved_batch_size = self.runner.batch_size
                            self.runner = ETLRunner(
                                self.uri,
                                self.user,
                                self.password,
                                batch_size=saved_batch_size,
                                database=self.db_name,
                            )
                            LOGGER.info("[Progress] Connection restored")
                        except Exception as reconnect_err:
                            LOGGER.error("[Progress] Reconnection failed: %s", reconnect_err)
                    # Record query error but don't interrupt entire flow
                    query_metrics = {
                        "error": str(e),
                        "timeout": True,
                    }
                LOGGER.info("[Progress] Query completed (model %s)", tag.upper())

            # Save batch_size for recreating runner (if needed)
            saved_batch_size = self.runner.batch_size

            # Close database connection to ensure data is written to disk
            self.runner.close()
            time.sleep(3)  # Wait for data to be written to disk

            # Stop database and measure final size
            if self.uri and self.user and self.password:
                stop_database(self.uri, self.user, self.password, self.db_name)
            time.sleep(1)  # Wait for stop to complete

            final_size = self._measure_store_size()
            store_bytes = (final_size - origin_size) if (final_size and origin_size) else None

            if store_bytes is not None:
                LOGGER.info("[Measure] Data storage usage: %.2f MB (baseline: %.2f MB, final: %.2f MB)",
                           store_bytes, origin_size, final_size)

            # Note: No need to restart database, because next model will rebuild database
            # If this is the last model, or there are subsequent operations, can be handled externally
            # Here we don't recreate runner, because next model will rebuild database and recreate runner

            results[tag.upper()] = {
                "cost_ingest_s": ingest_cost,
                "store_mb": store_bytes,
                "query_latency_ms": query_metrics,
                "n_facts": facts_count,  # Fact count
                "n_nodes": graph_stats.get("n_nodes"),  # Node count
                "n_edges": graph_stats.get("n_edges"),  # Edge count
            }
            LOGGER.info("[Progress] Model %s completed (%d/%d)", tag.upper(), model_idx, total_models)

        LOGGER.info("[Progress] Experiment configuration %s completed, all models processed", config_name)
        return {
            "config": config_name,
            "params": config,
            "n_facts": facts_count,  # Also record fact count at configuration level
            "results": results
        }

    def _measure_store_size(self) -> Optional[float]:
        if self.db_parent:
            db_path = os.path.join(self.db_parent, self.db_name)
            return get_db_size(db_path)

        if self.store_path:
            return get_db_size(str(self.store_path))

        LOGGER.warning("db_parent or store_path not configured, unable to measure storage overhead.")
        return None

    def _get_graph_statistics(self) -> Optional[Dict[str, int]]:
        """
        Count nodes and edges in graph database
        
        Returns: {"n_nodes": int, "n_edges": int} or None (if unable to get)
        """
        try:
            # After ETL completes, runner may have been closed, need to recreate session
            # But here we call before closing runner after ETL completes, so runner should still be active
            if not self.runner or not self.runner.driver:
                LOGGER.warning("[Statistics] Runner or Driver unavailable, unable to count graph database")
                return None
            
            with self.runner.driver.session(database=self.runner.database) as session:
                # Count nodes
                result_nodes = session.run("MATCH (n) RETURN count(n) AS count")
                node_count = result_nodes.single()["count"] if result_nodes.peek() else 0
                
                # Count edges
                result_edges = session.run("MATCH ()-[r]->() RETURN count(r) AS count")
                edge_count = result_edges.single()["count"] if result_edges.peek() else 0
                
                return {
                    "n_nodes": node_count,
                    "n_edges": edge_count,
                }
        except Exception as e:
            LOGGER.warning("[Statistics] Error getting graph database statistics: %s", e)
            return None

    def _generate_profile(self, synth: DataSynthesizer) -> SynthProfile:
        profile = synth.build_profile()
        if self.profile_path:
            payload = {
                "entities": profile.entities,
                "dynamic_attrs": profile.dynamic_attrs,
                "relations": profile.relations,
                "time_span": profile.time_span,
                "time_points_sample": profile.time_points[:100],
            }
            self.profile_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return profile

    def _build_param_sampler(
        self, profile: SynthProfile, model_tag: str
    ) -> Callable[[str, Dict[str, Any]], Dict[str, Any]]:
        def sampler(query_name: str, defaults: Dict[str, Any]) -> Dict[str, Any]:
            params = dict(defaults)
            if query_name == "Latency_Snapshot":
                attr = params.get("attr") or next(iter(profile.dynamic_attrs.keys()), "population")
                params["attr"] = attr
                params["entityId"] = profile.pick_entity_with_attr(attr) or profile.entities[0]
                params["time"] = random.choice(profile.time_points)
            elif query_name == "Latency_History":
                attr = params.get("attr") or next(iter(profile.dynamic_attrs.keys()), "population")
                params["attr"] = attr
                params["entityId"] = profile.pick_entity_with_attr(attr) or profile.entities[0]
            elif query_name == "Latency_Evolution":
                rel_attr = params.get("relAttr") or next(iter(profile.relations.keys()), "rel_0")
                params["relAttr"] = rel_attr
                params["relType"] = params.get("relType") or rel_attr.upper()
                params["entityId"] = profile.pick_relation_entity(rel_attr) or profile.entities[0]
                params["time"] = random.choice(profile.time_points)
            elif query_name == "Latency_SpatioTemporal":
                attr = params.get("attr") or next(iter(profile.dynamic_attrs.keys()), "population")
                params["attr"] = attr
                rel_attr = params.get("relAttr") or next(iter(profile.relations.keys()), "rel_0")
                params["relAttr"] = rel_attr
                params["relType"] = params.get("relType") or rel_attr.upper()
                region_targets = profile.relations.get(rel_attr, {}).get("targets") or []
                fallback_region = region_targets[0] if region_targets else "Region_1"
                params["regionId"] = params.get("regionId") or random.choice(region_targets or [fallback_region])
                params["time"] = random.choice(profile.time_points)
                params.setdefault("minValue", 500000)
            return params

        return sampler


# ---------------------------------------------------------------------------
# Example execution entry-point
# ---------------------------------------------------------------------------


def _demo_runner() -> None:
    demo_config = {
        "n_entities": 10,
        "t_end": 19950101,
        "relation_targets": ["Region_A", "Region_B"],
        "seed": 123,
    }
    synth = DataSynthesizer(**demo_config)
    runner = ETLRunner(
        "bolt://localhost:7687",
        "neo4j",
        "YOUR_PASSWORD",
        batch_size=2000,
        database="neo4j",
    )
    static_attrs = synth.get_static_attributes()
    labels = synth.get_entity_labels()

    for model in ("a", "b", "c"):
        LOGGER.info("=== Demo run for Model %s ===", model.upper())
        runner.clear_database_and_setup_indices(labels)
        elapsed = getattr(runner, f"run_etl_model_{model}")(
            synth.iter_for_model(model), static_attrs
        )
        LOGGER.info("Model %s ingestion time: %.4f s", model.upper(), elapsed)

    runner.close()


def _load_query_workload(path: Path, warmups: int, runs: int, database: str) -> QueryWorkload:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Query workload file must be JSON object mapping name -> spec")
    queries: Dict[str, QuerySpec] = {}
    for name, spec in data.items():
        queries[name] = spec
    return QueryWorkload(queries, warmups=warmups, runs=runs, database=database)


def _default_settings() -> Dict[str, Any]:
    def _optional_path(value: Optional[str]) -> Optional[Path]:
        if not value:
            return None
        return Path(value).expanduser()

    queries_path = _optional_path(os.environ.get("BENCHMARK_QUERIES", "queries.json"))
    profile_dir = _optional_path(os.environ.get("BENCHMARK_PROFILE_DIR", "profiles"))
    results_path = _optional_path(os.environ.get("BENCHMARK_RESULTS", "results/all_runs.json"))
    store_path = _optional_path(os.environ.get("NEO4J_STORE_PATH"))

    config_env = os.environ.get("BENCHMARK_CONFIGS")
    if config_env:
        config_sequence = [item.strip() for item in config_env.split(",") if item.strip()]
    else:
        config_sequence = DEFAULT_CONFIG_SEQUENCE

    models_env = os.environ.get("BENCHMARK_MODELS")
    if models_env:
        models = tuple(m.strip().lower() for m in models_env.split(",") if m.strip())
    else:
        models = ("a", "b", "c")

    isolated_db = os.environ.get("NEO4J_ISOLATED_DB", "bench_db")
    if isolated_db and isolated_db.lower() in ("", "none"):
        isolated_db = None

    database = os.environ.get("NEO4J_DATABASE", "neo4j")
    target_db = isolated_db or database

    return {
        "uri": os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        "user": os.environ.get("NEO4J_USER", "neo4j"),
        "password": os.environ.get("NEO4J_PASSWORD", "neo4j"),
        "database": target_db,
        "isolated_db": isolated_db,
        "db_parent": os.environ.get("NEO4J_DB_PARENT"),
        "store_path": store_path,
        "queries_path": queries_path,
        "profile_dir": profile_dir,
        "results_path": results_path,
        "samples_per_query": int(os.environ.get("BENCHMARK_SAMPLES", "50")),
        "config_sequence": config_sequence,
        "models": models,
        "batch_size": int(os.environ.get("BENCHMARK_BATCH_SIZE", "5000")),
    }


def run_full_benchmark(
    config_sequence: Optional[List[str]] = None,
    settings: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    settings = settings or _default_settings()
    config_map = DataSynthesizer.get_experiment_configs()
    plan = config_sequence or settings["config_sequence"]
    plan = [cfg for cfg in plan if cfg in config_map]
    if not plan:
        raise ValueError("No executable experiment configurations.")

    profile_dir = settings.get("profile_dir")
    if profile_dir:
        profile_dir.mkdir(parents=True, exist_ok=True)

    queries_path = settings.get("queries_path")
    query_workload = None
    if queries_path and queries_path.exists():
        query_workload = _load_query_workload(
            queries_path,
            warmups=1,
            runs=3,
            database=settings["database"],
        )
    elif queries_path:
        LOGGER.warning("Query template %s does not exist, will skip latency measurement.", queries_path)

    reset_fn = None
    if settings.get("isolated_db"):
        def _reset() -> None:
            prepare_isolated_db(
                settings["uri"],
                settings["user"],
                settings["password"],
                settings["isolated_db"],
            )

        _reset()
        reset_fn = _reset

    runner = ETLRunner(
        settings["uri"],
        settings["user"],
        settings["password"],
        batch_size=settings["batch_size"],
        database=settings["database"],
    )

    results: List[Dict[str, Any]] = []
    try:
        for cfg in plan:
            LOGGER.info("=== Running experiment %s ===", cfg)
            profile_path = profile_dir / f"{cfg}.profile.json" if profile_dir else None
            suite = BenchmarkSuite(
                runner,
                query_workload=query_workload,
                store_path=settings.get("store_path"),
                db_parent=settings.get("db_parent"),
                db_name=settings["database"],
                reset_db_fn=reset_fn,
                profile_path=profile_path,
                samples_per_query=settings["samples_per_query"],
                uri=settings["uri"],
                user=settings["user"],
                password=settings["password"],
            )
            result = suite.run_configuration(
                cfg,
                config_map[cfg],
                models=tuple(m.lower() for m in settings["models"]),
            )
            results.append(result)
    finally:
        runner.close()

    results_path = settings.get("results_path")
    if results_path:
        results_path.parent.mkdir(parents=True, exist_ok=True)
        results_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
        LOGGER.info("All experiment results written to %s", results_path)

    return results


if __name__ == "__main__":  # pragma: no cover
    # Main entry point removed, please use run_full_experiments.py to run experiments
    # This avoids accidentally executing experiment code when importing
    import sys
    print("=" * 80)
    print("  main_etl_v2_REVISED.py is the core ETL and query module")
    print("  Please use run_full_experiments.py to run complete experiments")
    print("=" * 80)
    print("\nUsage:")
    print("  python run_full_experiments.py --experiment all")
    print("  python run_full_experiments.py --experiment G1 --skip-large")
    print("\nMore options:")
    print("  python run_full_experiments.py --help")
    sys.exit(0)


