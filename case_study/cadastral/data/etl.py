#!/usr/bin/env python3
"""
Case Study: Cadastral Evolution ETL

Converts NYC MapPLUTO cadastral data (2011-2020) into atomic facts
for spatiotemporal graph storage benchmarking.

Processes:
- Parcel attribute changes (assessment, land use, physical properties, etc.)
- Topology evolution events (split, merge, re-identification)
- Generates atomic facts for property attributes and parcel relationships
"""

import geopandas as gpd
import pandas as pd
import numpy as np
import warnings
import os
import gc
from shapely.validation import make_valid

# Suppress non-fatal warnings like CRS conversion
warnings.filterwarnings('ignore')


class ScientificCadastralETL:
    def __init__(self, data_root, years):
        """
        :param data_root: Data root directory
        :param years: List of consecutive years
        """
        self.data_root = data_root
        self.years = sorted(years)
        self.facts = []

        # In-memory index: For quickly closing previous record
        # Key: (EntityID, AttrName) -> Value: Index in self.facts list
        self.active_indices = {}

        # --- 1. Wide table attribute mapping (18 dimensions + identifiers) ---
        # Key: Possible column names in Shapefile (auto-converted to uppercase for matching)
        # Value: Standard attribute names stored in database
        self.ATTR_MAP = {
            # > Group 0: Filtering and primary key
            'BBL': 'BBL',
            'BOROUGH': 'Borough',  # Used for filtering regions

            # > Group A: Core values and identifiers
            'ASSESSTOT': 'AssessTot',
            'LANDUSE': 'LandUse',
            'OWNERNAME': 'OwnerName',

            # > Group B: Physical form (usually change together)
            'LOTAREA': 'LotArea',
            'BLDGAREA': 'BldgArea',
            'NUMFLOORS': 'NumFloors',
            'BLDGFRONT': 'BldgFront',
            'BLDGDEPTH': 'BldgDepth',
            'LOTFRONT': 'LotFront',
            'LOTDEPTH': 'LotDepth',
            'YEARBUILT': 'YearBuilt',
            'BLDGCLASS': 'BldgClass',

            # > Group C: Subdivided areas (high-pressure test area for Model C)
            'COMAREA': 'ComArea',  # Commercial
            'RESAREA': 'ResArea',  # Residential
            'OFFICEAREA': 'OfficeArea',
            'RETAILAREA': 'RetailArea',
            'GARAGEAREA': 'GarageArea',
            'FACTRYAREA': 'FactryArea'
        }

        # Generate actual monitoring list (exclude ID and filter fields)
        self.monitor_attrs = [v for k, v in self.ATTR_MAP.items() if v not in ['BBL', 'Borough']]

        # --- 2. Scientific parameters ---
        # Topology determination threshold: 30%
        # Explanation: Only when overlap area > 30% (whether relative to parent or child) is considered valid evolution
        self.RATIO_THRESHOLD = 0.30

        # Force projection: NYC Long Island (Ft) - Ensure accurate area calculation
        self.TARGET_CRS = "EPSG:2263"

    def _clean_val(self, val):
        """
        Data cleaning core:
        1. Handle None/NaN -> ""
        2. Handle float "100.0" -> string "100"
        3. Preserve "0"
        """
        if pd.isna(val) or val is None:
            return ""
        s_val = str(val).strip()

        try:
            f_val = float(s_val)
            # If integer (123.0), convert to string without decimal point
            if f_val.is_integer():
                return str(int(f_val))
            # Otherwise keep as is (123.5)
            return str(f_val)
        except ValueError:
            return s_val

    def _add_record(self, eid, label, attr, val, vtype, start, end=9999):
        """Write atomic fact: [ID, Label, Attr, Val, Type, Start, End]"""
        clean_v = self._clean_val(val)
        if clean_v == "": return  # Reject empty values

        # End defaults to 9999 (infinity)
        new_row = [str(eid), label, attr, clean_v, int(vtype), int(start), end]
        self.facts.append(new_row)

        # Register active index for subsequent closing
        if vtype == 0:
            self.active_indices[(str(eid), attr)] = len(self.facts) - 1

    def _close_record(self, eid, attr, end_time):
        """Close time interval"""
        key = (str(eid), attr)
        if key in self.active_indices:
            idx = self.active_indices[key]
            self.facts[idx][-1] = int(end_time)  # Modify End Time
            del self.active_indices[key]

    def _load_and_standardize_shp(self, year):
        """
        Smart loader:
        1. Auto-search file names (supports MN-prefixed or Citywide files)
        2. Standardize column names
        3. [Core] Force filter Manhattan data
        4. Unify projection & fix geometry
        """
        year_suffix = str(year)[2:]
        candidates = [
            # Prefer Manhattan partition
            os.path.join(self.data_root, str(year), f"MNMapPLUTO.shp"),
            # If not found, use citywide full dataset
            os.path.join(self.data_root, str(year), f"MapPLUTO.shp")
        ]

        path = None
        for p in candidates:
            if os.path.exists(p):
                path = p
                break

        if not path:
            print(f"❌ Error: Data missing for year {year}")
            return None

        print(f"    Loading: {path} ...")
        try:
            # 1. Load data (use pyogrio engine for speed)
            # Don't specify columns first, need dynamic mapping
            gdf = gpd.read_file(path, engine='pyogrio')

            # 2. Column name mapping (convert to uppercase then match)
            current_cols = {c.upper(): c for c in gdf.columns}
            rename_dict = {}

            # Must contain BBL
            if 'BBL' in current_cols:
                rename_dict[current_cols['BBL']] = 'BBL'
            else:
                print(f"❌ Critical: 'BBL' column missing in {year} data!")
                return None

            # Map other attributes
            found_attrs = 0
            for map_key, standard_name in self.ATTR_MAP.items():
                if map_key in current_cols:
                    rename_dict[current_cols[map_key]] = standard_name
                    if standard_name not in ['BBL', 'Borough']: found_attrs += 1

            # Rename columns
            gdf = gdf.rename(columns=rename_dict)

            # --- 3. [Key step] Manhattan data filtering ---
            initial_len = len(gdf)
            # Strategy: Prefer Borough field, if not available use BBL first digit
            if 'Borough' in gdf.columns:
                gdf = gdf[gdf['Borough'] == 'MN']
            else:
                # Ensure BBL is string before filtering
                temp_bbl = pd.to_numeric(gdf['BBL'], errors='coerce').fillna(0).astype(int).astype(str)
                gdf = gdf[temp_bbl.str.startswith('1')]

            if len(gdf) < initial_len:
                print(f"    -> Filtered for Manhattan: {len(gdf)} / {initial_len} records.")

            # 4. Trim columns (keep only needed)
            cols_to_keep = ['geometry'] + [v for k, v in self.ATTR_MAP.items() if v in gdf.columns]
            gdf = gdf[cols_to_keep]

            print(f"    -> Mapped {found_attrs}/{len(self.monitor_attrs)} attributes.")

            # 5. Projection and cleaning
            if gdf.crs != self.TARGET_CRS:
                gdf = gdf.to_crs(self.TARGET_CRS)

            # Final BBL cleaning
            gdf['BBL'] = pd.to_numeric(gdf['BBL'], errors='coerce').fillna(0).astype(int).astype(str)
            gdf = gdf[gdf['BBL'] != '0']

            # 6. Geometry repair (handle bowties, self-intersections)
            gdf['geometry'] = gdf.apply(lambda row: make_valid(row.geometry) if row.geometry else None, axis=1)
            gdf = gdf[~gdf.is_empty & gdf.geometry.notna()]

            return gdf

        except Exception as e:
            print(f"❌ Read Error in {year}: {e}")
            return None

    def run(self):
        print(f"=== Starting Scientific ETL Pipeline ({self.years[0]}-{self.years[-1]}) ===")
        print(f"=== Monitoring {len(self.monitor_attrs)} Attributes ===")

        # --- 1. Initialize base year (Base Year) ---
        print(f"\n>>> Processing Base Year: {self.years[0]}")
        gdf_prev = self._load_and_standardize_shp(self.years[0])
        if gdf_prev is None: return

        # Write initial snapshot
        for _, row in gdf_prev.iterrows():
            eid = row['BBL']
            for attr in self.monitor_attrs:
                if attr in row:
                    self._add_record(eid, "Parcel", attr, row[attr], 0, self.years[0])

        # --- 2. Year-by-year rolling comparison ---
        for i in range(len(self.years) - 1):
            y_curr = self.years[i]
            y_next = self.years[i + 1]
            print(f"\n>>> Transition: {y_curr} -> {y_next}")

            gdf_next = self._load_and_standardize_shp(y_next)
            if gdf_next is None: continue

            # Set operations
            set_prev = set(gdf_prev['BBL'])
            set_next = set(gdf_next['BBL'])

            vanished = set_prev - set_next
            new_born = set_next - set_prev
            stable = set_prev & set_next

            print(f"    [Stable: {len(stable)} | Vanished: {len(vanished)} | New: {len(new_born)}]")

            # =========================================================
            # A. Topology evolution determination (Spatial Join + Bidirectional Check)
            # =========================================================
            sub_vanished = gdf_prev[gdf_prev['BBL'].isin(vanished)]
            sub_new = gdf_next[gdf_next['BBL'].isin(new_born)]

            if not sub_vanished.empty and not sub_new.empty:
                print("    Calculating Topology...")

                # 1. Coarse screening
                candidates = gpd.sjoin(sub_vanished, sub_new, how='inner', predicate='intersects')

                # 2. Pre-compute cache
                geom_old = sub_vanished['geometry'].to_dict()
                geom_new = sub_new['geometry'].to_dict()
                area_old = sub_vanished.geometry.area.to_dict()
                area_new = sub_new.geometry.area.to_dict()
                id_old = sub_vanished['BBL'].to_dict()
                id_new = sub_new['BBL'].to_dict()

                valid_links = []

                # 3. Precise calculation
                for idx_old, row in candidates.iterrows():
                    idx_new = row['index_right']
                    try:
                        poly_o = geom_old.get(idx_old)
                        poly_n = geom_new.get(idx_new)

                        if poly_o and poly_n:
                            inter_area = poly_o.intersection(poly_n).area
                            a_old = area_old.get(idx_old, 0)
                            a_new = area_new.get(idx_new, 0)

                            if a_old < 1 or a_new < 1: continue

                            # [Bidirectional threshold]: Can capture both Split and Merge
                            ratio_new = inter_area / a_new
                            ratio_old = inter_area / a_old

                            if ratio_new > self.RATIO_THRESHOLD or ratio_old > self.RATIO_THRESHOLD:
                                valid_links.append((id_old[idx_old], id_new[idx_new]))
                    except Exception:
                        continue

                valid_links = list(set(valid_links))

                # 4. Relationship classification
                from collections import defaultdict
                p_deg = defaultdict(int)
                c_deg = defaultdict(int)
                for p, c in valid_links:
                    p_deg[p] += 1
                    c_deg[c] += 1

                count_evol = 0
                for p, c in valid_links:
                    rtype = "EVOLVED_TO"  # Default value
                    if p_deg[p] > 1:
                        rtype = "SPLIT_INTO"
                    elif c_deg[c] > 1:
                        rtype = "MERGED_INTO"
                    elif p_deg[p] == 1 and c_deg[c] == 1:
                        rtype = "REID_AS"

                    # Write relationship (Type=1)
                    self._add_record(p, "Parcel", rtype, c, 1, y_next, y_next)
                    count_evol += 1

                print(f"    -> Identified {count_evol} verified events.")

            # =========================================================
            # B. Wide table attribute change determination (Robust Attribute Changes)
            # =========================================================
            # Extract only Stable portion for acceleration
            df_p = pd.DataFrame(gdf_prev[gdf_prev['BBL'].isin(stable)]).set_index('BBL')
            df_n = pd.DataFrame(gdf_next[gdf_next['BBL'].isin(stable)]).set_index('BBL')

            common_idx = df_p.index.intersection(df_n.index)

            for attr in self.monitor_attrs:
                # Error tolerance: Only compare when attribute exists in both years
                if attr not in df_p.columns or attr not in df_n.columns:
                    continue

                # Strict cleaning and comparison
                s_p = df_p.loc[common_idx, attr].apply(self._clean_val)
                s_n = df_n.loc[common_idx, attr].apply(self._clean_val)

                diff = s_p != s_n
                changes = s_n[diff]

                if not changes.empty:
                    # Batch update
                    for bbl, new_val in changes.items():
                        self._close_record(bbl, attr, y_next)
                        self._add_record(bbl, "Parcel", attr, new_val, 0, y_next)

            # =========================================================
            # C. Lifecycle management (Lifecycle)
            # =========================================================
            # 1. Vanished entities -> Close
            for bbl in vanished:
                for attr in self.monitor_attrs:
                    self._close_record(bbl, attr, y_next)

            # 2. New entities -> Initialize
            if not sub_new.empty:
                for idx, row in sub_new.iterrows():
                    eid = row['BBL']
                    for attr in self.monitor_attrs:
                        if attr in row:
                            self._add_record(eid, "Parcel", attr, row[attr], 0, y_next)

            # Garbage collection
            del gdf_prev
            gc.collect()
            gdf_prev = gdf_next

        print("\n=== Pipeline Complete ===")

    def export(self, out_path):
        print(f">>> Exporting {len(self.facts)} atomic facts to CSV...")
        columns = ['EntityID', 'Label', 'Attribute', 'Value', 'Type', 'Start', 'End']
        df_out = pd.DataFrame(self.facts, columns=columns)

        # Simple statistics
        n_rels = len(df_out[df_out['Type'] == 1])
        n_attrs = len(df_out[df_out['Type'] == 0])
        print(f"    - Dynamic Relations: {n_rels}")
        print(f"    - Dynamic Attributes: {n_attrs}")

        df_out.to_csv(out_path, index=False)
        print(f"✅ Data saved to {out_path}")


if __name__ == "__main__":
    # --- Configuration area ---
    # Assume data path: ./MapPLUTO_Data/2010/MNMapPLUTO.shp (or Citywide MapPLUTO.shp)
    DATA_DIR = './MapPLUTO_Data'

    # Recommend using consecutive years for best results
    YEARS_TO_PROCESS = [2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]
    OUTPUT_FILE = 'final_manhattan_facts.csv'

    # --- Execute ---
    etl = ScientificCadastralETL(DATA_DIR, YEARS_TO_PROCESS)
    etl.run()
    etl.export(OUTPUT_FILE)