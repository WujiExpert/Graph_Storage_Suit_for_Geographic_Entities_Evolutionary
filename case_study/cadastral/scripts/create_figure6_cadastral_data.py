#!/usr/bin/env python3
"""
Figure 6 - Cadastral Case Study Data Visualization

Generate data overview figure for cadastral evolution case study.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
import pandas as pd
import numpy as np
import geopandas as gpd
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Tuple, Optional
from scipy import stats
from shapely.geometry import Point
from shapely.ops import unary_union

# ============================================================================
# Visualization Configuration
# ============================================================================

# Font configuration
plt.rcParams['font.family'] = 'Times New Roman'
plt.rcParams['font.serif'] = ['Times New Roman']
plt.rcParams['mathtext.fontset'] = 'stix'
plt.rcParams['font.size'] = 10
plt.rcParams['axes.labelsize'] = 11
plt.rcParams['axes.titlesize'] = 12
plt.rcParams['xtick.labelsize'] = 10
plt.rcParams['ytick.labelsize'] = 10
plt.rcParams['legend.fontsize'] = 9
plt.rcParams['figure.titlesize'] = 12

# Color scheme
COLORS = {
    'split': '#C62828',  # Red - split
    'merge': '#1565C0',  # Blue - merge
    'reid': '#2E7D32',  # Green - re-identification
    'economic': '#F57C00',  # Orange - economic attribute
    'physical': '#7B1FA2',  # Purple - physical attribute
    'planning': '#00897B',  # Cyan - planning attribute
    'spatial': '#5D4037',  # Brown - spatial attribute
    'subdivision': '#E64A19',  # Dark orange - subdivision area
    'default': '#CCCCCC'  # Gray - default color
}

# Line width and marker size
LINE_WIDTH = 1.5
MARKER_SIZE = 4


# ============================================================================
# Helper functions
# ============================================================================

def add_panel_label(ax, label: str, x: float = -0.12, y: float = 1.02,
                    fontsize: int = 14, weight: str = 'bold'):
    """Add panel label (A, B, C, etc.)"""
    ax.text(x, y, label, transform=ax.transAxes,
            fontsize=fontsize, weight=weight, va='bottom', ha='right',
            family='Times New Roman')


def load_cadastral_data(csv_path: Path) -> Dict:
    """Load cadastral case study data"""
    print(f"Loading data: {csv_path}")
    df = pd.read_csv(csv_path)

    # Separate attribute changes (Type=0) and relations (Type=1)
    attributes = df[df['Type'] == 0].copy()
    relations = df[df['Type'] == 1].copy()

    print(f"  Attribute change records: {len(attributes):,}")
    print(f"  Topological relation records: {len(relations):,}")

    return {
        'attributes': attributes,
        'relations': relations,
        'all_facts': df
    }


def load_shapefile(shp_path: Path) -> Optional[gpd.GeoDataFrame]:
    """Load shapefile"""
    try:
        print(f"Loading shapefile: {shp_path}")
        gdf = gpd.read_file(shp_path, engine='pyogrio')
        initial_len = len(gdf)

        # --- 1. Manhattan data filtering ---
        # Strategy: prioritize Borough field, if not available use BBL first digit
        if 'Borough' in gdf.columns:
            gdf = gdf[gdf['Borough'] == 'MN']
        else:
            # Ensure BBL is string before filtering
            temp_bbl = pd.to_numeric(gdf['BBL'], errors='coerce').fillna(0).astype(int).astype(str)
            gdf = gdf[temp_bbl.str.startswith('1')]

        if len(gdf) < initial_len:
            print(f"    -> Filtered for Manhattan: {len(gdf)} / {initial_len} records.")

        # --- 2. Exclude park land ---
        # Park land has large area but may have abnormal land prices, affecting visualization
        # before_park_filter = len(gdf)
        # if 'ZoneDist1' in gdf.columns:
        #     # Exclude records with ZoneDist1='PARK'
        #     gdf = gdf[gdf['ZoneDist1'] != 'PARK']
        #     # Also exclude cases where ZoneDist1 is empty but may contain 'PARK'
        #     gdf = gdf[~gdf['ZoneDist1'].astype(str).str.contains('PARK', case=False, na=False)]
        #     if len(gdf) < before_park_filter:
        #         print(f"    -> Excluded parks: {len(gdf)} / {before_park_filter} records remaining.")

        print(f"  Loading complete: {len(gdf)} parcels")
        return gdf
    except Exception as e:
        print(f"  ❌ Loading failed: {e}")
        return None


def load_manhattan_boundary(base_dir: Path) -> Optional[gpd.GeoDataFrame]:
    """Load official Manhattan boundary data (from nybb_20d)"""
    try:
        boundary_path = base_dir / 'nybb_20d/nybb.shp'
        print(f"Loading official boundary data: {boundary_path}")

        if not boundary_path.exists():
            print(f"  ❌ Boundary file does not exist: {boundary_path}")
            return None

        gdf = gpd.read_file(boundary_path, engine='pyogrio')
        print(f"  Original record count: {len(gdf)}")

        # Filter Manhattan by BoroCode=1
        if 'BoroCode' in gdf.columns:
            gdf = gdf[gdf['BoroCode'] == 1]
            print(f"  -> After filtering (BoroCode=1): {len(gdf)} records")
        elif 'BOROCODE' in gdf.columns:
            gdf = gdf[gdf['BOROCODE'] == 1]
            print(f"  -> After filtering (BOROCODE=1): {len(gdf)} records")
        else:
            print(f"  ⚠️ BoroCode field not found, available fields: {gdf.columns.tolist()}")
            return None

        if len(gdf) == 0:
            print(f"  ❌ Manhattan boundary data not found")
            return None

        print(f"  ✅ Successfully loaded Manhattan boundary")
        return gdf
    except Exception as e:
        print(f"  ❌ Failed to load boundary: {e}")
        return None


def create_figure_data_overview(data: Dict, shp_path: Optional[Path], boundary_dir: Optional[Path], output_dir: Path):
    """Create Figure 6: Data overview figure (horizontal 2x2 layout)"""
    print("Creating Figure 6: Cadastral Data Overview...")

    attributes = data['attributes']
    relations = data['relations']

    # Horizontal layout: better suited for Manhattan's elongated geographic features
    fig = plt.figure(figsize=(16, 8))
    gs = GridSpec(2, 2, figure=fig, hspace=0.35, wspace=0.20,  # Reduce left-right column spacing
                  left=0.08, right=0.95, top=0.95, bottom=0.08)

    attributes = data['attributes']
    relations = data['relations']

    # ========================================================================
    # Panel A: Manhattan parcel spatial distribution (two subplots: original distribution + change kernel density)
    # ========================================================================
    # Use fig.add_gridspec to create nested grid, divide Panel A into left and right subplots
    # Get Panel A position information
    ax1_placeholder = fig.add_subplot(gs[0, 0])
    ax1_placeholder.remove()  # Remove placeholder, we'll create two subplots at the same position

    # Get original Panel A position (bbox)
    bbox = ax1_placeholder.get_position()

    # Calculate positions of left and right subplots
    left_margin = 0.02  # Left margin
    right_margin = 0.02  # Right margin
    middle_gap = 0.05  # Middle gap (increased to avoid axis overlap)

    left_width = (bbox.width - left_margin - right_margin - middle_gap) / 2
    right_width = left_width

    # Create left subplot
    ax1a = fig.add_axes([bbox.x0 + left_margin, bbox.y0, left_width, bbox.height])
    # Create right subplot
    ax1b = fig.add_axes([bbox.x0 + left_margin + left_width + middle_gap, bbox.y0,
                         right_width, bbox.height])

    # ========================================================================
    # Process shp data: add change count field and save
    # ========================================================================
    if shp_path and shp_path.exists():
        gdf = load_shapefile(shp_path)
        if gdf is None:
            print("  ❌ Unable to load shapefile")
        else:
            print("  Processing shp data: adding change count field...")

            # Ensure BBL column exists
            if 'BBL' not in gdf.columns:
                print("  ❌ shapefile missing BBL column")
            else:
                # Convert to numeric type
                gdf['BBL'] = pd.to_numeric(gdf['BBL'], errors='coerce')
                gdf = gdf.dropna(subset=['BBL'])
                gdf['BBL'] = gdf['BBL'].astype(str)

                # Convert to projected coordinate system (EPSG:2263, NY State Plane Long Island, unit: feet)
                if gdf.crs is None:
                    gdf.set_crs(epsg=2263, inplace=True)
                elif gdf.crs.to_string() != 'EPSG:2263':
                    if gdf.crs.to_string() == 'EPSG:4326':
                        gdf = gdf.to_crs(epsg=2263)
                    else:
                        gdf = gdf.to_crs(epsg=4326).to_crs(epsg=2263)

                print(f"  ✅ Parcel data: {len(gdf)} parcels, CRS: {gdf.crs}")

                # Calculate change count for each parcel
                if 'Year' not in attributes.columns:
                    attributes['Start'] = pd.to_numeric(attributes['Start'], errors='coerce')
                    attributes = attributes.dropna(subset=['Start'])
                    attributes['Year'] = attributes['Start'].astype(int)

                base_year = 2011
                changes_only = attributes[attributes['Year'] > base_year].copy()

                # Calculate change counts
                change_counts = changes_only.groupby(changes_only['EntityID'].astype(str)).size()

                # Match BBL and add change count field (column name limited to 10 characters, use ChgCount)
                gdf['BBL_str'] = gdf['BBL'].astype(str)
                gdf['ChgCount'] = gdf['BBL_str'].map(change_counts).fillna(0).astype(int)

                # If direct matching fails, try numeric format matching
                if gdf['ChgCount'].sum() == 0:
                    print(f"  ⚠️ String matching failed, trying numeric format matching...")
                    try:
                        changed_bbls_numeric = {str(int(float(bbl))): count for bbl, count in change_counts.items()}
                        gdf['BBL_numeric'] = pd.to_numeric(gdf['BBL'], errors='coerce').astype('Int64').astype(str)
                        gdf['ChgCount'] = gdf['BBL_numeric'].map(changed_bbls_numeric).fillna(0).astype(int)
                        print(f"  ✅ Numeric format matching successful")
                    except Exception as e:
                        print(f"  ❌ Numeric format matching also failed: {e}")

                # Statistics
                total_changes = gdf['ChgCount'].sum()
                parcels_with_changes = (gdf['ChgCount'] > 0).sum()
                print(f"  ✅ Change statistics: Total changes={total_changes}, Parcels with changes={parcels_with_changes}/{len(gdf)}")

                # Convert to point file (parcel center positions)
                gdf_points = gdf.copy()
                gdf_points['geometry'] = gdf_points.geometry.centroid

                # Only keep BBL and ChgCount attributes
                gdf_points = gdf_points[['BBL', 'ChgCount', 'geometry']].copy()

                # Ensure BBL is string type
                gdf_points['BBL'] = gdf_points['BBL'].astype(str)

                # Save as point shp file
                output_shp_path = output_dir / 'parcel_centroids_with_change_count.shp'
                # Ensure output directory exists
                output_dir.mkdir(parents=True, exist_ok=True)

                # Save shp file
                gdf_points.to_file(output_shp_path, driver='ESRI Shapefile')
                print(f"  ✅ Point file saved to: {output_shp_path}")
                print(f"  ✅ Contains {len(gdf_points)} points, attributes: BBL, ChgCount")

    # ========================================================================
    # Subplots A and B left empty (user will create with other software later)
    # ========================================================================
    ax1a.text(0.5, 0.5, 'Panel A\n(To be added)',
              ha='center', va='center', transform=ax1a.transAxes,
              fontsize=12, color='gray')
    ax1a.set_title('Parcel Distribution', fontsize=11, weight='bold', pad=10)
    ax1a.axis('off')

    ax1b.text(0.5, 0.5, 'Panel B\n(To be added)',
              ha='center', va='center', transform=ax1b.transAxes,
              fontsize=12, color='gray')
    ax1b.set_title('Change Density', fontsize=11, weight='bold', pad=10)
    ax1b.axis('off')

    print(f"  ✅ Subplots A and B left empty")

    add_panel_label(ax1a, 'A', x=-0.12, y=1.0)

    # ========================================================================
    # Panel B: Attribute change frequency (swapped to top-right, as it has attribute classification info)
    # ========================================================================
    ax4 = fig.add_subplot(gs[0, 1])

    # Count attribute changes per year
    # Note: 2011 is the base year, all records with Start=2011 are initial values, not changes
    # Only records with Start>=2012 are actual changes
    attributes['Start'] = pd.to_numeric(attributes['Start'], errors='coerce')
    attributes = attributes.dropna(subset=['Start'])
    attributes['Year'] = attributes['Start'].astype(int)

    # Exclude base year (2011) records, only count changes
    base_year = 2011
    changes_only = attributes[attributes['Year'] > base_year].copy()

    # Classify according to attribute mapping in ETL code
    attr_categories = {
        'Economic': ['AssessTot', 'OwnerName'],
        'Physical': ['LotArea', 'BldgArea', 'NumFloors', 'BldgFront', 'BldgDepth',
                     'LotFront', 'LotDepth', 'YearBuilt', 'BldgClass'],
        'Planning': ['LandUse'],
        'Subdivision': ['ComArea', 'ResArea', 'OfficeArea', 'RetailArea',
                        'GarageArea', 'FactryArea']
    }

    # Color mapping
    colors_map = {
        'Economic': COLORS['economic'],
        'Physical': COLORS['physical'],
        'Planning': COLORS['planning'],
        'Spatial': COLORS['spatial'],
        'Subdivision': COLORS['subdivision']
    }

    # Draw horizontal bar chart (attribute change frequency)
    # Count changes per attribute
    attr_counts = changes_only.groupby('Attribute').size().sort_values(ascending=True)

    # Assign color to each attribute (according to category)
    attr_colors = []
    for attr in attr_counts.index:
        color = COLORS['default']
        for cat, attrs in attr_categories.items():
            if attr in attrs:
                color = colors_map.get(cat, COLORS['default'])
                break
        attr_colors.append(color)

    # Draw horizontal bar chart
    y_pos = np.arange(len(attr_counts))
    bars = ax4.barh(y_pos, attr_counts.values, color=attr_colors, alpha=0.8)

    # Use logarithmic axis (as AssessTot has far more changes than other attributes)
    ax4.set_xscale('log')

    # Set y-axis labels
    ax4.set_yticks(y_pos)
    ax4.set_yticklabels(attr_counts.index, fontsize=9)
    ax4.set_xlabel('Number of Changes (log scale)', fontsize=11, weight='bold')
    ax4.set_title('Attribute Change Frequency', fontsize=12, weight='bold', pad=10)
    ax4.grid(True, alpha=0.3, linestyle='--', axis='x', which='both')

    # Add value labels on bars
    for i, (attr, val) in enumerate(attr_counts.items()):
        label = f'{int(val):,}' if val >= 1000 else str(int(val))
        ax4.text(val, i, label, va='center', fontsize=8)

    # Create legend (show attribute classification)
    legend_elements = [
        plt.Rectangle((0, 0), 1, 1, facecolor=colors_map['Economic'], alpha=0.8, label='Economic'),
        plt.Rectangle((0, 0), 1, 1, facecolor=colors_map['Physical'], alpha=0.8, label='Physical'),
        plt.Rectangle((0, 0), 1, 1, facecolor=colors_map['Planning'], alpha=0.8, label='Planning'),
        plt.Rectangle((0, 0), 1, 1, facecolor=colors_map['Subdivision'], alpha=0.8, label='Subdivision')
    ]
    ax4.legend(handles=legend_elements, loc='lower right', fontsize=8.5,
               frameon=True, fancybox=True, shadow=True)
    add_panel_label(ax4, 'B', x=-0.1, y=1.0)

    # ========================================================================
    # Panel C: Topology evolution event time distribution
    # ========================================================================
    ax3 = fig.add_subplot(gs[1, 0])

    # Count topology events
    # Note: Topology relations are created in y_next year (Start=y_next), so 2011 should not have topology relations
    # But to be safe, we also exclude 2011
    relations['Start'] = pd.to_numeric(relations['Start'], errors='coerce')
    relations = relations.dropna(subset=['Start'])
    relations['Year'] = relations['Start'].astype(int)

    # Exclude base year (2011), only count changes
    base_year = 2011
    topology_changes = relations[relations['Year'] > base_year].copy()

    # Count by event type and year
    event_types = ['SPLIT_INTO', 'MERGED_INTO', 'REID_AS']
    type_colors = {
        'SPLIT_INTO': COLORS['split'],
        'MERGED_INTO': COLORS['merge'],
        'REID_AS': COLORS['reid']
    }

    # Get all years (starting from 2012)
    topology_years = sorted(topology_changes['Year'].unique()) if len(topology_changes) > 0 else []

    # Count for each event type
    type_data = {}
    for event_type in event_types:
        type_relations = topology_changes[topology_changes['Attribute'] == event_type]
        if len(type_relations) > 0:
            type_yearly = type_relations.groupby('Year').size()
            type_data[event_type] = [type_yearly.get(y, 0) for y in topology_years]
        else:
            type_data[event_type] = [0] * len(topology_years)

    # Draw grouped bar chart
    if len(topology_years) > 0:
        x = np.arange(len(topology_years))
        width = 0.25

        for i, event_type in enumerate(event_types):
            offset = (i - 1) * width
            ax3.bar(x + offset, type_data[event_type], width,
                    label=event_type.replace('_', ' '),
                    color=type_colors[event_type], alpha=0.8)

        ax3.set_xticks(x)
        ax3.set_xticklabels([int(y) for y in topology_years], rotation=45, ha='right')
    else:
        ax3.text(0.5, 0.5, 'No topology events',
                 ha='center', va='center', transform=ax3.transAxes)

    ax3.set_xlabel('Year', fontsize=11, weight='bold')
    ax3.set_ylabel('Number of Topology Events', fontsize=11, weight='bold')
    ax3.set_title('Topology Evolution Events Over Time', fontsize=12, weight='bold', pad=10)
    ax3.legend(loc='upper left', fontsize=8.5, frameon=True, fancybox=True, shadow=True)
    ax3.grid(True, alpha=0.3, linestyle='--', axis='y')
    add_panel_label(ax3, 'C', x=0, y=1.0)

    # ========================================================================
    # Panel D: Attribute change time distribution (swapped to bottom-right)
    # ========================================================================
    ax2 = fig.add_subplot(gs[1, 1])

    # Get all years (starting from 2012)
    all_years = sorted(changes_only['Year'].unique()) if len(changes_only) > 0 else []

    # Count for each category (only count changes, exclude base year)
    category_data = {}
    for category, attrs in attr_categories.items():
        if attrs:
            category_attrs = changes_only[changes_only['Attribute'].isin(attrs)]
            if len(category_attrs) > 0:
                category_yearly = category_attrs.groupby('Year').size()
                category_data[category] = [category_yearly.get(y, 0) for y in all_years]
            else:
                category_data[category] = [0] * len(all_years)
        else:
            category_data[category] = [0] * len(all_years)

    # Draw stacked area plot
    if len(all_years) > 0:
        bottom = np.zeros(len(all_years))
        colors_map = {
            'Economic': COLORS['economic'],
            'Physical': COLORS['physical'],
            'Planning': COLORS['planning'],
            'Spatial': COLORS['spatial'],
            'Subdivision': COLORS['subdivision']
        }

        for category in ['Economic', 'Physical', 'Planning', 'Subdivision']:
            if category in category_data:
                ax2.fill_between(all_years, bottom,
                                 [bottom[i] + category_data[category][i] for i in range(len(all_years))],
                                 alpha=0.7, label=category, color=colors_map[category])
                bottom = [bottom[i] + category_data[category][i] for i in range(len(all_years))]

    ax2.set_xlabel('Year', fontsize=11, weight='bold')
    ax2.set_ylabel('Number of Attribute Changes', fontsize=11, weight='bold')
    ax2.set_title('Attribute Changes Over Time', fontsize=12, weight='bold', pad=10)
    ax2.legend(loc='upper left', fontsize=8.5, frameon=True, fancybox=True, shadow=True)
    ax2.grid(True, alpha=0.3, linestyle='--')
    add_panel_label(ax2, 'D', x=-0.1, y=1.0)

    # ========================================================================
    # Save figure
    # ========================================================================
    output_path = output_dir / 'Figure6_Cadastral_Data'
    fig.savefig(f'{output_path}.pdf', dpi=300, bbox_inches='tight')
    fig.savefig(f'{output_path}.png', dpi=300, bbox_inches='tight')
    print(f"✅ Figure 6 saved to: {output_path}.pdf/png")
    plt.close()


def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Generate cadastral case study visualization figures',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--data', '-d',
        type=str,
        default='data/final_manhattan_facts.csv',
        help='Data file path'
    )

    parser.add_argument(
        '--shp', '-s',
        type=str,
        default='data/MapPLUTO_Data/2020/MapPLUTO.shp',
        help='Shapefile path (for Panel A)'
    )

    parser.add_argument(
        '--boundary', '-b',
        type=str,
        default='data',
        help='Boundary data directory (contains nybb_20d folder)'
    )

    parser.add_argument(
        '--output', '-o',
        type=str,
        default='../../figures',
        help='Output directory'
    )

    parser.add_argument(
        '--figure', '-f',
        type=str,
        choices=['overview', 'all'],
        default='all',
        help='Type of figure to generate'
    )

    args = parser.parse_args()

    # Prepare paths
    base_dir = Path(__file__).parent.parent
    data_path = base_dir / args.data
    shp_path = base_dir / args.shp if args.shp else None
    boundary_dir = base_dir / args.boundary if args.boundary else None
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not data_path.exists():
        print(f"❌ Error: Data file does not exist: {data_path}")
        return

    # Load data
    print("Loading data...")
    data = load_cadastral_data(data_path)

    # Generate figures
    if args.figure in ['overview', 'all']:
        create_figure_data_overview(data, shp_path, boundary_dir, output_dir)

    print("\n" + "=" * 60)
    print("✅ All figures generated successfully!")
    print("=" * 60)


if __name__ == '__main__':
    main()

