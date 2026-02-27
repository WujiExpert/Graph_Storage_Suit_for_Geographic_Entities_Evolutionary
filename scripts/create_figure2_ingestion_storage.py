#!/usr/bin/env python3
"""
Figure 2 - Ingestion Efficiency and Storage Scalability

Generates a comprehensive figure showing ingestion efficiency and storage scalability
across four experiment groups (G1, G2, G3, G4).

Layout: 4 rows x 2 columns
- Each row corresponds to an experiment group
- Left column: Build time and storage usage comparison
- Right column: Node and edge count statistics
"""

from pathlib import Path
import os
import json
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

# Configure Matplotlib cache directory first to avoid read/write issues
SCRIPT_DIR = Path(__file__).resolve().parent
MPL_CACHE_DIR = SCRIPT_DIR / ".mpl_cache"
MPL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
os.environ["MPLCONFIGDIR"] = str(MPL_CACHE_DIR)

import numpy as np
import pandas as pd
import seaborn as sns
from scipy import stats

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpecFromSubplotSpec

# Set global font to Times New Roman
plt.rcParams['font.family'] = 'Times New Roman'
plt.rcParams['font.serif'] = ['Times New Roman']
plt.rcParams['mathtext.fontset'] = 'stix'  # Math font also uses STIX similar to Times

# Try importing scienceplots (if installed)
try:
    import scienceplots
    plt.style.use(['science', 'ieee', 'no-latex'])
except:
    plt.style.use('default')

# Color scheme
COLORS = {
    'A': '#2E86AB',  # Blue
    'B': '#F18F01',  # Orange
    'C': '#C73E1D'   # Red
}

# Model labels
MODEL_LABELS = {
    'A': 'AMA',
    'B': 'SVM',
    'C': 'PAM'
}

# Font configuration - use Times New Roman
FONT_FAMILY = 'Times New Roman'
LABEL_FONT = {'size': 14, 'weight': 'bold', 'family': FONT_FAMILY}
TITLE_FONT = {'size': 16, 'weight': 'bold', 'family': FONT_FAMILY}
LEGEND_FONT = {'size': 12, 'family': FONT_FAMILY}
TICK_FONT = {'size': 12, 'family': FONT_FAMILY}

# Global style settings - use Times New Roman
plt.rcParams.update({
    'font.family': 'serif',
    'font.serif': ['Times New Roman', 'Times', 'DejaVu Serif'],
    'font.size': 12,
    'axes.labelsize': 12,
    'axes.titlesize': 14,
    'xtick.labelsize': 10,
    'ytick.labelsize': 10,
    'legend.fontsize': 10,
    'figure.titlesize': 16,
    'savefig.format': 'eps',
    'axes.linewidth': 1.5,
    'grid.linewidth': 0.5,
    'lines.linewidth': 2.5,
    'lines.markersize': 8,
    'patch.linewidth': 1.5,
    'mathtext.fontset': 'stix',
})


def format_scale_label(n):
    """Format entity scale label (exponential notation)"""
    exp = int(np.floor(np.log10(n)))
    coeff = n / (10 ** exp)
    coeff_str = ('%.1f' % coeff).rstrip('0').rstrip('.')
    if coeff_str == '' or coeff_str == '1':
        return f'$10^{{{exp}}}$'
    return rf'${coeff_str}\times10^{{{exp}}}$'


def format_vol_label(v: float) -> str:
    """Format volatility value (0-1) as percentage string"""
    return f"{int(round(v * 100))}%"


def format_density_label(v: float) -> str:
    """Format attribute density display"""
    if float(v).is_integer():
        return f"{int(v)} attrs"
    return f"{v:.1f} attrs"


def load_all_experiment_data(file_path: str = 'experiment_results.json') -> Dict:
    """Load data for all four experiment groups"""
    with open(file_path, 'r', encoding='utf-8') as f:
        all_results = json.load(f)
    
    data = {
        'G1_Scale': {
            'resource': defaultdict(lambda: {'storage': [], 'ingest': [], 'n_entities': []}),
            'graph_stats': defaultdict(lambda: {'n_nodes': [], 'n_edges': [], 'n_entities': []}),
        },
        'G2_Vol_Attr': {
            'resource': defaultdict(lambda: {'storage': [], 'ingest': [], 'volatility': []}),
            'graph_stats': defaultdict(lambda: {'n_nodes': [], 'n_edges': [], 'volatility': []}),
        },
        'G3_Vol_Rel': {
            'resource': defaultdict(lambda: {'storage': [], 'ingest': [], 'volatility': []}),
            'graph_stats': defaultdict(lambda: {'n_nodes': [], 'n_edges': [], 'volatility': []}),
        },
        'G4_Density_Props': {
            'resource': defaultdict(lambda: {'storage': [], 'ingest': [], 'density': []}),
            'graph_stats': defaultdict(lambda: {'n_nodes': [], 'n_edges': [], 'density': []}),
        },
    }
    
    for result in all_results:
        config_name = result.get('config', '')
        params = result.get('params', {})
        model_results = result.get('results', {})
        
        # G1_Scale experiment group
        if config_name.startswith('G1_Scale'):
            n_entities = params.get('n_entities', 0)
            for model, model_data in model_results.items():
                storage_mb = model_data.get('store_mb', 0)
                ingest_s = model_data.get('cost_ingest_s', 0)
                n_nodes = model_data.get('n_nodes', 0)
                n_edges = model_data.get('n_edges', 0)
                
                data['G1_Scale']['resource'][model]['storage'].append(storage_mb)
                data['G1_Scale']['resource'][model]['ingest'].append(ingest_s)
                data['G1_Scale']['resource'][model]['n_entities'].append(n_entities)
                
                data['G1_Scale']['graph_stats'][model]['n_nodes'].append(n_nodes)
                data['G1_Scale']['graph_stats'][model]['n_edges'].append(n_edges)
                data['G1_Scale']['graph_stats'][model]['n_entities'].append(n_entities)
        
        # G2_Vol_Attr experiment group
        elif config_name.startswith('G2_Vol_Attr'):
            volatility = params.get('v_attribute', 0.0)
            for model, model_data in model_results.items():
                storage_mb = model_data.get('store_mb', 0)
                ingest_s = model_data.get('cost_ingest_s', 0)
                n_nodes = model_data.get('n_nodes', 0)
                n_edges = model_data.get('n_edges', 0)
                
                data['G2_Vol_Attr']['resource'][model]['storage'].append(storage_mb)
                data['G2_Vol_Attr']['resource'][model]['ingest'].append(ingest_s)
                data['G2_Vol_Attr']['resource'][model]['volatility'].append(volatility)
                
                data['G2_Vol_Attr']['graph_stats'][model]['n_nodes'].append(n_nodes)
                data['G2_Vol_Attr']['graph_stats'][model]['n_edges'].append(n_edges)
                data['G2_Vol_Attr']['graph_stats'][model]['volatility'].append(volatility)
        
        # G3_Vol_Rel experiment group
        elif config_name.startswith('G3_Vol_Rel'):
            volatility = params.get('v_relation', 0.0)
            for model, model_data in model_results.items():
                storage_mb = model_data.get('store_mb', 0)
                ingest_s = model_data.get('cost_ingest_s', 0)
                n_nodes = model_data.get('n_nodes', 0)
                n_edges = model_data.get('n_edges', 0)
                
                data['G3_Vol_Rel']['resource'][model]['storage'].append(storage_mb)
                data['G3_Vol_Rel']['resource'][model]['ingest'].append(ingest_s)
                data['G3_Vol_Rel']['resource'][model]['volatility'].append(volatility)
                
                data['G3_Vol_Rel']['graph_stats'][model]['n_nodes'].append(n_nodes)
                data['G3_Vol_Rel']['graph_stats'][model]['n_edges'].append(n_edges)
                data['G3_Vol_Rel']['graph_stats'][model]['volatility'].append(volatility)
        
        # G4_Density_Props experiment group
        elif config_name.startswith('G4_Density'):
            density = float(params.get('n_dynamic_props', 0))
            for model, model_data in model_results.items():
                storage_mb = model_data.get('store_mb', 0)
                ingest_s = model_data.get('cost_ingest_s', 0)
                n_nodes = model_data.get('n_nodes', 0)
                n_edges = model_data.get('n_edges', 0)
                
                data['G4_Density_Props']['resource'][model]['storage'].append(storage_mb)
                data['G4_Density_Props']['resource'][model]['ingest'].append(ingest_s)
                data['G4_Density_Props']['resource'][model]['density'].append(density)
                
                data['G4_Density_Props']['graph_stats'][model]['n_nodes'].append(n_nodes)
                data['G4_Density_Props']['graph_stats'][model]['n_edges'].append(n_edges)
                data['G4_Density_Props']['graph_stats'][model]['density'].append(density)
    
    return data


def setup_axes_style(ax, grid=True):
    """Set axis style"""
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_linewidth(1.5)
    ax.spines['bottom'].set_linewidth(1.5)
    if grid:
        ax.grid(True, linestyle='--', alpha=0.3, linewidth=0.5)
    ax.tick_params(axis='both', which='major', labelsize=TICK_FONT['size'])
    for label in ax.get_xticklabels():
        label.set_family(TICK_FONT['family'])
    for label in ax.get_yticklabels():
        label.set_family(TICK_FONT['family'])


def add_panel_label(ax, label: str, x: float = -0.1, y: float = 1.05):
    """Add panel label (A, B, C...)"""
    ax.text(x, y, label, transform=ax.transAxes,
            fontsize=TITLE_FONT['size'], fontweight=TITLE_FONT['weight'],
            family=TITLE_FONT['family'],
            va='bottom', ha='right')


def plot_resource_panel(ax, ax_twin, exp_data, x_key, x_label_func, row_label):
    """Plot resource panel (storage + build time)"""
    # Prepare data
    all_x_values = set()
    for model in ['A', 'B', 'C']:
        resource_data = exp_data['resource'][model]
        if resource_data[x_key]:
            all_x_values.update(resource_data[x_key])
    
    x_values_sorted = sorted(all_x_values)
    width = 0.25
    
    # Prepare storage and build time data
    storage_data = {model: {} for model in ['A', 'B', 'C']}
    ingest_data = {model: {} for model in ['A', 'B', 'C']}
    
    for model in ['A', 'B', 'C']:
        resource_data = exp_data['resource'][model]
        if not resource_data[x_key]:
            continue
        
        for i, x_val in enumerate(resource_data[x_key]):
            storage_data[model][x_val] = resource_data['storage'][i]
            ingest_data[model][x_val] = resource_data['ingest'][i]
    
    # Draw storage cost (bar chart)
    x_pos = np.arange(len(x_values_sorted))
    
    for i, model in enumerate(['A', 'B', 'C']):
        storage_values = [storage_data[model].get(x, 0) for x in x_values_sorted]
        bar_positions = x_pos + i * width - width
        
        ax.bar(bar_positions, storage_values, width=width, alpha=0.6,
               color=COLORS[model], label=f'{MODEL_LABELS[model]} (Storage)',
               edgecolor='black', linewidth=1)
    
    # Draw build time (line chart)
    for model in ['A', 'B', 'C']:
        ingest_values = [ingest_data[model].get(x, 0) for x in x_values_sorted]
        ax_twin.plot(x_pos, ingest_values, 's--', linewidth=2.5,
                     color=COLORS[model], label=f'{MODEL_LABELS[model]} (Ingest)',
                     markersize=6, markeredgecolor='white', markeredgewidth=1)
    
    # Set axis labels (use LaTeX format)
    # Determine experiment group type based on row_label
    if row_label in ['A', 'B']:
        x_label = r'Entity scale $\left|\mathbf{E}\right|$'
    elif row_label in ['C', 'D']:
        x_label = r'Attribute volatility $V_{\mathrm{attr}}$'
    elif row_label in ['E', 'F']:
        x_label = r'Relation volatility $V_{\mathrm{rel}}$'
    elif row_label in ['G', 'H']:
        x_label = r'Attribute density $D$'
    else:
        x_label = x_key.replace('_', ' ').title()
    ax.set_xlabel(x_label, fontsize=LABEL_FONT['size'], family=LABEL_FONT['family'])
    ax.set_ylabel('Storage Cost (MB)', fontsize=LABEL_FONT['size'], color='black', family=LABEL_FONT['family'])
    ax_twin.set_ylabel('Ingest Time (s)', fontsize=LABEL_FONT['size'], color='black', family=LABEL_FONT['family'])
    
    ax.set_xticks(x_pos)
    ax.set_xticklabels([x_label_func(x) for x in x_values_sorted], rotation=0, ha='center', family=FONT_FAMILY)
    
    # Use linear scale uniformly
    ax.set_yscale('linear')
    ax_twin.set_yscale('linear')
    
    # Legend
    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax_twin.get_legend_handles_labels()
    
    order = ['A', 'B', 'C']
    storage_handles = []
    storage_labels = []
    ingest_handles = []
    ingest_labels = []
    
    for model in order:
        for handle, label in zip(lines1, labels1):
            if MODEL_LABELS[model] in label and 'Storage' in label:
                storage_handles.append(handle)
                storage_labels.append(label.replace(' (Storage)', ''))
                break
        for handle, label in zip(lines2, labels2):
            if MODEL_LABELS[model] in label and 'Ingest' in label:
                ingest_handles.append(handle)
                ingest_labels.append(label.replace(' (Ingest)', ''))
                break
    
    all_legend_handles = storage_handles + ingest_handles
    all_legend_labels = storage_labels + ingest_labels
    
    # Only show legend in subplot A
    if row_label == 'A':
        ax.legend(all_legend_handles, all_legend_labels, title='Storage Cost   Ingest Time',
                  frameon=True, fontsize=LEGEND_FONT['size'], 
                  prop={'family': LEGEND_FONT['family']}, 
                  loc='upper left', ncol=2, 
                  columnspacing=1.2, handletextpad=0.5)
    
    setup_axes_style(ax, grid=True)
    setup_axes_style(ax_twin, grid=False)
    ax_twin.spines['top'].set_visible(False)
    add_panel_label(ax, row_label)


def plot_graph_stats_panel(ax, ax_twin, exp_data, x_key, x_label_func, row_label):
    """Plot graph statistics panel (node count + edge count)"""
    # Prepare data
    all_x_values = set()
    for model in ['A', 'B', 'C']:
        graph_stats_data = exp_data['graph_stats'][model]
        if graph_stats_data[x_key]:
            all_x_values.update(graph_stats_data[x_key])
    
    x_values_sorted = sorted(all_x_values)
    width = 0.25
    
    # Prepare node and edge count data
    nodes_data = {model: {} for model in ['A', 'B', 'C']}
    edges_data = {model: {} for model in ['A', 'B', 'C']}
    
    for model in ['A', 'B', 'C']:
        graph_stats_data = exp_data['graph_stats'][model]
        if not graph_stats_data[x_key]:
            continue
        
        for i, x_val in enumerate(graph_stats_data[x_key]):
            # Convert node and edge counts to millions (×10^6)
            nodes_data[model][x_val] = graph_stats_data['n_nodes'][i] / 1e6
            edges_data[model][x_val] = graph_stats_data['n_edges'][i] / 1e6
    
    # Draw node count (bar chart)
    x_pos = np.arange(len(x_values_sorted))
    
    for i, model in enumerate(['A', 'B', 'C']):
        nodes_values = [nodes_data[model].get(x, 0) for x in x_values_sorted]
        bar_positions = x_pos + i * width - width
        
        ax.bar(bar_positions, nodes_values, width=width, alpha=0.6,
               color=COLORS[model], label=f'{MODEL_LABELS[model]} (Nodes)',
               edgecolor='black', linewidth=1)
    
    # Draw edge count (line chart)
    for model in ['A', 'B', 'C']:
        edges_values = [edges_data[model].get(x, 0) for x in x_values_sorted]
        ax_twin.plot(x_pos, edges_values, 's--', linewidth=2.5,
                     color=COLORS[model], label=f'{MODEL_LABELS[model]} (Edges)',
                     markersize=6, markeredgecolor='white', markeredgewidth=1)
    
    # Set axis labels (use LaTeX format)
    # Determine experiment group type based on row_label
    if row_label in ['A', 'B']:
        x_label = r'Entity scale $\left|\mathbf{E}\right|$'
    elif row_label in ['C', 'D']:
        x_label = r'Attribute volatility $V_{\mathrm{attr}}$'
    elif row_label in ['E', 'F']:
        x_label = r'Relation volatility $V_{\mathrm{rel}}$'
    elif row_label in ['G', 'H']:
        x_label = r'Attribute density $D$'
    else:
        x_label = x_key.replace('_', ' ').title()
    ax.set_xlabel(x_label, fontsize=LABEL_FONT['size'], family=LABEL_FONT['family'])
    ax.set_ylabel('Number of Nodes ($\\times 10^6$)', fontsize=LABEL_FONT['size'], color='black', family=LABEL_FONT['family'])
    ax_twin.set_ylabel('Number of Edges ($\\times 10^6$)', fontsize=LABEL_FONT['size'], color='black', family=LABEL_FONT['family'])
    
    ax.set_xticks(x_pos)
    ax.set_xticklabels([x_label_func(x) for x in x_values_sorted], rotation=0, ha='center', family=FONT_FAMILY)
    
    # Use linear scale uniformly
    ax.set_yscale('linear')
    ax_twin.set_yscale('linear')
    
    # Legend
    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax_twin.get_legend_handles_labels()
    
    order = ['A', 'B', 'C']
    nodes_handles = []
    nodes_labels = []
    edges_handles = []
    edges_labels = []
    
    for model in order:
        for handle, label in zip(lines1, labels1):
            if MODEL_LABELS[model] in label and 'Nodes' in label:
                nodes_handles.append(handle)
                nodes_labels.append(label.replace(' (Nodes)', ''))
                break
        for handle, label in zip(lines2, labels2):
            if MODEL_LABELS[model] in label and 'Edges' in label:
                edges_handles.append(handle)
                edges_labels.append(label.replace(' (Edges)', ''))
                break
    
    all_legend_handles = nodes_handles + edges_handles
    all_legend_labels = nodes_labels + edges_labels
    
    # Only show legend in subplot B
    if row_label == 'B':
        ax.legend(all_legend_handles, all_legend_labels, title='Number of Nodes   Number of Edges',
                  frameon=True, fontsize=LEGEND_FONT['size'], 
                  prop={'family': LEGEND_FONT['family']}, 
                  loc='upper left', ncol=2, 
                  columnspacing=1.2, handletextpad=0.5)
    
    setup_axes_style(ax, grid=True)
    setup_axes_style(ax_twin, grid=False)
    ax_twin.spines['top'].set_visible(False)
    add_panel_label(ax, row_label)


def create_figure2_ingestion_storage(data: Dict, output_dir: Path):
    """Create Figure 2: Ingestion Efficiency and Storage Scalability (4 rows x 2 columns)"""
    print("Creating Figure 2 (Ingestion Efficiency and Storage Scalability)...")
    
    # 4 rows x 2 columns layout
    fig = plt.figure(figsize=(14, 20))
    gs = fig.add_gridspec(4, 2, hspace=0.3, wspace=0.25,
                          left=0.08, right=0.95, top=0.97, bottom=0.05)
    
    # ========================================================================
    # Row 1: G1_Scale experiment group
    # ========================================================================
    exp_data = data['G1_Scale']
    
    # Left column: Resource panel
    ax1 = fig.add_subplot(gs[0, 0])
    ax1_twin = ax1.twinx()
    plot_resource_panel(ax1, ax1_twin, exp_data, 'n_entities', format_scale_label, 'A')
    
    # Right column: Graph statistics panel
    ax2 = fig.add_subplot(gs[0, 1])
    ax2_twin = ax2.twinx()
    plot_graph_stats_panel(ax2, ax2_twin, exp_data, 'n_entities', format_scale_label, 'B')
    
    # ========================================================================
    # Row 2: G2_Vol_Attr experiment group
    # ========================================================================
    exp_data = data['G2_Vol_Attr']
    
    # Left column: Resource panel
    ax3 = fig.add_subplot(gs[1, 0])
    ax3_twin = ax3.twinx()
    plot_resource_panel(ax3, ax3_twin, exp_data, 'volatility', format_vol_label, 'C')
    
    # Right column: Graph statistics panel
    ax4 = fig.add_subplot(gs[1, 1])
    ax4_twin = ax4.twinx()
    plot_graph_stats_panel(ax4, ax4_twin, exp_data, 'volatility', format_vol_label, 'D')
    
    # ========================================================================
    # Row 3: G3_Vol_Rel experiment group
    # ========================================================================
    exp_data = data['G3_Vol_Rel']
    
    # Left column: Resource panel
    ax5 = fig.add_subplot(gs[2, 0])
    ax5_twin = ax5.twinx()
    plot_resource_panel(ax5, ax5_twin, exp_data, 'volatility', format_vol_label, 'E')
    
    # Right column: Graph statistics panel
    ax6 = fig.add_subplot(gs[2, 1])
    ax6_twin = ax6.twinx()
    plot_graph_stats_panel(ax6, ax6_twin, exp_data, 'volatility', format_vol_label, 'F')
    
    # ========================================================================
    # Row 4: G4_Density_Props experiment group
    # ========================================================================
    exp_data = data['G4_Density_Props']
    
    # Left column: Resource panel
    ax7 = fig.add_subplot(gs[3, 0])
    ax7_twin = ax7.twinx()
    plot_resource_panel(ax7, ax7_twin, exp_data, 'density', format_density_label, 'G')
    
    # Right column: Graph statistics panel
    ax8 = fig.add_subplot(gs[3, 1])
    ax8_twin = ax8.twinx()
    plot_graph_stats_panel(ax8, ax8_twin, exp_data, 'density', format_density_label, 'H')
    
    # Save
    output_dir.mkdir(exist_ok=True, parents=True)
    plt.savefig(output_dir / 'Figure2_Ingestion_Storage.pdf', format='pdf', dpi=600, bbox_inches='tight')
    plt.savefig(output_dir / 'Figure2_Ingestion_Storage.png', format='png', dpi=600, bbox_inches='tight')
    print(f"  ✅ Figure 2 saved")
    plt.close()


if __name__ == '__main__':
    # Load data
    data = load_all_experiment_data('../experiment_results.json')
    
    # Create figure
    create_figure2_ingestion_storage(data, Path('figures'))
    
    print("\n✅ Figure 2 (Ingestion Efficiency and Storage Scalability) completed!")
    print("   - Row 1: G1_Scale (Entity Scale)")
    print("   - Row 2: G2_Vol_Attr (Attribute Volatility)")
    print("   - Row 3: G3_Vol_Rel (Relation Volatility)")
    print("   - Row 4: G4_Density_Props (Property Density)")
    print("   - Left column: Storage Cost + Ingest Time")
    print("   - Right column: Number of Nodes + Number of Edges")

