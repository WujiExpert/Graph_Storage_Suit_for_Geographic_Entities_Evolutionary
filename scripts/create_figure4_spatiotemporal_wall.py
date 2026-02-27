#!/usr/bin/env python3
"""
Figure 4 - Bottleneck Analysis: The Spatio-Temporal Wall

Analyzes spatiotemporal query performance across four experiment groups,
highlighting performance bottlenecks and relative model performance.

Layout: 2 rows x 2 columns
- Each subplot shows spatiotemporal query performance for one experiment group
- Includes performance ratio markers (e.g., "C/A 10x slower")
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
plt.rcParams['mathtext.fontset'] = 'stix'

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

# Font configuration
FONT_FAMILY = 'Times New Roman'
LABEL_FONT = {'size': 14, 'weight': 'bold', 'family': FONT_FAMILY}
TITLE_FONT = {'size': 16, 'weight': 'bold', 'family': FONT_FAMILY}
LEGEND_FONT = {'size': 12, 'family': FONT_FAMILY}
TICK_FONT = {'size': 12, 'family': FONT_FAMILY}

# Global style settings
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
    """Load spatiotemporal query data for all four experiment groups"""
    with open(file_path, 'r', encoding='utf-8') as f:
        all_results = json.load(f)
    
    data = {
        'G1_Scale': defaultdict(list),  # {model: [(n_entities, raw_samples)]}
        'G2_Vol_Attr': defaultdict(list),  # {model: [(volatility, raw_samples)]}
        'G3_Vol_Rel': defaultdict(list),  # {model: [(volatility, raw_samples)]}
        'G4_Density_Props': defaultdict(list),  # {model: [(density, raw_samples)]}
    }
    
    for result in all_results:
        config_name = result.get('config', '')
        params = result.get('params', {})
        model_results = result.get('results', {})
        
        # G1_Scale experiment group
        if config_name.startswith('G1_Scale'):
            n_entities = params.get('n_entities', 0)
            for model, model_data in model_results.items():
                query_latency = model_data.get('query_latency_ms', {})
                st_query = query_latency.get('Latency_SpatioTemporal', {})
                if st_query and 'raw_samples_ms' in st_query:
                    raw_samples = st_query['raw_samples_ms']
                    data['G1_Scale'][model].append((n_entities, raw_samples))
        
        # G2_Vol_Attr experiment group
        elif config_name.startswith('G2_Vol_Attr'):
            volatility = params.get('v_attribute', 0.0)
            for model, model_data in model_results.items():
                query_latency = model_data.get('query_latency_ms', {})
                st_query = query_latency.get('Latency_SpatioTemporal', {})
                if st_query and 'raw_samples_ms' in st_query:
                    raw_samples = st_query['raw_samples_ms']
                    data['G2_Vol_Attr'][model].append((volatility, raw_samples))
        
        # G3_Vol_Rel experiment group
        elif config_name.startswith('G3_Vol_Rel'):
            volatility = params.get('v_relation', 0.0)
            for model, model_data in model_results.items():
                query_latency = model_data.get('query_latency_ms', {})
                st_query = query_latency.get('Latency_SpatioTemporal', {})
                if st_query and 'raw_samples_ms' in st_query:
                    raw_samples = st_query['raw_samples_ms']
                    data['G3_Vol_Rel'][model].append((volatility, raw_samples))
        
        # G4_Density_Props experiment group
        elif config_name.startswith('G4_Density'):
            density = float(params.get('n_dynamic_props', 0))
            for model, model_data in model_results.items():
                query_latency = model_data.get('query_latency_ms', {})
                st_query = query_latency.get('Latency_SpatioTemporal', {})
                if st_query and 'raw_samples_ms' in st_query:
                    raw_samples = st_query['raw_samples_ms']
                    data['G4_Density_Props'][model].append((density, raw_samples))
    
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


def plot_spatiotemporal_panel(ax, exp_data: Dict, x_label_func, x_label_text: str, 
                               panel_label: str, highlight_x_value=None, show_legend: bool = False):
    """Plot spatiotemporal query panel (reference original subplot B)"""
    # Prepare data
    spatio_mean_map = {}
    
    # Unify x-axis positions
    all_x_values = sorted({float(x) for model in ['A', 'B', 'C']
                           for x, samples in exp_data[model] if samples})
    x_to_idx = {x: idx for idx, x in enumerate(all_x_values)}
    
    for model in ['A', 'B', 'C']:
        model_data = exp_data[model]
        if not model_data:
            continue
        
        # Sort by x value
        model_data.sort(key=lambda x: x[0])
        
        x_list = []
        means = []
        stds = []
        p25s = []
        p75s = []
        
        for x_val, raw_samples in model_data:
            if not raw_samples:
                continue
            
            samples = np.array(raw_samples)
            x_list.append(float(x_val))
            means.append(np.mean(samples))
            stds.append(np.std(samples))
            p25s.append(np.percentile(samples, 25))
            p75s.append(np.percentile(samples, 75))
        
        if not x_list:
            continue
        
        positions = np.array([x_to_idx[x] for x in x_list])
        means = np.array(means)
        stds = np.array(stds)
        p25s = np.array(p25s)
        p75s = np.array(p75s)
        
        # Draw error band (use P25-P75 as fluctuation range)
        ax.fill_between(positions, p25s, p75s, 
                       alpha=0.2, color=COLORS[model], label='')
        
        # Draw mean line
        ax.plot(positions, means, 'o-', linewidth=2.5, 
               color=COLORS[model], label=MODEL_LABELS[model],
               markersize=6, markeredgecolor='white', markeredgewidth=1)
        
        # Save mean values for subsequent annotation
        spatio_mean_map[model] = {float(x): float(m) for x, m in zip(x_list, means)}
    
    # Highlight differences compressed in log scale (C vs A)
    base_model = 'A'
    compare_model = 'C'
    
    # Determine highlighted x value
    if highlight_x_value is None:
        # Default to maximum x value
        highlight_x_value = max(all_x_values) if all_x_values else None
    
    if (highlight_x_value is not None and
            base_model in spatio_mean_map and compare_model in spatio_mean_map and
            highlight_x_value in spatio_mean_map[base_model] and 
            highlight_x_value in spatio_mean_map[compare_model]):
        base_val = spatio_mean_map[base_model][highlight_x_value]
        compare_val = spatio_mean_map[compare_model][highlight_x_value]
        if base_val > 0 and compare_val > 0:
            ratio = compare_val / base_val
            ratio_text = f"{ratio:.0f}× slower"
            pos_x = x_to_idx.get(highlight_x_value)
            if pos_x is not None:
                ax.annotate(
                    ratio_text,
                    xy=(pos_x, compare_val),
                    xytext=(pos_x + 0.8, compare_val * 0.6),
                    textcoords='data',
                    arrowprops=dict(arrowstyle='->', color=COLORS[compare_model], linewidth=1.5),
                    fontsize=LABEL_FONT['size'],
                    family=LABEL_FONT['family'],
                    bbox=dict(boxstyle='round,pad=0.2', fc='white', ec=COLORS[compare_model], alpha=0.9)
                )
                # Adjust C/A marker position and font size based on panel_label
                if panel_label in ['B', 'C', 'D']:
                    # Subplots B-D: Move marker down, enlarge font
                    y_pos = compare_val * 1.5
                    font_size = LABEL_FONT['size'] + 3
                else:
                    # Subplot A: Keep original position
                    y_pos = compare_val * 1.8
                    font_size = LABEL_FONT['size'] + 3  # Enlarge font for all subplots
                
                ax.text(
                    pos_x,
                    y_pos,
                    r"$\frac{C}{A}$",
                    color=COLORS[compare_model],
                    fontsize=font_size,
                    family=LABEL_FONT['family'],
                    ha='center'
                )
    
    # Set axes
    ax.set_xlabel(x_label_text, fontsize=LABEL_FONT['size'], family=LABEL_FONT['family'])
    ax.set_ylabel('SpatioTemporal Query Latency (ms, log scale)', fontsize=LABEL_FONT['size'], family=LABEL_FONT['family'])
    ax.set_xscale('linear')
    ax.set_yscale('log')
    
    # Set X-axis ticks
    ax.set_xticks(range(len(all_x_values)))
    ax.set_xticklabels([x_label_func(x) for x in all_x_values], family=FONT_FAMILY)
    ax.set_xlim(-0.5, len(all_x_values) - 0.5)
    
    # Legend (only show in first subplot)
    if show_legend:
        handles, labels = ax.get_legend_handles_labels()
        order = ['A', 'B', 'C']
        ordered_handles = []
        ordered_labels = []
        for model in order:
            for handle, label in zip(handles, labels):
                if MODEL_LABELS[model] == label:
                    ordered_handles.append(handle)
                    ordered_labels.append(label)
                    break
        ax.legend(ordered_handles, ordered_labels,
                 frameon=True, fontsize=LEGEND_FONT['size'], 
                 prop={'family': LEGEND_FONT['family']})
    
    setup_axes_style(ax, grid=True)
    add_panel_label(ax, panel_label)


def create_figure4_spatiotemporal_wall(data: Dict, output_dir: Path):
    """Create Figure 4: The Spatio-Temporal Wall (2 rows x 2 columns)"""
    print("Creating Figure 4 (The Spatio-Temporal Wall)...")
    
    # 2 rows x 2 columns layout
    fig = plt.figure(figsize=(14, 12))
    gs = fig.add_gridspec(2, 2, hspace=0.25, wspace=0.22,
                          left=0.08, right=0.95, top=0.97, bottom=0.08)
    
    # ========================================================================
    # Panel A: G1_Scale experiment group
    # ========================================================================
    ax1 = fig.add_subplot(gs[0, 0])
    plot_spatiotemporal_panel(ax1, data['G1_Scale'], format_scale_label,
                             r'Entity scale $\left|\mathbf{E}\right|$', 'A',
                             highlight_x_value=100000, show_legend=True)
    
    # ========================================================================
    # Panel B: G2_Vol_Attr experiment group
    # ========================================================================
    ax2 = fig.add_subplot(gs[0, 1])
    plot_spatiotemporal_panel(ax2, data['G2_Vol_Attr'], format_vol_label,
                             r'Attribute volatility $V_{\mathrm{attr}}$', 'B',
                             highlight_x_value=0.6)
    
    # ========================================================================
    # Panel C: G3_Vol_Rel experiment group
    # ========================================================================
    ax3 = fig.add_subplot(gs[1, 0])
    plot_spatiotemporal_panel(ax3, data['G3_Vol_Rel'], format_vol_label,
                             r'Relation volatility $V_{\mathrm{rel}}$', 'C',
                             highlight_x_value=0.2)
    
    # ========================================================================
    # Panel D: G4_Density_Props experiment group
    # ========================================================================
    ax4 = fig.add_subplot(gs[1, 1])
    plot_spatiotemporal_panel(ax4, data['G4_Density_Props'], format_density_label,
                             r'Attribute density $D$', 'D',
                             highlight_x_value=15)
    
    # Save
    output_dir.mkdir(exist_ok=True, parents=True)
    plt.savefig(output_dir / 'Figure4_SpatioTemporal_Wall.pdf', format='pdf', dpi=600, bbox_inches='tight')
    plt.savefig(output_dir / 'Figure4_SpatioTemporal_Wall.png', format='png', dpi=600, bbox_inches='tight')
    print(f"  ✅ Figure 4 saved")
    plt.close()


if __name__ == '__main__':
    # Load data
    data = load_all_experiment_data('../experiment_results.json')
    
    # Create figure
    create_figure4_spatiotemporal_wall(data, Path('figures'))
    
    print("\n✅ Figure 4 (The Spatio-Temporal Wall) completed!")
    print("   - Panel A: G1_Scale (Entity Scale)")
    print("   - Panel B: G2_Vol_Attr (Attribute Volatility)")
    print("   - Panel C: G3_Vol_Rel (Relation Volatility)")
    print("   - Panel D: G4_Density_Props (Attribute Density)")

