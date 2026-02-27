#!/usr/bin/env python3
"""
Figure 3 - Temporal and Topological Query Performance
Main figure: G1_Scale experiment group's three queries (Snapshot, History, Evolution) - 1 row x 3 columns
Supplement figures: Three query figures for G2/G3/G4 experiment groups
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
from matplotlib.lines import Line2D

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


def load_experiment_data(file_path: str, exp_group: str) -> Dict:
    """Load data for specified experiment group"""
    with open(file_path, 'r', encoding='utf-8') as f:
        all_results = json.load(f)
    
    # Filter specified experiment group
    if exp_group == 'G1_Scale':
        exp_results = [r for r in all_results if r.get('config', '').startswith('G1_Scale')]
        x_key = 'n_entities'
    elif exp_group == 'G2_Vol_Attr':
        exp_results = [r for r in all_results if r.get('config', '').startswith('G2_Vol_Attr')]
        x_key = 'v_attribute'
    elif exp_group == 'G3_Vol_Rel':
        exp_results = [r for r in all_results if r.get('config', '').startswith('G3_Vol_Rel')]
        x_key = 'v_relation'
    elif exp_group == 'G4_Density_Props':
        exp_results = [r for r in all_results if r.get('config', '').startswith('G4_Density')]
        x_key = 'n_dynamic_props'
    else:
        raise ValueError(f"Unknown experiment group: {exp_group}")
    
    data = {
        'baseline': defaultdict(lambda: defaultdict(list)),  # {model: {query_type: [(x_value, raw_samples)]}}
    }
    
    for result in exp_results:
        params = result.get('params', {})
        model_results = result.get('results', {})
        
        x_value = params.get(x_key, 0)
        if exp_group == 'G4_Density_Props':
            x_value = float(x_value)
        
        for model, model_data in model_results.items():
            query_latency = model_data.get('query_latency_ms', {})
            
            # Basic queries (Snapshot, History, Evolution)
            for query_type in ['Latency_Snapshot', 'Latency_History', 'Latency_Evolution']:
                query_data = query_latency.get(query_type, {})
                if query_data and 'raw_samples_ms' in query_data:
                    raw_samples = query_data['raw_samples_ms']
                    query_name = query_type.replace('Latency_', '')
                    data['baseline'][model][query_name].append((x_value, raw_samples))
    
    return data, x_key


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


def plot_query_panel(ax, data: Dict, query_type: str, x_key: str, x_label_func, x_label_text: str, panel_label: str, show_legend: bool = False):
    """Plot single query panel (violin plot + box plot)"""
    # Prepare data
    violin_data = []
    for model in ['A', 'B', 'C']:
        query_data = data['baseline'][model][query_type]
        if not query_data:
            continue
        
        for x_val, raw_samples in query_data:
            if not raw_samples:
                continue
            
            for sample in raw_samples:
                violin_data.append({
                    'Model': model,
                    'X_Value': x_val,
                    'Latency_ms': sample
                })
    
    if not violin_data:
        return
    
    df_violin = pd.DataFrame(violin_data)
    x_values_unique = sorted(df_violin['X_Value'].unique())
    
    # Prepare plotting data
    plot_data = []
    plot_positions = []
    plot_colors = []
    x_ticks = []
    x_labels = []
    
    pos = 0
    for x_val in x_values_unique:
        group_data = []
        group_colors = []
        for model in ['A', 'B', 'C']:
            model_data = df_violin[(df_violin['Model'] == model) & (df_violin['X_Value'] == x_val)]
            if not model_data.empty:
                group_data.append(model_data['Latency_ms'].values)
                group_colors.append(COLORS[model])
        
        if group_data:
            plot_data.extend(group_data)
            plot_colors.extend(group_colors)
            # Calculate positions
            for j in range(len(group_data)):
                plot_positions.append(pos + j * 0.3)
            # X-axis label position (middle of three models)
            x_ticks.append(pos + 0.3)
            x_labels.append(x_val)
            pos += len(group_data) * 0.3 + 0.5  # Spacing between scales
    
    if plot_data:
        # 1. Draw violin plot (as background)
        parts = ax.violinplot(plot_data, positions=plot_positions,
                              widths=0.3, showmeans=False, showmedians=False,
                              showextrema=False)
        
        # Set violin plot colors
        for pc, color in zip(parts['bodies'], plot_colors):
            pc.set_facecolor(color)
            pc.set_alpha(0.3)
            pc.set_edgecolor('black')
            pc.set_linewidth(0.5)
        
        # 2. Draw box plot (overlay on violin plot)
        for i, (data_array, pos, color) in enumerate(zip(plot_data, plot_positions, plot_colors)):
            # Calculate box plot statistics
            q1 = np.percentile(data_array, 25)
            q2 = np.percentile(data_array, 50)  # Median
            q3 = np.percentile(data_array, 75)
            iqr = q3 - q1
            
            # Calculate whisker range
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            # Find min and max values within non-outlier range
            non_outlier_data = data_array[(data_array >= lower_bound) & (data_array <= upper_bound)]
            if len(non_outlier_data) > 0:
                lower_whisker = np.min(non_outlier_data)
                upper_whisker = np.max(non_outlier_data)
            else:
                lower_whisker = q1
                upper_whisker = q3
            
            # Draw box
            box_width = 0.2
            ax.add_patch(plt.Rectangle((pos - box_width/2, q1), box_width, q3 - q1,
                                       facecolor=color, edgecolor='black', linewidth=1.5, alpha=0.7))
            # Draw median line
            ax.plot([pos - box_width/2, pos + box_width/2], [q2, q2], 
                   color='black', linewidth=2.0, linestyle='-', solid_capstyle='butt')
            # Draw whisker lines
            ax.plot([pos, pos], [lower_whisker, q1], 
                   color='black', linewidth=1.5, linestyle='-', solid_capstyle='butt')
            ax.plot([pos, pos], [q3, upper_whisker], 
                   color='black', linewidth=1.5, linestyle='-', solid_capstyle='butt')
            # Draw whisker endpoints
            ax.plot([pos - 0.05, pos + 0.05], [lower_whisker, lower_whisker],
                   color='black', linewidth=1.5, linestyle='-', solid_capstyle='butt')
            ax.plot([pos - 0.05, pos + 0.05], [upper_whisker, upper_whisker],
                   color='black', linewidth=1.5, linestyle='-', solid_capstyle='butt')
    
    # Set X-axis
    ax.set_xticks(x_ticks)
    # Format X-axis labels
    formatted_labels = []
    for l in x_labels:
        try:
            formatted_labels.append(x_label_func(l))
        except:
            formatted_labels.append(str(l))
    ax.set_xticklabels(formatted_labels, rotation=0, ha='center', fontsize=11, family=FONT_FAMILY)
    ax.set_xlabel(x_label_text, fontsize=LABEL_FONT['size'], family=LABEL_FONT['family'])
    ax.set_ylabel('Latency (ms)', fontsize=LABEL_FONT['size'], family=LABEL_FONT['family'])
    
    # Set Y-axis range
    ax.set_ylim(0, 10)
    ax.set_yscale('linear')
    
    # Set title
    title_text = f"{query_type} Query"
    ax.set_title(title_text, fontsize=14, fontweight='bold', family=TITLE_FONT['family'])
    
    setup_axes_style(ax, grid=True)
    add_panel_label(ax, panel_label, x=-0.05, y=1.02)
    
    # Legend (only show in first subplot)
    if show_legend:
        legend_elements = [Line2D([0], [0], color=COLORS[m], lw=4, label=MODEL_LABELS[m]) 
                          for m in ['A', 'B', 'C']]
        ax.legend(handles=legend_elements, 
                 frameon=True, fontsize=LEGEND_FONT['size'], 
                 prop={'family': LEGEND_FONT['family']}, loc='upper left')


def create_figure3_main(data: Dict, x_key: str, x_label_func, x_label_text: str, output_dir: Path):
    """Create Figure 3 main figure: G1_Scale experiment group's three queries (3 rows x 1 column)"""
    print("Creating Figure 3 (Main - G1_Scale)...")
    
    fig = plt.figure(figsize=(10, 12))
    gs = fig.add_gridspec(3, 1, hspace=0.25, wspace=0.25,
                          left=0.10, right=0.95, top=0.97, bottom=0.08)
    
    query_types = ['Snapshot', 'History', 'Evolution']
    panel_labels = ['A', 'B', 'C']
    
    for i, (query_type, panel_label) in enumerate(zip(query_types, panel_labels)):
        ax = fig.add_subplot(gs[i, 0])
        plot_query_panel(ax, data, query_type, x_key, x_label_func, x_label_text, 
                        panel_label, show_legend=(i == 0))
    
    # Save
    output_dir.mkdir(exist_ok=True, parents=True)
    plt.savefig(output_dir / 'Figure3_Temporal_Topological_Main.pdf', format='pdf', dpi=600, bbox_inches='tight')
    plt.savefig(output_dir / 'Figure3_Temporal_Topological_Main.png', format='png', dpi=600, bbox_inches='tight')
    print(f"  ✅ Figure 3 (Main) saved")
    plt.close()


def create_figure3_supplement(data: Dict, x_key: str, x_label_func, x_label_text: str, 
                                 exp_group: str, output_dir: Path):
    """Create Figure 3 supplement figure: G2/G3/G4 experiment groups' three queries (3 rows x 1 column)"""
    print(f"Creating Figure 3 Supplement ({exp_group})...")
    
    fig = plt.figure(figsize=(10, 12))
    gs = fig.add_gridspec(3, 1, hspace=0.25, wspace=0.25,
                          left=0.10, right=0.95, top=0.97, bottom=0.08)
    
    query_types = ['Snapshot', 'History', 'Evolution']
    panel_labels = ['A', 'B', 'C']
    
    for i, (query_type, panel_label) in enumerate(zip(query_types, panel_labels)):
        ax = fig.add_subplot(gs[i, 0])
        plot_query_panel(ax, data, query_type, x_key, x_label_func, x_label_text, 
                        panel_label, show_legend=(i == 0))
    
    # Save
    output_dir.mkdir(exist_ok=True, parents=True)
    filename_base = f'Figure3_Supplement_{exp_group}'
    plt.savefig(output_dir / f'{filename_base}.pdf', format='pdf', dpi=600, bbox_inches='tight')
    plt.savefig(output_dir / f'{filename_base}.png', format='png', dpi=600, bbox_inches='tight')
    print(f"  ✅ Figure 3 Supplement ({exp_group}) saved")
    plt.close()


if __name__ == '__main__':
    file_path = '../experiment_results.json'
    output_dir = Path('figures')
    
    # Main figure: G1_Scale
    data_g1, x_key_g1 = load_experiment_data(file_path, 'G1_Scale')
    create_figure3_main(data_g1, x_key_g1, format_scale_label, 
                         r'Entity scale $\left|\mathbf{E}\right|$', output_dir)
    
    # Supplement figure 1: G2_Vol_Attr
    data_g2, x_key_g2 = load_experiment_data(file_path, 'G2_Vol_Attr')
    create_figure3_supplement(data_g2, x_key_g2, format_vol_label,
                                r'Attribute volatility $V_{\mathrm{attr}}$', 
                                'G2_Vol_Attr', output_dir)
    
    # Supplement figure 2: G3_Vol_Rel
    data_g3, x_key_g3 = load_experiment_data(file_path, 'G3_Vol_Rel')
    create_figure3_supplement(data_g3, x_key_g3, format_vol_label,
                                r'Relation volatility $V_{\mathrm{rel}}$', 
                                'G3_Vol_Rel', output_dir)
    
    # Supplement figure 3: G4_Density_Props
    data_g4, x_key_g4 = load_experiment_data(file_path, 'G4_Density_Props')
    create_figure3_supplement(data_g4, x_key_g4, format_density_label,
                                r'Attribute density $D$', 
                                'G4_Density_Props', output_dir)
    
    print("\n✅ Figure 3 completed!")
    print("   - Main figure: G1_Scale (Entity Scale)")
    print("   - Supplement figure 1: G2_Vol_Attr (Attribute Volatility)")
    print("   - Supplement figure 2: G3_Vol_Rel (Relation Volatility)")
    print("   - Supplement figure 3: G4_Density_Props (Attribute Density)")

