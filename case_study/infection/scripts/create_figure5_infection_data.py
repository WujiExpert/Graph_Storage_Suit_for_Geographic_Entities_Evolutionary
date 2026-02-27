#!/usr/bin/env python3
"""
Figure 5 - Infection Case Study Data Visualization

Generate 4 subplots:
A. Contact Network - Contact network graph
B. Contact Activity Over Time - Contact activity time series
C. SEIR Epidemic Progression - SEIR state evolution
D. Contact Frequency Distribution - Contact frequency distribution
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import networkx as nx
from pathlib import Path
from collections import defaultdict
import sys

# Set global font to Times New Roman
plt.rcParams['font.family'] = 'Times New Roman'
plt.rcParams['font.serif'] = ['Times New Roman']
plt.rcParams['mathtext.fontset'] = 'stix'

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
    'legend.fontsize': 12,
    'figure.titlesize': 16,
    'savefig.format': 'eps',
    'axes.linewidth': 1.5,
    'grid.linewidth': 0.5,
    'lines.linewidth': 2.5,
    'lines.markersize': 8,
    'patch.linewidth': 1.5,
    'mathtext.fontset': 'stix',
})

# SEIR state colors
SEIR_COLORS = {
    'S': '#82B185',  # Green - Susceptible
    'E': '#F9AF66',  # Orange - Exposed
    'I': '#DC7E7E',  # Red - Infected
    'R': '#71A2DD'   # Blue - Recovered
}

# SEIR state labels
SEIR_LABELS = {
    'S': 'Susceptible',
    'E': 'Exposed',
    'I': 'Infected',
    'R': 'Recovered'
}


def load_data(facts_path: Path, metadata_path: Path):
    """Load fact data and metadata"""
    print("Loading data...")
    facts = pd.read_csv(facts_path)
    metadata = pd.read_csv(metadata_path, sep='\t', header=None, names=['id', 'class', 'gender'])
    
    # Get time range
    t_start = int(facts['Start'].min())
    # For End field, need to handle NaN values
    # For health_status, find the maximum End value of all state records
    health_facts = facts[facts['Attribute'] == 'health_status']
    if not health_facts.empty:
        valid_ends = health_facts['End'].dropna()
        if len(valid_ends) > 0:
            t_end = int(valid_ends.max())
        else:
            t_end = int(health_facts['Start'].max())
    else:
        # If no health_status records, use maximum End of all records
        valid_ends = facts['End'].dropna()
        if len(valid_ends) > 0:
            t_end = int(valid_ends.max())
        else:
            t_end = int(facts['Start'].max())
    
    print(f"   Time range: {t_start} to {t_end} (seconds)")
    print(f"   Time range: {(t_end - t_start) / 3600:.2f} hours")
    
    return facts, metadata, t_start, t_end


def get_patient_zero(facts: pd.DataFrame, t_start: int):
    """Identify patient zero (person with state I at t_start)"""
    # Find person with health_status I at t_start
    health_facts = facts[(facts['Attribute'] == 'health_status') & 
                        (facts['Start'] == t_start) & 
                        (facts['Value'] == 'I')]
    if not health_facts.empty:
        return health_facts.iloc[0]['EntityID']
    return None


def get_current_status(facts: pd.DataFrame, person_id: str, time: int):
    """Get a person's health status at a specific time point"""
    # Ensure person_id is string type
    person_id_str = str(person_id)
    
    # Find all health_status records for this person
    # Try multiple matching methods, as EntityID may be int or str
    person_facts = facts[
        (facts['EntityID'].astype(str) == person_id_str) | 
        (facts['EntityID'].astype(int).astype(str) == person_id_str)
    ]
    health_facts = person_facts[person_facts['Attribute'] == 'health_status']
    
    if health_facts.empty:
        return 'S'  # Default return Susceptible
    
    # Sort by Start time
    health_facts = health_facts.sort_values('Start')
    
    # Find state record containing this time point
    # Search in reverse order by Start time, find first record containing this time point (i.e., latest state)
    for idx in range(len(health_facts) - 1, -1, -1):
        fact = health_facts.iloc[idx]
        start = float(fact['Start'])
        end_val = fact['End']
        
        # Handle End field
        if pd.isna(end_val) or end_val == 'None' or str(end_val).strip() == '':
            end = float('inf')
        else:
            try:
                end = float(end_val)  # Keep as float, as End may be 148120.0
            except (ValueError, TypeError):
                end = float('inf')
        
        # Check if time point is within this state interval (note: End is inclusive)
        # Use float comparison, as time may be int and end may be float
        time_float = float(time)
        if start <= time_float <= end:
            return str(fact['Value']).strip()
    
    # If no record containing this time point is found, return the last state
    # This applies when query time exceeds End time of all records
    # The last state represents the person's final state
    if not health_facts.empty:
        last_fact = health_facts.iloc[-1]
        last_start = int(float(last_fact['Start']))
        last_end_val = last_fact['End']
        
        # If last state's End is None or inf, and time point >= Start, return this state
        if pd.isna(last_end_val) or last_end_val == 'None' or str(last_end_val).strip() == '':
            if float(time) >= float(last_start):
                return str(last_fact['Value']).strip()
        else:
            try:
                last_end = float(last_end_val)
                # If query time >= last state's Start, return last state
                # This indicates this state is the final state (even if query time exceeds End, return this state)
                if float(time) >= float(last_start):
                    return str(last_fact['Value']).strip()
            except (ValueError, TypeError):
                if float(time) >= float(last_start):
                    return str(last_fact['Value']).strip()
    
    return 'S'  # Default return Susceptible


def sample_network_keep_connectivity(G: nx.Graph, max_edges: int = 500):
    """Sample network while maintaining connectivity"""
    if len(G.edges()) <= max_edges:
        return G
    
    # Get all connected components
    components = list(nx.connected_components(G))
    
    # Build sampled subgraph for each connected component
    sampled_edges = set()
    
    for component in components:
        subgraph = G.subgraph(component)
        
        if len(subgraph.edges()) <= max_edges // len(components):
            # If component is small enough, keep all edges
            sampled_edges.update(subgraph.edges())
        else:
            # Use minimum spanning tree to maintain connectivity
            mst = nx.minimum_spanning_tree(subgraph)
            sampled_edges.update(mst.edges())
            
            # Add additional edges, prioritizing edges between high-degree nodes
            remaining_edges = list(set(subgraph.edges()) - set(mst.edges()))
            remaining_edges.sort(key=lambda e: subgraph.degree(e[0]) + subgraph.degree(e[1]), reverse=True)
            
            edges_to_add = min(len(remaining_edges), (max_edges // len(components)) - len(mst.edges()))
            sampled_edges.update(remaining_edges[:edges_to_add])
    
    # Build sampled graph
    G_sampled = nx.Graph()
    G_sampled.add_nodes_from(G.nodes())
    G_sampled.add_edges_from(sampled_edges)
    
    return G_sampled


def plot_contact_network(ax, facts: pd.DataFrame, metadata: pd.DataFrame, 
                         t_start: int, patient_zero: str, t_end: int):
    """Plot contact network graph (Panel A) - show final state"""
    print("Plotting Contact Network...")
    
    # Build complete network graph (using all contact relationships)
    G_full = nx.Graph()
    
    # Add all persons as nodes
    # Only add persons with health_status records (to ensure status can be retrieved)
    contact_facts = facts[facts['Attribute'] == 'contacts']
    health_persons = set(str(pid) for pid in facts[facts['Attribute'] == 'health_status']['EntityID'].unique())
    
    # Get all persons from contact relationships, but only keep those with health_status records
    contact_persons = set(str(pid) for pid in contact_facts['EntityID'].unique())
    contact_persons.update(str(pid) for pid in contact_facts['Value'].unique())
    
    # Only keep persons with both contact records and health_status records
    all_persons = contact_persons & health_persons
    
    for person_id in all_persons:
        G_full.add_node(str(person_id))
    
    # Add edges (only add once, as contacts are bidirectional)
    # Only add edges where both nodes are in all_persons
    edges_added = set()
    for _, fact in contact_facts.iterrows():
        p1, p2 = str(fact['EntityID']), str(fact['Value'])
        # Only add edges where both nodes are in the network
        if p1 in all_persons and p2 in all_persons:
            edge_key = tuple(sorted([p1, p2]))
            if edge_key not in edges_added:
                G_full.add_edge(p1, p2)
                edges_added.add(edge_key)
    
    # Sample while maintaining connectivity
    print(f"   Original network: {len(G_full.nodes())} nodes, {len(G_full.edges())} edges")
    G = sample_network_keep_connectivity(G_full, max_edges=500)
    print(f"   Sampled network: {len(G.nodes())} nodes, {len(G.edges())} edges")
    
    # Calculate layout
    pos = nx.spring_layout(G, k=0.3, iterations=50, seed=42)
    
    # Adjust layout: place patient zero at center
    patient_zero_str = str(patient_zero)
    if patient_zero_str in pos:
        # Calculate center position of all nodes
        all_x = [pos[node][0] for node in G.nodes()]
        all_y = [pos[node][1] for node in G.nodes()]
        center_x = (min(all_x) + max(all_x)) / 2
        center_y = (min(all_y) + max(all_y)) / 2
        
        # Move patient zero to center, slightly upward
        pos[patient_zero_str] = (center_x, center_y + 0.1)
    
    # Get final state of each node (using state at t_end)
    node_colors = {}
    node_sizes = {}
    node_edgewidths = {}
    
    # Count status distribution (for debugging)
    status_counts = {'S': 0, 'E': 0, 'I': 0, 'R': 0}
    
    # Ensure node list is stable
    node_list = list(G.nodes())
    
    # Debug: check facts DataFrame and t_end
    health_facts_check = facts[facts['Attribute'] == 'health_status']
    print(f"   Debug: health_status facts count: {len(health_facts_check)}")
    print(f"   Debug: t_end = {t_end}")
    
    # Test status retrieval for first few nodes
    test_count = 0
    for node in node_list:
        node_str = str(node)
        status = get_current_status(facts, node_str, t_end)
        if status not in status_counts:
            print(f"   Warning: Unknown status '{status}' for node {node}")
            status = 'S'  # Default value
        status_counts[status] = status_counts.get(status, 0) + 1
        node_colors[node_str] = SEIR_COLORS.get(status, '#95a5a6')
        
        # Patient zero uses larger node and thicker border
        if node_str == patient_zero_str:
            node_sizes[node_str] = 200  # Larger node
            node_edgewidths[node_str] = 3
        else:
            node_sizes[node_str] = 100
            node_edgewidths[node_str] = 1
        
        # Debug first 5 nodes
        if test_count < 5:
            person_data = health_facts_check[health_facts_check['EntityID'].astype(str) == node_str]
            if not person_data.empty:
                last_record = person_data.sort_values('Start').iloc[-1]
                print(f"   Debug node {node}: status={status}, last_record={last_record['Value']} (Start={last_record['Start']}, End={last_record['End']})")
            test_count += 1
    
    print(f"   Final status distribution: {status_counts}")
    print(f"   Total nodes: {len(node_list)}")
    
    # Separate patient zero and other nodes
    other_nodes = [n for n in node_list if str(n) != patient_zero_str]
    patient_zero_node = [n for n in node_list if str(n) == patient_zero_str]
    
    # Draw network: draw edges first
    nx.draw_networkx_edges(G, pos, ax=ax, edge_color='#e74c3c', alpha=0.3, width=0.5)
    
    # Draw other nodes (draw first, at bottom layer)
    if other_nodes:
        other_colors = [node_colors[str(n)] for n in other_nodes]
        other_sizes = [node_sizes[str(n)] for n in other_nodes]
        other_edgewidths = [node_edgewidths[str(n)] for n in other_nodes]
        nx.draw_networkx_nodes(G, pos, nodelist=other_nodes, ax=ax,
                               node_color=other_colors, node_size=other_sizes,
                               edgecolors='black', linewidths=other_edgewidths,
                               alpha=0.99)
    
    # Finally draw patient zero node (draw last, ensure it's on top layer and not occluded)
    if patient_zero_node:
        patient_zero_color = [node_colors[patient_zero_str]]
        patient_zero_size = [node_sizes[patient_zero_str]]
        patient_zero_edgewidth = [node_edgewidths[patient_zero_str]]
        nx.draw_networkx_nodes(G, pos, nodelist=patient_zero_node, ax=ax,
                               node_color=patient_zero_color, node_size=patient_zero_size,
                               edgecolors='black', linewidths=patient_zero_edgewidth,
                               alpha=0.99)
    
    ax.set_title("Contact Network", fontsize=TITLE_FONT['size'], 
                 fontweight=TITLE_FONT['weight'], family=TITLE_FONT['family'])
    ax.axis('off')
    
    # Add legend (use circles, increase size, add gray background, place at top-left)
    from matplotlib.patches import Circle
    from matplotlib.legend_handler import HandlerPatch
    
    class HandlerCircle(HandlerPatch):
        def create_artists(self, legend, orig_handle,
                          xdescent, ydescent, width, height, fontsize, trans):
            center = 0.5 * width - 0.5 * xdescent, 0.5 * height - 0.5 * ydescent
            # Increase circle radius
            p = Circle(xy=center, radius=min(width, height) * 0.6)
            self.update_prop(p, orig_handle, legend)
            p.set_transform(trans)
            return [p]
    
    legend_elements = [
        Circle((0, 0), radius=1.0, facecolor=SEIR_COLORS['S'], edgecolor='black', linewidth=1.5, label=SEIR_LABELS['S']),
        Circle((0, 0), radius=1.0, facecolor=SEIR_COLORS['E'], edgecolor='black', linewidth=1.5, label=SEIR_LABELS['E']),
        Circle((0, 0), radius=1.0, facecolor=SEIR_COLORS['I'], edgecolor='black', linewidth=1.5, label=SEIR_LABELS['I']),
        Circle((0, 0), radius=1.0, facecolor=SEIR_COLORS['R'], edgecolor='black', linewidth=1.5, label=SEIR_LABELS['R']),
        Circle((0, 0), radius=1.0, facecolor='white', edgecolor='black', linewidth=3, label='Patient Zero')
    ]
    legend = ax.legend(handles=legend_elements, loc='upper left', 
             fontsize=LEGEND_FONT['size'], prop={'family': LEGEND_FONT['family']},
             handler_map={Circle: HandlerCircle()},
             framealpha=0.8, facecolor='lightgray', edgecolor='black', frameon=True)


def plot_contact_activity(ax, facts: pd.DataFrame, t_start: int, patient_zero_time: int):
    """Plot contact activity time series (Panel B) - aggregated by hour"""
    print("Plotting Contact Activity Over Time...")
    
    # Count contacts at each time point
    contact_facts = facts[facts['Attribute'] == 'contacts'].copy()
    
    # Aggregate contact counts by hour
    contact_facts['Hour'] = ((contact_facts['Start'] - t_start) / 3600.0).astype(int)
    hourly_counts = contact_facts.groupby('Hour').size()
    
    # Convert to hours (starting from 0)
    times_hours = hourly_counts.index.values
    counts = hourly_counts.values
    
    # Draw area plot
    ax.fill_between(times_hours, 0, counts, alpha=0.3, color='#3498db')
    ax.plot(times_hours, counts, linewidth=2, color='#2980b9', marker='o', markersize=4)
    
    # Mark patient zero time
    patient_zero_hour = (patient_zero_time - t_start) / 3600.0
    ax.axvline(x=patient_zero_hour, color='red', linestyle='--', linewidth=2, label='Patient Zero')
    
    ax.set_xlabel('Time (hours)', fontsize=LABEL_FONT['size'], family=LABEL_FONT['family'])
    ax.set_ylabel('Contact Count per Hour', fontsize=LABEL_FONT['size'], family=LABEL_FONT['family'])
    ax.set_title("Contact Activity Over Time", fontsize=TITLE_FONT['size'], 
                 fontweight=TITLE_FONT['weight'], family=TITLE_FONT['family'])
    ax.grid(True, linestyle='--', alpha=0.3)
    ax.legend(fontsize=LEGEND_FONT['size'], prop={'family': LEGEND_FONT['family']})
    
    # Set axis style
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)


def plot_seir_progression(ax, facts: pd.DataFrame, t_start: int, t_end: int):
    """Plot SEIR state evolution (Panel C)"""
    print("Plotting SEIR Epidemic Progression...")
    
    # Get all health_status records
    health_facts = facts[facts['Attribute'] == 'health_status'].copy()
    
    # Ensure t_end is integer
    if pd.isna(t_end) or t_end == 'None':
        t_end = int(facts['Start'].max())
    else:
        t_end = int(float(t_end))
    
    # Create time series (sample every 30 minutes for higher precision)
    time_step = 1800  # 30 minutes
    times = np.arange(t_start, t_end + time_step, time_step)
    times_hours = (times - t_start) / 3600.0
    
    print(f"   Processing {len(times)} time points...")
    
    # Preprocess health_facts: convert End field
    def parse_end(end_val):
        if pd.isna(end_val) or end_val == 'None' or str(end_val).strip() == '':
            return float('inf')
        try:
            return float(end_val)
        except (ValueError, TypeError):
            return float('inf')
    
    health_facts['End_parsed'] = health_facts['End'].apply(parse_end)
    health_facts['Start_float'] = health_facts['Start'].astype(float)
    health_facts['Value_str'] = health_facts['Value'].astype(str).str.strip()
    health_facts['EntityID_str'] = health_facts['EntityID'].astype(str)
    
    # Group by person, build state timeline for each person
    person_timelines = {}
    for person_id, person_facts in health_facts.groupby('EntityID_str'):
        # Sort by Start time
        person_facts_sorted = person_facts.sort_values('Start_float')
        person_timelines[person_id] = person_facts_sorted[['Start_float', 'End_parsed', 'Value_str']].values
    
    # Count SEIR state population at each time point
    seir_counts = {status: [] for status in ['S', 'E', 'I', 'R']}
    total_persons = len(person_timelines)
    
    for i, t in enumerate(times):
        if (i + 1) % 10 == 0 or i == 0:
            print(f"   Progress: {i+1}/{len(times)} time points ({100*(i+1)/len(times):.1f}%)")
        
        counts = {status: 0 for status in ['S', 'E', 'I', 'R']}
        t_float = float(t)
        
        for person_id, timeline in person_timelines.items():
            # Find state record containing this time point
            status = None
            for record in timeline:
                start, end, val = record[0], record[1], record[2]
                # Check if time point is within this state interval
                if start <= t_float <= end:
                    status = val
                    break
                # If End is inf and time point >= Start, use this state
                elif end == float('inf') and t_float >= start:
                    status = val
                    break
            
            # If no record containing this time point is found, use last state
            # This applies when query time exceeds End time of all records
            if status is None:
                if len(timeline) > 0:
                    # Check last state
                    last_record = timeline[-1]
                    last_start, last_end, last_val = last_record[0], last_record[1], last_record[2]
                    # If last state's End is inf, or time point >= last state's Start, use last state
                    if last_end == float('inf') or t_float >= last_start:
                        status = last_val
                    else:
                        # Time point earlier than all records' Start, use first state
                        status = timeline[0][2] if len(timeline) > 0 else 'S'
                else:
                    status = 'S'
            
            if status in counts:
                counts[status] += 1
        
        for status in ['S', 'E', 'I', 'R']:
            seir_counts[status].append(counts[status])
    
    print(f"   Completed processing {len(times)} time points")
    
    # Draw stacked area plot
    ax.stackplot(times_hours, 
                 seir_counts['S'], seir_counts['E'], seir_counts['I'], seir_counts['R'],
                 colors=[SEIR_COLORS['S'], SEIR_COLORS['E'], SEIR_COLORS['I'], SEIR_COLORS['R']],
                 alpha=1.0, labels=[SEIR_LABELS[s] for s in ['S', 'E', 'I', 'R']])
    
    # Get current y-axis range, increase upper limit to leave space for legend
    y_min, y_max = ax.get_ylim()
    ax.set_ylim(y_min, y_max * 1.05)  # Increase upper limit by 15%
    
    ax.set_xlabel('Time (hours)', fontsize=LABEL_FONT['size'], family=LABEL_FONT['family'])
    ax.set_ylabel('Number of Students', fontsize=LABEL_FONT['size'], family=LABEL_FONT['family'])
    ax.set_title("SEIR Epidemic Progression", fontsize=TITLE_FONT['size'], 
                 fontweight=TITLE_FONT['weight'], family=TITLE_FONT['family'])
    ax.grid(True, linestyle='--', alpha=0.3)
    # Place legend at top-right, horizontal arrangement (refer to image example)
    # Use bbox_to_anchor to fine-tune position: (x, y) coordinates, can adjust these values to fine-tune position
    # x: 0=left boundary, 1=right boundary; y: 0=bottom boundary, 1=top boundary
    legend = ax.legend(fontsize=LEGEND_FONT['size'], 
             prop={'family': LEGEND_FONT['family']}, ncol=4, framealpha=0.99,
             bbox_to_anchor=(0.91, 1.0), loc='upper right')
    
    # Set axis style
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)


def plot_contact_frequency(ax, facts: pd.DataFrame):
    """Plot contact frequency distribution (Panel D)"""
    print("Plotting Contact Frequency Distribution...")
    
    # Count contact frequency for each person
    # Note: contacts are bidirectional (A contacts B and B contacts A),
    # so each contact event produces two records
    # We count the number of times each person appears as EntityID (active contact count)
    # Due to bidirectional records, EntityID and Value should contain the same people, and counts should be equal
    contact_facts = facts[facts['Attribute'] == 'contacts']
    
    # Count contact frequency for each person as EntityID
    contact_counts = contact_facts.groupby('EntityID').size()
    
    # Convert to array
    counts_array = contact_counts.values
    
    print(f"   Total persons with contacts: {len(counts_array)}")
    print(f"   Contact count range: {counts_array.min()} - {counts_array.max()}")
    print(f"   Mean: {np.mean(counts_array):.1f}, Median: {np.median(counts_array):.1f}")
    
    # Draw histogram - use 13 bins
    # Calculate appropriate bins range, divide into 13 groups
    min_count = 0
    max_count = counts_array.max()
    bins = np.linspace(min_count, max_count, 14)  # 14 boundary points = 13 groups
    n, bins, patches = ax.hist(counts_array, bins=bins, alpha=0.7, 
                              color='#3498db', edgecolor='black', linewidth=1)
    
    # Calculate mean and median
    mean_count = np.mean(counts_array)
    median_count = np.median(counts_array)
    
    # Add mean and median lines
    ax.axvline(x=mean_count, color='red', linestyle='--', linewidth=2, label=f'Mean: {int(mean_count)}')
    ax.axvline(x=median_count, color='orange', linestyle='--', linewidth=2, label=f'Median: {int(median_count)}')
    
    ax.set_xlabel('Contact Count per Student', fontsize=LABEL_FONT['size'], 
                  family=LABEL_FONT['family'])
    ax.set_ylabel('Number of Students', fontsize=LABEL_FONT['size'], 
                  family=LABEL_FONT['family'])
    ax.set_title("Contact Frequency Distribution", fontsize=TITLE_FONT['size'], 
                 fontweight=TITLE_FONT['weight'], family=TITLE_FONT['family'])
    ax.grid(True, linestyle='--', alpha=0.3, axis='y')
    ax.legend(fontsize=LEGEND_FONT['size'], prop={'family': LEGEND_FONT['family']})
    
    # Set axis style
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)


def add_panel_label(ax, label: str, x: float = -0.1, y: float = 1.05):
    """Add panel label (A, B, C, D)"""
    ax.text(x, y, label, transform=ax.transAxes,
            fontsize=TITLE_FONT['size'], fontweight=TITLE_FONT['weight'],
            family=TITLE_FONT['family'],
            va='bottom', ha='right')


def create_figure(facts_path: Path, metadata_path: Path, output_path: Path):
    """Create complete figure"""
    print("Creating infection data visualization...")
    
    # Load data
    facts, metadata, t_start, t_end = load_data(facts_path, metadata_path)
    
    # Identify patient zero
    patient_zero = get_patient_zero(facts, t_start)
    patient_zero_time = t_start
    
    # Create 2x2 layout
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # Panel A: Contact Network
    plot_contact_network(axes[0, 0], facts, metadata, t_start, patient_zero, t_end)
    add_panel_label(axes[0, 0], 'A')
    
    # Panel B: Contact Activity Over Time
    plot_contact_activity(axes[0, 1], facts, t_start, patient_zero_time)
    add_panel_label(axes[0, 1], 'B')
    
    # Panel C: SEIR Epidemic Progression
    plot_seir_progression(axes[1, 0], facts, t_start, t_end)
    add_panel_label(axes[1, 0], 'C')
    
    # Panel D: Contact Frequency Distribution
    plot_contact_frequency(axes[1, 1], facts)
    add_panel_label(axes[1, 1], 'D')
    
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    
    # Save figure
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path.with_suffix('.pdf'), format='pdf', dpi=600, bbox_inches='tight')
    plt.savefig(output_path.with_suffix('.png'), format='png', dpi=600, bbox_inches='tight')
    print(f"✅ Figure saved to {output_path}")
    plt.close()


if __name__ == '__main__':
    # Set paths
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'data'
    output_dir = script_dir.parent.parent.parent / 'figures'
    
    facts_path = data_dir / 'infection_facts.csv'
    metadata_path = data_dir / 'metadata_primaryschool.txt'
    output_path = output_dir / 'Figure5_Infection_Data'
    
    if not facts_path.exists():
        print(f"Error: {facts_path} not found. Please run ETL first.")
        sys.exit(1)
    
    if not metadata_path.exists():
        print(f"Error: {metadata_path} not found.")
        sys.exit(1)
    
    create_figure(facts_path, metadata_path, output_path)
    print("✅ Visualization complete!")

