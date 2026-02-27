#!/usr/bin/env python3
"""
Case Study: Epidemic Dynamics ETL

Converts Primary School SocioPatterns contact network data into atomic facts
for spatiotemporal graph storage benchmarking.

Processes:
- Contact network data (temporal contact events)
- SEIR epidemic model simulation
- Generates atomic facts for person health status and contact relationships
"""

import pandas as pd
import numpy as np
import random
import sys

# Set random seed to ensure reproducible results
random.seed(42)


class PrimarySchoolETL:
    def __init__(self, meta_path, contact_path, output_path):
        self.meta_path = meta_path
        self.contact_path = contact_path
        self.output_path = output_path
        self.facts = []

        # SEIR parameters (for 20-second time steps)
        # Assuming R0≈3, adjust infection rate
        self.BETA = 0.05  # Infection rate (per contact)
        self.T_INCUBATION = 3600 * 4  # 4 hours incubation (to show changes within a day in data)
        self.T_RECOVERY = 3600 * 24  # 24 hours recovery

        # State tracking: {pid: {'status': 'S', 'change_time': start_time}}
        self.agent_states = {}
        # State history cache: for generating interval records at the end
        # {pid: [(status, start_time, end_time), ...]}
        self.history_log = {}

    def _add_fact(self, eid, label, attr, val, vtype, start, end):
        """Standard atomic fact adder"""
        # Ensure all fields are converted to strings or integers, avoid floating point errors
        self.facts.append([str(eid), str(label), str(attr), str(val), int(vtype), int(start), end])

    def run(self):
        print("1. Loading Metadata...")
        # Format: ID Class Gender
        meta = pd.read_csv(self.meta_path, sep='\t', header=None, names=['id', 'class', 'gender'])

        # Get earliest and latest times from data, for static attribute intervals
        contacts = pd.read_csv(self.contact_path, sep='\t', header=None, names=['t', 'i', 'j', 'ci', 'cj'])
        contacts = contacts.sort_values('t')
        t_start_global = contacts['t'].min()
        t_end_global = contacts['t'].max()

        print(f"   Data spans from {t_start_global} to {t_end_global} (seconds)")

        # --- A. Initialize static attributes ---
        person_ids = []
        for _, row in meta.iterrows():
            pid = str(row['id'])
            person_ids.append(pid)

            label = "Person"  # Unified Label for easy querying

            # Static attributes: Type=0, Start=t_start, End=Inf
            # Note: Label can also be stored as an attribute for Model A/B filtering
            role = 'Teacher' if row['class'] == 'Teachers' else 'Student'

            self._add_fact(pid, label, 'gender', row['gender'], 0, t_start_global, None)
            self._add_fact(pid, label, 'class_name', row['class'], 0, t_start_global, None)
            self._add_fact(pid, label, 'role', role, 0, t_start_global, None)

            # Initialize SEIR state
            self.agent_states[pid] = {'status': 'S', 'next_event': float('inf')}
            self.history_log[pid] = []  # Don't write to database yet, wait for state change

        # --- B. Set patient zero ---
        patient_zero = random.choice(person_ids)
        self.agent_states[patient_zero]['status'] = 'I'
        self.agent_states[patient_zero]['next_event'] = t_start_global + self.T_RECOVERY
        # Record patient zero's initial state
        self.history_log[patient_zero].append({'status': 'I', 'start': t_start_global})
        # Record other people's initial states
        for pid in person_ids:
            if pid != patient_zero:
                self.history_log[pid].append({'status': 'S', 'start': t_start_global})

        print(f"   🦠 Patient Zero: {patient_zero}")

        # --- C. Time-driven state transitions + contact event processing ---
        print("2. Processing Contact Stream & SEIR Simulation (Time-driven)...")

        # Aggregate contact events by time step for faster processing
        contacts_by_time = contacts.groupby('t')
        
        # Collect all time points that need to be checked:
        # 1. All time points when contact events occur
        # 2. All expected state transition time points (next_event)
        time_points = set(contacts['t'].unique())
        
        # Collect all next_event time points during initialization
        for pid in person_ids:
            state = self.agent_states[pid]
            if state['next_event'] != float('inf') and state['next_event'] <= t_end_global:
                time_points.add(state['next_event'])
        
        # Use a set to track processed time points, avoid duplicate processing
        processed_times = set()
        
        # Continue processing until no new time points need processing
        while time_points:
            # Get pending time points
            pending_times = time_points - processed_times
            if not pending_times:
                break
            
            # Get next earliest time point
            t = min(pending_times)
            if t > t_end_global:
                break
            
            processed_times.add(t)
            
            # C.1: Automatic state transitions (E->I, I->R) - time-driven, regardless of contact events
            for pid in person_ids:
                state = self.agent_states[pid]
                if t >= state['next_event']:
                    old_s = state['status']
                    new_s = None
                    if old_s == 'E':
                        new_s = 'I'
                        state['next_event'] = t + self.T_RECOVERY
                        # If new next_event is within time range and not processed, add to time points set
                        if state['next_event'] <= t_end_global and state['next_event'] not in processed_times:
                            time_points.add(state['next_event'])
                    elif old_s == 'I':
                        new_s = 'R'
                        state['next_event'] = float('inf')

                    if new_s:
                        state['status'] = new_s
                        # [Core Logic] State changed: close previous segment, open new segment
                        last_record = self.history_log[pid][-1]
                        last_record['end'] = t  # Close
                        self.history_log[pid].append({'status': new_s, 'start': t})  # Open

            # C.2: Process contact events (only at time points with contact records)
            if t in contacts_by_time.groups:
                group = contacts_by_time.get_group(t)
                for _, row in group.iterrows():
                    p1, p2 = str(row['i']), str(row['j'])

                    # Generate dynamic edges (Type=1)
                    # Model C advantage: massive dynamic edges. Duration 20s
                    # Bidirectional edges: A contacts B, B contacts A
                    self._add_fact(p1, 'Person', 'contacts', p2, 1, t, t)
                    self._add_fact(p2, 'Person', 'contacts', p1, 1, t, t)

                    # Virus transmission logic
                    # Only occurs when I contacts S
                    s1 = self.agent_states.get(p1, {}).get('status')
                    s2 = self.agent_states.get(p2, {}).get('status')

                    target = None
                    if s1 == 'I' and s2 == 'S': target = p2
                    if s2 == 'I' and s1 == 'S': target = p1

                    if target and random.random() < self.BETA:
                        # Successful infection S -> E
                        self.agent_states[target]['status'] = 'E'
                        self.agent_states[target]['next_event'] = t + self.T_INCUBATION
                        
                        # If new next_event is within time range and not processed, add to time points set
                        if self.agent_states[target]['next_event'] <= t_end_global and self.agent_states[target]['next_event'] not in processed_times:
                            time_points.add(self.agent_states[target]['next_event'])

                        # Record change
                        last_record = self.history_log[target][-1]
                        last_record['end'] = t
                        self.history_log[target].append({'status': 'E', 'start': t})
        
        print(f"   Processed {len(processed_times)} time points (including state transitions)")

        # --- D. Finalize: Convert state history to facts ---
        print("3. Finalizing Health History...")
        for pid, history in self.history_log.items():
            for record in history:
                # If no 'end', state continues until the end
                end_time = record.get('end', t_end_global)

                # Generate dynamic attribute facts (Type=0)
                # This demonstrates Model C's compression advantage: store one record for a long time period
                self._add_fact(pid, 'Person', 'health_status', record['status'], 0, record['start'], end_time)

        # --- E. Export ---
        print(f"4. Exporting to {self.output_path}...")
        columns = ['EntityID', 'Label', 'Attribute', 'Value', 'Type', 'Start', 'End']
        df_out = pd.DataFrame(self.facts, columns=columns)
        df_out.to_csv(self.output_path, index=False)
        print(f"✅ Done! Generated {len(df_out)} atomic facts.")
        print(f"   - Dynamic Relations (Contacts): {len(df_out[df_out['Type'] == 1])}")
        print(
            f"   - Dynamic Attributes (Health): {len(df_out[(df_out['Type'] == 0) & (df_out['Attribute'] == 'health_status')])}")


# --- Entry point ---
if __name__ == "__main__":
    # Please ensure these two files are in the current directory
    etl = PrimarySchoolETL(
        meta_path='metadata_primaryschool.txt',
        contact_path='primaryschool.csv',
        output_path='infection_facts.csv'
    )
    etl.run()