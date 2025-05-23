#!/usr/bin/env python3
"""
Performance Comparison Tool

Script to compare performance metrics between different runs of county_stats.py
to understand efficiency gains from code optimizations.
"""

import json
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import argparse

def load_performance_metrics(file_path):
    """Load performance metrics from JSON file."""
    with open(file_path, 'r') as f:
        return json.load(f)

def compare_metrics(baseline_file, optimized_file, output_dir="output/performance_comparison"):
    """Compare performance metrics between baseline and optimized runs."""
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Load metrics
    baseline = load_performance_metrics(baseline_file)
    optimized = load_performance_metrics(optimized_file)
    
    print("="*60)
    print("PERFORMANCE COMPARISON REPORT")
    print("="*60)
    
    # Overall comparison
    print(f"\nOVERALL PERFORMANCE:")
    print(f"Baseline total time:  {baseline['total_time']:.2f} seconds ({baseline['total_time']/60:.2f} minutes)")
    print(f"Optimized total time: {optimized['total_time']:.2f} seconds ({optimized['total_time']/60:.2f} minutes)")
    
    speedup = baseline['total_time'] / optimized['total_time'] if optimized['total_time'] > 0 else 0
    time_saved = baseline['total_time'] - optimized['total_time']
    percent_improvement = (time_saved / baseline['total_time']) * 100 if baseline['total_time'] > 0 else 0
    
    print(f"Speedup: {speedup:.2f}x")
    print(f"Time saved: {time_saved:.2f} seconds ({time_saved/60:.2f} minutes)")
    print(f"Percent improvement: {percent_improvement:.1f}%")
    
    # Memory comparison
    print(f"\nMEMORY USAGE:")
    print(f"Baseline peak memory:  {baseline['peak_memory_gb']:.2f} GB")
    print(f"Optimized peak memory: {optimized['peak_memory_gb']:.2f} GB")
    
    memory_reduction = baseline['peak_memory_gb'] - optimized['peak_memory_gb']
    memory_percent = (memory_reduction / baseline['peak_memory_gb']) * 100 if baseline['peak_memory_gb'] > 0 else 0
    
    print(f"Memory reduction: {memory_reduction:.2f} GB ({memory_percent:.1f}%)")
    
    # Task comparison
    print(f"\nDASK TASK EFFICIENCY:")
    print(f"Baseline max concurrent tasks:  {baseline['max_concurrent_tasks']}")
    print(f"Optimized max concurrent tasks: {optimized['max_concurrent_tasks']}")
    
    # Stage-by-stage comparison
    print(f"\nSTAGE-BY-STAGE COMPARISON:")
    print(f"{'Stage':<40} {'Baseline (s)':<15} {'Optimized (s)':<15} {'Speedup':<10} {'Improvement':<12}")
    print("-" * 95)
    
    all_stages = set(baseline['stage_times'].keys()) | set(optimized['stage_times'].keys())
    stage_comparisons = []
    
    for stage in sorted(all_stages):
        baseline_time = baseline['stage_times'].get(stage, 0)
        optimized_time = optimized['stage_times'].get(stage, 0)
        
        if baseline_time > 0 and optimized_time > 0:
            stage_speedup = baseline_time / optimized_time
            stage_improvement = ((baseline_time - optimized_time) / baseline_time) * 100
        else:
            stage_speedup = 0
            stage_improvement = 0
        
        print(f"{stage:<40} {baseline_time:<15.2f} {optimized_time:<15.2f} {stage_speedup:<10.2f} {stage_improvement:<12.1f}%")
        
        stage_comparisons.append({
            'stage': stage,
            'baseline_time': baseline_time,
            'optimized_time': optimized_time,
            'speedup': stage_speedup,
            'improvement_percent': stage_improvement
        })
    
    # Create visualizations
    create_comparison_plots(stage_comparisons, baseline, optimized, output_dir)
    
    # Save comparison report
    report = {
        'comparison_date': datetime.now().isoformat(),
        'overall': {
            'speedup': speedup,
            'time_saved_seconds': time_saved,
            'percent_improvement': percent_improvement,
            'memory_reduction_gb': memory_reduction,
            'memory_percent_reduction': memory_percent
        },
        'baseline': baseline,
        'optimized': optimized,
        'stage_comparisons': stage_comparisons
    }
    
    report_file = os.path.join(output_dir, 'performance_comparison_report.json')
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"\nDetailed comparison report saved to: {report_file}")
    print(f"Visualization plots saved to: {output_dir}")
    
    return report

def create_comparison_plots(stage_comparisons, baseline, optimized, output_dir):
    """Create visualization plots for performance comparison."""
    
    # Set up the plotting style
    plt.style.use('seaborn-v0_8')
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
    
    # 1. Stage timing comparison
    df_stages = pd.DataFrame(stage_comparisons)
    if not df_stages.empty:
        stages = df_stages['stage'].tolist()
        baseline_times = df_stages['baseline_time'].tolist()
        optimized_times = df_stages['optimized_time'].tolist()
        
        x = range(len(stages))
        width = 0.35
        
        ax1.bar([i - width/2 for i in x], baseline_times, width, label='Baseline', alpha=0.8)
        ax1.bar([i + width/2 for i in x], optimized_times, width, label='Optimized', alpha=0.8)
        ax1.set_xlabel('Processing Stages')
        ax1.set_ylabel('Time (seconds)')
        ax1.set_title('Stage-by-Stage Performance Comparison')
        ax1.set_xticks(x)
        ax1.set_xticklabels([s.replace('_', '\n') for s in stages], rotation=45, ha='right')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
    
    # 2. Speedup by stage
    if not df_stages.empty:
        speedups = df_stages['speedup'].tolist()
        ax2.bar(range(len(stages)), speedups, alpha=0.8, color='green')
        ax2.axhline(y=1, color='red', linestyle='--', alpha=0.7, label='No improvement')
        ax2.set_xlabel('Processing Stages')
        ax2.set_ylabel('Speedup Factor')
        ax2.set_title('Speedup by Stage')
        ax2.set_xticks(range(len(stages)))
        ax2.set_xticklabels([s.replace('_', '\n') for s in stages], rotation=45, ha='right')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
    
    # 3. Overall metrics comparison
    metrics = ['Total Time (min)', 'Peak Memory (GB)', 'Max Concurrent Tasks']
    baseline_vals = [
        baseline['total_time'] / 60,
        baseline['peak_memory_gb'],
        baseline['max_concurrent_tasks']
    ]
    optimized_vals = [
        optimized['total_time'] / 60,
        optimized['peak_memory_gb'],
        optimized['max_concurrent_tasks']
    ]
    
    x = range(len(metrics))
    width = 0.35
    
    ax3.bar([i - width/2 for i in x], baseline_vals, width, label='Baseline', alpha=0.8)
    ax3.bar([i + width/2 for i in x], optimized_vals, width, label='Optimized', alpha=0.8)
    ax3.set_xlabel('Metrics')
    ax3.set_ylabel('Value')
    ax3.set_title('Overall Performance Metrics')
    ax3.set_xticks(x)
    ax3.set_xticklabels(metrics)
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # 4. Improvement percentages
    if not df_stages.empty:
        improvements = df_stages['improvement_percent'].tolist()
        colors = ['green' if imp > 0 else 'red' for imp in improvements]
        ax4.bar(range(len(stages)), improvements, alpha=0.8, color=colors)
        ax4.axhline(y=0, color='black', linestyle='-', alpha=0.7)
        ax4.set_xlabel('Processing Stages')
        ax4.set_ylabel('Improvement (%)')
        ax4.set_title('Performance Improvement by Stage')
        ax4.set_xticks(range(len(stages)))
        ax4.set_xticklabels([s.replace('_', '\n') for s in stages], rotation=45, ha='right')
        ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'performance_comparison.png'), dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Performance comparison plots saved to {output_dir}/performance_comparison.png")

def main():
    parser = argparse.ArgumentParser(description='Compare performance metrics between runs')
    parser.add_argument('baseline', help='Path to baseline performance metrics JSON file')
    parser.add_argument('optimized', help='Path to optimized performance metrics JSON file')
    parser.add_argument('--output', '-o', default='output/performance_comparison',
                       help='Output directory for comparison results')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.baseline):
        print(f"Error: Baseline file not found: {args.baseline}")
        return
    
    if not os.path.exists(args.optimized):
        print(f"Error: Optimized file not found: {args.optimized}")
        return
    
    compare_metrics(args.baseline, args.optimized, args.output)

if __name__ == "__main__":
    main() 