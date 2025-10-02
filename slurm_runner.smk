# DAG Profiling Runner Snakemake Workflow
# This workflow orchestrates different DAG building scenarios for profiling
import pandas as pd
from pathlib import Path

# Configuration
# Job scaling controls
SAMPLES = config.get("samples", 10)  # Default 10 samples
REPS = config.get("reps", 10)  # Default 10 reps

# Calculate total jobs: samples * reps * 2 + reps
TOTAL_JOBS = SAMPLES * REPS * 2 + REPS
print(
    f"Profiling configuration: {SAMPLES} samples × {REPS} reps = {TOTAL_JOBS} jobs total"
)

# Config string to pass to all snakemake calls
CONFIG_STR = f"samples={SAMPLES} reps={REPS}"

SCENARIOS = [
    "fresh",
    "resume25",
    "resume50",
    "resume75",
    "codechange",
    "paramchange",
]
PERSISTENCE_IMPL = ["lmdb", "json"]


rule all:
    input:
        f"dag_profiling_summary_{TOTAL_JOBS}jobs.csv",


rule profile_fresh:
    """Profile fresh DAG building"""
    output:
        temp("results/fresh_{persistence_impl}_done.txt"),
    params:
        workflow_file=str(Path(workflow.basedir, "workflows/Snakefile")),
        run_dir="fresh_{persistence_impl}",
        use_lmdb=lambda w: "1" if w.persistence_impl == "lmdb" else "0",
        config_str=CONFIG_STR,
    benchmark:
        repeat("benchmarks/{persistence_impl}/fresh.txt", 5)
    log:
        "logs/fresh_{persistence_impl}_profile.log",
    shell:
        """
        exec > {log} 2>&1
        
        # Clean the specific run directory
        rm -rf {params.run_dir}
        mkdir -p {params.run_dir}
        
        # Time the dry run (DAG building) in the isolated directory
        echo "Profiling DAG building  scenario fresh with {wildcards.persistence_impl}..."
        SNAKEMAKE_USE_LMDB_PERSISTENCE={params.use_lmdb} snakemake -s {params.workflow_file} --executor slurm -j 200 -d {params.run_dir} --config {params.config_str} --dry-run --quiet
        
        # Mark as done
        echo "DAG profiling completed _fresh_{wildcards.persistence_impl} at $(date)" > {output}
        """


rule setup_resume:
    """Setup workflow to partial completion for resume testing"""
    output:
        temp("setup/resume{percent}_{persistence_impl}_setup_done.txt"),
    params:
        workflow_file=str(Path(workflow.basedir, "workflows/Snakefile")),
        run_dir="resume{percent}_{persistence_impl}",
        completion=lambda w: int(w.percent) / 100.0,
        use_lmdb=lambda w: "1" if w.persistence_impl == "lmdb" else "0",
        config_str=CONFIG_STR,
    log:
        "logs/resume{percent}_{persistence_impl}_setup.log",
    shell:
        """
        exec > {log} 2>&1
        
        # Clean the specific run directory
        rm -rf {params.run_dir}
        mkdir -p {params.run_dir}
        
        # Run to specified completion percentage
        echo "Running to {wildcards.percent}% completion with {wildcards.persistence_impl}..."
        SNAKEMAKE_USE_LMDB_PERSISTENCE={params.use_lmdb} snakemake -s {params.workflow_file} --executor slurm -j 200 -d {params.run_dir} --config {params.config_str} multiplier={params.completion} -c {threads}
        
        # Mark setup as done
        echo "Setup completed _resume{wildcards.percent}_{wildcards.persistence_impl} at $(date)" > {output}
        """


rule profile_resume:
    """Profile DAG building after partial completion (resume scenario)"""
    input:
        "setup/resume{percent}_{persistence_impl}_setup_done.txt",
    output:
        temp("results/resume{percent}_{persistence_impl}_done.txt"),
    params:
        workflow_file=str(Path(workflow.basedir, "workflows/Snakefile")),
        run_dir="resume{percent}_{persistence_impl}",
        use_lmdb=lambda w: "1" if w.persistence_impl == "lmdb" else "0",
        config_str=CONFIG_STR,
    benchmark:
        repeat("benchmarks/{persistence_impl}/resume{percent}.txt", 5)
    log:
        "logs/resume{percent}_{persistence_impl}_profile.log",
    shell:
        """
        exec > {log} 2>&1
        
        # Profile the DAG building for the full workflow (resume scenario)
        echo "Profiling DAG building  resume after {wildcards.percent}% with {wildcards.persistence_impl}..."
        SNAKEMAKE_USE_LMDB_PERSISTENCE={params.use_lmdb} snakemake -s {params.workflow_file} --executor slurm -j 200 -d {params.run_dir} --config {params.config_str} --dry-run
        
        # Mark as done
        echo "DAG profiling completed for resume{wildcards.percent}_{wildcards.persistence_impl} at $(date)" > {output}
        """


rule setup_codechange:
    """Setup workflow to completion, then modify code for testing code change detection"""
    output:
        temp("setup/codechange_{persistence_impl}_setup_done.txt"),
    params:
        workflow_file=str(Path(workflow.basedir, "workflows/Snakefile")),
        run_dir="codechange_{persistence_impl}",
        use_lmdb=lambda w: "1" if w.persistence_impl == "lmdb" else "0",
        config_str=CONFIG_STR,
    log:
        "logs/codechange_{persistence_impl}_setup.log",
    shell:
        """
        exec > {log} 2>&1
        
        # Clean the specific run directory
        rm -rf {params.run_dir}
        mkdir -p {params.run_dir}
        
        # Copy Snakefile to run directory for isolated modification
        cp {params.workflow_file} {params.run_dir}/Snakefile
        
        # Run workflow to completion using the copied file
        echo "Running workflow to completion for code change test with {wildcards.persistence_impl}..."
        SNAKEMAKE_USE_LMDB_PERSISTENCE={params.use_lmdb} snakemake -s {params.run_dir}/Snakefile -d {params.run_dir} --config {params.config_str} -c {threads}
        
        # Mark setup as done
        echo "Setup completed for codechange_{wildcards.persistence_impl} at $(date)" > {output}
        """


rule profile_codechange:
    """Profile DAG building after code changes (with --rerun-triggers code)"""
    input:
        "setup/codechange_{persistence_impl}_setup_done.txt",
    output:
        temp("results/codechange_{persistence_impl}_done.txt"),
    params:
        workflow_file=str(Path(workflow.basedir, "workflows/Snakefile")),
        run_dir="codechange_{persistence_impl}",
        use_lmdb=lambda w: "1" if w.persistence_impl == "lmdb" else "0",
        config_str=CONFIG_STR,
    benchmark:
        repeat("benchmarks/{persistence_impl}/codechange.txt", 5)
    log:
        "logs/codechange_{persistence_impl}_profile.log",
    shell:
        """
        exec > {log} 2>&1
        
        # Modify the copied Snakefile to trigger code changes in a shell command
        sed 's/# COMMENT_PLACEHOLDER/# Modified shell command/g' {params.run_dir}/Snakefile > {params.run_dir}/Snakefile.tmp
        mv {params.run_dir}/Snakefile.tmp {params.run_dir}/Snakefile
        
        # Profile DAG building with only code trigger enabled
        echo "Profiling DAG building for code changes with {wildcards.persistence_impl}..."
        SNAKEMAKE_USE_LMDB_PERSISTENCE={params.use_lmdb} snakemake -s {params.run_dir}/Snakefile -d {params.run_dir} --config {params.config_str} --dry-run --rerun-triggers code
        
        # Mark as done
        echo "DAG profiling completed for codechange_{wildcards.persistence_impl} at $(date)" > {output}
        """


rule setup_paramchange:
    """Setup workflow to completion for testing param change detection"""
    output:
        temp("setup/paramchange_{persistence_impl}_setup_done.txt"),
    params:
        workflow_file=str(Path(workflow.basedir, "workflows/Snakefile")),
        run_dir="paramchange_{persistence_impl}",
        use_lmdb=lambda w: "1" if w.persistence_impl == "lmdb" else "0",
        config_str=CONFIG_STR,
    log:
        "logs/paramchange_{persistence_impl}_setup.log",
    shell:
        """
        exec > {log} 2>&1
        
        # Clean the specific run directory
        rm -rf {params.run_dir}
        mkdir -p {params.run_dir}
        
        # Run workflow to completion with initial params
        echo "Running workflow to completion for param change test with {wildcards.persistence_impl}..."
        SNAKEMAKE_USE_LMDB_PERSISTENCE={params.use_lmdb} snakemake -s {params.workflow_file} --executor slurm -j 200 -d {params.run_dir} --config {params.config_str} prefix=Initial -c {threads}
        
        # Mark setup as done
        echo "Setup completed for paramchange_{wildcards.persistence_impl} at $(date)" > {output}
        """


rule profile_paramchange:
    """Profile DAG building after param changes (with --rerun-triggers params)"""
    input:
        "setup/paramchange_{persistence_impl}_setup_done.txt",
    output:
        temp("results/paramchange_{persistence_impl}_done.txt"),
    params:
        workflow_file=str(Path(workflow.basedir, "workflows/Snakefile")),
        run_dir="paramchange_{persistence_impl}",
        use_lmdb=lambda w: "1" if w.persistence_impl == "lmdb" else "0",
        config_str=CONFIG_STR,
    benchmark:
        repeat("benchmarks/{persistence_impl}/paramchange.txt", 5)
    log:
        "logs/paramchange_{persistence_impl}_profile.log",
    shell:
        """
        exec > {log} 2>&1
        
        # Profile DAG building with changed params and only params trigger enabled
        echo "Profiling DAG building for param changes with {wildcards.persistence_impl}..."
        SNAKEMAKE_USE_LMDB_PERSISTENCE={params.use_lmdb} snakemake -s {params.workflow_file} --executor slurm -j 200 -d {params.run_dir} --dry-run --rerun-triggers params --config {params.config_str} prefix=Modified
        
        # Mark as done
        echo "DAG profiling completed for paramchange_{wildcards.persistence_impl} at $(date)" > {output}
        """


rule collect_benchmarks:
    """Collect all benchmark files into a CSV for analysis"""
    input:
        expand(
            "benchmarks/{persistence_impl}/{scenario}.txt",
            persistence_impl=PERSISTENCE_IMPL,
            scenario=SCENARIOS,
        ),
        expand(
            "results/{scenario}_{persistence_impl}_done.txt",
            scenario=SCENARIOS,
            persistence_impl=PERSISTENCE_IMPL,
        ),
    output:
        f"dag_profiling_summary_{TOTAL_JOBS}jobs.csv",
    run:
        results = []
        for persistence_impl in PERSISTENCE_IMPL:
            for scenario in SCENARIOS:
                benchmark_file = f"benchmarks/{persistence_impl}/{scenario}.txt"
                if Path(benchmark_file).exists():
                    # Read benchmark data (format: s	h:m:s	max_rss	max_vms	max_uss	max_pss	io_in	io_out	mean_load	cpu_time)
                    with open(benchmark_file) as f:
                        next(f)  # Skip header
                        for line in f:
                            line = line.strip()
                            if line:
                                parts = line.split("\t")
                                if len(parts) >= 1:
                                    runtime = float(parts[0])  # Runtime in seconds
                                    results.append(
                                        {
                                            "persistence_impl": persistence_impl,
                                            "scenario": scenario,
                                            "runtime": runtime,
                                            "jobs": TOTAL_JOBS,
                                        }
                                    )

        df = pd.DataFrame(results)
        df.to_csv(output[0], index=False)
        print(f"Collected {len(results)} benchmark results into {output[0]}")
