import os
from pathlib import Path
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


WORKFLOWS = ["linear", "fanout", "diamond"]
PERSISTENCE = ["json", "lmdb", "lmdb_mtime"]
SCENARIOS = [
    "fresh_run",
    "param_change",
    "code_change",
    "resume_25",
    "resume_50",
    "resume_75",
]

# Configuration for workflow sizes - only n_samples is common across all workflows
N_SAMPLES = config.get("n_samples", 100)  # Full size for most scenarios

# Resume scenarios: run to X% completion, then dry-run full workflow
RESUME_PERCENTAGES = {
    "resume_25": 0.25,
    "resume_50": 0.50,
    "resume_75": 0.75,
}


def get_config_string(n_samples):
    """Build --config string for n_samples."""
    return f"--config n_samples={n_samples}"


def get_resume_config(wildcards):
    """Get config for resume scenario setup - run partial workflow."""
    percentage = RESUME_PERCENTAGES[wildcards.scenario]
    partial_samples = int(N_SAMPLES * percentage)
    return get_config_string(partial_samples)


def get_full_config(wildcards):
    """Get config for full workflow runs."""
    return get_config_string(N_SAMPLES)


def parse_benchmarks(benchmarks, outcsv):
    """Parse all benchmark files and aggregate into a DataFrame."""
    import re
    import sys

    records = []
    for bench_file in benchmarks:
        parts = Path(bench_file).parts[-3:]
        workflow = parts[0]
        persistence = parts[1]
        scenario = parts[2].replace(".txt", "")

        # Read benchmark file (tab-separated)
        df = pd.read_csv(bench_file, sep="\t")

        # Add metadata columns
        df["workflow"] = workflow
        df["persistence"] = persistence
        df["scenario"] = scenario

        # Try to read corresponding log file to extract job count
        log_file = bench_file.replace("benchmarks/", "logs/profile/").replace(
            ".txt", ".log"
        )
        total_jobs = None
        if os.path.exists(log_file):
            with open(log_file, "r") as f:
                log_content = f.read()
                # Look for "total" line in job stats
                match = re.search(r"^total\s+(\d+)$", log_content, re.MULTILINE)
                if match:
                    total_jobs = int(match.group(1))

        df["total_jobs"] = total_jobs

        records.append(df)

    if not records:
        print(f"No benchmark files found", file=sys.stderr)
        return None

    # Combine all benchmark data
    all_data = pd.concat(records, ignore_index=True)
    all_data.to_csv(outcsv)
    return all_data


def create_plots(df, outpath):
    """Create comparison plots for different persistence implementations."""
    import matplotlib

    matplotlib.use("agg")
    sns.set_theme(style="whitegrid")

    workflows = sorted(df["workflow"].unique())
    palette = dict(zip(workflows, sns.color_palette(n_colors=len(workflows))))

    g = sns.catplot(
        data=df,
        x="persistence",
        y="s",
        col="scenario",
        col_wrap=3,
        kind="box",
        height=5,
        aspect=1.2,
        showfliers=False,
        boxprops=dict(facecolor="none"),
        medianprops=dict(color="black", linewidth=2),
    )
    g.map_dataframe(
        sns.stripplot,
        x="persistence",
        y="s",
        hue="workflow",
        palette=palette,
        dodge=False,
        alpha=0.8,
        size=8,
    )

    from matplotlib.patches import Patch

    legend_elements = [Patch(facecolor=palette[wf], label=wf) for wf in workflows]
    g.fig.legend(
        handles=legend_elements,
        title="Workflow",
        loc="upper right",
        bbox_to_anchor=(0.98, 0.98),
    )

    g.set_axis_labels("Persistence Type", "Runtime (seconds)")
    g.set_titles("{col_name}")
    g.set(yscale="log")
    plt.tight_layout()

    plt.savefig(outpath, dpi=300)
    plt.close()


rule all:
    input:
        benchmarks=expand(
            "benchmarks/{workflow}/{persistence}/{scenario}.txt",
            workflow=WORKFLOWS,
            persistence=PERSISTENCE,
            scenario=SCENARIOS,
        ),
    output:
        plot="comparison.png",
        csv="comparison.csv",
    run:
        df = parse_benchmarks(input.benchmarks, output.csv)
        create_plots(df, output.plot)


def get_persistence_env(wildcards):
    """Generate environment variable settings for persistence type."""
    if wildcards.persistence == "lmdb":
        return "SNAKEMAKE_USE_LMDB_PERSISTENCE=1 SNAKEMAKE_USE_LMDB_PERSISTENCE_MTIME=0"
    elif wildcards.persistence == "lmdb_mtime":
        return "SNAKEMAKE_USE_LMDB_PERSISTENCE=1 SNAKEMAKE_USE_LMDB_PERSISTENCE_MTIME=1"
    else:
        return "SNAKEMAKE_USE_LMDB_PERSISTENCE=0 SNAKEMAKE_USE_LMDB_PERSISTENCE_MTIME=0"


# Setup metadata 'templates' by running workflows to completion
rule setup_template:
    input:
        wf=str(Path(workflow.basedir, "workflows/{workflow}.smk")),
    output:
        wf="templates/{workflow}/{persistence}/Snakefile",
        snkdir=directory("templates/{workflow}/{persistence}/.snakemake"),
        result_dir=directory("templates/{workflow}/{persistence}/results"),
    log:
        "logs/setup_{workflow}/{persistence}.log",
    params:
        outdir=subpath(output.wf, parent=True),
        env_vars=get_persistence_env,
        config=get_full_config,
    shell:
        """
        exec > {log} 2>&1
        cp {input.wf} {output.wf}
        {params.env_vars} snakemake -s {output.wf} -d {params.outdir} {params.config} --cores {threads}
        """


# Special setup for resume scenarios - run to partial completion
rule setup_resume_template:
    input:
        wf=str(Path(workflow.basedir, "workflows/{workflow}.smk")),
    output:
        wf="templates/{workflow}/{persistence}/{scenario}/Snakefile",
        snkdir=directory("templates/{workflow}/{persistence}/{scenario}/.snakemake"),
        result_dir=directory("templates/{workflow}/{persistence}/{scenario}/results"),
    log:
        "logs/setup_{workflow}/{persistence}/{scenario}.log",
    params:
        outdir=subpath(output.wf, parent=True),
        env_vars=get_persistence_env,
        config=get_resume_config,
    wildcard_constraints:
        scenario="resume_.*",
    shell:
        """
        exec > {log} 2>&1
        cp {input.wf} {output.wf}
        {params.env_vars} snakemake -s {output.wf} -d {params.outdir} {params.config} --cores {threads}
        """


rule prepare_test:
    localrule: True
    input:
        wf="templates/{workflow}/{persistence}/Snakefile",
        snkdir="templates/{workflow}/{persistence}/.snakemake",
        result_dir="templates/{workflow}/{persistence}/results",
    output:
        wf="test_runs/{workflow}/{persistence}/{scenario}/Snakefile",
        snkdir=directory("test_runs/{workflow}/{persistence}/{scenario}/.snakemake"),
        result_dir=directory("test_runs/{workflow}/{persistence}/{scenario}/results"),
    wildcard_constraints:
        scenario="(fresh_run|param_change|code_change)",
    shell:
        """
        cp {input.wf} {output.wf}
        cp -r {input.snkdir} {output.snkdir}
        cp -r {input.result_dir} {output.result_dir}
        """


rule prepare_test_resume:
    localrule: True
    input:
        wf="templates/{workflow}/{persistence}/{scenario}/Snakefile",
        snkdir="templates/{workflow}/{persistence}/{scenario}/.snakemake",
        result_dir="templates/{workflow}/{persistence}/{scenario}/results",
    output:
        wf="test_runs/{workflow}/{persistence}/{scenario}/Snakefile",
        snkdir=directory("test_runs/{workflow}/{persistence}/{scenario}/.snakemake"),
        result_dir=directory("test_runs/{workflow}/{persistence}/{scenario}/results"),
    wildcard_constraints:
        scenario="resume_.*",
    shell:
        """
        cp {input.wf} {output.wf}
        cp -r {input.snkdir} {output.snkdir}
        cp -r {input.result_dir} {output.result_dir}
        """


rule profile_codechange:
    """
    Profile DAG building after code changes (with --rerun-triggers code)
    """
    input:
        wf="test_runs/{workflow}/{persistence}/code_change/Snakefile",
    output:
        done="test_runs/{workflow}/{persistence}/code_change/.done",
    params:
        outdir=subpath(input.wf, parent=True),
        env_vars=get_persistence_env,
        config=get_full_config,
        snake_cli_extra="--rerun-triggers code",
    benchmark:
        repeat("benchmarks/{workflow}/{persistence}/code_change.txt", 10)
    log:
        "logs/profile/{workflow}/{persistence}/code_change.log",
    shell:
        """
        exec > {log} 2>&1
        RANDOM_VALUE=$RANDOM
        sed "s/# EXTRACT_COMMENT: /# modified $RANDOM_VALUE/" {input.wf} > {input.wf}.tmp
        {params.env_vars} snakemake -s {input.wf}.tmp -d {params.outdir} {params.config} {params.snake_cli_extra} --dry-run --cores 1
        rm {input.wf}.tmp
        touch {output.done}
        """


rule profile_param_change:
    """
    Profile DAG building after params changes (with --rerun-triggers params)
    """
    input:
        wf="test_runs/{workflow}/{persistence}/param_change/Snakefile",
    output:
        done="test_runs/{workflow}/{persistence}/param_change/.done",
    params:
        outdir=subpath(input.wf, parent=True),
        env_vars=get_persistence_env,
        config=lambda w: get_config_string(N_SAMPLES),
        snake_cli_extra="--rerun-triggers params",
    benchmark:
        repeat("benchmarks/{workflow}/{persistence}/param_change.txt", 10)
    log:
        "logs/profile/{workflow}/{persistence}/param_change.log",
    shell:
        """
        exec > {log} 2>&1
        RANDOM_VALUE=$RANDOM
        {params.env_vars} snakemake -s {input.wf} -d {params.outdir} {params.config} extract_method="$RANDOM" {params.snake_cli_extra} --dry-run --cores 1
        touch {output.done}
        """


rule profile_fresh_run:
    """
    Benchmark fresh run
    """
    input:
        wf=str(Path(workflow.basedir, "workflows/{workflow}.smk")),
    output:
        done="test_runs/{workflow}/{persistence}/fresh_run/.done",
    params:
        outdir=subpath(output.done, parent=True),
        env_vars=get_persistence_env,
        config=get_full_config,
    benchmark:
        repeat("benchmarks/{workflow}/{persistence}/fresh_run.txt", 10)
    log:
        "logs/profile/{workflow}/{persistence}/fresh_run.log",
    shell:
        """
        exec > {log} 2>&1
        {params.env_vars} snakemake -s {input.wf} -d {params.outdir} {params.config} --dry-run --cores 1
        touch {output.done}
        """


rule profile_resume:
    """
    Profile DAG building when resuming from partial completion
    Template was run to X% completion, now dry-run full workflow
    """
    input:
        wf="test_runs/{workflow}/{persistence}/{scenario}/Snakefile",
    output:
        done="test_runs/{workflow}/{persistence}/{scenario}/.done",
    params:
        outdir=subpath(input.wf, parent=True),
        env_vars=get_persistence_env,
        config=get_full_config,  # Full size for benchmark
    wildcard_constraints:
        scenario="resume_.*",
    benchmark:
        repeat("benchmarks/{workflow}/{persistence}/{scenario}.txt", 10)
    log:
        "logs/profile/{workflow}/{persistence}/{scenario}.log",
    shell:
        """
        exec > {log} 2>&1
        {params.env_vars} snakemake -s {input.wf} -d {params.outdir} {params.config} --dry-run --cores 1
        touch {output.done}
        """
