import os
from pathlib import Path
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


WORKFLOWS = ["linear", "diamond", "fanout"]
PERSISTENCE = ["json", "lmdb", "lmdb_mtime"]
SCENARIOS = ["no_change", "param_change", "code_change"]


def parse_benchmarks(benchmarks, outcsv):
    """Parse all benchmark files and aggregate into a DataFrame."""
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

        records.append(df)

    if not records:
        print(f"No benchmark files found in {benchmark_dir}", file=sys.stderr)
        return None

    # Combine all benchmark data
    all_data = pd.concat(records, ignore_index=True)
    all_data.to_csv(outcsv)
    return all_data


def create_plots(df, outpath):
    """Create comparison plots for different persistence implementations."""
    import matplotlib

    matplotlib.use("agg")
    # Set style
    sns.set_theme(style="whitegrid")

    # Define color palette for workflows
    workflows = sorted(df["workflow"].unique())
    palette = dict(zip(workflows, sns.color_palette(n_colors=len(workflows))))

    # Boxplots faceted by scenario with workflow markers
    g = sns.catplot(
        data=df,
        x="persistence",
        y="s",
        col="scenario",
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

    # Add a single legend manually
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
        return (
            "SNAKEMAKE_USE_LMDB_PERSISTENCE=1"
            + " SNAKEMAKE_USE_LMDB_PERSISTENCE_MTIME=0"
        )
    elif wildcards.persistence == "lmdb_mtime":
        return (
            "SNAKEMAKE_USE_LMDB_PERSISTENCE=1"
            + " SNAKEMAKE_USE_LMDB_PERSISTENCE_MTIME=1"
        )
    else:
        return (
            "SNAKEMAKE_USE_LMDB_PERSISTENCE=0"
            + " SNAKEMAKE_USE_LMDB_PERSISTENCE_MTIME=0"
        )


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
    shell:
        """
        exec > {log} 2>&1
        cp {input.wf} {output.wf}
        {params.env_vars} snakemake -s {output.wf} -d {params.outdir} --cores {threads}
        """


rule prepare_test:
    input:
        wf="templates/{workflow}/{persistence}/Snakefile",
        snkdir="templates/{workflow}/{persistence}/.snakemake",
        result_dir="templates/{workflow}/{persistence}/results",
    output:
        wf="test_runs/{workflow}/{persistence}/{scenario}/Snakefile",
        snkdir=directory("test_runs/{workflow}/{persistence}/{scenario}/.snakemake"),
        result_dir=directory("test_runs/{workflow}/{persistence}/{scenario}/results"),
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
        snake_cli_extra="--rerun-triggers code",
    benchmark:
        repeat("benchmarks/{workflow}/{persistence}/code_change.txt", 10)
    log:
        "logs/profile/{workflow}/{persistence}/code_change.log",
    shell:
        """
        exec > {log} 2>&1
        sed 's/# EXTRACT_COMMENT: Initial data extraction step/# modified/' {input.wf} > {input.wf}.tmp && mv {input.wf}.tmp {input.wf}
        {params.env_vars} snakemake -s {input.wf} -d {params.outdir} {params.snake_cli_extra} --dry-run
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
        snake_cli_extra="--config extract_method='changed' --rerun-triggers params",
    benchmark:
        repeat("benchmarks/{workflow}/{persistence}/param_change.txt", 10)
    log:
        "logs/profile/{workflow}/{persistence}/param_change.log",
    shell:
        """
        exec > {log} 2>&1
        {params.env_vars} snakemake -s {input.wf} -d {params.outdir} {params.snake_cli_extra} --dry-run
        touch {output.done}
        """


rule profile_no_change:
    """
    Benchmark fresh run
    """
    input:
        wf=str(Path(workflow.basedir, "workflows/{workflow}.smk")),
    output:
        done="test_runs/{workflow}/{persistence}/no_change/.done",
    params:
        outdir=subpath(output.done, parent=True),
        env_vars=get_persistence_env,
        snake_cli_extra="",
    benchmark:
        repeat("benchmarks/{workflow}/{persistence}/no_change.txt", 10)
    log:
        "logs/profile/{workflow}/{persistence}/no_change.log",
    shell:
        """
        exec > {log} 2>&1
        {params.env_vars} snakemake -s {input.wf} -d {params.outdir} {params.snake_cli_extra} --dry-run
        touch {output.done}
        """
