#diamond
N_SAMPLES = config.get("n_samples", 10)
SAMPLES = [f"sample_{i:03d}" for i in range(N_SAMPLES)]
GROUPS = ["groupA", "groupB"]
CONDITIONS = ["control", "treatment"]


rule all:
    input:
        "results/final/summary.txt",
        "results/qc/full_report.html",
        "results/reports/comprehensive_report.html",


rule extract:
    output:
        "results/extracted/{sample}.txt",
    params:
        method=config.get("extract_method", "extract"),
        quality=config.get("extract_quality", 30),
        format=config.get("extract_format", "raw"),
    shell:
        """
        # EXTRACT_COMMENT: Extract raw data from source
        touch {output}
        """


rule trim:
    input:
        "results/extracted/{sample}.txt",
    output:
        "results/trimmed/{sample}.txt",
    params:
        min_length=config.get("trim_min_length", 20),
        quality_cutoff=config.get("trim_quality_cutoff", 20),
    shell:
        """
        # TRIM_COMMENT: Trim low quality regions
        touch {output}
        """


rule classify:
    input:
        "results/trimmed/{sample}.txt",
    output:
        expand("results/classified/{{sample}}_{group}.txt", group=GROUPS),
    params:
        algorithm=config.get("classify_algorithm", "ml_classifier"),
        confidence=config.get("classify_confidence", 0.95),
        model_version=config.get("classify_model_version", 2),
    shell:
        """
        # CLASSIFY_COMMENT: Classify samples into groups
        touch {output}
        """


rule assign_condition:
    input:
        "results/classified/{sample}_{group}.txt",
    output:
        expand(
            "results/conditioned/{{sample}}_{{group}}_{condition}.txt",
            condition=CONDITIONS,
        ),
    params:
        assignment_rule=config.get("condition_rule", "balanced"),
    shell:
        """
        # ASSIGN_CONDITION_COMMENT: Assign experimental conditions
        touch {output}
        """


rule normalize_condition:
    input:
        expand(
            "results/conditioned/{{sample}}_{{group}}_{condition}.txt",
            condition=CONDITIONS,
        ),
    output:
        "results/normalized/{sample}_{group}.txt",
    params:
        method=config.get("normalize_method", "quantile"),
    shell:
        """
        # NORMALIZE_CONDITION_COMMENT: Normalize across conditions
        touch {output}
        """


rule group_aggregate:
    input:
        expand("results/normalized/{sample}_{{group}}.txt", sample=SAMPLES),
    output:
        "results/grouped/{group}_summary.txt",
    params:
        method=config.get("group_aggregate_method", "mean"),
        weighted=config.get("group_aggregate_weighted", True),
        min_samples=config.get("group_aggregate_min_samples", 5),
    shell:
        """
        # GROUP_AGGREGATE_COMMENT: Aggregate within each group
        touch {output}
        """


rule differential_analysis:
    input:
        expand("results/grouped/{group}_summary.txt", group=GROUPS),
    output:
        "results/differential/comparison.txt",
    params:
        fdr_cutoff=config.get("diff_fdr_cutoff", 0.05),
        log2fc_cutoff=config.get("diff_log2fc_cutoff", 1.0),
    shell:
        """
        # DIFFERENTIAL_COMMENT: Perform differential analysis between groups
        touch {output}
        """


rule annotate_results:
    input:
        "results/differential/comparison.txt",
    output:
        "results/annotated/comparison_annotated.txt",
    params:
        annotation_db=config.get("annotation_db", "ensembl"),
        species=config.get("annotation_species", "human"),
    shell:
        """
        # ANNOTATE_RESULTS_COMMENT: Annotate differential results
        touch {output}
        """


rule merge:
    input:
        expand("results/grouped/{group}_summary.txt", group=GROUPS),
        "results/annotated/comparison_annotated.txt",
    output:
        "results/merged/combined.txt",
    params:
        merge_strategy=config.get("merge_strategy", "outer"),
        handle_missing=config.get("merge_handle_missing", "interpolate"),
        validate=config.get("merge_validate", True),
    shell:
        """
        # MERGE_COMMENT: Merge all analysis results
        touch {output}
        """


rule filter_significant:
    input:
        "results/merged/combined.txt",
    output:
        "results/filtered/significant.txt",
    params:
        pvalue_cutoff=config.get("filter_pvalue", 0.05),
        effect_size=config.get("filter_effect_size", 0.5),
    shell:
        """
        # FILTER_SIGNIFICANT_COMMENT: Filter for significant results
        touch {output}
        """


rule final:
    input:
        "results/filtered/significant.txt",
    output:
        "results/final/summary.txt",
    params:
        format=config.get("final_format", "report"),
        include_stats=config.get("final_include_stats", True),
        timestamp=config.get("final_timestamp", True),
    shell:
        """
        # FINAL_COMMENT: Create final summary report
        touch {output}
        """


rule qc_per_group:
    input:
        expand("results/normalized/{sample}_{{group}}.txt", sample=SAMPLES),
    output:
        "results/qc/per_group/{group}_qc.txt",
    params:
        min_coverage=config.get("qc_min_coverage", 10),
    shell:
        """
        # QC_GROUP_COMMENT: Quality control per group
        touch {output}
        """


rule aggregate_qc:
    input:
        expand("results/qc/per_group/{group}_qc.txt", group=GROUPS),
        expand("results/normalized/{sample}_{group}.txt", sample=SAMPLES, group=GROUPS),
    output:
        "results/qc/full_report.html",
    params:
        report_title=config.get("qc_report_title", "Diamond Pipeline QC"),
    shell:
        """
        # QC_FULL_COMMENT: Generate comprehensive QC report
        touch {output}
        """


rule comprehensive_report:
    input:
        expand("results/extracted/{sample}.txt", sample=SAMPLES),
        expand("results/trimmed/{sample}.txt", sample=SAMPLES),
        expand("results/classified/{sample}_{group}.txt", sample=SAMPLES, group=GROUPS),
        expand(
            "results/conditioned/{sample}_{group}_{condition}.txt",
            sample=SAMPLES,
            group=GROUPS,
            condition=CONDITIONS,
        ),
        expand("results/normalized/{sample}_{group}.txt", sample=SAMPLES, group=GROUPS),
        expand("results/grouped/{group}_summary.txt", group=GROUPS),
        "results/differential/comparison.txt",
        "results/annotated/comparison_annotated.txt",
        "results/merged/combined.txt",
        "results/filtered/significant.txt",
        "results/final/summary.txt",
        expand("results/qc/per_group/{group}_qc.txt", group=GROUPS),
        "results/qc/full_report.html",
    output:
        "results/reports/comprehensive_report.html",
    params:
        report_title=config.get(
            "comprehensive_report_title", "Diamond Pipeline Comprehensive Report"
        ),
        include_all_files=config.get("comprehensive_include_all", True),
    shell:
        """
        # COMPREHENSIVE_REPORT_COMMENT: Generate comprehensive report of entire pipeline
        touch {output}
        """
