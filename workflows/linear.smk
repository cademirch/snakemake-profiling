# workflows/linear.smk


N_SAMPLES = config.get("n_samples")
SAMPLES = [f"sample_{i}" for i in range(N_SAMPLES)]


rule all:
    input:
        expand("results/final/{sample}.txt", sample=SAMPLES),
        "results/qc/multiqc_report.html",


rule extract:
    output:
        "results/extracted/{sample}.txt",
    params:
        method=config.get("extract_method", "extract"),
        quality=config.get("extract_quality", 30),
        threads=config.get("extract_threads", 1),
    shell:
        """
        # EXTRACT_COMMENT: Initial data extraction step
        touch {output}
        """


rule validate:
    input:
        "results/extracted/{sample}.txt",
    output:
        "results/validated/{sample}.txt",
    params:
        min_size=config.get("validate_min_size", 10),
        max_errors=config.get("validate_max_errors", 5),
    shell:
        """
        # VALIDATE_COMMENT: Validate extracted data quality
        touch {output}
        """


rule filter:
    input:
        "results/validated/{sample}.txt",
    output:
        "results/filtered/{sample}.txt",
    params:
        threshold=config.get("filter_threshold", 0.05),
        method=config.get("filter_method", "filter"),
        min_quality=config.get("filter_min_quality", 20),
    shell:
        """
        # FILTER_COMMENT: Apply quality filters to validated data
        touch {output}
        """


rule normalize:
    input:
        "results/filtered/{sample}.txt",
    output:
        "results/normalized/{sample}.txt",
    params:
        method=config.get("normalize_method", "zscore"),
        scale=config.get("normalize_scale", 1.0),
    shell:
        """
        # NORMALIZE_COMMENT: Normalize filtered data
        touch {output}
        """


rule transform:
    input:
        "results/normalized/{sample}.txt",
    output:
        "results/transformed/{sample}.txt",
    params:
        algorithm=config.get("transform_algorithm", "standard"),
        iterations=config.get("transform_iterations", 100),
        scale=config.get("transform_scale", 1.5),
    shell:
        """
        # TRANSFORM_COMMENT: Transform normalized data using algorithm
        touch {output}
        """


rule annotate:
    input:
        "results/transformed/{sample}.txt",
    output:
        "results/annotated/{sample}.txt",
    params:
        database=config.get("annotate_database", "default"),
        version=config.get("annotate_version", "v1"),
    shell:
        """
        # ANNOTATE_COMMENT: Annotate transformed data with metadata
        touch {output}
        """


rule aggregate:
    input:
        "results/annotated/{sample}.txt",
    output:
        "results/aggregated/{sample}.txt",
    params:
        window_size=config.get("aggregate_window_size", 10),
        method=config.get("aggregate_method", "mean"),
        normalize=config.get("aggregate_normalize", True),
    shell:
        """
        # AGGREGATE_COMMENT: Aggregate annotated data by window
        touch {output}
        """


rule score:
    input:
        "results/aggregated/{sample}.txt",
    output:
        "results/scored/{sample}.txt",
    params:
        scoring_method=config.get("score_method", "weighted"),
        weights=config.get("score_weights", "1,2,3"),
    shell:
        """
        # SCORE_COMMENT: Score aggregated data
        touch {output}
        """


rule final:
    input:
        "results/scored/{sample}.txt",
    output:
        "results/final/{sample}.txt",
    params:
        format=config.get("final_format", "summary"),
        precision=config.get("final_precision", 3),
        compress=config.get("final_compress", False),
    shell:
        """
        # FINAL_COMMENT: Generate final output with formatting
        touch {output}
        """


rule qc_per_sample:
    input:
        "results/final/{sample}.txt",
    output:
        "results/qc/per_sample/{sample}_qc.txt",
    params:
        qc_threshold=config.get("qc_threshold", 0.9),
    shell:
        """
        # QC_SAMPLE_COMMENT: Quality control check per sample
        touch {output}
        """


rule aggregate_qc:
    input:
        expand("results/qc/per_sample/{sample}_qc.txt", sample=SAMPLES),
    output:
        "results/qc/aggregate_qc.txt",
    params:
        min_pass_rate=config.get("qc_min_pass_rate", 0.95),
    shell:
        """
        # QC_AGGREGATE_COMMENT: Aggregate all QC results
        touch {output}
        """


rule multiqc:
    input:
        expand("results/extracted/{sample}.txt", sample=SAMPLES),
        expand("results/validated/{sample}.txt", sample=SAMPLES),
        expand("results/filtered/{sample}.txt", sample=SAMPLES),
        expand("results/normalized/{sample}.txt", sample=SAMPLES),
        expand("results/transformed/{sample}.txt", sample=SAMPLES),
        expand("results/annotated/{sample}.txt", sample=SAMPLES),
        expand("results/final/{sample}.txt", sample=SAMPLES),
        expand("results/qc/per_sample/{sample}_qc.txt", sample=SAMPLES),
        "results/qc/aggregate_qc.txt",
    output:
        "results/qc/multiqc_report.html",
    params:
        title=config.get("multiqc_title", "Linear Pipeline Report"),
    shell:
        """
        # MULTIQC_COMMENT: Generate comprehensive QC report
        touch {output}
        """
