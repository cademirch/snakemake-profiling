N_SAMPLES = config.get("n_samples", 20)
N_REPLICATES = config.get("n_replicates", 5)
N_BATCHES = config.get("n_batches", 3)

SAMPLES = [f"sample_{i:03d}" for i in range(N_SAMPLES)]
REPLICATES = [f"rep{i}" for i in range(N_REPLICATES)]
BATCHES = [f"batch{i}" for i in range(N_BATCHES)]


rule all:
    input:
        expand("results/processed/{sample}_{rep}.txt", sample=SAMPLES, rep=REPLICATES),
        "results/summary/final_summary.txt",
        "results/reports/master_report.html",


rule extract:
    output:
        "results/prepared/data.txt",
    params:
        method=config.get("extract_method", "extract"),
        quality=config.get("extract_quality", 30),
        format=config.get("extract_format", "raw"),
    shell:
        """
        # PREPARE_COMMENT: Initialize dataset for processing
        touch {output}
        """


rule preprocess:
    input:
        "results/prepared/data.txt",
    output:
        "results/preprocessed/data.txt",
    params:
        trim_length=config.get("preprocess_trim_length", 100),
        adapter=config.get("preprocess_adapter", "AGATCGGAAGAG"),
    shell:
        """
        # PREPROCESS_COMMENT: Preprocess prepared data
        touch {output}
        """


rule split:
    input:
        "results/preprocessed/data.txt",
    output:
        expand("results/split/{sample}.txt", sample=SAMPLES),
    params:
        method=config.get("split_method", "balanced"),
        chunk_size=config.get("split_chunk_size", 1000),
    shell:
        """
        # SPLIT_COMMENT: Split preprocessed data into samples
        touch {output}
        """


rule align:
    input:
        "results/split/{sample}.txt",
    output:
        "results/aligned/{sample}.txt",
    params:
        algorithm=config.get("align_algorithm", "bowtie2"),
        mismatch_penalty=config.get("align_mismatch", 6),
    shell:
        """
        # ALIGN_COMMENT: Align split samples to reference
        touch {output}
        """


rule deduplicate:
    input:
        "results/aligned/{sample}.txt",
    output:
        "results/deduped/{sample}.txt",
    params:
        method=config.get("dedup_method", "picard"),
        optical_distance=config.get("dedup_optical_dist", 100),
    shell:
        """
        # DEDUPLICATE_COMMENT: Remove duplicate reads
        touch {output}
        """


rule batch_split:
    input:
        "results/deduped/{sample}.txt",
    output:
        expand("results/batched/{{sample}}_{batch}.txt", batch=BATCHES),
    params:
        strategy=config.get("batch_strategy", "random"),
    shell:
        """
        # BATCH_SPLIT_COMMENT: Split samples into processing batches
        touch {output}
        """


rule process_batch:
    input:
        "results/batched/{sample}_{batch}.txt",
    output:
        expand("results/batch_processed/{{sample}}_{{batch}}_{rep}.txt", rep=REPLICATES),
    params:
        threads=config.get("process_threads", 4),
        memory=config.get("process_memory", "8G"),
        mode=config.get("process_mode", "parallel"),
    shell:
        """
        # PROCESS_BATCH_COMMENT: Process batches with technical replicates
        touch {output}
        """


rule merge_batches:
    input:
        expand("results/batch_processed/{{sample}}_{batch}_{{rep}}.txt", batch=BATCHES),
    output:
        "results/merged/{sample}_{rep}.txt",
    params:
        merge_method=config.get("merge_method", "concatenate"),
    shell:
        """
        # MERGE_BATCHES_COMMENT: Merge batches back together
        touch {output}
        """


rule process:
    input:
        "results/merged/{sample}_{rep}.txt",
    output:
        "results/processed/{sample}_{rep}.txt",
    params:
        filter_quality=config.get("process_filter_quality", 30),
        min_length=config.get("process_min_length", 50),
    shell:
        """
        # PROCESS_COMMENT: Final processing of merged data
        touch {output}
        """


rule qc_replicate:
    input:
        expand("results/processed/{{sample}}_{rep}.txt", rep=REPLICATES),
    output:
        "results/qc/replicate/{sample}_rep_qc.txt",
    params:
        correlation_threshold=config.get("qc_correlation", 0.95),
    shell:
        """
        # QC_REPLICATE_COMMENT: Check replicate correlation
        touch {output}
        """


rule summarize_samples:
    input:
        expand("results/processed/{sample}_{rep}.txt", sample=SAMPLES, rep=REPLICATES),
    output:
        "results/summary/sample_summary.txt",
    params:
        summary_type=config.get("summary_type", "comprehensive"),
    shell:
        """
        # SUMMARIZE_SAMPLES_COMMENT: Create sample-level summary
        touch {output}
        """


rule final_summary:
    input:
        "results/summary/sample_summary.txt",
        expand("results/qc/replicate/{sample}_rep_qc.txt", sample=SAMPLES),
    output:
        "results/summary/final_summary.txt",
    params:
        include_stats=config.get("summary_include_stats", True),
    shell:
        """
        # FINAL_SUMMARY_COMMENT: Generate final comprehensive summary
        touch {output}
        """


rule master_report:
    input:
        expand("results/split/{sample}.txt", sample=SAMPLES),
        expand("results/aligned/{sample}.txt", sample=SAMPLES),
        expand("results/deduped/{sample}.txt", sample=SAMPLES),
        expand("results/batched/{sample}_{batch}.txt", sample=SAMPLES, batch=BATCHES),
        expand(
            "results/batch_processed/{sample}_{batch}_{rep}.txt",
            sample=SAMPLES,
            batch=BATCHES,
            rep=REPLICATES,
        ),
        expand("results/processed/{sample}_{rep}.txt", sample=SAMPLES, rep=REPLICATES),
        expand("results/qc/replicate/{sample}_rep_qc.txt", sample=SAMPLES),
        "results/summary/sample_summary.txt",
        "results/summary/final_summary.txt",
    output:
        "results/reports/master_report.html",
    params:
        report_title=config.get("report_title", "Fanout Pipeline Master Report"),
    shell:
        """
        # MASTER_REPORT_COMMENT: Generate master report with all pipeline outputs
        touch {output}
        """
