# Sample Snakefile for profiling
import time
import random

# Configuration
SAMPLES = ["sample1", "sample2", "sample3", "sample4", "sample5"]


rule all:
    input:
        "results/summary.txt",


rule process_sample:
    output:
        "results/{sample}.processed",
    run:
        # Simulate some work
        time.sleep(random.uniform(0.1, 0.5))
        with open(output[0], "w") as f:
            f.write(f"Processed {wildcards.sample}\n")


rule analyze_sample:
    input:
        "results/{sample}.processed",
    output:
        "results/{sample}.analyzed",
    run:
        # Simulate analysis work
        time.sleep(random.uniform(0.2, 0.8))
        with open(output[0], "w") as f:
            f.write(f"Analyzed {wildcards.sample}\n")


rule summarize:
    input:
        expand("results/{sample}.analyzed", sample=SAMPLES),
    output:
        "results/summary.txt",
    run:
        # Simulate summary work
        time.sleep(0.5)
        with open(output[0], "w") as f:
            f.write("Summary of all samples:\n")
            for sample in SAMPLES:
                f.write(f"- {sample}\n")
