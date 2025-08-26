# Sample Snakefile for profiling
import time
import random

# Configuration
SAMPLES = [f"sample{i}" for i in range(30)]
REPS = range(1000)


rule all:
    input:
        expand("{rep}/results/summary.txt", rep=REPS),


rule process_sample:
    output:
        "{rep}/results/{sample}.processed",
    run:
        # Simulate some work
        time.sleep(random.uniform(0.1, 0.5))
        with open(output[0], "w") as f:
            f.write(f"Processed {wildcards.sample}\n")


rule analyze_sample:
    input:
        "{rep}/results/{sample}.processed",
    output:
        "{rep}/results/{sample}.analyzed",
    run:
        # Simulate analysis work
        time.sleep(random.uniform(0.2, 0.8))
        with open(output[0], "w") as f:
            f.write(f"Analyzed {wildcards.sample}\n")


rule summarize:
    input:
        expand("{{rep}}/results/{sample}.analyzed", sample=SAMPLES),
    output:
        "{rep}/results/summary.txt",
    run:
        # Simulate summary work
        time.sleep(0.5)
        with open(output[0], "w") as f:
            f.write("Summary of all samples:\n")
            for sample in SAMPLES:
                f.write(f"- {sample}\n")
