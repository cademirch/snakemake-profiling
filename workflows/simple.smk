# Simple workflow for testing profiling
import time
import random

SAMPLES = ["A", "B", "C"]


rule all:
    input:
        "output/final_result.txt",


rule process:
    output:
        "output/{sample}_processed.txt",
    run:
        # Simulate some computational work
        time.sleep(random.uniform(0.1, 0.3))
        with open(output[0], "w") as f:
            f.write(f"Processed sample {wildcards.sample}\n")


rule combine:
    input:
        expand("output/{sample}_processed.txt", sample=SAMPLES),
    output:
        "output/final_result.txt",
    run:
        # Simulate combining results
        time.sleep(0.2)
        with open(output[0], "w") as f:
            f.write("Combined results:\n")
            for inp in input:
                with open(inp) as inf:
                    f.write(inf.read())
