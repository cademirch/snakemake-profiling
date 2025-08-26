# Snakemake Profiling with Pyinstrument

This repository provides a setup for profiling Snakemake workflows using pyinstrument. 

## Setup

1. Install Pixi if you haven't already:
   ```bash
   curl -fsSL https://pixi.sh/install.sh | bash
   ```

2. If you are wanting to profile your local version of Snakemake, simply edit the pixi.toml like so:
      ```toml
      #... everything above this stays the same.
      [dependencies]
      # removed snakemake from here
      python = ">=3.13.5,<3.14"
      pip = ">=25.2,<26"

      [pypi-dependencies]
      pyinstrument = ">=5.0.3, <6"
      snakemake = {path = "path/to/your/snakemake/repo", editable = true} # <- add this line
      ```
3. Install dependencies:
   ```bash
   pixi install
   ```



## Pixi Tasks
There are some pixi tasks for convenience.

- `pixi run profile-simple [dryrun]` - Profile the simple workflow
      ```
      # Run and profile the workflow
      pixi run profile-simple

      # Profile the dry run (useful for profiling dag building and such)
      pixi run profile-simple true
      ```
- `pixi run view-profile <profile_file>` - View a profile in your browser
- `pixi run clean` - Clean up workflow files in `workflows/output`