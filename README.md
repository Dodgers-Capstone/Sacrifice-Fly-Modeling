# Sacrifice-Fly-Modeling

[![CC BY-NC-SA 4.0][cc-by-nc-sa-shield]][cc-by-nc-sa]

## Project Objective

1. Estimate the probability that a runner is thrown out at home when runners are on 2nd and 3rd.
2. Two targets may be favorable P(attempt) and P(success | attempt)

## Deliverables

- [ ] Model performance metrics (meanSquared error, or calculated brier score)
- [ ] Feature importance with high levels of explainability
- [ ] Model Calibration
- [ ] Outfielder position recommendation
- [ ] Webapp demonstration

## Potential Extentions

- [ ] Evaluate 3rd base coach decisions
- [ ] Evaluate runner talent based on opportunities
- [ ] Evaluate fielder talent based on opportunities

## Major Milestones

- [ ] Data Early Discovery Analysis (EDA) and feature engineering
- [ ] Completed and tested model to Estimate the probability that a runner is thrown out at home when runners are on 2nd and 3rd.
- [ ] Model Product Visualization and webapp completion

## Installation and Setup

### Install the Python Package Manager, `uv`

Install uv for MacOS:

```{bash}
brew install uv
```

Install uv for Linux and WSL:

```{bash}
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Pin a Python version ≥ 3.11

```{bash}
# List available python versions
uv python list

# Install a python version from the list. Replace <python_name> with the python name form the list.
uv python install <python_name>

# Pin the python version to the project. Replace <python_name> with the python name that was installed.
uv python pin <python_name>
```

### Sync and Add Project Python Packages to the project environment
```{bash}
# sync python environment packages with the project
uv sync

# add python packages to the project. Replace <package_name> with the package name you wish to add.
uv add <package_name>
```

## Feature Development Pipeline

### Early Discovery Analysis (EDA) and Feature Engineering

- [ ] Game State Filtering
- [ ] Fielder Positioning
- [ ] Fielder Throw Velocity and Accuracy
- [x] Runner Acceleration and Velocity
- [ ] Coordinates of Caught Ball
- [ ] Fielder who Caught Ball
- [ ] Field Normalization
- [ ] Ball Airtime Calculation

### Model Development and Testing

- [ ] Input Features
- [ ] Output Features
- [ ] Embedding Pipeline
- [ ] Model Calibration
- [ ] Model Validation Results

### Model Product Visualization and Web Application Completion

- [ ] Available Web-app Input Variables and Reasoning
- [ ] Available Web-app Output Variables and Reasoning
- [ ] Output Variable Visualization and Reasoning
- [ ] UI Development and Use Documentation

---

Major League Baseball trademarks and copyrights are used with permission of MLB Advanced Media, L.P. All rights reserved

This work is licensed under a [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License][cc-by-nc-sa].

[![CC BY-NC-SA 4.0][cc-by-nc-sa-image]][cc-by-nc-sa]

[cc-by-nc-sa]: http://creativecommons.org/licenses/by-nc-sa/4.0/
[cc-by-nc-sa-image]: https://licensebuttons.net/l/by-nc-sa/4.0/88x31.png
[cc-by-nc-sa-shield]: https://img.shields.io/badge/License-CC%20BY--NC--SA%204.0-lightgrey.svg
