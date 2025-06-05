# Project Setup Guide

How to test transpile queries.

## 1. Cloning the Project from GitHub

To get a local copy of this project, you need to clone it from the gpicode GitHub [repository](https://github.com/gpicode/sqlglot).

**Clone the repository:**

```
git clone git@github.com:gpicode/sqlglot.git
```

or

```
git clone https://github.com/gpicode/sqlglot.git
```

## 2. Creating and Activating a Python Virtual Environment

1. **Navigate into the project directory:**
   Change your directory to the cloned project folder: `sqlglot`

2. **Create a virtual environment:**
   Use the `venv` module to create a new virtual environment.
   `    python3 -m venv .venv
   `

3. **Activate the virtual environment:**

- **On macOS:**

  ```
  source .venv/bin/activate
  ```

## 3. Installing the Project Locally

Once your virtual environment is active, you can install the project's dependencies and the project itself using `pip`.

**Ensure your virtual environment is active:**

**Option A: Editable Install**
The `-e` (or `--editable`) flag means that `pip` will install the project in "editable" mode. Any changes you make to the source code will immediately reflect without needing to reinstall.

```
pip install -e .
```

**Option B: Standard Install**

Changes to source code are not reflected. Reinstall is required.

```
pip install .
```

# 4. Use transpiler-script.py

**In the project repository run**

```
python transpiler-script.py
```
