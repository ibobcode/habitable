# 🤝 Contribution Guidelines

This repository enforces strict branch protection on `master`. All code integration requires a Pull Request (PR) and peer review approval. 🛡️

## 🛤️ Standard Workflow

1.  **Fork** the repository. 🍴
2.  **Develop** on an isolated branch. 🌿
3.  **Submit** a Pull Request targeting the `master` branch. Automatic workflow execution is suspended until the PR is merged. ✅

## ➕ Adding a New Data Source

### 1. Define the Schema 📝
Update `scripts/fr/schema.py` to declare your new data attributes. This file is the single source of truth. The final assembly script will automatically discard any column not explicitly declared here.

### 2. Implement the Script 💻
Create `scripts/fr/data_<name>.py`.
**Structural Constraints:**
* **Input:** Load the spatial reference grid from `parquets/fr/base_grid.parquet`.
* **Vectorization:** Perform spatial operations using vectorized methods (Pandas, NumPy, SciPy). Iterative coordinate loops (`iterrows`) are strictly prohibited. ⚡
* **Typing:** Apply explicit type casting using the `SCHEMA` dictionary before saving.
* **Output:** Save the result to `parquets/fr/h3_<name>.parquet`.

### 3. Configure Assembly 🧩
Update `scripts/fr/assemble.py`.
Add your output Parquet filename (e.g., `h3_<name>.parquet`) to the `SOURCES` list.

### 4. CI/CD Setup 🤖
* Create a workflow file `.github/workflows/fr_data_<name>.yml` modeled after existing ones. Set it to trigger on modifications to your specific script or `schema.py`.
* Update `.github/workflows/fr_assemble.yml`: Add your new workflow's name to the `workflow_run` trigger list to ensure the GeoJSON is rebuilt after your data updates.

## 📏 Code Standards

* **English Only:** All variable names, functions, and comments must be written in English. 🇬🇧
* **Separation of Concerns:** One data domain equals one computation script and one independent Parquet file. 🎯
