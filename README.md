# Healthtech Analytics Pipeline: MIMIC-III on BigQuery

## Section III: The Cornerstone Project — A Production-Grade Health-Tech Analytics Pipeline

This project is the centerpiece of my portfolio and demonstrates, end-to-end, the competencies of a Senior Analytics Engineer for health technology. It leverages the MIMIC-III public clinical dataset hosted on Google BigQuery, and incorporates analytics engineering (dbt), machine learning (Python), and a live reporting interface (Streamlit).

---

### 3.1 Project Concept: Predicting Organ Failure and ICU Outcomes

**Dataset:** [MIMIC-III](https://mimic.mit.edu/) clinical database, hosted on Google BigQuery.

**Key focus:**

- Ingest and model ICU patient data
- Compute SOFA (Sequential Organ Failure Assessment) scores using modular SQL (dbt)
- Train and evaluate machine learning models for ICU risk prediction
- Deliver results through an interactive Streamlit dashboard for clinical stakeholders

**Narrative:**

> This project demonstrates a full-stack analytics pipeline, from raw EHR data to live clinical risk dashboards, enabling clinicians to explore SOFA score drivers, predict patient deterioration, and support early intervention using actionable analytics.

---

### 3.2 Technical Architecture & Blueprint

| Tool/Stack                               | Role in Project                | Key Features Showcased                                            |
| ---------------------------------------- | ------------------------------ | ----------------------------------------------------------------- |
| **Google BigQuery**                      | Data Warehouse                 | Serverless analytics; granular access; fast SQL on clinical data  |
| **dbt Core**                             | Data Transformation & Modeling | Modular, tested SQL; clinical logic tests; docs & exposures       |
| **Python (scikit-learn, XGBoost, etc.)** | Predictive Modeling            | Jupyter ML; cross-validation; feature engineering; explainability |
| **Streamlit**                            | End-User Dashboard             | Real-time UI for clinicians; connects to BigQuery & ML models     |
| **Apache Airflow (optional)**            | Orchestration                  | Idempotent DAGs for production workflows                          |
| **Git/GitHub**                           | Version Control & CI/CD        | Clean commits; PRs; dbt+ML workflow                               |

#### Pipeline Overview

1. **Data Ingestion & Warehousing (BigQuery)**

   - Load MIMIC-III data into `src_mimic` BigQuery dataset
   - Add dataset, table, and column descriptions in BigQuery UI
   - IAM/service account best practices

2. **Data Transformation (dbt Core)**

   - Directory: `models/staging/`, `models/intermediate/`, `models/marts/`
   - Staging: clean/standardize source tables (e.g., `chartevents`, `labevents`)
   - Intermediate: join and compose shared logic
   - Marts: analytic outputs & SOFA scores (see `models/marts/sofa.sql`)
   - Advanced dbt: custom tests, clinical macros, docs, and exposures

3. **Predictive Modeling (Python ML)**

   - Feature selection from marts (SOFA, labs, vitals)
   - Model training: logistic regression, XGBoost
   - Validation: cross-validation, ROC-AUC, SHAP
   - Export: model pickle & inference code

4. **Interactive Dashboard (Streamlit)**

   - KPI dashboards for SOFA, mortality, and ventilator use
   - Interactive cohort filtering
   - Predictive scoring: user inputs patient data, returns ML risk score
   - Live queries to BigQuery

5. **Orchestration (Apache Airflow — optional/advanced)**

   - Production DAG runs dbt, tests, ML retraining, notifications

---

### 3.3 Repo Structure

```
healthtech-analytics-pipeline/
├── README.md
├── dbt_project.yml
├── packages.yml
├── models/
│   ├── staging/
│   ├── intermediate/
│   ├── marts/
│   │   └── sofa.sql
│   └── schema.yml
├── notebooks/
│   ├── ml_training.ipynb
│   └── feature_engineering.ipynb
├── streamlit_app/
│   ├── app.py
│   └── requirements.txt
├── docs/
│   ├── data_dictionary.md
│   └── bigquery_screenshot.png
├── airflow/
│   └── dag.py
└── .gitignore
```

---

### 3.4 Data Dictionary

See [`docs/data_dictionary.md`](docs/data_dictionary.md) for a full BigQuery markdown data dictionary (generated automatically with a Python script).

---

### 3.5 Usage

**To Run the Pipeline Locally**

1. Clone repo; install dbt, Python, and Streamlit dependencies.
2. Set up GCP credentials (`GOOGLE_APPLICATION_CREDENTIALS`).
3. Run dbt transformations:
   ```bash
   dbt deps
   dbt run
   dbt test
   dbt docs generate
   ```
4. Train models in Jupyter:
   ```bash
   cd notebooks
   jupyter notebook
   ```
5. Launch Streamlit app:
   ```bash
   cd streamlit_app
   streamlit run app.py
   ```
6. (Optional) Start Airflow for orchestration

---

### 3.6 Key Features Demonstrated

- **Cloud analytics engineering:** Modular dbt+BigQuery, data quality, and reproducible SQL
- **Clinical feature engineering:** SOFA and derived scores, clinical event sequencing
- **Machine learning modeling:** Feature selection, cross-validation, explainable ML
- **Interactive analytics:** Real-time Streamlit UI for clinical decision-makers
- **End-to-end design:** Version control, documentation, orchestration, and CI/CD

---

### 3.7 Real-World Impact

By using **MIMIC-III data and open-source tech**, this pipeline demonstrates senior-level skills in analytics engineering, clinical data science, and product delivery—while avoiding PHI/HIPAA risk.

---

## References

- [MIMIC-III on BigQuery](https://console.cloud.google.com/marketplace/product/bigquery-public-datasets/mimic-iii-clinical)
- [dbt Documentation](https://docs.getdbt.com/)
- [Streamlit Docs](https://docs.streamlit.io/)
- [scikit-learn](https://scikit-learn.org/)
- [XGBoost](https://xgboost.readthedocs.io/)
- [Apache Airflow](https://airflow.apache.org/)

---

