# B2B Workforce Prediction

This project uses historical data on the number of hours allocated to each district to predict future number of hours of work (SWT). The model training will happen monthly and results will be exported and consumed by a Looker Studio Dashboard.

## 🛠️ Data Structures

Results and intermediate data is stored in the B2B AI dataset on BigQuery `b2b_wf_prediction`.

### 📥 Data Source Inputs

- `bq_wf_historical`: historical records using DataHub sources for job assignments;
- `bq_wf_predictions`: generated table with the granularity and horizon forecast desired for the new predictions;

### 📤 Data Source Outputs

- `bq_wf_forecast`: table with the forecasted values after batch prediction.

## 🏗️ Workflows and Pipelines

In this project, two workflows are used:

- Workflow 1: Backfill Workflow, for collecting the historical data and load the first batch of information.
- Workflow 2: Monthly Workflow, for running monthly the data update and trigger model update.

These workflows trigger the following Pipelines on Vertex AI:

- Vertex AI AutoML Training Pipeline;
- Vertex AI Model Registry Update;
- Vertex AI Batch Prediction;

## 📊 Data Visualization

Results generated for the Prediction Model are displayed in a Looker Studio Dashboard reading from a display view in the BigQuery dataset named `vw_wf_display`.

![Looker Dashboard View](./img/wf_prediction_dashboard.png)

## 🗺️ Architecture

![Simplified Architecture](./img/wf_prediction_simplified_architecture.png)

## 💼 Technical Team

- Ritwick Dutta (ritwick.dutta@telus.com)
- Viviane Silva (viviane.silva@telus.com)
- Prabhsimran Singh (prabhsimran.singh@telus.com)
- Melissa Freitas (melissa.freitas@telus.com)
- Eliezer Bernart (eliezer.bernart@telus.com)