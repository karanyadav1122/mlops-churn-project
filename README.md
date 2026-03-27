#  Customer Churn Prediction MLOps System

##  Overview

This project is an end-to-end MLOps pipeline for predicting customer churn using PySpark, FastAPI, Docker, and MLflow.

It demonstrates how machine learning models are built, tracked, deployed, and served in a production-like environment.

---

##  Architecture

Batch Flow:
Data → Feature Engineering → Model Training → MLflow Tracking → Model Registry → FastAPI Serving → Docker

(Streaming integration with Kafka coming next)

---

##  Tech Stack

* PySpark (Feature Engineering + Model Training)
* MLflow (Experiment Tracking + Model Registry)
* FastAPI (Model Serving API)
* Docker (Containerization)
* Kafka (Streaming - upcoming)

---

##  Project Structure

```
mlops-churn-project/
├── api/               # FastAPI app
├── ml/                # Training code
├── streaming/         # Kafka producer/consumer
├── data/              # Bronze/Silver/Gold data
├── models/            # Saved models
├── mlruns/            # MLflow tracking
├── logs/              # Application logs
├── notebooks/         # Exploration
├── Dockerfile
├── requirements.txt
└── README.md
```

---

##  Model Details

* Algorithm: Logistic Regression
* Threshold-based classification
* Custom evaluation metrics:

  * Accuracy
  * Precision
  * Recall
  * F1 Score
  * ROC AUC

---

##  MLflow Tracking

* Experiments logged with parameters & metrics
* Model registered in MLflow Model Registry
* Versioning enabled

---

##  Run the Project

### 1. Create environment

```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Train model

```
python ml/train_model.py
```

### 3. Run API

```
uvicorn api.app:app --reload
```

### 4. Test API

Open:

```
http://127.0.0.1:8000/docs
```

---

##  Docker

```
docker build -t churn-api .
docker run -p 8000:8000 churn-api
```

---

## Future Improvements

* Kafka streaming pipeline
* Real-time prediction system
* CI/CD with GitHub Actions
* Kubernetes deployment
* Monitoring & alerting

---

##  Author

Karan Yadav



https://chatgpt.com/c/69bd62e5-cc7c-832b-84f2-f62b42e656b8


