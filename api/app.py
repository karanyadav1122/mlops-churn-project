import os
from typing import Literal
import logging
from fastapi import FastAPI
from pydantic import BaseModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from fastapi import HTTPException
import mlflow.spark

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
  handlers= [
    logging.FileHandler("logs/app.log"),
    logging.StreamHandler()
   ]
 
)

logger= logging.getLogger("churn_api")


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# MODEL_PATH = os.path.join(BASE_DIR, "models","churn_lr_pipeline")
MODEL_URI = "models:/churn_model@champion"
THRESHOLD = 0.4

app = FastAPI(title= "Churn prediction API")

class CustomerInput(BaseModel):
  gender: Literal["Male","Female"]
  location: str
  subscription_type: Literal["Basic","Standard","Premium"]
  tenure_months: int
  monthly_charges:float
  support_tickets: int
  late_payments: int
  tenure_bucket:Literal["new","mid","loyal"]
  charge_bucket: Literal["low","medium","high"]
  
spark = SparkSession.builder \
       .appName("ChurnPredictionAPI") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions","2") \
        .getOrCreate()
        
        
spark.sparkContext.setLogLevel("WARN")

model = mlflow.spark.load_model(MODEL_URI)

logger.info(f"Model loaded from: {MODEL_URI}")
logger.info(f"Using threshold: {THRESHOLD}")


@app.get("/")
def root():
  return {"message":"Churn Prediction API is running"}

@app.get("/health")
def health():
  logger.info("Health check called")
  return {"status":"ok"}

@app.post("/predict")
def predict(customer: CustomerInput):
  try:
    logger.info(
      "Received prediction request: "
      f"gender={customer.gender}, "
      f"location={customer.location}, "
      f"subscription_type={customer.subscription_type}, "
      f"monthly_charges={customer.monthly_charges}, "
      f"support_tickets={customer.support_tickets}, "
      f"late_payments={customer.late_payments}, "
      f"tenure months={customer.tenure_months}, "
      f"tenure_bucket={customer.tenure_bucket}, "
      f"charge_bucket={customer.charge_bucket}"
      
    )
    
    
    
    
    data = [{
    "gender":customer.gender,
    "location": customer.location,
    "subscription_type": customer.subscription_type,
    "monthly_charges": float(customer.monthly_charges),
    "tenure_months": int(customer.tenure_months),
    "support_tickets":int(customer.support_tickets),
    "late_payments": int(customer.late_payments),
    "tenure_bucket": customer.tenure_bucket,
    "charge_bucket": customer.charge_bucket
    
    }   
    ] 
  
  
    df = spark.createDataFrame(data)
  
    predictions = model.transform(df)
  
    result = predictions.withColumn(
    "churn_prob",
    vector_to_array(col("probability"))[1]
    ).withColumn(
    "prediction_custom",
    when(col("churn_prob") >= THRESHOLD,1).otherwise(0)
    ).select("churn_prob","prediction_custom").collect()[0]
  
    response = {
    "churn_probability": float(result["churn_prob"]),
    "prediction": int(result["prediction_custom"]),
    "threshold":THRESHOLD
    }
  
    logger.info(f"Prediction response: {response}")
    return response

  except Exception :
    logger.exception("Prediction failed")
    raise HTTPException(status_code = 500, detail= "Internal Sever Error")

           
  
  