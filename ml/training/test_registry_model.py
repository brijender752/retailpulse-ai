import os

import mlflow


mlflow.set_tracking_uri(
    os.getenv(
        "MLFLOW_TRACKING_URI",
        "http://mlflow:5000",
    )
)


MODEL_URI = (
    "models:/RetailPulseChurnModel@champion"
)


print(
    "Loading:",
    MODEL_URI
)


model = mlflow.sklearn.load_model(
    MODEL_URI
)


print(
    "SUCCESS"
)

print(
    "Model loaded from "
    "MLflow Registry."
)

print(
    type(model)
)