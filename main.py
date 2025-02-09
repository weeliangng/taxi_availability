import ingestion
import transformation

ingestion.taxi_ingestion_pipeline("2024-12-30")
transformation.taxi_transformation_pipeline("2024-12-30")