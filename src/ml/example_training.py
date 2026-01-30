"""
Data Lakehouse Platform - Example ML Training Job
Master's Degree Final Project

This script demonstrates ML experiment tracking with MLflow.
"""

from src.ml.mlflow_ops import MLflowManager, MLExperimentTracker
from src.core import SparkSessionBuilder
from pyspark.sql.functions import col, when
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main ML training pipeline."""
    
    logger.info("Starting ML training pipeline")
    
    # ========================================================================
    # 1. Initialize MLflow
    # ========================================================================
    mlflow_manager = MLflowManager(tracking_uri="http://mlflow:5000")
    tracker = MLExperimentTracker(
        mlflow_manager,
        experiment_name="customer_clustering"
    )
    
    logger.info(f"Experiment created: customer_clustering")
    
    # ========================================================================
    # 2. Load and Prepare Data
    # ========================================================================
    spark = SparkSessionBuilder.build(
        app_name="ml_training",
        master="spark://spark-master:7077",
        use_nessie=True
    )
    
    # Read analytics table
    df = spark.read.table("nessie.analytics.customer_metrics")
    
    # Prepare features
    feature_cols = ["total_orders", "total_spent", "avg_order_value"]
    
    features_df = df.select(
        col("customer_id"),
        col("total_orders").cast("double").alias("total_orders"),
        col("total_spent").cast("double").alias("total_spent"),
        col("avg_order_value").cast("double").alias("avg_order_value")
    ).fillna(0)
    
    # Convert to Pandas
    features_pd = features_df.select(feature_cols).toPandas()
    
    logger.info(f"Loaded {len(features_pd)} samples for training")
    
    # ========================================================================
    # 3. Feature Scaling
    # ========================================================================
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features_pd)
    
    logger.info("Features scaled")
    
    # ========================================================================
    # 4. Train Models with Hyperparameter Tuning
    # ========================================================================
    best_silhouette = -1
    best_params = {}
    
    for n_clusters in [3, 4, 5, 6]:
        # Start training run
        run_id = tracker.start_training(
            run_name=f"kmeans_k{n_clusters}",
            params={"n_clusters": n_clusters, "random_state": 42},
            tags={"model_type": "kmeans", "data": "customer_metrics"}
        )
        
        try:
            # Train model
            model = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            clusters = model.fit_predict(X_scaled)
            
            # Calculate metrics
            silhouette = silhouette_score(X_scaled, clusters)
            inertia = model.inertia_
            
            # Log metrics
            tracker.log_training_metrics(
                train_metrics={
                    "silhouette_score": silhouette,
                    "inertia": inertia,
                    "n_clusters": n_clusters
                },
                step=0
            )
            
            logger.info(f"k={n_clusters}: silhouette={silhouette:.3f}, inertia={inertia:.2f}")
            
            # Track best model
            if silhouette > best_silhouette:
                best_silhouette = silhouette
                best_params = {
                    "n_clusters": n_clusters,
                    "silhouette": silhouette,
                    "inertia": inertia
                }
            
            tracker.end_run("FINISHED")
            
        except Exception as e:
            logger.error(f"Training failed for k={n_clusters}: {e}")
            tracker.end_run("FAILED")
    
    # ========================================================================
    # 5. Train Best Model
    # ========================================================================
    logger.info(f"Best params: {best_params}")
    
    # Start final training run
    run_id = tracker.start_training(
        run_name="final_model",
        params={"n_clusters": best_params["n_clusters"], "random_state": 42},
        tags={"model_type": "kmeans", "status": "final", "data": "customer_metrics"}
    )
    
    try:
        model = KMeans(
            n_clusters=best_params["n_clusters"],
            random_state=42,
            n_init=20
        )
        model.fit(X_scaled)
        
        silhouette = silhouette_score(X_scaled, model.labels_)
        
        tracker.log_training_metrics(
            train_metrics={
                "final_silhouette": silhouette,
                "final_inertia": model.inertia_
            },
            step=0
        )
        
        logger.info(f"Final model trained: silhouette={silhouette:.3f}")
        
        # Save model
        import pickle
        import os
        
        model_dir = "/tmp/models"
        os.makedirs(model_dir, exist_ok=True)
        
        model_path = os.path.join(model_dir, "kmeans_model.pkl")
        with open(model_path, "wb") as f:
            pickle.dump(model, f)
        
        # Log model artifacts
        mlflow_manager.log_artifacts(model_dir, artifact_path="model")
        
        # Register model
        model_version = mlflow_manager.register_model(
            model_uri=f"runs://{run_id}/model",
            model_name="customer_clustering_kmeans",
            description=f"K-means clustering with k={best_params['n_clusters']}"
        )
        
        logger.info(f"Model registered: v{model_version}")
        
        # Transition to staging
        mlflow_manager.transition_model_stage(
            model_name="customer_clustering_kmeans",
            model_version=model_version,
            stage="Staging"
        )
        
        logger.info("Model transitioned to Staging")
        
        tracker.end_run("FINISHED")
        
    except Exception as e:
        logger.error(f"Final training failed: {e}")
        tracker.end_run("FAILED")
        raise
    
    logger.info("✓ ML training pipeline completed successfully")


if __name__ == "__main__":
    main()
