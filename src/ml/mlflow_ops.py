"""
Data Lakehouse Platform - ML Operations Module
Master's Degree Final Project

This module provides ML operations capabilities including:
- MLflow experiment tracking
- Model registration and versioning
- Model deployment utilities
"""

import os
from typing import Dict, Any, Optional, List
import mlflow
from mlflow.tracking import MlflowClient
from mlflow.entities import Run
import logging

logger = logging.getLogger(__name__)


class MLflowManager:
    """Manager for MLflow operations."""
    
    def __init__(self, tracking_uri: Optional[str] = None):
        """
        Initialize MLflow manager.
        
        Args:
            tracking_uri: MLflow tracking server URI
        """
        self.tracking_uri = tracking_uri or os.getenv(
            "MLFLOW_TRACKING_URI", 
            "http://mlflow:5000"
        )
        
        mlflow.set_tracking_uri(self.tracking_uri)
        self.client = MlflowClient(tracking_uri=self.tracking_uri)
        
        logger.info(f"MLflow manager initialized with URI: {self.tracking_uri}")
    
    def create_experiment(
        self,
        experiment_name: str,
        artifact_location: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Create or get an MLflow experiment.
        
        Args:
            experiment_name: Name of the experiment
            artifact_location: S3 location for artifacts
            tags: Experiment tags
            
        Returns:
            Experiment ID
        """
        try:
            # Check if experiment exists
            experiment = self.client.get_experiment_by_name(experiment_name)
            
            if experiment:
                experiment_id = experiment.experiment_id
                logger.info(f"Using existing experiment: {experiment_name} (ID: {experiment_id})")
            else:
                # Create new experiment
                if artifact_location is None:
                    artifact_location = f"s3://mlflow-artifacts/{experiment_name}"
                
                experiment_id = self.client.create_experiment(
                    name=experiment_name,
                    artifact_location=artifact_location,
                    tags=tags
                )
                logger.info(f"Created experiment: {experiment_name} (ID: {experiment_id})")
            
            return experiment_id
            
        except Exception as e:
            logger.error(f"Failed to create experiment: {e}")
            raise
    
    def start_run(
        self,
        experiment_id: str,
        run_name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None
    ) -> Run:
        """
        Start an MLflow run.
        
        Args:
            experiment_id: Experiment ID
            run_name: Name of the run
            tags: Run tags
            params: Run parameters
            
        Returns:
            MLflow Run object
        """
        try:
            with mlflow.start_run(
                experiment_id=experiment_id,
                run_name=run_name,
                tags=tags
            ) as run:
                if params:
                    mlflow.log_params(params)
                
                logger.info(f"Started run: {run.info.run_id}")
                return run
                
        except Exception as e:
            logger.error(f"Failed to start run: {e}")
            raise
    
    def log_metrics(self, metrics: Dict[str, float], step: int = 0) -> bool:
        """
        Log metrics to MLflow.
        
        Args:
            metrics: Dictionary of metric names and values
            step: Step/epoch number
            
        Returns:
            True if successful
        """
        try:
            mlflow.log_metrics(metrics, step=step)
            logger.info(f"Logged {len(metrics)} metrics")
            return True
        except Exception as e:
            logger.error(f"Failed to log metrics: {e}")
            return False
    
    def log_artifacts(self, local_dir: str, artifact_path: Optional[str] = None) -> bool:
        """
        Log artifacts to MLflow.
        
        Args:
            local_dir: Local directory containing artifacts
            artifact_path: Remote artifact path
            
        Returns:
            True if successful
        """
        try:
            mlflow.log_artifacts(local_dir, artifact_path=artifact_path)
            logger.info(f"Logged artifacts from: {local_dir}")
            return True
        except Exception as e:
            logger.error(f"Failed to log artifacts: {e}")
            return False
    
    def register_model(
        self,
        model_uri: str,
        model_name: str,
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Register a model in MLflow Model Registry.
        
        Args:
            model_uri: Model URI (runs:// or models:/)
            model_name: Name for the model
            description: Model description
            tags: Model tags
            
        Returns:
            Model version
        """
        try:
            model_details = mlflow.register_model(
                model_uri=model_uri,
                name=model_name,
                description=description,
                tags=tags
            )
            
            logger.info(f"Registered model: {model_name} (Version: {model_details.version})")
            return model_details.version
            
        except Exception as e:
            logger.error(f"Failed to register model: {e}")
            raise
    
    def transition_model_stage(
        self,
        model_name: str,
        model_version: str,
        stage: str
    ) -> bool:
        """
        Transition model to a different stage (Staging, Production, Archived).
        
        Args:
            model_name: Name of the model
            model_version: Model version
            stage: Target stage (Staging, Production, Archived)
            
        Returns:
            True if successful
        """
        try:
            self.client.transition_model_version_stage(
                name=model_name,
                version=model_version,
                stage=stage
            )
            
            logger.info(f"Transitioned {model_name} v{model_version} to {stage}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to transition model: {e}")
            return False
    
    def load_model(self, model_uri: str):
        """
        Load a model from MLflow.
        
        Args:
            model_uri: Model URI (models://<model_name>/<version> or runs://<run_id>/<path>)
            
        Returns:
            Loaded model
        """
        try:
            model = mlflow.pyfunc.load_model(model_uri)
            logger.info(f"Loaded model: {model_uri}")
            return model
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise
    
    def get_best_run(
        self,
        experiment_id: str,
        metric_name: str,
        order: str = "desc"
    ) -> Optional[Run]:
        """
        Get the best run from an experiment based on a metric.
        
        Args:
            experiment_id: Experiment ID
            metric_name: Metric to optimize
            order: Sorting order (asc or desc)
            
        Returns:
            Best Run object
        """
        try:
            order_str = f"{metric_name} {order}".replace("desc", "DESC").replace("asc", "ASC")
            
            runs = self.client.search_runs(
                experiment_ids=[experiment_id],
                order_by=[order_str],
                max_results=1
            )
            
            if runs:
                return runs[0]
            
            logger.warning(f"No runs found for experiment: {experiment_id}")
            return None
            
        except Exception as e:
            logger.error(f"Failed to get best run: {e}")
            return None
    
    def get_experiment_runs(
        self,
        experiment_id: str,
        max_results: int = 100
    ) -> List[Run]:
        """
        Get all runs from an experiment.
        
        Args:
            experiment_id: Experiment ID
            max_results: Maximum number of runs to retrieve
            
        Returns:
            List of Run objects
        """
        try:
            runs = self.client.search_runs(
                experiment_ids=[experiment_id],
                max_results=max_results
            )
            
            logger.info(f"Retrieved {len(runs)} runs from experiment: {experiment_id}")
            return runs
            
        except Exception as e:
            logger.error(f"Failed to get experiment runs: {e}")
            return []


class MLExperimentTracker:
    """Helper class for tracking ML experiments."""
    
    def __init__(self, manager: MLflowManager, experiment_name: str):
        """
        Initialize experiment tracker.
        
        Args:
            manager: MLflowManager instance
            experiment_name: Name of the experiment
        """
        self.manager = manager
        self.experiment_id = manager.create_experiment(experiment_name)
        self.current_run = None
    
    def start_training(
        self,
        run_name: str,
        params: Dict[str, Any],
        tags: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Start a training run.
        
        Args:
            run_name: Name of the run
            params: Training parameters
            tags: Run tags
            
        Returns:
            Run ID
        """
        self.current_run = self.manager.start_run(
            experiment_id=self.experiment_id,
            run_name=run_name,
            params=params,
            tags=tags
        )
        
        return self.current_run.info.run_id
    
    def log_training_metrics(
        self,
        train_metrics: Dict[str, float],
        val_metrics: Optional[Dict[str, float]] = None,
        step: int = 0
    ) -> bool:
        """
        Log training metrics.
        
        Args:
            train_metrics: Training metrics
            val_metrics: Validation metrics (optional)
            step: Training step
            
        Returns:
            True if successful
        """
        all_metrics = {f"train_{k}": v for k, v in train_metrics.items()}
        
        if val_metrics:
            all_metrics.update({f"val_{k}": v for k, v in val_metrics.items()})
        
        return self.manager.log_metrics(all_metrics, step=step)
    
    def end_run(self, status: str = "FINISHED") -> bool:
        """
        End the current run.
        
        Args:
            status: Run status (FINISHED, FAILED)
            
        Returns:
            True if successful
        """
        try:
            mlflow.end_run(status=status)
            logger.info(f"Ended run with status: {status}")
            return True
        except Exception as e:
            logger.error(f"Failed to end run: {e}")
            return False
