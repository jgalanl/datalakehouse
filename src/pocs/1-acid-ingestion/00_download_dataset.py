"""
POC 1: ACID Ingestion - Dataset Download Script
================================================
This script downloads the Amazon Sales dataset from Kaggle using kagglehub.

Dataset: https://www.kaggle.com/datasets/karkavelrajaj/amazon-sales-dataset

Prerequisites:
- kagglehub library installed (pip install kagglehub)
- Kaggle API credentials configured (~/.kaggle/kaggle.json)

Author: Data Lakehouse POC
Date: 2026-02-15
"""

import os
import sys
import shutil
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def download_dataset():
    """
    Download Amazon Sales dataset from Kaggle using kagglehub

    Returns:
        str: Path to the downloaded dataset

    Raises:
        ImportError: If kagglehub is not installed
        ValueError: If KAGGLE_API_TOKEN is not set
        Exception: If download fails
    """
    logger.info("Starting dataset download from Kaggle")

    # Check for Kaggle API token
    kaggle_token = os.getenv("KAGGLE_API_TOKEN")
    if not kaggle_token:
        logger.error("KAGGLE_API_TOKEN environment variable not found")
        logger.error("Please set the KAGGLE_API_TOKEN in your .env file")
        raise ValueError("KAGGLE_API_TOKEN environment variable is required")

    try:
        import kagglehub

        # Configure Kaggle authentication using token
        logger.info("Configuring Kaggle authentication using API token")
        os.environ["KAGGLE_API_TOKEN"] = kaggle_token

        # Download latest version
        logger.info("Downloading dataset...")
        path = kagglehub.dataset_download("karkavelrajaj/amazon-sales-dataset")

        logger.info(f"Dataset downloaded to: {path}")

        # List files in the downloaded dataset
        files = os.listdir(path)
        logger.info(f"Dataset contains {len(files)} file(s)")
        for file in files:
            file_path = os.path.join(path, file)
            file_size = os.path.getsize(file_path)
            logger.info(f"  - {file} ({file_size:,} bytes)")

        return path

    except ImportError:
        logger.error("kagglehub library not found")
        logger.error("Please install it using: pip install kagglehub")
        raise

    except Exception as e:
        logger.error(f"Failed to download dataset: {str(e)}")
        logger.error("Make sure you have:")
        logger.error("1. KAGGLE_API_TOKEN environment variable set")
        logger.error("2. Internet connection")
        logger.error("3. Accepted the dataset terms on Kaggle website")
        raise


def copy_to_data_folder(source_path):
    """
    Copy downloaded dataset to the local data folder

    Args:
        source_path: Path where kagglehub downloaded the dataset

    Returns:
        str: Path to the data folder

    Raises:
        Exception: If copy operation fails
    """
    logger.info("Copying dataset to local data folder")

    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "data")

    # Create data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)
    logger.info(f"Data directory: {data_dir}")

    # Copy CSV files
    copied_files = []
    for file in os.listdir(source_path):
        if file.endswith('.csv'):
            source_file = os.path.join(source_path, file)
            dest_file = os.path.join(data_dir, file)

            shutil.copyfile(source_file, dest_file)
            copied_files.append(file)
            logger.info(f"Copied: {file}")

    if copied_files:
        logger.info(f"Successfully copied {len(copied_files)} file(s)")
        return data_dir
    else:
        logger.warning("No CSV files found in the downloaded dataset")
        return None


def main():
    """
    Main execution function
    """
    logger.info("=" * 80)
    logger.info("POC 1: ACID INGESTION - DATASET DOWNLOAD")
    logger.info("Dataset: Amazon Sales Dataset")
    logger.info("Source: Kaggle (karkavelrajaj/amazon-sales-dataset)")
    logger.info("=" * 80)

    try:
        # Download dataset
        download_path = download_dataset()

        # Copy to local data folder
        data_folder = copy_to_data_folder(download_path)

        if data_folder:
            logger.info("=" * 80)
            logger.info("Dataset download completed successfully")
            logger.info(f"Dataset location: {data_folder}")
            logger.info("Next step: Run 01_ingest_data.py to load data into Iceberg table")
            logger.info("=" * 80)
            return 0
        else:
            logger.error("Failed to copy dataset files")
            return 1

    except Exception as e:
        logger.error("=" * 80)
        logger.error("Dataset download failed")
        logger.error(f"Error: {str(e)}")
        logger.error("=" * 80)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
