-- OpenMetadata PostgreSQL Initialization Script
-- Create the openmetadata_user role and database

-- Create superuser role if it doesn't exist
CREATE ROLE openmetadata_user WITH LOGIN PASSWORD 'openmetadata_password' CREATEDB;

-- Create the OpenMetadata database
CREATE DATABASE openmetadata_db OWNER openmetadata_user;

-- Grant all privileges
GRANT ALL PRIVILEGES ON DATABASE openmetadata_db TO openmetadata_user;

-- Connect to the database and set schema ownership
\c openmetadata_db postgres;

-- Ensure openmetadata_user can create schemas
ALTER DEFAULT PRIVILEGES GRANT ALL ON SCHEMAS TO openmetadata_user;
ALTER DEFAULT PRIVILEGES GRANT ALL ON TABLES TO openmetadata_user;
ALTER DEFAULT PRIVILEGES GRANT ALL ON SEQUENCES TO openmetadata_user;
