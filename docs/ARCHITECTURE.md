# Architecture & Design Decisions

## Technical Architecture

This document details the architectural decisions and technical rationale behind the data lakehouse platform.

## 1. Storage Layer: MinIO

### Decision Rationale

- **S3 Compatibility**: MinIO provides an S3-compatible interface, enabling seamless integration with AWS SDK
- **Self-Hosted**: Full control over data locality and compliance
- **Performance**: Multi-node support for horizontal scaling
- **Simplicity**: Single-binary deployment

### Trade-offs

- **vs. AWS S3**: MinIO offers on-premise control but requires infrastructure management
- **vs. HDFS**: S3 provides better compatibility with modern big data tools

### Configuration

- Path-style access enabled for better compatibility
- Virtual host-style addressing not required
- Configurable storage backend

## 2. Metadata Layer: Nessie + Iceberg

### Decision Rationale

#### Nessie (Versioning)

- **Git-like semantics**: Familiar branching and tagging paradigm
- **ACID transactions**: Atomic changes across multiple tables
- **Time travel**: Query historical data versions
- **Multi-user collaboration**: Isolated development branches

#### Iceberg (Table Format)

- **Open table format**: Not vendor-locked
- **ACID guarantees**: Full transaction support
- **Schema evolution**: Non-breaking schema updates
- **Hidden partitioning**: Partition pruning without exposing partition columns
- **Snapshot isolation**: Multiple readers/writers without conflicts

### Integration Pattern

```
Data Writers → Iceberg → Nessie (Versions) → MinIO (Storage)
                ↓
          REST Catalog
                ↓
Data Readers (Spark, Dremio)
```

### Trade-offs

- **vs. Delta Lake**: Delta is coupled to Parquet; Iceberg is format-agnostic
- **vs. Apache Hudi**: Iceberg has better analytical query support; Hudi better for streaming
- **Complexity**: Multiple systems to manage vs. single unified solution

## 3. Compute Layer: Apache Spark

### Architecture

#### Master-Worker Model

```
Spark Master (8080, 7077)
    ├── Worker 1 (8081)
    ├── Worker 2 (8082)
    └── Driver (4040)
```

#### Configuration Strategy

- **Standalone cluster mode**: Simpler than Kubernetes for PoC
- **Pre-configured Iceberg**: Jars included in custom Docker image
- **Nessie integration**: REST-based, no local state required

### Resource Allocation

- Master: 1 core (orchestration only)
- Workers: 2 cores each, 2GB memory (tunable)
- Driver: 1GB memory

### Trade-offs

- **vs. Kubernetes**: Standalone simpler for PoC; K8s better for production
- **vs. Flink**: Spark better batch/SQL support; Flink better streaming

## 4. Query Engine: Dremio

### Design Rationale

- **Semantic layer**: Abstract complex data models
- **Iceberg-native**: First-class support for Iceberg tables
- **Distributed query execution**: Fault-tolerant analytics
- **Multiple data sources**: Query across heterogeneous systems

### Catalog Integration

```
Dremio UI/SQL
    ↓
Iceberg REST Catalog
    ↓
MinIO (Storage)
```

### Query Optimization

- Predicate pushdown to Iceberg
- Partition pruning
- Columnar data skipping

### Trade-offs

- **vs. Apache Drill**: Dremio has better Iceberg integration
- **vs. Presto**: Dremio includes semantic layer out-of-box
- **vs. Tableau/Power BI**: Open source but less BI features

## 5. Governance Layer: OpenMetadata

### Decision Rationale

- **Metadata extraction**: Automated discovery from catalogs
- **Lineage tracking**: End-to-end data flow visualization
- **Collaboration**: Team-based data documentation
- **Quality monitoring**: Data quality assertions

### Architecture

```
OpenMetadata Server
    ├── MySQL (Metadata store)
    ├── Elasticsearch (Search)
    └── Connectors
        ├── Iceberg connector
        ├── Dremio connector
        └── Spark connector
```

### Trade-offs

- **vs. Collibra**: OpenMetadata open source; Collibra enterprise features
- **vs. Apache Atlas**: OpenMetadata better UI; Atlas more mature lineage

## 6. ML Operations Layer: MLflow

### Decision Rationale

- **Experiment tracking**: Reproducible ML research
- **Model registry**: Version control for models
- **Artifact storage**: S3 integration for large model files
- **Standardized format**: MLmodel standard for serving

### Pipeline Integration

```
ML Code
    ↓
MLflow (Tracking Server)
    ├── PostgreSQL (Runs metadata)
    └── MinIO S3 (Artifacts)
        ↓
    Model Registry
        ↓
    Serve/Deploy
```

### Trade-offs

- **vs. Kubeflow**: MLflow simpler; Kubeflow more production-ready
- **vs. Neptune**: MLflow open source; Neptune more collaboration features

## 7. Database Backend Strategy

### PostgreSQL Services

| Service | Purpose          | Database   | Port |
| ------- | ---------------- | ---------- | ---- |
| Nessie  | Catalog metadata | PostgreSQL | 5432 |
| Iceberg | REST catalog     | PostgreSQL | 5433 |
| Dremio  | Coordination     | PostgreSQL | 5434 |
| MLflow  | Tracking         | PostgreSQL | 5435 |

**Rationale**: PostgreSQL chosen for:

- Reliability and ACID guarantees
- JSONB support for complex metadata
- Wide adoption in open-source projects

### MySQL Service

| Service      | Purpose        | Database | Port |
| ------------ | -------------- | -------- | ---- |
| OpenMetadata | Metadata store | MySQL    | 3306 |

**Rationale**: OpenMetadata's native database choice

## 8. Network Architecture

### Docker Network

```
Bridge Network (172.20.0.0/16)
    ├── Storage: minio
    ├── Metadata: nessie-server, iceberg-rest
    ├── Compute: spark-master, spark-worker-1, spark-worker-2
    ├── Query: dremio
    ├── Governance: openmetadata
    └── ML: mlflow

Service Discovery: Internal DNS (e.g., minio:9000)
```

### Port Mapping Strategy

- **Public ports**: User-facing services (9001, 8080, 8585, 5000)
- **Internal ports**: Container-to-container (9000, 19120, 8181, 31010)
- **Avoidance**: No port conflicts between services

## 9. Data Flow Patterns

### Batch Ingestion Pattern

```
1. Source Data (CSV/Parquet)
2. Spark Ingestion Job
3. Iceberg Table (Nessie catalog)
4. Dremio Query Layer
5. Analytics/BI Tools
```

### ML Pipeline Pattern

```
1. Feature Engineering (Spark)
2. Model Training (MLflow experiment)
3. Model Registry (MLflow)
4. Batch Predictions (Spark)
5. Results Storage (Iceberg)
```

### Query Pattern

```
1. User SQL Query (Dremio Web/API)
2. Query Planning (Dremio)
3. Predicate Pushdown (Iceberg)
4. Partition Pruning (Nessie metadata)
5. S3 Read (MinIO)
6. Results Return
```

## 10. Security Architecture

### Development Mode

- All services accessible on localhost
- Default credentials (no enforcement)
- No TLS/encryption
- Internal Docker network for inter-service communication

### Production Considerations (Future)

1. **Authentication**
   - OAuth2 for user authentication
   - Service-to-service JWT tokens
   - LDAP/Active Directory integration

2. **Encryption**
   - TLS 1.3 for all external communications
   - Encryption at rest for S3 data
   - Field-level encryption for sensitive data

3. **Authorization**
   - Role-based access control (RBAC)
   - Attribute-based access control (ABAC)
   - Data masking policies

4. **Audit Logging**
   - Immutable audit trails
   - Data access logging
   - Configuration change tracking

## 11. Scalability Considerations

### Current Limits

- Single MinIO node (not distributed)
- 2 Spark workers
- Single OpenMetadata instance
- Single MLflow instance

### Scaling Strategies

1. **Storage**: MinIO distributed deployment with multiple nodes
2. **Compute**: Kubernetes cluster for dynamic Spark deployment
3. **Query**: Dremio executor node scaling
4. **Governance**: OpenMetadata high-availability setup
5. **ML**: MLflow server load balancing

## 12. Monitoring & Observability

### Current Monitoring

- Service health checks (Docker healthcheck)
- Web UI dashboards
- Log aggregation (Docker logs)

### Production Enhancements

- Prometheus metrics collection
- Grafana dashboards
- ELK stack for centralized logging
- Distributed tracing (Jaeger/Zipkin)
- Alert management (AlertManager)

## References

### Academic Papers

- Iceberg: Open Table Formats for Big Data
- Nessie: Branching and Versioning for Data Lakes
- Delta Lake: High-Performance ACID Table Storage

### Technical Specifications

- Apache Iceberg Spec: https://iceberg.apache.org/spec/
- S3 API Reference: https://docs.aws.amazon.com/s3/latest/API/
- Spark Documentation: https://spark.apache.org/docs/

---

**Last Updated**: January 2026
