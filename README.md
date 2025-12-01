
# Distributed Key-Value Store

A distributed, fault-tolerant key-value store implemented using a controller–worker architecture. The system supports key-space partitioning, three-way replication, quorum-based writes, REST APIs for client interaction, and automatic recovery from worker failures. It is fully containerized using Docker and deployable on Kubernetes.

---

## Table of Contents
- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Key Features](#key-features)
- [Components](#components)
  - [Controller](#controller)
  - [Worker Nodes](#worker-nodes)
  - [Metadata and Storage Services](#metadata-and-storage-services)
- [API Documentation](#api-documentation)
  - [Controller REST API](#controller-rest-api)
  - [Worker REST API](#worker-rest-api)
- [Replication Model](#replication-model)
- [Failure Handling](#failure-handling)
- [Deployment](#deployment)
  - [Docker Compose](#docker-compose)
  - [Kubernetes](#kubernetes)
- [Project Structure](#project-structure)
- [Technologies Used](#technologies-used)
- [Future Improvements](#future-improvements)
- [License](#license)

---

## Overview

This project implements a distributed key-value store designed to achieve high availability, scalability, and fault tolerance.
The system consists of:
- A single **controller node**
- **Four worker nodes**
- Replication across **three workers** per key
- **REST API** for GET and PUT operations
- **gRPC-based** internal communication between nodes

The system ensures strong consistency through quorum-based writes and maintains availability through periodic worker heartbeats and automated re-replication.

---

## System Architecture

The architecture is divided into:
1.  **Controller (Control Plane)** – manages metadata, partitions keys, assigns replicas, and monitors workers.
2.  **Workers (Data Plane)** – store key-value data, handle REST operations, and manage replication.
3.  **Supporting Services** – MongoDB for storage, Redis and etcd for coordination, and Kubernetes for orchestration.

**Communication flow:**
1.  Clients query the controller to locate the appropriate worker.
2.  Clients perform GET/PUT on the designated worker.
3.  Replication is performed synchronously (1 replica) and asynchronously (1 replica).

---

## Key Features

- Key-space partitioning across 4 workers
- Three-way replication for all keys
- Quorum-based write acknowledgment
- Low-latency GET operations from primary worker
- Automatic detection of worker failure
- Automated re-replication to maintain replica count
- Deployment support via Docker Compose and Kubernetes
- REST API (client) + gRPC (internal)

---

## Components

### Controller
- Manages key-space partitioning
- Stores metadata in etcd or Redis
- Exposes REST endpoints for partition mapping and worker status
- Initiates re-replication on worker failure
- Receives periodic heartbeats from workers

### Worker Nodes
- Store data using local MongoDB instance
- Handle GET/PUT REST operations
- Communicate with other workers via gRPC for replication
- Participate in synchronous + asynchronous replication
- Maintain Merkle trees for validating replica consistency (optional)

### Metadata and Storage Services
- **MongoDB**: Stores key-value pairs for each worker
- **Redis**: Auxiliary controller/operator storage
- **etcd**: Cluster metadata and configuration state
- **Kubernetes Operator**: Automates worker management and recovery

---

## API Documentation

### Controller REST API

#### Get key partition
Returns the primary worker and replica nodes for a given key.

```http
GET /partition/<key>
````

**Response:**

```json
{
  "primary": "worker2",
  "replicas": ["worker3", "worker4"]
}
```

#### Get all workers

Returns a list of workers and their health status.

```http
GET /workers
```

#### Heartbeat (internal)

Used by workers to signal availability.

```http
POST /heartbeat
```

### Worker REST API

#### Get key

Retrieves the value for a specific key.

```http
GET /kv/<key>
```

#### Store key-value pair

Accepts a JSON payload to store and triggers replication.

```http
PUT /kv
```

**Body:**

```json
{
  "key": "example",
  "value": "123"
}
```

-----

## Replication Model

  - **Redundancy:** Each key is stored on three workers.
  - **PUT Operation (Quorum):**
    1.  Primary worker receives request.
    2.  Worker performs **synchronous** replication to one replica.
    3.  Worker performs **asynchronous** replication to the third replica.
    4.  Operation is successful after two replicas acknowledge (Primary + 1 Sync).
  - **GET Operation:**
      - Always served by the primary worker for low latency.

-----

## Failure Handling

  - Workers send periodic heartbeats to the controller.
  - **Detection:** If heartbeats are missing, the worker is marked as failed.
  - **Recovery:** The controller reassigns key partitions and triggers automated re-replication to restore the 3-replica count.
  - This ensures fault tolerance and continuous availability even if a node goes down.

-----

## Deployment

### Docker Compose

To run the entire system locally for development:

```bash
docker-compose up --build
```

### Kubernetes

To deploy the system on a Kubernetes cluster, apply the manifests in order:

```bash
kubectl apply -f k8s/namespace.yml
kubectl apply -f k8s/etcd/
kubectl apply -f k8s/redis/
kubectl apply -f k8s/mongo/
kubectl apply -f k8s/controller/
kubectl apply -f k8s/workers/
```

-----

## Project Structure

```text
Cloud-Distributed-DB-main/
│
├── controller/         # Controller logic and API
├── worker/             # Worker node logic and storage
├── operator/           # Kubernetes operator for automation
├── k8s/                # Kubernetes manifests
│   ├── controller/
│   ├── workers/
│   ├── mongo/
│   ├── redis/
│   ├── etcd/
│   └── namespace.yml
├── proto/              # gRPC protobuf definitions
├── scripts/            # Helper scripts
├── docker-compose.yml  # Local deployment config
└── README.md
```

-----

## Technologies Used

  - **Python**: Core implementation language
  - **FastAPI**: REST API interface
  - **gRPC**: Efficient internal node communication
  - **MongoDB**: Persistent data storage
  - **Redis**: Metadata buffer and state management
  - **etcd**: Distributed cluster coordination
  - **Docker**: Containerization
  - **Kubernetes**: Orchestration and scaling
  - **Merkle Trees**: Data integrity verification

-----

## Future Improvements

  - Consistent hashing for dynamic scaling
  - Horizontal auto-scaling of worker pods
  - Leader election for controller high availability
  - Multi-region replication
  - Load balancing for high-throughput workloads
  - Comprehensive monitoring using Prometheus and Grafana

-----

## License

This project is released under the MIT License.

