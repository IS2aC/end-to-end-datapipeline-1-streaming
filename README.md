# Real-Time Analytics Platform with Docker 📊
![image](https://github.com/user-attachments/assets/bba88205-0555-498c-93f7-58548f3cb1d2)

🚀 Overview
This project implements a real-time data processing and analytics architecture using a modern microservices stack powered by Docker. The goal is to collect, process, store, and visualize live data streams while enabling real-time monitoring through a web application.


📦 Tech Stack
<pre>
- Orchestration: Docker & Docker Compose
- Workflow Scheduler: Apache Airflow
- Data Ingestion: Python scripts (triggered by Airflow)
- Message Broker: Apache Kafka (Zookeeper, Schema Registry, Control Center)
- Stream Processing: Apache Spark (master + workers)
- Database: Apache Cassandra
- External API: Data source via HTTP
- Real-Time Web App: Socket.IO + Web Dashboard (Vanilla Java Script)
</pre>


📐 Architecture Description
1. Ingestion
Airflow runs Python scripts that make HTTP requests to an external API to fetch live data.
The fetched data is then pushed to Apache Kafka for real-time streaming.

2. Streaming
Kafka acts as the data bus to stream the ingested data.
Apache Spark Streaming processes the Kafka stream, transforming and aggregating data as needed.

3. Storage
The processed data is stored in Apache Cassandra, a distributed NoSQL database optimized for high-speed writes and horizontal scalability.

4. Visualization
A real-time web table to monitor ingestion on our system.

🐳 Dockerized Services
All components are containerized using Docker:
- airflow
- postgres (Airflow metadata database)
- kafka, zookeeper, schema-registry, control-center
- spark-master, spark-worker-1/2/3
- cassandra
- socket.io-server
- web-frontend


🔄 >> Data Flow Summary >>>
<pre>
External API → [Airflow] → [Kafka] → [Spark Streaming] → [Cassandra] → [Socket.IO] → Real-Time Dashboard
</pre>
