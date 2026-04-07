Data Engineering Project – Part 1
You are required to implement an ingestion API based on data governance rules. First, go through the following information.
********************************************
BACKGROUND AND DESCRIPTION OF TASKS
********************************************
In your mind, think about 3 different planes for this project:
•	Control Plane: Here, define your policies and governance.
o	Examples for ingestion: schema policies, lineage policies. cost policies

•	Data Plane: Here, handle the actual operations.
o	Example for ingestion: batch loads, stream ingestion, CDC replication

•	Observability Plane: Here, you tracks the system state.
o	Example for ingestion: latency, data freshness, error metrics, lineage

Task 1: Define classes for the following entities/operations/actions:
•	Data Source – where the data comes from – e.g., MySQL, REST API (e.g., weather API), Kafka stream, CSV files
o	Possible attributes below:
	source_id 
	source_type (db, api, stream, file) 
	extraction_mode (Pull (API calls), Push (stream), Query-based)
	change_capture_mode 
•	FULL_SNAPSHOT: Load everything every time
•	INCREMENTAL: Only new/updated rows (e.g., based on timestamp)
•	CDC_LOG_BASED: Read DB logs (most efficient, e.g., Debezium)
•	CDC_TRIGGER_BASED: DB triggers capture changes
•	STREAM_EVENT: Real-time events (Kafka)
	expected_schema 
	ingestion_frequency: How often data arrives - hourly, daily, real-time 

•	Dataset – A dataset is NOT raw data — it’s a curated logical entity - what logical data we produce by ingestion, e.g., “Customer Transactions Dataset” and “Website Clickstream Dataset”. 
o	Possible attributes below:
	dataset_id 
	domain - Business area - marketing, finance, healthcare
	classification_level  - Data sensitivity - public / internal / confidential / restricted
	schema_version  - Version control of schema - v1, v2 (important for evolution)
	retention_policy - How long data is kept - 30 days, 1 year, forever

•	IngestionJob: How the data is brought in - defines execution of ingestion - Defines how data moves from Source → Dataset. 
o	Possible attributes below:
	job_id 
	dataset_id 
	source_id 
	execution_mode:
•	BATCH → Runs periodically (e.g., daily ETL) 
•	MICRO_BATCH → Small frequent batches (e.g., every 5 min) 
•	STREAMING → Real-time processing 
•	CDC_CONTINUOUS → Continuous change capture

•	EventEnvelope: Metadata wrapper for every record - Every ingested record must be wrapped with metadata 
o	Without this, the data lakes become messy and with this, you get lineage, replay, debugging - It ensures traceability, schema evolution, auditability and time consistency.
o	Possible attributes below:
	event_id
	event_timestamp
	source_timestamp
	schema_version
	ingestion_timestamp
	operation_type - What happened: INSERT, UPDATE, DELETE
	trace_id - Used to track flow across systems

Task 2: Define Data Contracts:
A data contract is like an agreement between a Data Producer (source system) and a Data Consumer (analytics, ML, dashboards). It ensures data will always come in a predictable structure and format.
Without contracts, the schema changes silently (schema drift) and pipelines can break unexpectedly. Dashboards can show wrong numbers and ML models can degrade. For example, yesterday: Income = INT and today: Income = STRING ("50k").
Every ingestion endpoint must enforce data contracts. A data contract must specify schema, field constraints, allowed values and semantic definitions.
You must include:
•	required_fields – attributes that must always be there
•	nullable_fields – attributes that can be null
•	type_constraints – definition of data types
•	unit_constraints – ensures measurement consistency (eg currency must be PKR)
•	enumerations - Defines allowed values for string variables

Potential violation policies are:
•	REJECT - Data is completely discarded and pipeline may fail
•	QUARANTINE - Bad data is stored separately and good data continues processing
•	AUTO_COERCE - System tries to fix the data errors automatically, e.g., “100” - 100

Task 3: Change Data Capture Governance
CDC ingestion must support three strategies.
Log-Based CDC
•	Reads database transaction logs.
•	Examples: WAL (Postgres), BINLOG (mysql), REDO (oracle)
•	Advantages: minimal source impact, exact change order 

Trigger-Based CDC
•	Database triggers capture change events.
•	Used when logs unavailable.

Timestamp-Based CDC
•	Extracts rows using change timestamp columns.
•	Example: last_updated > watermark

CDC Event Types
Events must contain operation semantics: 
•	INSERT: New row
•	UPDATE: Existing row changed
•	DELETE: Row removed
•	SNAPSHOT: Initial full load

Exactly-Once Processing: 
•	During failures, data may be reprocessed (so duplicates occur) and data may be skipped (so loss occurs). 
•	Exactly-once ensures each change is processed once and only once, even if failures occur. 
•	This ensure that changes appear once without duplication during failures.
•	Goal: No duplicates + No missing data

Implement CDC to support exactly-once using: 
•	Checkpointing: Save progress periodically
o	Last processed log position = 5000
o	After restart: Resume from 5001
•	Transactional sinks: 
o	Data is written atomically
o	Either fully written or not written at all 
o	This prevents partial writes
•	Offset management: 
o	Used in streaming systems (e.g., Kafka)
o	Each message has an offset, and the system tracks last committed offset

Task 4: Observability Policy
This concerns observability in data pipelines—i.e., making sure you can see, measure, and debug what your ingestion system is doing in real time. An observability policy defines: “What metrics every ingestion job must produce so we can monitor health, detect failures, and optimize performance.”

Every ingestion job must emit telemetry. Without telemetry, pipelines can fail silently, data delays can go unnoticed, and your debugging will become mere guesswork. The telemetry metrics include:
•	records_ingested
•	records_failed: Number of records that failed validation/processing
•	ingestion_latency: Time taken from data generation to ingestion
•	processing_lag: Delay between latest available data and processed data
•	throughput (records processed/sec)
Additional monitoring metrics (bonus):
•	file_count_per_partition
•	snapshot_count: Number of table versions/snapshots
•	compaction_lag: Delay in merging small files into optimized files

These metrics help detect ingestion bottlenecks and system health issues. 

********************************************
WHAT TO DO EXACTLY
********************************************
Phase 1: Implement Task 1 [2%]
•	Submit code with comments and brief justifications

Phase 2: Implement Task 2 [2%]
•	Submit code with comments and brief justifications

Phase 3: Implement a data generator for each data source that has been given to you (for all batch files and the streaming one). [2%] For this:
•	Implement Distribution Profiling: Capture the statistical "fingerprint" or distribution of each dataset.
•	Implement Synthetic Data Generation: Use the distribution learnt in previous step to model each source as a Data Producer which can continuously generate data. 
•	Upsampling: Use the fitted distributions to generate new, unique records that mimic the original metadata.
•	Prove that your generated data is close to the original one through TSNE plots or any other way, e.g., KL divergence [GPT for code and more information]


Phase 4: Implement ingestion (initial load) of each data source (3%): For this, you will need storage and other things but use any random storage you want. Storage dynamics will be asked in Part 2.

Phase 5: The "CDC Trigger": [2%] To test CDC, please note that your generators shouldn't just create new rows; it should also randomly select existing rows to Update or Delete. This creates the INSERT, UPDATE, and DELETE logs that CDC tools are designed to catch.
To test CDC scenarios (variable speeds), or the robustness of the CDC implementation, wrap your data generation in a "throttling" loop.
1. The Steady Stream (Baseline)
Send 10 records per second. This tests if the CDC mechanism (like a Debezium connector or a custom Python watcher) captures every change without lag.
2. The Burst Scenario (Stress Test)
Generate 5,000 records in a 1-second "burst," then pause. This tests the buffer capacity of the CDC system. Does it drop records, or does it process the backlog successfully?
Provide all outputs/snapshots of the steady and burst scenarios with your analysis. This will prove to me that your data sources are now realistic and representing the actual dynamics.

Phase 6: Now implement log-based, trigger-based and timestamp-based CDC for both steady and burst scenarios – use debezium or any other tool you want [5%] 

Phase 7: Output telemetry for each of the usecase above [1%]



