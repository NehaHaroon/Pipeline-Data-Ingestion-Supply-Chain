Data Engineering Project – Part 2 (22%)
You are required to implement a transformation and storage API based on data governance rules. The actual outputs required are all highlighted in yellow. And important info is highlighted in blue but please, read the whole document carefully.
Note that several KPIs in this assignment are related to metadata. So, maybe you should implement a separate class for metadata as well. However, the decision is yours.

DESCRIPTION OF TASKS
Note that you have now one more plane to consider for your design, i.e., the storage plane. Please note below.
1. Control Plane
Implement Policies for: 
•	Storage optimization 
•	Partitioning 
•	Schema evolution 
•	Transformation contracts 
2. Data Plane
This plan handles: 
•	Batch ingestion → Iceberg, 
•	Streaming ingestion → Iceberg, 
•	CDC → Iceberg, 
•	Transformations (Bronze → Silver → Gold) 
3. Observability Plane
Track: Ingestion KPIs, Storage KPIs, Transformation KPIs 
4. Storage Plane 
On this plane, design and manage Iceberg tables:
•	Snapshots
•	Partitioning
•	File layout 
•	Compaction
In the report, you can refer to these planes to describe your plan/strategy.
Task 5: Use case Selection (0.5%)
Select an analytical use case from the supply chain use cases, which utilizes both streaming data and batch data. You can also select individual ones but at least one use case should use both streaming and batch data. 
However, understand all the use cases because you will need to implement them later on. Understanding them right now will help you to design your pipelines in flexible ways to accommodate future loads.
Mention which use case you are selecting. Note that you can also create your own use case. For your selected use case, define the business problem you are solving. The solution will be achieved either through simple SQL queries (dimensional or operational), or a BI dashboard, or predictive model, or some AI model. 
Note that your selected use case will require transformation. You will first implement Bronze layer which will only store raw data in Iceberg. Then, Silver and Golden will implement transformed ones.

Task 6: Airflow Implementation (2.5%)
You will need Airflow now for the future tasks. Implement airflow and map your previous ingestion task and all future tasks as Airflow tasks. In the end, everything would be completely automated. 
In your report, show Airflow snapshots of ingestion, transformation, storage etc. Also define your strategy, e.g., Dockerized Airflow. On the whole, it is best that you dockerize the whole DE pipeline.

Task 7: Implement Lambda Architecture (4%)
You have ingested batch and streaming data in Part 1. Now, implement a lambda architecture, e.g., a separate pipeline for streaming data and a separate one for batch data. 
First show the design of lambda on paper (insert paper snapshot) and then tell how you implemented it in Python (e.g., tell the Classes you implemented for it, etc.) and other design decisions that are necessary for a big data architecture. 
Initially, the lambda will transform each data type (batch/streaming) separately and then will store them in Iceberg. Obviously, your ingestion will also become part of these pipelines: streaming data will be ingested and transformed in streaming pipeline and batch data will be ingested and transformed in streaming pipeline.
Remember that all storage will be done in Iceberg. If you plan to use different Iceberg instances for batch and streaming, you can put each in its respective pipeline of Lambda. Otherwise, you can implement a separate layer in Lambda for storing batch and streaming  data collectively in the same Iceberg table. The decision is entirely yours because it depends on the types of queries and their operational workload.

Task 8: Design and Implement Iceberg Storage (Bronze Layer) (7%)
First, consider the storage KPIs you need to show on your dashboard for the Bronze layer:
1. File-Level Metrics (Detect small file problem (critical in streaming))
•	file_count_per_partition, avg_file_size, small_file_ratio 
2. Snapshot Metrics (Shows versioning efficiency)
•	snapshot_count, snapshot_creation_rate, time_travel_latency 
3. Storage Efficiency Metrics
•	total_storage_size, data_vs_metadata_ratio, compression_ratio 
4. Compaction Metrics
•	compaction_lag, files_compacted_per_run, compaction_time
5. Partition Health
partition_skew, records_per_partition, hot_partition_detection

And here are some hints for implementation:
Define IcebergTable Entity
Attributes:
•	table_id 
•	dataset_id 
•	table_layer: 
o	BRONZE (raw ingested) 
o	SILVER (cleaned/transformed) 
o	GOLD (aggregated/business-ready) 
•	partition_spec: e.g., date(event_timestamp), hour(event_timestamp) 
•	file_format: parquet / avro / orc 
•	compaction_policy: size_threshold (e.g., 128MB), frequency (e.g., every 30 min) 
•	snapshot_retention: number_of_snapshots OR time-based 

Requirements
Based on above requirements, implement a storage strategy. You must store ALL supply chain data (batch + streaming) in Iceberg tables. Either in individual tables or collectively together in single table is your decision. You need to justify it in your report. On the whole, it is best that if you want to query batch and streaming data together, then store them data together. 
You can also store them separately earlier on during ingestion and transformation (staging area – tempory db), and then combine them later on. 
Now, implement the Iceberg Schema. Show snapshots of stored data. 
Provide the following insights: 
•	Is streaming causing too many small files? 
•	Is compaction keeping up? 
•	Are partitions balanced?
Task 9: Transformation Layer (Mandatory) (6%)
It is now time to add layers for transformation: Silver and Gold. Silver is transformation Phase 1 (more refined than bronze) and Gold is transformation Phase 2 (more refined than silver).
Now, consider also the KPIs required on the transformation dashboard:
•	Data Quality Metrics: records_cleaned, records_rejected, null_percentage_per_column schema_violation_count 
•	Transformation Efficiency: transformation_latency, records_transformed/sec, pipeline_stage_latency (Bronze → Silver → Gold) 
•	Deduplication Metrics: duplicate_records_detected, duplicate_removal_rate 
•	CDC Consistency Metrics: late_arriving_records, out_of_order_events, correction_updates_applied 
•	Business-Level Metrics (Gold Layer): aggregation_accuracy, metric_drift (day-to-day change)
Based on the above, for your use case, determine transformations for silver and for gold. I have given some hints below, but you should finalize your own transformations based on your use case. Write these down and justify them briefly. Refer to 27 transformations in Lecture 9. Note that you can add or remove from them as per your discussion with group partner.
•	Bronze Layer (Raw)
o	Direct ingestion from Part 1 
o	Minimal transformation 
o	Add EventEnvelope 
•	Silver Layer (Cleaned)
o	Transformations: Schema validation (contracts), Deduplication (CDC conflicts), Null handling, Type casting, Late data handling 
•	Gold Layer (Business)
o	Transformations: Aggregations (e.g., revenue per day), Window functions, Feature engineering (for ML) 

Now answer the following: 
•	Is data quality improving from Bronze → Gold? 
•	Are transformations introducing latency? 

Task 10: Unified Dashboard (2%)
Dashboarding Suggested Tools: Grafana, Apache Superset, Power BI
Ingestion (from Part 1)
•	throughput 
•	ingestion latency 
•	failure rate 
Storage 
•	file sizes 
•	snapshot count 
•	compaction lag 
Transformation 
•	data quality 
•	transformation latency 
•	deduplication 

Evaluation Criteria 
Area	What You’re Testing
Architecture	Correct Iceberg usage (batch + streaming unified)
Storage	Handling small files, partitions, snapshots
Transformations	Correct Bronze → Silver → Gold logic
Observability	Meaningful KPIs + insights (not just charts)
Analysis	Ability to explain bottlenecks

Here are some further points to consider about the transformation that you should keep in mind: 
•	What’s the cost and return on investment (ROI) of the transformation? 
•	What is the associated business value? 
•	Is the transformation as simple and self-isolated as possible? 
•	What business rules do the transformations support?


