Data Engineering Project – Part 3 (46%)
This phase extends the same use case implemented in Parts 1 and 2. Your project is expected to be now converted into a modern lakehouse + semantic analytics platform project. The intent is to follow standards of companies like Databricks and Snowflake.
All tasks to be done are highlighted in yellow.
Following is a summary of the tasks you must do:
•	Data Governance 
•	Data Contracts 
•	Workload-aware SQL analytics 
•	Semantic modelling using dbt
•	BI dashboards 
•	Analytical workload monitoring 
•	Live dashboard updates 

OVERALL GOAL: To design a complete modern analytics stack: Source Systems → CDC/Streaming → Iceberg → Bronze/Silver/Gold → dbt Semantic Layer → BI Dashboard → Workload Monitoring Dashboard

REQUIREMENTS:
This third part of the project can be based on implementing the following four planes (details to follow): 
•	Control Plane: Implements governance, workload policies, semantic consistency, query access control, data contracts, and SLA/SLO definitions. 
•	Data Plane: Implements ingestion, CDC, transformation, analytical querying, BI serving layer. 
•	Observability Plane: Implements telemetry, analytical workload KPIs, query performance, dashboard freshness, semantic layer monitoring. 
•	Semantic Plane: Implements dbt models, metric definitions, reusable business logic, semantic consistency across dashboards. 


TASK 1 —Data Governance Policy (6%)
For the given supply chain use case, define and describe two governance policies each for:
•	Data Access: Example: Finance users cannot access PII, Analysts can access aggregated Gold data only, Streaming operational data accessible only for 24 hours 
•	Retention: Example: Bronze retained for 30 days, Silver retained for 1 year, Gold retained indefinitely 
•	Cost Governance: Example: Expensive joins restricted during business hours, Queries exceeding threshold auto-terminated, Streaming compaction frequency optimization 
•	Workload Isolation: For instance, separate resources for: BI dashboards, ad hoc SQL, CDC ingestion 
•	Schema Evolution: Example: Additive schema changes allowed, Breaking changes require version increment, dbt tests mandatory before deployment 
•	Data Quality SLAs: Example: Null percentage < 2%, Dashboard freshness < 5 min, Query latency < 3 sec 
In designing the above, some things you can think of are business value, operational impact, and governance tradeoffs. 

TASK 2 — Advanced Data Contracts (4%)
Now implement and show enterprise-grade contracts. For all the datasets regarding your selected use case, implement: 
•	Structural Contract: schema, datatypes, required fields, primary keys, partitioning keys 
•	Semantic Contract: Example: revenue must be in PKR, inventory cannot be negative, shipment_status ∈ {Pending, InTransit, Delivered} 
•	Freshness Contract: Example: streaming events delayed > 2 min are quarantined 
•	Lineage Contract: Track: Source → Bronze → Silver → Gold → Dashboard

TASK 3 — Analytical SQL Workloads (10%)
You are now supposed to design SQL workloads, i.e., design SQL queries, for different types of analytics categories. 
Specifically, you need to create queries for four types of query workloads which are defined as follows and the exact tasks to be done as in yellow below.
•	Operational Analytics (both OLTP and OLAP possible)
o	Examples: Current inventory, Delayed shipments, Supplier bottlenecks 
o	Requirements: low latency and near real-time 
•	Strategic Analytics (OLAP, slim chance of OLTP since it is strategic)
o	Examples: monthly revenue trends, regional demand forecasting, supplier performance 
o	Requirements: large aggregations, historical analysis 
•	Executive BI(your charts will be converted to SQL queries)
o	Examples: KPI summaries, profitability dashboards, operational efficiency metrics 
o	Requirements: optimized star schema querying 
•	Ad Hoc Analytics (both OLTP and OLAP possible, mainly OLAP)
o	Examples: exploratory SQL, investigative joins, root cause analysis 
Tasks to be done:
•	For each of the above query workload types, create, write and execute two queries. 
•	Categorize the complexity of each query (low, medium, analytical, resource intensive) 
•	Identify the characteristics of each query (partitioning, joins, aggregations, time travel)
•	Tell your plan for optimizing each query: Demonstrate partitioning benefits, compaction impact, materialized views, Iceberg snapshot querying and caching strategies) 

TASK 4 — Design your BI dashboard (3%)
For your selected use case, design your BI dashboard on paper and paste it in the report. 
•	You must show how your dashboard is solving your business problem.
•	You must cater for all the BI dashboarding requirements (spacing between charts, interactivity, use of filters and scorecards etc.)
•	From this activity, you will have an idea of the SQL query behind each element of the dashboard (either direct query or indirect query) -  note this well because you will need it in the next part

TASK 5 — dbt Semantic Layer Implementation (7%)
In this section, as taught in the last lecture, you must implement a semantic layer behind the BI dashboard you have drawn in previous task. This layer models the semantic meaning of all the charts/tables/scorecards on your BI dashboard. You must use GPT to find out how this semantic meaning will be implemented through dbt tool.
The advantage of the semantic layer is that it can also serve as a cache for BI dashboard update. But the main advantage is that the meaning of your dashboard will be saved and can be recorded in data lineage. Guidance is as follows:
•	Implement dbt models - staging models, intermediate models, marts etc as feasible
•	Semantic Metrics - Examples: total_revenue, delivery_delay_rate, supplier_reliability_score, inventory_turnover, daily_order_growth 
•	dbt Tests: Examples: uniqueness, null checks, referential integrity, accepted values 
•	Semantic Documentation: You must generate: dbt docs, lineage graphs, metric catalogs (as applicable)

TASK 6 — Business Intelligence Dashboard (4%)
"You must now build the actual dashboard from your paper prototype". Build it and connect it to the semantic layer. 
Recommended Tools: Power BI, Superset, Grafana, Tableau 

TASK 7 — Analytical Workload Monitoring Dashboard (8%)
"Create dashboards now for the following workload observability". You understand the meaning of all the following – add and remove as per your liking but at least 2 should be there from each category.
•	Query Performance KPIs: avg_query_latency, p95_query_latency, query_failure_rate, concurrent_queries query_queue_time 
•	Resource KPIs: CPU utilization, memory utilization, disk I/O, cache hit ratio 
•	BI Usage KPIs: dashboard refresh frequency, dashboard load time, active users, most expensive dashboard 
•	SQL Workload KPIs: top expensive queries, partition pruning efficiency, bytes scanned/query, snapshot scan count 
•	Semantic Layer KPIs: dbt model runtime, failed dbt tests, stale semantic models, lineage depth 

TASK 8 — End-to-End Automation & Live Demonstration (4%)
Make a video that shows everything automated from ingestion to BI dashboard refresh along with all the other dashboards your have created (ingestion, storage, querying, semantic etc.) 
Video should not be between 5-8 minutes.



