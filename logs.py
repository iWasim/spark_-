CREATE OR REPLACE TABLE your_new_table_name AS
SELECT 
    object_id, 
    view_name, 
    CASE 
        WHEN MAX(event_time) < DATE_SUB(current_date(), INTERVAL 1 YEAR) THEN '1y'
        WHEN MAX(event_time) < DATE_SUB(current_date(), INTERVAL 6 MONTH) THEN '6m'
    END AS flag
FROM 
    your_table_name
GROUP BY 
    object_id, 
    view_name
HAVING 
    MAX(event_time) < DATE_SUB(current_date(), INTERVAL 6 MONTH)
    OR MAX(event_time) < DATE_SUB(current_date(), INTERVAL 1 YEAR)

=========================================================================

CREATE OR REPLACE TABLE your_new_table_name AS
SELECT 
    object_id, 
    view_name, 
    CASE 
        WHEN MAX(event_time) < DATE_SUB(current_date(), 365) THEN '1y'
        WHEN MAX(event_time) < DATE_SUB(current_date(), 180) THEN '6m'
    END AS flag
FROM 
    your_table_name
GROUP BY 
    object_id, 
    view_name
HAVING 
    MAX(event_time) < DATE_SUB(current_date(), 180)
    OR MAX(event_time) < DATE_SUB(current_date(), 365)


==========================================================================
To optimize the query and reduce the amount of data processed, you can first filter the views that haven't been used in the last 6 months and 1 year into a temporary table or DataFrame. After that, you can add the flag column based on the filtered results. Here's how you can do this in Databricks SQL:

Step 1: Create a Temporary Table
First, create a temporary table that contains only the views not used in the last 6 months and 1 year.

sql


CREATE OR REPLACE TEMPORARY VIEW unused_views AS
SELECT 
    object_id, 
    view_name, 
    MAX(event_time) AS last_used_time
FROM 
    your_table_name
GROUP BY 
    object_id, 
    view_name
HAVING 
    MAX(event_time) < DATE_SUB(current_date(), 180)  -- Not used in the last 6 months
    OR MAX(event_time) < DATE_SUB(current_date(), 365)  -- Not used in the last 1 year
Step 2: Add the Flag Column
Now, you can create a new table or DataFrame from this temporary view and add the flag column.

sql


CREATE OR REPLACE TABLE your_new_table_name AS
SELECT 
    object_id, 
    view_name, 
    CASE 
        WHEN last_used_time < DATE_SUB(current_date(), 365) THEN '1y'
        WHEN last_used_time < DATE_SUB(current_date(), 180) THEN '6m'
    END AS flag
FROM 
    unused_views
Explanation:
Temporary View:

The first query creates a temporary view (unused_views) that filters out the views not used in the last 6 months or 1 year. This reduces the dataset size for the next operation.
Flag Column:

The second query selects from this temporary view and adds a flag column based on the last used time.
Step 3: Scheduling the Notebook
You can then follow the previous instructions to schedule this notebook to run every month, ensuring your results stay updated.

Additional Notes:
Using a temporary view helps reduce the amount of data processed in subsequent queries, improving performance.
Make sure to replace your_table_name and your_new_table_name with the appropriate names for your use case.


==========================================================================================================================

    from pyspark.sql import functions as F

# Get the current date
current_date = F.current_date()

# Calculate last used time and flag
unused_objects_df = df.groupBy("object_id").agg(
    F.max("event_time").alias("last_used_time")
).filter(
    (F.max("event_time") < F.date_sub(current_date, 180)) | 
    (F.max("event_time") < F.date_sub(current_date, 365))
)

# Add the flag column
unused_objects_with_flags_df = unused_objects_df.withColumn(
    "unused_interval",
    F.when(F.col("last_used_time") < F.date_sub(current_date, 365), "1y")
     .when(F.col("last_used_time") < F.date_sub(current_date, 180), "6m")
)

# Show the results
unused_objects_with_flags_df.show()









=========================================================================================

from pyspark.sql import functions as F

# Load and cache the DataFrame
df = spark.table("your_table_name").cache()

# Filter and select necessary columns early
df_filtered = df.select("object_id", "event_time").filter(df.event_time.isNotNull())

# Group and aggregate
unused_objects_df = df_filtered.groupBy("object_id").agg(
    F.max("event_time").alias("last_used_time")
).filter(
    (F.max("event_time") < F.date_sub(F.current_date(), 180)) | 
    (F.max("event_time") < F.date_sub(F.current_date(), 365))
)

# Add the flag column
unused_objects_with_flags_df = unused_objects_df.withColumn(
    "unused_interval",
    F.when(F.col("last_used_time") < F.date_sub(F.current_date(), 365), "1y")
     .when(F.col("last_used_time") < F.date_sub(F.current_date(), 180), "6m")
)

# Write to a new table
unused_objects_with_flags_df.write.mode("overwrite").saveAsTable("your_new_table_name")
