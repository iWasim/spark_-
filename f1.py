from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import re

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RTF to Plain Text Conversion") \
    .getOrCreate()

def strip_rtf(text):
    text = re.sub(r'\\fonttbl.*?;', '', text)  # Remove font table
    text = re.sub(r'\\f\d+.*?;', '', text)     # Remove individual font definitions
    text = text.replace('\\par', '\n')         # Convert \par to newline
    text = re.sub(r'\\cf\d+', '', text)        # Remove color formatting
    text = re.sub(r'\\[a-z]+\d*', '', text)    # Remove other RTF control words
    text = re.sub(r'[{}]', '', text)            # Remove braces
    text = re.sub(r'\*', '', text)              # Remove asterisks
    text = re.sub(r';+', '', text)               # Remove multiple semicolons
    text = re.sub(r'\b[dD]\b', '', text)        # Remove standalone 'd' or 'D'
    text = re.sub(r'\s+', ' ', text)            # Replace multiple spaces with a single space
    return text.strip()                         # Remove leading/trailing whitespace

def remove_dates(text):
    date_pattern = r'\b\d{1,2}/\d{1,2}/\d{2,4}\b'
    return re.sub(date_pattern, '', text)      # Remove only dates

def parse_rtf(rtf_text):
    cleaned_text = strip_rtf(rtf_text)
    cleaned_text = remove_dates(cleaned_text)
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text)  # Normalize whitespace again
    cleaned_text = re.sub(r'\s*\.\s*', '. ', cleaned_text)  # Ensure space after periods
    cleaned_text = re.sub(r'\s*,\s*', ', ', cleaned_text)    # Ensure space after commas
    cleaned_text = re.sub(r',\s*$', '', cleaned_text)        # Remove trailing comma if it exists
    return cleaned_text.strip()                             # Remove leading/trailing whitespace

# Create a UDF for RTF parsing
parse_rtf_udf = udf(parse_rtf, StringType())

# Sample DataFrame
data = [
    (0, r"{\rtf1\ansi\ansicpg1252\deff0\deflang3081\fonttbl\f0\fswiss\fcharset0 Courier New;\par 15/10/2020\par Text 1\par}", 1),
    (1, r"{\rtf1\ansi\ansicpg1252\deff0\deflang3081\fonttbl\f0\fswiss\fcharset0 Courier New;\par Some text with flag 1\par}", 2),
    (0, r"{\rtf1\ansi\ansicpg1252\deff0\deflang3081\fonttbl\f0\fswiss\fcharset0 Courier New;\par 19/7/20 MIVI Dan Doe\par}", 3),
]

# Create DataFrame
df = spark.createDataFrame(data, ["flag", "rtf", "id"])

# Apply the UDF based on the flag value
result_df = df.withColumn("plain_text", 
                           when(col("flag") == 0, parse_rtf_udf(col("rtf"))).otherwise(col("rtf")))

# Show the resulting DataFrame
result_df.show(truncate=False)

# Stop Spark session
spark.stop()
