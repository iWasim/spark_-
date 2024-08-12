
i already have a dataframe called df, no need to create it , replace the hardcoded rtf_text="""{data}""" with that dataframe





Got it! If you already have a DataFrame called df that contains an RTF text column, you can modify the existing code to use that DataFrame directly.

Example Code

Assuming your DataFrame df has a column named rtf_text and another column named type, here’s how you can process it:



from pyspark.sql import SparkSession

from pyspark.sql.functions import udf, when

from pyspark.sql.types import StringType

import re

# Create a Spark session

spark = SparkSession.builder.appName("RTF Processing").getOrCreate()

# Assuming df is your existing DataFrame with columns 'rtf_text' and 'type'

def strip_rtf(text):

  # Step 1: Remove font table and individual font definitions

  text = re.sub(r'\\fonttbl.*?;', '', text) # Remove font table

  text = re.sub(r'\\f\d+.*?;', '', text) # Remove individual font definitions

  # Step 2: Replace paragraph markers with newlines

  text = text.replace('\\par', '\n') # Convert \par to newline

  # Step 3: Remove other RTF control words

  text = re.sub(r'\\cf\d+', '', text) # Remove color formatting

  text = re.sub(r'\\[a-z]+\d*', '', text) # Remove other RTF control words

  text = re.sub(r'[{}]', '', text) # Remove braces

  text = re.sub(r'\*', '', text) # Remove asterisks

  text = re.sub(r';+', '', text) # Remove multiple semicolons

  # Step 4: Normalize whitespace

  text = re.sub(r'\s+', ' ', text) # Replace multiple spaces with a single space

  return text.strip() # Remove leading/trailing whitespace

def remove_dates(text):

  # Regex pattern to match dates in the format DD/MM/YYYY

  date_pattern = r'\b\d{1,2}/\d{1,2}/\d{2,4}\b'

  return re.sub(date_pattern, '', text) # Remove only dates

# Register the UDFs

strip_rtf_udf = udf(strip_rtf, StringType())

remove_dates_udf = udf(remove_dates, StringType())

# Apply the UDF conditionally based on the 'type' column

cleaned_df = df.withColumn(

  "cleaned_text",

  when(df.type == 0, strip_rtf_udf(df.rtf_text)).otherwise(df.rtf_text)

)

# Remove dates from the cleaned text

cleaned_df = cleaned_df.withColumn(

  "cleaned_text",

  remove_dates_udf(cleaned_df.cleaned_text)

)

# Show the output

cleaned_df.select("cleaned_text", "type").show(truncate=False)











Explanation

Using Existing DataFrame: This code assumes that the DataFrame df already exists and contains the columns rtf_text and type.

RTF Processing Functions: The strip_rtf and remove_dates functions clean the RTF text and remove date formats.

User Defined Functions (UDFs): The functions are registered as UDFs so they can be applied to DataFrame columns.

Conditional Application: The when function is used to apply the strip_rtf_udf only when the type column is 0. If the type is 1, it keeps the original rtf_text.

Remove Dates: The remove_dates_udf is applied to the cleaned text to ensure that date formats are removed.

Expected Output

When you run this code, you should see the cleaned text for rows where type is 0, and the original text for rows where type is 1. If you have further questions or need additional modifications, feel free to ask!






