# spark_-


Explanation
Using Existing DataFrame: This code assumes that the DataFrame df already exists and contains the columns rtf_text and type.

RTF Processing Functions: The strip_rtf and remove_dates functions clean the RTF text and remove date formats.

User Defined Functions (UDFs): The functions are registered as UDFs so they can be applied to DataFrame columns.

Conditional Application: The when function is used to apply the strip_rtf_udf only when the type column is 0. If the type is 1, it keeps the original rtf_text.

Remove Dates: The remove_dates_udf is applied to the cleaned text to ensure that date formats are removed.

Expected Output
When you run this code, you should see the cleaned text for rows where type is 0, and the original text for rows where type is 1. If you have further questions or need additional modifications, feel free to ask!
