from pyspark.sql import SparkSession
from mssparkutils import mssparkutils

# Initialize Spark session
# spark = SparkSession.builder.appName("ListFiles").getOrCreate()

def identify_format(file_path):
    # Dummy implementation for identifying file format
    # Replace this with your actual logic
    if file_path.endswith('.json'):
        return 'JSON'
    elif file_path.endswith('.xml'):
        return 'XML'
    elif file_path.endswith('.csv'):
        return 'CSV'
    else:
        return 'Unknown'

def list_files_recursively(base_path):
    # Create a list to hold all file paths
    file_paths = []

    # Use mssparkutils to list files in the base path
    all_files = mssparkutils.fs.ls(base_path)

    # Iterate through the files and directories
    for item in all_files:
        # Check if the item is a directory
        if item.isDir:
            full_path = item.path
            print(f"Directory: {full_path}")
            file_format = identify_format(full_path)
            print(f"Format: {file_format}")

            # Recursively call the function for subdirectories
            sub_dir_paths = list_files_recursively(item.path)
            file_paths.extend(sub_dir_paths)

        else:
            # Append the full file path (including filename and extension)
            print(f"File: {item.path}")  # Print the file path for debugging
            file_paths.append(item.path)  # Use item.path to get the full path

    return file_paths

# Define your base path
base_path = "abfss://containert@stract.dfs.core.windows.net/"

# Get the list of file paths
result_file_paths = list_files_recursively(base_path)

# Display the results
print("All file paths:")
for path in result_file_paths:
    print(path)
