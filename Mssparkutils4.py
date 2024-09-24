from mssparkutils import spark

def list_files_recursively(base_path):
    file_paths = []
    
    # List all files in the base path
    all_files = mssparkutils.fs.ls(base_path)

    # Iterate through the files and directories
    for item in all_files:
        # Check if the item is a directory
        if item.isDir:
            full_path = item.path
            print(f"Directory: {full_path}")

            # Recursively call the function for subdirectories
            sub_dir_paths = list_files_recursively(item.path)
            file_paths.extend(sub_dir_paths)

        else:
            # Append the full file path (including filename and extension)
            file_paths.append(item.path)  # Use item.path to get the full path

    return file_paths

# Define your base path
base_path = "abfss://containert@stract.dfs.core.windows.net/"  # Your actual base path

# Get the list of file paths
result_file_paths = list_files_recursively(base_path)

# Display the results
for path in result_file_paths:
    print(path)
