import os
import pyarrow.parquet as pq
import pyarrow as pa

# Set your root folder path here
root_folder = "."

# Columns to keep (original names)
columns_to_keep = ['num', 'question', 'category', 'sub_category', 'trans_response']
# Rename map
rename_map = {'trans_response': 'response'}

# Traverse directories recursively
for subdir, _, files in os.walk(root_folder):
    for file in files:
        if file.endswith(".parquet"):
            file_path = os.path.join(subdir, file)
            try:
                # Read the Parquet file
                table = pq.read_table(file_path)
                schema_names = table.schema.names

                # Filter columns to keep if they exist
                existing_columns = [col for col in columns_to_keep if col in schema_names]
                filtered_table = table.select(existing_columns)

                # Rename column(s)
                new_columns = []
                for col in filtered_table.schema.names:
                    new_name = rename_map[col] if col in rename_map else col
                    new_columns.append(pa.field(new_name, filtered_table.schema.field(col).type))

                # Rebuild schema and table
                new_schema = pa.schema(new_columns)
                renamed_table = pa.Table.from_arrays(filtered_table.columns, schema=new_schema)

                # Overwrite the original file
                pq.write_table(renamed_table, file_path)
                print(f"Processed: {file_path}")
            except Exception as e:
                print(f"Failed to process {file_path}: {e}")
