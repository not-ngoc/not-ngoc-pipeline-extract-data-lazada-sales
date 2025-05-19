import datetime

import pandas as pd
from clickhouse_driver import Client


def _map_dtype(dtype_str, column_name=""):  # Thêm column_name để xử lý đặc biệt nếu cần
    dtype_str_lower = str(dtype_str).lower()
    # print(f"DEBUG map_dtype: Input for column '{column_name}': dtype_str_lower = '{dtype_str_lower}'")

    if column_name in ['Customer ID', 'CustomerID']:  # Thêm các biến thể tên cột
        # print(f"DEBUG map_dtype: Matched '{column_name}', returning 'UInt32'")
        return 'UInt32'  # Hoặc UInt64 nếu ID rất lớn
    if column_name in ['Recency', 'Frequency', 'RFM_Score']:
        # print(f"DEBUG map_dtype: Matched '{column_name}', returning 'Int32'")
        return 'Int32'  # Int16 cũng có thể đủ, nhưng Int32 an toàn hơn
    if column_name in ['R_Score', 'F_Score', 'M_Score']:  # Nếu đã convert sang int ở Pandas
        # print(f"DEBUG map_dtype: Matched '{column_name}' (expected int), returning 'Int8'")
        return 'Int8'  # Vì chúng thường là 1-5

    # Xử lý chung dựa trên dtype
    if 'int' in dtype_str_lower:
        # Mặc định cho các cột int khác nếu không được xử lý đặc biệt ở trên
        # print("DEBUG map_dtype: Matched 'int' (general), returning 'Int32'")
        return 'Int32'  # Hoặc Int64 nếu cần phạm vi lớn hơn
    elif 'float' in dtype_str_lower:
        # print("DEBUG map_dtype: Matched 'float', returning 'Float64'")
        return 'Float64'
    elif 'bool' in dtype_str_lower:
        # print("DEBUG map_dtype: Matched 'bool', returning 'UInt8'")
        return 'UInt8'
    elif 'datetime' in dtype_str_lower:
        # print("DEBUG map_dtype: Matched 'datetime', returning 'DateTime'")
        return 'DateTime'
    elif 'category' in dtype_str_lower:
        # Nếu R_Score, F_Score, M_Score chưa được convert sang int ở Pandas
        # và bạn muốn chúng là String trong DB (không khuyến khích nếu chúng là số)
        print(
            f"DEBUG map_dtype: Matched 'category' for column '{column_name}', defaulting to 'String'. Consider converting to int in Pandas if it's numeric.")
        return 'String'
    elif 'object' in dtype_str_lower or 'string' in dtype_str_lower:
        # print(f"DEBUG map_dtype: Matched 'object/string' for column '{column_name}', returning 'String'")
        return 'String'
    else:
        # print(f"DEBUG map_dtype: No match for '{dtype_str_lower}' for column '{column_name}', defaulting to 'String'")
        return 'String'


def create_table_if_not_exists(df_schema, table_name_local, client_conn, database_local='default'):
    try:
        tables = client_conn.execute(f"SHOW TABLES FROM {database_local}")
        if (table_name_local,) in tables:
            print(f"Table '{table_name_local}' already exists in database '{database_local}'")
            return

        columns = []
        print(f"\n--- DEBUG: DataFrame dtypes for table '{table_name_local}' ---")  # DEBUG
        print(df_schema.info())  # DEBUG: In kiểu dữ liệu của DataFrame đầu vào cho create_table
        print("--- END DEBUG DataFrame dtypes ---\n")  # DEBUG

        for col_name in df_schema.columns:
            original_dtype_str = str(df_schema[col_name].dtype)
            # Truyền col_name vào map_dtype
            ch_type = _map_dtype(original_dtype_str, col_name)
            print(
                f"DEBUG: Column: '{col_name}', Pandas Dtype: '{original_dtype_str}', Mapped ClickHouse Type: '{ch_type}'")
            columns.append(f"`{col_name.replace('`', '``')}` {ch_type}")
        # ...
        columns_def = ",\n    ".join(columns)

        order_by_col = df_schema.columns[0].replace('`', '``')
        numeric_cols = [col for col in df_schema.columns if pd.api.types.is_numeric_dtype(df_schema[col])]
        if numeric_cols:
            order_by_col = numeric_cols[0].replace('`', '``')

        create_query = f"""
        CREATE TABLE {database_local}.`{table_name_local}` (
            {columns_def}
        )
        ENGINE = MergeTree()
        ORDER BY `{order_by_col}`
        """
        # DÒNG QUAN TRỌNG NHẤT ĐỂ DEBUG
        print(f"\n--- DEBUG: Generated CREATE TABLE query for '{table_name_local}' ---")
        print(create_query)
        print("--- END DEBUG CREATE TABLE query ---\n")

        client_conn.execute(create_query)
        print(f"Table '{table_name_local}' created successfully in database '{database_local}'")
    except Exception as e:
        print(f"Error creating table '{table_name_local}': {e}")
        raise


def insert_chunk_thread_safe(chunk_data, table_name_local, conn_params):
    thread_client = None
    # Chuyển đổi dữ liệu chunk thành list of tuples
    data_to_insert = []
    for row_tuple in chunk_data.itertuples(index=False, name=None):
        processed_row = []
        for val in row_tuple:
            if pd.isna(val):  # Kiểm tra NaN hoặc NaT
                processed_row.append(None)  # ClickHouse sẽ xử lý NULL
            elif isinstance(val, (pd.Timestamp, datetime.datetime)):
                # ClickHouse driver thường xử lý datetime objects trực tiếp
                # nhưng đảm bảo nó không phải NaT đã được xử lý ở trên
                processed_row.append(val)
            else:
                processed_row.append(val)
        data_to_insert.append(tuple(processed_row))

    if not data_to_insert:
        print(f"Chunk for table '{table_name_local}' is empty, skipping insert.")
        return 0

    try:
        thread_client = Client(**conn_params)
        thread_client.execute(
            f"INSERT INTO `{table_name_local}` VALUES",
            data_to_insert,
            types_check=True  # Giữ lại để an toàn
        )
        # print(f"Inserted {len(chunk_data)} rows into table '{table_name_local}' by thread.")
        return len(chunk_data)
    except Exception as e:
        print(f"Error inserting chunk into '{table_name_local}': {e}")
        return 0  # Trả về 0 nếu có lỗi
    finally:
        if thread_client:
            thread_client.disconnect()
