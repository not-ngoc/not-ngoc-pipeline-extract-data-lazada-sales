import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import hdbscan
import pandas as pd
from sklearn.preprocessing import StandardScaler

from db_connection.db_config import CLICKHOUSE_DATABASE
from db_connection.db_connection import get_db_connection, DB_CONNECTION_PARAMS
from utils.dataframe_utils import split_dataframe
from utils.db_utils import create_table_if_not_exists, insert_chunk_thread_safe


def check_cols(df, columns_need_check):
    for col in columns_need_check:
        df_filter = df[df[col] != '']
        # Group by 'Design No.' and collect unique values in each group
        grouped_data = df_filter.groupby('Design No.').agg({
            col: lambda x: list(set(x))
        }).reset_index()

        for _, row in grouped_data.iterrows():
            if len(row[col]) > 1:
                print(
                    f"Warning: Kiểm tra lại dữ liệu trong cột '{col}' cho Design No. '{row['Design No.']}' - Có nhiều giá trị: {row[col]}")
                return False

    return True


# tạo dict chứa "design no" và cate, color tương ứng
def create_dict_with_design_cat_color(df, lst_cols):
    dict_design = {}
    for _, row in df.iterrows():
        design_no = row['Design No.']
        if design_no not in dict_design and all(row[col] != '' for col in lst_cols):
            dict_design[design_no] = {
                col: row[col] for col in lst_cols
            }
    return dict_design


# fill dữ liệu cho các cột category, color vào các dòng missing
def fill_missing_data(df, design_data, columns_to_fill):
    # cột size cần hard code: là ký tự cuối cùng của cột sku code
    if 'Size' in df.columns:
        size_missing = df['Size'].astype(str).str.strip() == ''
        df.loc[size_missing, 'Size'] = df.loc[size_missing, 'SKU Code'].str.split('-').str[-1]
    if check_cols(df, columns_to_fill):
        # Lặp qua từng thiết kế và dữ liệu tương ứng
        for design_no, fill_values in design_data.items():
            for col in columns_to_fill:
                # Tạo điều kiện lọc: đúng thiết kế và cột đó đang bị thiếu (rỗng)
                is_target_row = (df['Design No.'] == design_no) & (df[col].astype(str).str.strip() == '')
                df.loc[is_target_row, col] = fill_values[col]
        return df


# fill sku code trống
def fill_sku_code(df_product, df_sale):
    # Find design numbers: (starting with #) in df_product
    mising_skus = df_product[(df_product['SKU Code'] == '')]
    duplicate_skus = df_product[df_product['SKU Code'].duplicated(keep=False)]
    print(mising_skus)
    design_nos = mising_skus['Design No.'].unique()

    for design_no in design_nos:
        sale_skus = df_sale[df_sale['SKU'].str.contains(design_no, na=False)]['SKU'].unique()
        if len(sale_skus) == 1:  # If there's exactly one SKU code in df_sale
            # Update SKU code in df_product
            mask = (df_product['Design No.'] == design_no) & (df_product['SKU Code'] == '')
            df_product.loc[mask, 'SKU Code'] = sale_skus[0]
        else:
            mask = (df_product['SKU Code'] == '') | (df_product['SKU Code'].isin(duplicate_skus['SKU Code']))
            df_product.loc[mask, 'SKU Code'] = (
                    df_product.loc[mask, 'Design No.'] + '-' +
                    df_product.loc[mask, 'Category'] + '-' +
                    df_product.loc[mask, 'Color'] + '-' +
                    df_product.loc[mask, 'Size']
            )

    return df_product


def normalize_data(df, valid_sizes):
    # Chuẩn hóa cột 'SKU Code'
    df['SKU Code'] = df['SKU Code'].str.strip().str.upper()
    # Chuẩn hoá cột 'Size':
    df['Size'] = df['Size'].str.strip().str.upper()
    df.loc[~df['Size'].isin(valid_sizes), 'Size'] = 'FREE'
    return df


def fill_currency(df):
    unique_currencies = df[df['currency'].str.strip() != '']['currency'].unique()
    if len(unique_currencies) == 1:
        df.loc[df['currency'].str.strip() == '', 'currency'] = unique_currencies[0]
    elif len(unique_currencies) > 1:
        print("Warning: Có nhiều hơn một loại tiền tệ trong cột 'currency'.")
        return df
    return df


def remove_order_id_duplicate(df):
    duplicates = df[df.duplicated(subset=['Order ID'], keep=False)]

    if not duplicates.empty:
        df_cleaned = df.sort_values('Date', ascending=False).drop_duplicates(
            subset=['Order ID'],
            keep='first'
        )
        rows_removed = len(df) - len(df_cleaned)
        if rows_removed > 0:
            print(f"Removed {rows_removed} duplicate Order ID entries, keeping the most recent dates")
        return df_cleaned
    return df


def split_address_to_field(df):
    new_table = df[['Type', 'Customer ID', 'Address']].copy()

    new_table['Country'] = new_table['Address'].str.split(',').str[-1].str.strip()
    new_table['State'] = new_table['Address'].str.split(',').str[-2].str.strip()
    new_table['City'] = new_table['Address'].apply(lambda x: ','.join(x.split(',')[:-2]).strip())

    new_table = new_table.drop_duplicates(subset='Customer ID', keep='first')

    return new_table


def assign_segment(r, f, m):
    if r == 5 and f == 5 and m == 5:
        return 'Champions'
    elif r >= 4 and f >= 4:
        return 'Loyal Customers'
    elif r == 5:
        return 'New Customers'
    elif f >= 3 and m >= 3:
        return 'Potential Loyalist'
    elif r >= 3 and f <= 2:
        return 'Need Attention'
    elif r <= 2 and f <= 2 and m <= 2:
        return 'Lost'
    else:
        return 'Others'


def cluster_rfm_by_hdbscan(df_fact, df_dim, start_date_str='2024-01-01'):
    df_fact['Date'] = pd.to_datetime(df_fact['Date'])
    today = pd.to_datetime(start_date_str)
    rfm = df_fact.groupby('Customer ID').agg({
        'Date': lambda x: (today - x.max()).days,  # Recency
        'Order ID': 'nunique',  # Frequency
        'Amount': 'sum'  # Monetary
    }).reset_index()

    rfm.columns = ['Customer ID', 'Recency', 'Frequency', 'Monetary']
    features = ['Recency', 'Frequency', 'Monetary']
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(rfm[features])

    # HDBSCAN clustering
    clusterer = hdbscan.HDBSCAN(min_cluster_size=10)
    rfm['Cluster'] = clusterer.fit_predict(X_scaled)

    # Trung bình RFM theo cụm
    cluster_profile = rfm.groupby('Cluster')[features].mean()

    # Trung bình toàn bộ để làm chuẩn so sánh
    rfm_mean = rfm[features].mean()

    def label_cluster(row):
        r, f, m = row['Recency'], row['Frequency'], row['Monetary']
        label = []

        # So sánh với trung bình
        if r < rfm_mean['Recency']:
            label.append('Gần đây')  # R thấp tốt
        else:
            label.append('Lâu rồi')

        if f > rfm_mean['Frequency']:
            label.append('Thường xuyên')  # F cao tốt
        else:
            label.append('Ít mua')

        if m > rfm_mean['Monetary']:
            label.append('Chi nhiều')  # M cao tốt
        else:
            label.append('Chi ít')

        # Gom lại nhãn mô tả
        return ', '.join(label)

    # Gắn nhãn mô tả từng cụm
    cluster_profile['Segment'] = cluster_profile.apply(label_cluster, axis=1)

    rfm = rfm.merge(cluster_profile['Segment'], on='Cluster', how='left')
    # Định nghĩa từng nhóm khách hàn
    segment_map = {
        'Gần đây, Thường xuyên, Chi nhiều': 'VIP / Champions',
        'Gần đây, Thường xuyên, Chi ít': 'Loyal - Low Value',
        'Gần đây, Ít mua, Chi nhiều': 'Potential Big Spender',
        'Gần đây, Ít mua, Chi ít': 'New Customers',

        'Lâu rồi, Thường xuyên, Chi nhiều': 'At Risk',
        'Lâu rồi, Thường xuyên, Chi ít': 'Unstable Buyers',
        'Lâu rồi, Ít mua, Chi nhiều': 'Hibernating Big Spender',
        'Lâu rồi, Ít mua, Chi ít': 'Lost Customers'
    }

    rfm['SegmentName'] = rfm['Segment'].map(segment_map).fillna('Others')
    df = df_dim.merge(rfm[['Customer ID', 'SegmentName']], on='Customer ID', how='left')
    return df


def define_fns_analysis(df_sale, df_product):
    today = pd.to_datetime("2024-01-01")
    latest_sale = df_sale.groupby('SKU')['Date'].max().reset_index()
    latest_sale['days_since_last_sale'] = (today - latest_sale['Date']).dt.days
    df_product = df_product.rename(columns={
        'SKU Code': 'SKU',
    })

    product = df_product.merge(latest_sale[['SKU', 'days_since_last_sale']], on='SKU', how='left')
    product['days_since_last_sale'] = product['days_since_last_sale'].fillna(0)

    # Phân loại FSN
    def classify_fsn(days):
        if not days:
            return 'Non-moving'
        elif days <= 30:
            return 'Fast-moving'
        elif days <= 90:
            return 'Slow-moving'
        else:
            return 'Non-moving'

    product['FSN_Status'] = product['days_since_last_sale'].apply(classify_fsn)
    return product


def create_table_from_dict(mapping_dict):
    df = pd.DataFrame(list(mapping_dict.items()), columns=["Status", "Type"])
    return df

def create_table_for_xyz_analysis(df):
    df['Date'] = pd.to_datetime(df['Date'])

    df['Month'] = df['Date'].dt.to_period('M').astype(str)

    monthly_sales = df.groupby(['SKU', 'Month'])['Qty'].sum().reset_index(name='Sales')

    all_skus = df['SKU'].unique()
    all_months = pd.date_range(start=df['Date'].min(), end=df['Date'].max(), freq='MS').to_period('M').astype(str)
    full_index = pd.MultiIndex.from_product([all_skus, all_months], names=['SKU', 'Month'])

    # Reindex để có đủ các tổ hợp SKU - Tháng, fillna với 0
    monthly_sales = monthly_sales.set_index(['SKU', 'Month']).reindex(full_index, fill_value=0).reset_index()

    pivot = monthly_sales.pivot_table(index='SKU', columns='Month', values='Sales', fill_value=0)

    # Tính độ lệch chuẩn (std) và trung bình (mean)
    pivot['std'] = pivot.std(axis=1)
    pivot['mean'] = pivot.mean(axis=1)

    # Tính hệ số biến thiên (CV = std / mean)
    pivot['cv'] = pivot['std'] / pivot['mean']
    pivot['cv'] = pivot['cv'].fillna(0)
    # Phân loại X, Y, Z theo hệ số biến thiên phổ biến
    def classify_xyz(cv):
        if cv <= 0.5:
            return 'X'
        elif cv <= 1.0:
            return 'Y'
        else:
            return 'Z'

    pivot['XYZ'] = pivot['cv'].apply(classify_xyz)
    return pivot


def import_db(df, table_name):
    max_workers = 8  # Số luồng tối đa

    # ----- Bắt đầu thực thi import_db -----
    start_time = time.time()
    main_ch_client = None  # Client chính để tạo bảng
    try:
        main_ch_client = get_db_connection()
        create_table_if_not_exists(df, table_name, main_ch_client, CLICKHOUSE_DATABASE)

        if len(df) == 0:
            print(f"DataFrame for table '{table_name}' is empty. Nothing to insert.")
        else:
            chunk_size = max(1, len(df) // max_workers)

            chunks = split_dataframe(df, chunk_size)

            print(f"Splitting {len(df)} rows into {len(chunks)} chunks for table '{table_name}'.")

            total_inserted_rows = 0
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(insert_chunk_thread_safe, chunk, table_name, DB_CONNECTION_PARAMS) for chunk
                           in chunks if not chunk.empty]

                for future in as_completed(futures):
                    try:
                        inserted_count = future.result()
                        total_inserted_rows += inserted_count
                    except Exception as e_thread:
                        print(f"Error in thread execution: {e_thread}")
            print(f"Successfully inserted {total_inserted_rows} rows into table '{table_name}'.")


    except Exception as e_main:
        print(f"Major error processing DataFrame for table '{table_name}': {e_main}")
    finally:
        if main_ch_client:
            main_ch_client.disconnect()

    end_time = time.time()
    print(f"Total execution time for table '{table_name}': {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    input_file_path = 'Lazada Sale Report.xlsx'
    status_mapping = {
        "Shipped": "Pending",
        "Shipped - Delivered to Buyer": "Completed",
        "Cancelled": "Cancelled",
        "payment_failed": "Cancelled",
        "Pending - Waiting for Pick Up": "Pending",
        "Shipped - Picked Up": "Completed",
        "Shipped - Returned to Seller": "Cancelled",
        "Pending": "Pending",
        "Shipped - Lost in Transit": "Cancelled",
        "Shipped - Out for Delivery": "Pending",
        "Shipped - Rejected by Buyer": "Cancelled",
        "Shipped - Returning to Seller": "Cancelled"
    }
    if len(sys.argv) > 1:
        input_file_path = sys.argv[1]

    df_product = pd.read_excel(input_file_path, sheet_name='Product', engine='calamine')
    df_sale = pd.read_excel(input_file_path, sheet_name='Sales', engine='calamine')

    # fill na values
    df_product.fillna('', inplace=True)
    df_sale.fillna('', inplace=True)

    # tạo bảng customer
    df_customer = split_address_to_field(df_sale)
    df_customer = cluster_rfm_by_hdbscan(df_sale, df_customer, start_date_str='2024-01-01')
    import_db(df_customer, 'customer')

    # xử lý bảng product
    lst_cols = ['Color', 'Category']
    lst_size_valid = df_product['Size'].unique().tolist()
    dict_design = create_dict_with_design_cat_color(df_product, lst_cols)
    df_product = fill_missing_data(df_product, dict_design, lst_cols)
    df_product = fill_sku_code(df_product, df_sale)
    df_product = normalize_data(df_product, lst_size_valid)
    import_db(df_product, 'products')
    # # xử lý bảng sale
    df_sale = fill_currency(df_sale)
    df_sale = df_sale.drop(['Type', 'Address'], axis=1)
    df_sale = remove_order_id_duplicate(df_sale)
    import_db(df_sale, 'sales')
    # # xử lý bảng stock
    df_fns = define_fns_analysis(df_sale, df_product)
    import_db(df_fns, 'stock_fns')
    # # xử lý bảng xyz
    df_xyz = create_table_for_xyz_analysis(df_sale)
    import_db(df_xyz, 'sku_analysis_xyz')
    df_type_of_status = create_table_from_dict(status_mapping)
    import_db(df_type_of_status, 'dim_TypeOfStatus')
