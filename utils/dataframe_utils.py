def split_dataframe(df, chunk_size):
    # Đảm bảo chunk_size ít nhất là 1
    num_chunks = max(1, len(df) // chunk_size) if chunk_size > 0 else 1
    if len(df) % chunk_size != 0 and chunk_size > 0 and len(df) > chunk_size:
        num_chunks += 1

    actual_chunk_size = len(df) // num_chunks
    if len(df) % num_chunks != 0:
        actual_chunk_size += 1

    return [df[i:i + actual_chunk_size].copy() for i in range(0, len(df), actual_chunk_size)]
