import pandas as pd
import time

start_time = time.time()

def step(msg):
    print(f"[INFO] {msg} ...")
    print(f"        >> Elapsed time: {round(time.time() - start_time, 2)} seconds")

# 檔案路徑設定
rank_path = r"/Users/kim/Documents/GitHub/pynb/ranking.csv"
detail_path = r"/Users/kim/Documents/GitHub/pynb/bookdetail.csv"

# 參數設定
chunk_size = 10000
start_date = '2024-10-01'
end_date = '2024-12-31'

# 儲存中間結果
total_rows = 0
processed_chunks = []
error_chunks = []

# Step 1: Load book details
step("Step 1: Loading book detail")
df_detail = pd.read_csv(detail_path)

# Step 2: Chunk-wise pre-filter and full processing
step("Step 2: Processing ranking.csv in chunks with pre-filter")
for chunk_idx, chunk in enumerate(pd.read_csv(rank_path, chunksize=chunk_size), start=1):
    # 保留有 ranking 的列並重命名
    chunk = chunk.dropna(subset=['ranking']).rename(columns={'ranking': 'ranking_raw'})

    # 2.1 先取第一筆 ranking (拆第一個 ";" 前的內容) 並擷取日期
    first_segment = chunk['ranking_raw'].str.split(';', n=1).str[0]
    first_date = first_segment.str.extract(r'(?P<date>\d{4}-\d{2}-\d{2})')['date']
    chunk['date'] = pd.to_datetime(first_date, errors='coerce')

    # 2.2 只保留落在指定區間的列
    chunk = chunk[chunk['date'].between(start_date, end_date)]
    if chunk.empty:
        continue

    rows_in_chunk = len(chunk)
    print(f"[Progress] Chunk {chunk_idx}: {rows_in_chunk} preliminarily in range; max bookid {chunk['bookid'].max()}")

    # 2.3 對篩選後的資料做完整 explode + extract (含 chart 名稱)
    chunk = chunk.assign(ranking_split=chunk['ranking_raw'].str.rstrip(';').str.split(';')).explode('ranking_split')
    # 丟掉空字串的 ranking_split
    chunk = chunk[chunk['ranking_split'].notna() & (chunk['ranking_split'] != '')]

    extracted = chunk['ranking_split'].str.extract(
        r'(?P<chart>[^,]+),(?P<date>\d{4}-\d{2}-\d{2}),(?P<rank_number>\d+)'
    )
    # 指派欄位
    chunk['chart'] = extracted['chart']
    chunk['date'] = pd.to_datetime(extracted['date'], errors='coerce')
    chunk['rank_number'] = pd.to_numeric(extracted['rank_number'], errors='coerce')

    # 2.4 錯誤處理：標記並收集錯誤列
    errs = chunk[chunk[['chart', 'date', 'rank_number']].isnull().any(axis=1)].copy()
    if not errs.empty:
        errs['error_reason'] = errs.apply(
            lambda r: '原始資料為空值' if pd.isna(r['ranking_split']) else '原始資料格式不符',
            axis=1
        )
        error_chunks.append(errs)

    # 2.5 丟掉不合格的列
    chunk = chunk.dropna(subset=['chart', 'date', 'rank_number'])

    # 2.6 只保留必要欄位
    chunk = chunk[['bookid', 'chart', 'date', 'rank_number']]

    # 累計並印出總進度
    total_rows += len(chunk)
    print(f"[Progress] Total valid rows processed so far: {total_rows}")

    processed_chunks.append(chunk)

# Step 3: 合併所有處理後的 chunk
step("Step 3: Combining processed chunks")
if processed_chunks:
    df_rank = pd.concat(processed_chunks, ignore_index=True)
else:
    df_rank = pd.DataFrame(columns=['bookid', 'chart', 'date', 'rank_number'])

# Step 4: 儲存並顯示錯誤資料
if error_chunks:
    step("Step 4: Saving error rows")
    df_errors = pd.concat(error_chunks, ignore_index=True)
    error_file = f"errors_in_ranking_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    df_errors.to_csv(error_file, index=False)
    print(f"[WARNING] Found {len(df_errors)} error rows, saved to {error_file}")
    print(df_errors.head(3))

# Step 5: Merge with book details
step("Step 5: Merging with book detail columns")
cols = ['bookid', 'title', 'isbn', 'author', 'publisher', 'publishing_date',
        'fixed_price', 'category', 'original_title', 'language', 'n_pages', 'translator', 'url']
_df_merged = pd.merge(df_rank, df_detail[cols], on='bookid', how='left')

# Step 6: Export final CSV
step("Step 6: Exporting merged data to CSV")
output_file = f"{start_date.replace('-', '')}_{end_date.replace('-', '')}_detail_ranking.csv"
_df_merged.to_csv(output_file, index=False)
print(f"[INFO] Exported to {output_file}")

# 最後 summary
print(f"[Summary] Completed. Total valid rows processed: {total_rows}")
