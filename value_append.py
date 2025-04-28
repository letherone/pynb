import pandas as pd
from datetime import datetime

# 1. 讀取檔案
stats_path  = "/Users/kim/Documents/GitHub/pynb/bookstats_2024Q4_20250426_143354.csv"
detail_path = "/Users/kim/Documents/GitHub/pynb/bookdetail.csv"

stats_df  = pd.read_csv(stats_path, dtype={'bookid': str})
detail_df = pd.read_csv(detail_path, dtype={'bookid': str})

# 2. 欲補值的欄位列表
fill_columns = [
    "isbn", "author", "publisher", "publishing_date",
    "fixed_price", "category", "language", "n_pages"
]

# 3. 找出在任一 fill_columns 為空的列，及其對應的唯一 bookid
null_mask = stats_df[fill_columns].isnull().any(axis=1)
null_bookids = stats_df.loc[null_mask, 'bookid'].unique()

# 4. 在 detail_df 中檢查這些 bookid 是否存在
detail_bookid_set = set(detail_df['bookid'])
found     = [bid for bid in null_bookids if bid in detail_bookid_set]
not_found = [bid for bid in null_bookids if bid not in detail_bookid_set]

print(f"需要補值的 bookid 總數: {len(null_bookids)}")
print(f"  在 detail.csv 中找到: {len(found)}")
print(f"  在 detail.csv 中未找到: {len(not_found)}")
if not_found:
    print("  未找到的 bookid 範例:", not_found[:10])

# 5. 以 bookid 為 key，左合併並回填
#merged = stats_df.merge(
    #detail_df[['bookid'] + fill_columns],
    #on='bookid',
    #how='left',
    #suffixes=('', '_detail')
#)

#for col in fill_columns:
#    merged[col] = merged[col].fillna(merged[f"{col}_detail"])
#    merged.drop(columns=[f"{col}_detail"], inplace=True)

# 6. 輸出回填後的 CSV
#ts = datetime.now().strftime('%Y%m%d_%H%M%S')
#output_path = stats_path.replace(
    #'.csv',
    #f'_null_edited_{ts}.csv'
#)
#merged.to_csv(output_path, index=False)
#print(f"已匯出回填後檔案：{output_path}")
