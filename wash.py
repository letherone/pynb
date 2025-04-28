# -*- coding: utf-8 -*-
"""
bookstats_2024Q4.py
計算 2024Q4 各書籍排名統計，並合併書籍明細，輸出最終 CSV。
"""

import pandas as pd
import numpy as np
from scipy.stats import skew
from datetime import datetime

# 檔案路徑
RANKING_FILE = '20241001_20241231_detail_ranking.csv'
DETAIL_FILE  = 'bookdetail.csv'
OUTPUT_FILE  = 'bookstats_2024Q4.csv'

# 期間設定，用於計算上榜覆蓋率
START_DATE = pd.to_datetime('2024-10-01')
END_DATE   = pd.to_datetime('2024-12-31')
TOTAL_DAYS = (END_DATE - START_DATE).days + 1  # 含首尾天數

# 1. 載入資料
df_rank = pd.read_csv(
    RANKING_FILE,
    parse_dates=['date', 'publishing_date'],
    dtype={'isbn': str}
)
df_rank = df_rank[
    (df_rank['date'] >= START_DATE) &
    (df_rank['date'] <= END_DATE)
].copy()

df_detail = pd.read_csv(
    DETAIL_FILE,
    parse_dates=['publishing_date'],
    dtype={'isbn': str}
)

# 2. 合併書籍明細（Step 6）
columns_needed = [
    'bookid', 'title', 'isbn', 'author', 'publisher',
    'publishing_date', 'fixed_price', 'category',
    'original_title', 'language', 'n_pages', 'url'
]
df_merged = pd.merge(
    df_rank,
    df_detail[columns_needed].drop_duplicates(subset='bookid'),
    on='bookid',
    how='left'
)

# 3. 計算統計指標（Step 7）
def compute_stats(group):
    # 上榜日期序列
    dates = group['date'].dropna().sort_values()
    if not dates.empty:
        first_seen = dates.min().date()
        last_seen  = dates.max().date()
        # 最長連續上榜天數
        diffs = dates.diff().dt.days.fillna(1)
        streak = diffs.groupby((diffs != 1).cumsum()).count().max()
        coverage = dates.dt.date.nunique() / TOTAL_DAYS * 100
    else:
        first_seen = last_seen = np.nan
        streak = coverage = np.nan

    # 名次序列
    rankings = group['rank_number'].dropna().astype(float)
    if not rankings.empty:
        best   = rankings.min()
        worst  = rankings.max()
        mean   = rankings.mean()
        median = rankings.median()
        mode_v = rankings.mode().iloc[0] if not rankings.mode().empty else np.nan
        std    = rankings.std()
        count  = rankings.count()
        cv     = std / mean if mean != 0 else np.nan
        skewn  = skew(rankings) if len(rankings) > 1 else np.nan
        # IQR
        q75, q25 = np.percentile(rankings, [75, 25])
        iqr    = q75 - q25
    else:
        best = worst = mean = median = mode_v = std = count = cv = skewn = iqr = np.nan

    return pd.Series({
        'Best_Rank':      best,
        'Worst_Rank':     worst,
        'Mean_Rank':      mean,
        'Median_Rank':    median,
        'Mode_Rank':      mode_v,
        'Std_Dev':        std,
        'Count':          count,
        'Streak':         streak,
        'First_Seen':     first_seen,
        'Last_Seen':      last_seen,
        'Coverage_Rate':  coverage,
        'CV':             cv,
        'Skewness':       skewn,
        'IQR':            iqr
    })

stats = df_merged.groupby('bookid').apply(compute_stats).reset_index()

# 4. 計算 Mean_Rank 的 Z-score（Step 8）
mean_of_means = stats['Mean_Rank'].mean()
std_of_means  = stats['Mean_Rank'].std(ddof=0)
stats['Z_score'] = (stats['Mean_Rank'] - mean_of_means) / std_of_means

# 5. 最終合併明細並輸出（Step 9 & 10）
result = pd.merge(
    stats,
    df_detail[columns_needed].drop_duplicates(subset='bookid'),
    on='bookid',
    how='left'
)

# 6. 儲存 CSV

timestamp   = datetime.now().strftime('%Y%m%d_%H%M%S')
OUTPUT_FILE = f'bookstats_2024Q4_{timestamp}.csv'
result.to_csv(OUTPUT_FILE, index=False)
print(f'已將結果儲存至 {OUTPUT_FILE}')

# 7. （選擇性）顯示前 10 列供檢查
print(result.head(10))
