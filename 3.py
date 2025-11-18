import pandas as pd
import numpy as np
import os
import re
from glob import glob
from datetime import datetime
from dateutil.relativedelta import relativedelta


# --- 第三步：合并考勤数据 ---

def parse_attendance_sheet(file_path):
    """
    解析单个考勤数据表 (宽表转长表)
    NEW: 遍历文件中的所有工作表 (e.g., 3月, 4月, ...)
    """

    all_melted_rows = []  # 存储此文件中所有工作表的记录

    try:
        # 1. 读取所有工作表
        all_sheets = pd.read_excel(file_path, header=None, sheet_name=None)
    except Exception as e:
        print(f"  错误: 打开文件 {file_path} 失败: {e}")
        return None

    # 2. 遍历该Excel文件中的每一个工作表
    for sheet_name, df_raw in all_sheets.items():
        if df_raw.empty:
            continue

        # print(f"    -> 正在扫描 Sheet: {sheet_name}") # (可选的调试信息)

        try:
            # 3. 从 *工作表名称* 提取月份
            month_match = re.search(r'(\d+)月', sheet_name)
            if not month_match:
                # print(f"    -> Sheet '{sheet_name}' 名称不匹配，跳过。") # (可选的调试信息)
                continue  # 跳过非月份的工作表 (例如 "封面" 或 "Sheet1")

            # 假设年份为 2025 (因为返工数据是 2025)
            year = 2025
            file_month = int(month_match.group(1))

            # 根据规则，N月的文件从 N/11 开始
            start_date = datetime(year, file_month, 11)

        except Exception as e:
            print(f"  错误: 处理工作表名 {sheet_name} 时出错: {e}")
            continue  # 跳到下一个工作表

        try:
            # 4. 查找表头行 (包含 '姓名' 和 '11' 的行)
            header_row_index = -1
            name_col_index = -1
            day_start_col_index = -1

            for i in range(min(10, len(df_raw))):
                row_str = ' '.join([str(x) for x in df_raw.iloc[i] if pd.notna(x)])
                if '姓名' in row_str and ('11' in row_str or '1' in row_str):
                    header_row_index = i
                    row_vals = df_raw.iloc[i].values
                    for j, val in enumerate(row_vals):
                        if pd.notna(val):
                            if '姓名' in str(val) and name_col_index == -1:
                                name_col_index = j
                            if str(val).strip().isdigit() and day_start_col_index == -1:
                                day_start_col_index = j
                    break

            if header_row_index == -1 or name_col_index == -1 or day_start_col_index == -1:
                # print(f"    -> Sheet '{sheet_name}' 未找到表头行，跳过。") # (可选的调试信息)
                continue

            # 5. 读取数据
            # 关键：指定 sheet_name 来读取正确的工作表
            df_data = pd.read_excel(file_path, header=header_row_index, sheet_name=sheet_name)

            day_columns = {}
            current_date = start_date

            for col in df_data.columns[day_start_col_index:]:
                try:
                    day_num_match = re.search(r'(\d+)', str(col))
                    if not day_num_match:
                        continue
                    day_num = int(day_num_match.group(1))

                    if day_num < current_date.day and day_num == 1:
                        current_date = current_date + relativedelta(months=1)

                    current_date = current_date.replace(day=day_num)
                    day_columns[col] = current_date.strftime('%Y-%m-%d')
                except Exception:
                    continue

            # 6. "Melt" (逆透视) 数据
            name_col_name = df_data.columns[name_col_index]

            for i, row in df_data.iterrows():
                worker_name = row[name_col_name]
                if pd.isna(worker_name) or '姓名' in str(worker_name):
                    continue

                for col_name, date_str in day_columns.items():
                    attendance_val = row[col_name]
                    if pd.notna(attendance_val):
                        all_melted_rows.append({
                            '姓名': str(worker_name).strip(),
                            '日期': date_str,
                            '出勤时间': str(attendance_val).strip()
                        })
        except Exception:
            continue  # 跳到下一个 sheet

    # 循环结束后
    if not all_melted_rows:
        return None

    return pd.DataFrame(all_melted_rows)


def merge_attendance_data():
    """
    主函数：读取第一步的结果，并并入考勤数据。
    """

    # 1. 读取第一步的结果
    base_file = "产量数据_工序_返工_合并_test.xlsx"

    if not os.path.exists(base_file):
        print(f"错误: 找不到基础文件 {base_file}")
        return

    print(f"正在读取 {base_file}...")
    try:
        df_main = pd.read_excel(base_file)
    except Exception as e:
        print(f"读取 {base_file} 出错: {e}")
        return

    print(f"成功读取主数据 {len(df_main)} 行")

    # 2. 读取所有考勤文件
    attendance_folder = "考勤"  # 文件夹名称

    attendance_files = glob(os.path.join(attendance_folder, "*.xlsx"))
    attendance_files.extend(glob(os.path.join(attendance_folder, "*.xls")))

    if not attendance_files:
        print(f"错误: 在 '{attendance_folder}' 文件夹中找不到任何考勤文件。")
        return

    print(f"找到 {len(attendance_files)} 个考勤文件，正在解析...")

    all_attendance_dfs = []
    for file in attendance_files:
        print(f"  正在处理: {os.path.basename(file)}")
        df_attendance_part = parse_attendance_sheet(file)
        if df_attendance_part is not None and not df_attendance_part.empty:
            all_attendance_dfs.append(df_attendance_part)
            print(f"    -> 成功解析 {len(df_attendance_part)} 条记录。")
        else:
            print(f"    -> 未解析到数据，跳过。")

    if not all_attendance_dfs:
        print("错误: 未能从任何考勤文件中解析出数据。")
        return

    df_attendance_total = pd.concat(all_attendance_dfs, ignore_index=True)
    # 去重，以防万一
    df_attendance_total = df_attendance_total.drop_duplicates(subset=['姓名', '日期'], keep='last')

    print(f"成功解析所有考勤文件，共 {len(df_attendance_total)} 条唯一的 (工人-日期) 考勤记录。")

    # 3. 准备合并键 (Keys)

    # --- 清洗主数据 (df_main) 的键 ---
    try:
        if '日期' in df_main.columns:
            df_main['merge_date'] = pd.to_datetime(df_main['日期']).dt.strftime('%Y-%m-%d')
        elif '生产时间' in df_main.columns:
            # 兼容 1.py 中可能存在的列名 (来自截图)
            df_main['merge_date'] = pd.to_datetime(df_main['生产时间']).dt.strftime('%Y-%m-%d')
        else:
            print("错误：主数据中找不到 '日期' 或 '生产时间' 列。")
            return
    except Exception as e:
        print(f"处理主数据日期时出错: {e}")
        return

    name_col = '姓名' if '姓名' in df_main.columns else '员工名称'
    if name_col not in df_main.columns:
        print(f"错误：主数据中找不到 '姓名' 或 '员工名称' 列。")
        return
    df_main['merge_name'] = df_main[name_col].astype(str).str.strip()

    # --- 准备考勤数据 (df_attendance_total) 的键 ---
    df_attendance_total['merge_date'] = df_attendance_total['日期'].astype(str)
    df_attendance_total['merge_name'] = df_attendance_total['姓名'].astype(str)

    # 4. 执行合并
    print("正在合并主数据和考勤数据 (使用 姓名+日期)...")
    df_final = pd.merge(
        df_main,
        df_attendance_total[['merge_name', 'merge_date', '出勤时间']],  # 只合并需要的列
        on=['merge_name', 'merge_date'],
        how='left'
    )

    # 5. 清理结果
    df_final.drop(columns=['merge_date', 'merge_name'], inplace=True, errors='ignore')

    print("合并完成。")

    # 6. 保存结果
    output_file = "产量数据_工序_返工_考勤_合并_test.xlsx"

    print(f"正在保存结果到 {output_file}...")
    try:
        df_final.to_excel(output_file, index=False, engine='openpyxl')
        print(f"成功保存！ {len(df_final)} 行。")

        # 显示统计
        attendance_matched_count = df_final['出勤时间'].notna().sum()
        print(f"\n--- 统计信息 ---")
        print(f"总行数: {len(df_final)}")
        print(
            f"匹配到出勤记录的行数: {attendance_matched_count} ({attendance_matched_count / len(df_final) * 100:.2f}%)")

    except Exception as e:
        print(f"保存文件时出错: {e}")


if __name__ == "__main__":
    print("--- 开始第三步：合并考勤数据 ---")
    merge_attendance_data()
    print("--- 第三步完成 ---")