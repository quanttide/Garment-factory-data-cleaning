import pandas as pd
import numpy as np
import os
import re
from glob import glob


def clean_style_number(style_num):
    """改进的款号清洗函数"""
    if pd.isna(style_num):
        return style_num
    style_str = str(style_num).strip()

    # 处理特殊案例
    special_cases = {
        r'76Z0001\d+': '76Z0001', r'31110011': '3111-0011', r'15CO156': '15C0156',
        r'15F0189B': '15F0189', r'12C0933加单': '12C0933', r'15C0196翻单': '15C0196',
        r'12C1536': '12C1450', r'67C0015': '12C1470', r'12C1572': '12C1450',
        r'16A0509.*': '16A0509', r'17A0025.*': '17A0025', r'18A0002.*': '18A0002',
    }
    for pattern, replacement in special_cases.items():
        if re.match(pattern, style_str):
            return replacement

    # 移除特定的后缀词
    cleaned = re.sub(r'(加单|翻单|男|女|加急|特急|返工|返修|补数).*$', '', style_str)
    cleaned = re.sub(r'-[A-Z]\d+$', '', cleaned)
    cleaned = re.sub(r'-\d{1,2}$', '', cleaned)

    # 处理数字+字母+数字的格式
    match = re.match(r'(\d+)([A-Z]+)(\d+)', cleaned)
    if match:
        num1, letters, num2 = match.groups()
        cleaned = f"{num1}{letters}{num2}"

    return cleaned


def parse_common_structure_fixed(df_raw, source_name, name_row_idx, process_row_idx, process_seq_row_idx,
                                 data_start_row):
    melted_rows = []

    try:
        # 员工姓名
        name_row = df_raw.iloc[name_row_idx].values if name_row_idx < len(df_raw) else []
        # 工序信息
        process_row = df_raw.iloc[process_row_idx].values if process_row_idx < len(df_raw) else []
        # 工序序号
        process_seq_row = df_raw.iloc[process_seq_row_idx].values if process_seq_row_idx < len(df_raw) else []

        employee_columns = {}
        for j in range(len(name_row)):
            if pd.notna(name_row[j]):
                cell_val = str(name_row[j]).strip()
                if (2 <= len(cell_val) <= 20 and
                        any('\u4e00' <= char <= '\u9fff' for char in cell_val) or
                        cell_val.isalpha() and 2 <= len(cell_val) <= 10):
                    employee_columns[j] = cell_val

        print(f"找到员工列: {len(employee_columns)} 个员工")

        # 工序名称及其范围
        process_ranges = []
        current_process = ""
        start_col = None

        for j in range(len(process_row)):
            cell_val = process_row[j] if pd.notna(process_row[j]) else ""
            cell_str = str(cell_val).strip()

            if cell_str:  # 有内容的单元格
                if current_process and current_process != cell_str:
                    # 遇到新的工序名称，保存前一个工序的范围
                    process_ranges.append((current_process, start_col, j - 1))
                    current_process = cell_str
                    start_col = j
                elif not current_process:
                    # 开始第一个工序
                    current_process = cell_str
                    start_col = j
            else:  # 空单元格
                if current_process:
                    # 继续当前工序的范围
                    continue

        # 处理最后一个工序
        if current_process and start_col is not None:
            process_ranges.append((current_process, start_col, len(process_row) - 1))

        # 识别工序序号及其范围
        process_seq_ranges = []
        current_process_seq = ""
        start_col_seq = None

        # 扫描工序序号行，识别每个工序序号的范围
        for j in range(len(process_seq_row)):
            cell_val = process_seq_row[j] if pd.notna(process_seq_row[j]) else ""
            cell_str = str(cell_val).strip()

            if cell_str:  # 有内容的单元格
                if current_process_seq and current_process_seq != cell_str:
                    # 遇到新的工序序号，保存前一个工序序号的范围
                    process_seq_ranges.append((current_process_seq, start_col_seq, j - 1))
                    current_process_seq = cell_str
                    start_col_seq = j
                elif not current_process_seq:
                    # 开始第一个工序序号
                    current_process_seq = cell_str
                    start_col_seq = j
            else:  # 空单元格
                if current_process_seq:
                    # 继续当前工序序号的范围
                    continue

        # 处理最后一个工序序号
        if current_process_seq and start_col_seq is not None:
            process_seq_ranges.append((current_process_seq, start_col_seq, len(process_seq_row) - 1))

        # 每个员工对应的工序名称和工序序号
        worker_process_info = {}
        worker_process_seq_info = {}

        for col_idx, employee_name in employee_columns.items():
            assigned_process = ""
            assigned_process_seq = ""

            # 查找这个员工列属于哪个工序名称的范围
            for process_name, start_col, end_col in process_ranges:
                if start_col <= col_idx <= end_col:
                    assigned_process = process_name
                    break

            # 查找这个员工列属于哪个工序序号的范围
            for process_seq, start_col_seq, end_col_seq in process_seq_ranges:
                if start_col_seq <= col_idx <= end_col_seq:
                    assigned_process_seq = process_seq
                    break

            worker_process_info[col_idx] = assigned_process
            worker_process_seq_info[col_idx] = assigned_process_seq

        assigned_count = sum(1 for process in worker_process_info.values() if process)
        assigned_seq_count = sum(1 for process_seq in worker_process_seq_info.values() if process_seq)
        print(f"工序分配: {assigned_count}/{len(employee_columns)} 个员工有工序名称")
        print(f"工序序号分配: {assigned_seq_count}/{len(employee_columns)} 个员工有工序序号")

        current_style = None

        for i in range(data_start_row, min(len(df_raw), data_start_row + 500)):
            row_data = df_raw.iloc[i]

            # 款号和日期
            style_val = row_data.iloc[0] if 0 < len(row_data) else None
            date_val = row_data.iloc[4] if 4 < len(row_data) else None

            # 更新当前款号
            if pd.notna(style_val):
                style_str = str(style_val).strip()
                if 'Total' not in style_str and '合计' not in style_str and style_str:
                    current_style = style_str

            # 处理有日期的行
            if pd.notna(date_val) and current_style:
                try:
                    date_obj = pd.to_datetime(str(date_val))

                    # 处理每个员工列的数据
                    for col_idx, employee_name in employee_columns.items():
                        if col_idx < len(row_data):
                            rework_val = row_data.iloc[col_idx]

                            if pd.notna(rework_val):
                                try:
                                    rework_count = float(rework_val)
                                    if rework_count > 0:
                                        process_name = worker_process_info.get(col_idx, "")
                                        process_seq = worker_process_seq_info.get(col_idx, "")

                                        record = {
                                            '款号': current_style,
                                            '日期': date_obj,
                                            '姓名': employee_name,
                                            '每日返工数量': rework_count,
                                            '返工率表格中的工序名称': process_name,
                                            '返工率表格中的工序序号': process_seq,  # 新增工序序号
                                            '组别': '',
                                            '生产工号': '',
                                            '来源': source_name
                                        }
                                        melted_rows.append(record)

                                except (ValueError, TypeError):
                                    continue

                except Exception as e:
                    continue

        records_with_process = sum(1 for record in melted_rows if record['返工率表格中的工序名称'])
        records_with_seq = sum(1 for record in melted_rows if record['返工率表格中的工序序号'])
        print(
            f"解析结果: {len(melted_rows)} 条记录，{records_with_process} 条有工序名称，{records_with_seq} 条有工序序号")
        return melted_rows

    except Exception as e:
        print(f"解析 {source_name} 文件时出错: {e}")
        import traceback
        traceback.print_exc()
        return melted_rows


# 不同表格使用不同解析方法，根据您提供的信息调整参数
def parse_chenyamei(df_raw):
    # 陈亚梅：名字在第3行，工序信息在第4行，工序序号在第5行
    return parse_common_structure_fixed(df_raw, '陈亚梅', 3, 4, 5, 9)


def parse_fanli(df_raw):
    # 范丽：名字在第7行，工序信息在第8行，工序序号在第5行
    return parse_common_structure_fixed(df_raw, '范丽', 7, 8, 5, 9)


def parse_fansihui(df_raw):
    # 范嗣惠：名字在第7行，工序信息在第5行，工序序号在第4行
    return parse_common_structure_fixed(df_raw, '范嗣惠', 7, 5, 4, 10)


def parse_zengfanli(df_raw):
    # 曾繁利：名字在第8行，工序信息在第3行，工序序号在第4行
    return parse_common_structure_fixed(df_raw, '曾繁利', 8, 3, 4, 9)


def parse_chendingfen(df_raw, group_name):
    # 陈定芬：名字在第8行，工序信息在第3行，工序序号在第4行
    records = parse_common_structure_fixed(df_raw, f'陈定芬{group_name}', 8, 3, 4, 9)
    for record in records:
        record['组别'] = group_name
        record['来源'] = f'陈定芬{group_name}'
    return records


def parse_lixiaoping(df_raw):
    # 李小萍：名字在第8行，工序信息在第4行，工序序号在第3行
    return parse_common_structure_fixed(df_raw, '李小萍', 8, 4, 3, 9)


def parse_zhangdali(df_raw):
    # 张大丽：名字在第6行，工序信息在第3行，工序序号在第4行
    return parse_common_structure_fixed(df_raw, '张大丽', 6, 3, 4, 9)


def parse_rework_sheet_customized(file_path):
    all_melted_rows = []

    try:
        all_sheets = pd.read_excel(file_path, header=None, sheet_name=None)
    except Exception as e:
        print(f"  错误: 打开文件 {file_path} 失败: {e}")
        return None

    for df_raw in all_sheets.values():
        if df_raw.empty:
            continue

        file_basename = os.path.basename(file_path)

        # 根据文件名选择对应的解析函数
        if '陈亚梅' in file_basename:
            records = parse_chenyamei(df_raw)
        elif '范丽' in file_basename:
            records = parse_fanli(df_raw)
        elif '范嗣惠' in file_basename:
            records = parse_fansihui(df_raw)
        elif '曾繁利' in file_basename:
            records = parse_zengfanli(df_raw)
        elif '陈定芬6组' in file_basename:
            records = parse_chendingfen(df_raw, '6组')
        elif '陈定芬7组' in file_basename:
            records = parse_chendingfen(df_raw, '7组')
        elif '李小萍' in file_basename:
            records = parse_lixiaoping(df_raw)
        elif '张大丽' in file_basename:
            records = parse_zhangdali(df_raw)
        else:
            print(f"未知文件类型: {file_basename}，使用默认解析")
            # 使用陈定芬的解析作为默认
            records = parse_chendingfen(df_raw, '')

        if records:
            all_melted_rows.extend(records)
        else:
            print(f"未解析到数据")

    if not all_melted_rows:
        return None

    return pd.DataFrame(all_melted_rows)


def merge_rework_data():
    # 读取数据
    base_file = "产量数据_工序表合并.xlsx"
    rework_folder = "返工数量"

    df_main = pd.read_excel(base_file, dtype={'工号': str, '工序序号': str})

    rework_files = glob(os.path.join(rework_folder, "*.xlsx"))
    rework_files = [f for f in rework_files if not os.path.basename(f).startswith('~$')]

    all_rework_dfs = []
    for file in rework_files:
        print(f"处理: {os.path.basename(file)}")
        df_rework_part = parse_rework_sheet_customized(file)
        if df_rework_part is not None and not df_rework_part.empty:
            all_rework_dfs.append(df_rework_part)

    if not all_rework_dfs:
        print("没有解析到返工数据")
        return

    df_rework_total = pd.concat(all_rework_dfs, ignore_index=True)
    print(f"解析到 {len(df_rework_total)} 条返工记录")

    # 主数据
    if '日期' in df_main.columns:
        df_main['merge_date'] = pd.to_datetime(df_main['日期']).dt.strftime('%Y-%m-%d')
    elif '生产时间' in df_main.columns:
        df_main['merge_date'] = pd.to_datetime(df_main['生产时间']).dt.strftime('%Y-%m-%d')

    df_main['merge_style'] = df_main['清洗后款号'].apply(clean_style_number).astype(str).str.strip()
    name_col = '姓名' if '姓名' in df_main.columns else '员工名称'
    df_main['merge_name'] = df_main[name_col].astype(str).str.strip()

    # 返工数据
    df_rework_total['merge_date'] = pd.to_datetime(df_rework_total['日期']).dt.strftime('%Y-%m-%d')
    df_rework_total['merge_style'] = df_rework_total['款号'].apply(clean_style_number).astype(str).str.strip()
    df_rework_total['merge_name'] = df_rework_total['姓名'].astype(str).str.strip()

    print("\n=== 匹配方案: 款号+日期+员工名称+工序序号一对一匹配 ===")

    # 主数据
    df_main['merge_process_seq'] = df_main['工序序号'].astype(str).str.strip()
    # 返工数据
    df_rework_total['merge_process_seq'] = df_rework_total['返工率表格中的工序序号'].astype(str).str.strip()

    df_final = pd.merge(
        df_main,
        df_rework_total[['merge_style', 'merge_date', 'merge_name', 'merge_process_seq',
                         '每日返工数量', '返工率表格中的工序名称', '返工率表格中的工序序号']],
        on=['merge_style', 'merge_date', 'merge_name', 'merge_process_seq'],
        how='left'
    )

    df_final['每日返工数量'] = df_final['每日返工数量'].fillna(0)
    df_final['返工率表格中的工序名称'] = df_final['返工率表格中的工序名称'].fillna('')
    # df_final['返工率表格中的工序序号'] = df_final['返工率表格中的工序序号'].fillna('')

    matched_count = (df_final['每日返工数量'] > 0).sum()
    print(f"精确匹配数: {matched_count}")

    # 清理临时列
    columns_to_drop = ['merge_date', 'merge_style', 'merge_name', 'merge_process_seq', '返工率表格中的工序序号']
    df_final = df_final.drop(columns=[col for col in columns_to_drop if col in df_final.columns])

    # 确保所有列都存在且位置正确
    if '每日返工数量' not in df_final.columns:
        df_final['每日返工数量'] = 0
    if '返工率表格中的工序名称' not in df_final.columns:
        df_final['返工率表格中的工序名称'] = ''

    # 保存结果
    output_file = "产量数据_工序_返工_合并_test.xlsx"
    df_final.to_excel(output_file, index=False)
    print(f"\n结果已保存到: {output_file}")
    print(f"总匹配数: {(df_final['每日返工数量'] > 0).sum()}")
    print(f"总返工件数: {df_final['每日返工数量'].sum()}")
    print(f"包含返工工序名称的记录数: {(df_final['返工率表格中的工序名称'] != '').sum()}")


if __name__ == "__main__":
    merge_rework_data()