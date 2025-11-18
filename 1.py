import pandas as pd
import numpy as np
import os
import re
from glob import glob
from difflib import SequenceMatcher


def clean_style_number(style_num):
    """
    改进的款号清洗函数 (最终版)
    - 仅负责“整容”和“后缀清理”，不负责“同款映射”
    """
    if pd.isna(style_num):
        return style_num

    style_str = str(style_num).strip()

    # 去除所有特殊标记和描述
    cleaned = re.sub(r'[加单|翻单|男|女|加急|特急|返工|返修|补数].*$', '', style_str)

    # 处理特殊格式 (只处理拼写错误、格式修正、后缀)
    special_cases = {
        r'76Z0001\d+': '76Z0001',  # 后缀
        r'31110011': '3111-0011',  # 格式修正
        r'15CO156': '15C0156',  # 拼写错误
        r'15F0189B': '15F0189',  # 拼写错误
        r'12C0933加单': '12C0933',  # 后缀
        r'15C0196翻单': '15C0196',  # 后缀
        # r'12C1536': '12C1450',  <-- 错误映射，已删除 (Bug Fix)
        # r'67C0015': '12C1470',    <-- 同款映射，已移至 known_mappings
        # r'12C1572': '12C1450',    <-- 同款映射，已移至 known_mappings
        r'16A0509.*': '16A0509',  # 后缀
        r'17A0025.*': '17A0025',  # 后缀
        r'18A0002.*': '18A0002',  # 后缀
    }

    for pattern, replacement in special_cases.items():
        if re.match(pattern, cleaned):
            cleaned = replacement
            break

    # 去除-Q3、-Q1等后缀
    cleaned = re.sub(r'-[A-Z]\d+$', '', cleaned)

    # 使规则更保守，只匹配-1或-10，而不是-0011
    cleaned = re.sub(r'-\d{1,2}$', '', cleaned)

    # --- (新增的修复) ---
    # 去除末尾单独的 '-', 比如 '1225-' -> '1225'
    cleaned = re.sub(r'-$', '', cleaned)
    # --- (修复结束) ---

    # 确保格式为数字+字母+数字
    match = re.match(r'(\d+)([A-Z]+)(\d+)', cleaned)
    if match:
        num1, letters, num2 = match.groups()
        cleaned = f"{num1}{letters}{num2}"

    return cleaned


def extract_style_from_filename(filename):
    """改进的文件名款号提取"""
    filename = os.path.basename(filename)

    # (Feedback 1) - 此逻辑已支持文件名中包含“同款”并提取多个款号
    if '同款' in filename:
        # 提取所有可能的款号
        styles = re.findall(r'(\d+[A-Z]+\d+)', filename)
        if styles:
            return [clean_style_number(style) for style in styles]

    # 单个款号提取
    patterns = [
        r'(\d+[A-Z]+\d+)',  # 标准格式：12C1450
        r'([A-Z]+\d+-\d+)',  # 如AAV10276-001
        r'([A-Z]+\d+)',  # 如 AAV10276, JDWM2500001
        r'(\d+-\d+)',  # 如 3111-0011
    ]

    for pattern in patterns:
        matches = re.findall(pattern, filename)
        if matches:
            # 优先匹配最长的（例如 AAV10276-001 而不是 AAV10276）
            matches.sort(key=len, reverse=True)
            return [clean_style_number(match) for match in matches]

    return []


def parse_process_sheet(file_path):
    """(重大更新) 改进的工序表解析 - 现在会读取并合并一个文件中的所有Sheet"""
    try:
        # sheet_name=None 会读取所有sheet到一个字典
        df_raw_dict = pd.read_excel(file_path, header=None, sheet_name=None)
    except Exception as e:
        print(f"  错误: 读取 Excel 文件 {file_path} 失败: {e}")
        return None, None, None, None, []

    style_number = None
    sewing_time = None
    sewing_10h_output = None
    found_same_styles = []

    all_data_frames = []  # 用于存储从所有sheet中提取的数据

    style_regex = r'(\d+[A-Z]+\d+|\d+-\d+|[A-Z]+\d+-\d+|[A-Z]+\d+)'

    # 遍历文件中的每一个Sheet
    for sheet_name, df_raw in df_raw_dict.items():
        # --- 1. 在每个Sheet的表头中查找款号、车缝时间等信息 ---
        #    (只在尚未找到时查找)
        if style_number is None or sewing_time is None or sewing_10h_output is None:
            for i in range(min(30, len(df_raw))):
                row = df_raw.iloc[i]
                for j, cell in enumerate(row):
                    if pd.notna(cell):
                        cell_str = str(cell).strip()

                        # 查找款号
                        if style_number is None and any(
                                keyword in cell_str for keyword in ['生产制单号', '客户款号', '款号', '款名', '款式']):
                            for k in range(j + 1, min(j + 3, len(row))):
                                if k < len(row) and pd.notna(row[k]):
                                    potential_style = str(row[k]).strip()
                                    style_match = re.search(style_regex, potential_style)
                                    if style_match:
                                        style_number = clean_style_number(style_match.group(1))
                                        break

                        # 查找“款式描述”同款
                        if '款式描述' in cell_str:
                            for k in range(j + 1, min(j + 4, len(row))):
                                if k < len(row) and pd.notna(row[k]):
                                    desc_str = str(row[k])
                                    if '同款' in desc_str:
                                        same_styles = re.findall(style_regex, desc_str)
                                        if same_styles:
                                            found_same_styles.extend(
                                                [clean_style_number(s) for s in same_styles]
                                            )

                        # 查找车缝时间
                        if sewing_time is None and any(
                                keyword in cell_str for keyword in ['车缝时间', '车缝时', '标准时间']):
                            for k in range(j + 1, min(j + 3, len(row))):
                                if k < len(row) and pd.notna(row[k]):
                                    sewing_time = row[k]
                                    break

                        # 查找车缝10H台产
                        if sewing_10h_output is None and any(
                                keyword in cell_str for keyword in ['车缝10H台产', '10H台产', '台产']):
                            for k in range(j + 1, min(j + 3, len(row))):
                                if k < len(row) and pd.notna(row[k]):
                                    sewing_10h_output = row[k]
                                    break

        # --- 2. 在每个Sheet中查找工序数据表 ---
        data_start_row = -1
        header_keywords = ['工序号', '工序名称', '机器类型', '基础时间']

        for i in range(len(df_raw)):
            row_str = ' '.join([str(x) for x in df_raw.iloc[i] if pd.notna(x)])
            if any(keyword in row_str for keyword in header_keywords):
                data_start_row = i
                break

        # 如果这个sheet没有工序表头，则跳过
        if data_start_row == -1:
            # print(f"    - 在 Sheet '{sheet_name}' 未找到工序表头，跳过。")
            continue

        # --- 3. 读取并处理这个Sheet的工序表 ---
        try:
            # 重新读取一次，这次指定 skiprows
            df_data_sheet = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=data_start_row)
        except Exception as e:
            print(f"    警告: 读取 Sheet '{sheet_name}' 数据时出错: {e}")
            continue

        original_col_names = {}
        found_name_1 = False
        for col in df_data_sheet.columns:
            col_str = str(col).strip()
            if '工序号' not in original_col_names and re.search(r'工序号|序号|编号', col_str):
                original_col_names['工序号'] = col
            elif not found_name_1 and re.search(r'工序名称|工序名|名称', col_str):
                original_col_names['工序名称_1'] = col
                found_name_1 = True
            elif '工序名称_2' not in original_col_names and re.search(r'工序名称|工序名|名称', col_str):
                original_col_names['工序名称_2'] = col
            elif '机器类型' not in original_col_names and re.search(r'机器类型|机器|设备', col_str):
                original_col_names['机器类型'] = col
            elif '基础时间' not in original_col_names and re.search(r'基础时间|标准时间|时间', col_str):
                original_col_names['基础时间'] = col
            elif 'GST时间' not in original_col_names and re.search(r'GST时间|GST', col_str):
                original_col_names['GST时间'] = col
            elif '工序等级' not in original_col_names and re.search(r'工序等级|等级', col_str):
                original_col_names['工序等级'] = col
            elif '小时目标' not in original_col_names and re.search(r'小时目标|目标|效率', col_str):
                original_col_names['小时目标'] = col

        if '工序名称_1' not in original_col_names and '工序号' not in original_col_names:
            print(f"    - 在 Sheet '{sheet_name}' 既没找到工序号也没找到工序名称，跳过。")
            continue

        keep_cols = list(original_col_names.values())
        df_selected = df_data_sheet[keep_cols].copy()

        rename_mapping = {v: k for k, v in original_col_names.items()}
        df_data_renamed = df_selected.rename(columns=rename_mapping)
        df_data = df_data_renamed

        if '工序名称_1' in df_data.columns:
            mask = ~df_data['工序名称_1'].astype(str).str.contains(
                '裁床|尾部|大烫|尾查|包装|检验|总检|终检|查衫',
                na=False
            )
            df_data = df_data[mask].copy()

            if df_data.empty:
                continue

            df_data['部位'] = df_data['工序名称_1'].apply(extract_part_from_process_name)
            df_data['工序名称_1_清洗后'] = df_data['工序名称_1'].apply(clean_process_name)

            all_data_frames.append(df_data)
            print(f"    - 成功解析 Sheet '{sheet_name}', 找到 {len(df_data)} 行工序。")

    # --- 4. 合并所有Sheet的数据 ---
    if not all_data_frames:
        print(f"  错误: 未能在文件 {file_path} 的任何Sheet中找到有效的工序数据。")
        return None, None, None, None, []

    final_combined_df = pd.concat(all_data_frames, ignore_index=True)

    # 清理从“款式描述”中找到的同款
    final_found_same_styles = list(set(s for s in found_same_styles if s and s != style_number))

    print(f"  成功解析 (合并所有Sheet): 款号={style_number}, 总工序数={len(final_combined_df)}")
    return final_combined_df, style_number, sewing_time, sewing_10h_output, final_found_same_styles


def extract_part_from_process_name(process_name):
    """
    (Plan Detail) - 从工序名称中提取部位信息
    注意：此实现是根据 *工序名称* 提取，而不是Excel中的 *章节标题*。
    请与用户确认这是否是期望的行为。
    """
    if pd.isna(process_name):
        return '其他'

    process_str = str(process_name).lower()

    # 部位关键词映射
    part_keywords = {
        '腰头': ['腰头', '腰', '裤头', '裙头'],
        '领子': ['领', '领子', '领圈', '领口'],
        '袖子': ['袖', '袖子', '袖口', '袖山', '袖窿'],
        '口袋': ['口袋', '袋', '兜'],
        '门筒': ['门筒', '门襟', '前中'],
        '下摆': ['下摆', '摆', '底摆'],
        '帽子': ['帽', '帽子', '帽檐'],
        '里布': ['里布', '里料', '内里'],
        '侧缝': ['侧缝', '肋缝', '摆缝'],
        '肩部': ['肩', '肩膀', '肩缝'],
        '前片': ['前片', '前幅', '前身'],
        '后片': ['后片', '后幅', '后身'],
    }

    for part, keywords in part_keywords.items():
        if any(keyword in process_str for keyword in keywords):
            return part

    return '其他'


def clean_process_name(name):
    """(Feedback 3) - 新增：用于清洗工序名称的辅助函数"""
    if pd.isna(name):
        return name
    # 移除所有空格和常见标点符号，并转为小写
    return re.sub(r'[\s\(\)（）\-+\\/]', '', str(name)).lower()


def similarity(a, b):
    """计算两个字符串的相似度"""
    return SequenceMatcher(None, str(a), str(b)).ratio()  # 确保a,b都已是清洗过的


def build_style_mapping(production_df, process_database, mappings_from_sheets):
    """
    构建款号映射关系 (最终版)
    - 仅负责“同款映射”，不负责“清洗”
    """
    style_mapping = {}

    # 1. (Plan Source 3) - 已知的同款映射 (唯一的“认亲”清单)
    known_mappings = {
        'BRYNN1412': '1412',
        '12C1572': '12C1450',  # <-- (Refactor) 从 special_cases 移入
        '67C0015': '12C1470',  # <-- (Refactor) 从 special_cases 移入
        '76Z00012025': '76Z0001',
        # '31110011': '3111-0011', # (Refactor) 已在 clean_style_number 中处理
        # '15CO156': '15C0156',   # (Refactor) 已在 clean_style_number 中处理
        # '15F0189B': '15F0189', # (Refactor) 已在 clean_style_number 中处理
        # '12C0933加单': '12C0933',# (Refactor) 已在 clean_style_number 中处理
        # '15C0196翻单': '15C0196',# (Refactor) 已在 clean_style_number 中处理

        # '12C1536': '12C1450',  <-- (Bug Fix) 已根据用户反馈删除

        # 这些是带-1/-Q3后缀的映射，必须保留
        '16A0509-1': '16A0509',
        '17A0025-1': '17A0025',
        '18A0002-1': '18A0002',
    }
    style_mapping.update(known_mappings)

    # 2. (Plan Source - "款式描述") - 从工序表中扫描到的映射
    style_mapping.update(mappings_from_sheets)

    # 3. (Feedback 2) - 移除了自动相似度匹配
    #    原来的自动匹配逻辑（for prod_style... similarity()）已删除
    #    以防止 12C1480, 12C1400-Q2 等被错误匹配
    print("已禁用自动相似度匹配。")

    print(f"产量数据款号数: {len(production_df['清洗后款号'].unique())}")
    print(f"工序表款号数: {len(process_database.keys())}")

    return style_mapping


def merge_production_process_data():
    """改进的数据合并函数"""

    # 1. 读取产量数据
    print("正在读取产量数据...")
    production_file = "半年产量数据/3-8月车缝数据.xlsx"

    if not os.path.exists(production_file):
        print(f"错误: 找不到产量数据文件 {production_file}")
        return None

    try:
        all_sheets = pd.read_excel(production_file, sheet_name=None)
        print(f"文件包含的Sheet: {list(all_sheets.keys())}")
        production_df = pd.concat(all_sheets.values(), ignore_index=True)
        print(f"成功读取并合并所有Sheet，共 {len(production_df)} 行")

        style_counts = production_df['款号'].value_counts()
        print(f"产量数据款号种类: {len(style_counts)}")
        print("前10个款号:")
        print(style_counts.head(10))

    except Exception as e:
        print(f"读取产量数据时出错: {e}")
        return None

    # (Plan Detail) - 清洗款号数据
    production_df['清洗后款号'] = production_df['款号'].apply(clean_style_number)

    # <--- NEW: (Feedback 3) 预清洗产量数据中的工序名称 ---
    production_df['工序_清洗后'] = production_df['工序'].apply(clean_process_name)

    # 2. 读取工序表数据
    print("\n正在读取工序表数据...")
    process_folder = "工序表"
    process_files = glob(os.path.join(process_folder, "*.xlsx"))
    process_files.extend(glob(os.path.join(process_folder, "*.xls")))

    print(f"找到 {len(process_files)} 个工序表文件")

    process_database = {}
    sewing_info_database = {}
    mappings_from_sheets = {}  # <--- NEW: 存储从“款式描述”中找到的映射

    for file_path in process_files:
        file_name = os.path.basename(file_path)
        print(f"\n处理文件: {file_name}")

        # (Plan Source 2) - 从文件名提取款号
        file_styles = extract_style_from_filename(file_path)
        if file_styles:
            print(f"  从文件名提取款号: {file_styles}")

        # (重大更新) 解析工序表 (现在会读取所有sheet)
        process_df, style_number, sewing_time, sewing_10h_output, found_same_styles = parse_process_sheet(file_path)

        # 优先使用从内容中提取的款号
        if style_number and style_number != 'None':
            if not file_styles:
                file_styles = [style_number]
            elif style_number not in file_styles:
                file_styles.append(style_number)
            print(f"  从内容中提取款号 (可能来自多个sheet): {style_number}")

        if not file_styles:
            print(f"  警告: 无法从文件 {file_name} 中提取款号")
            base_name = os.path.splitext(file_name)[0]
            potential_styles = re.findall(r'(\d+[A-Z]+\d+)', base_name)
            if potential_styles:
                file_styles = [clean_style_number(style) for style in potential_styles]
                print(f"  从基础文件名提取款号: {file_styles}")

        if process_df is None or process_df.empty:
            print(f"  警告: 文件 {file_name} 没有有效数据")
            continue

        # print(f"  找到 {len(process_df)} 行工序数据 (合并所有sheet)") # 已在parse_process_sheet中打印

        # <--- NEW: (Plan Detail) 处理从“款式描述”中找到的同款 ---
        if found_same_styles and file_styles:
            # 假设文件中的第一个款号是“主款号”
            main_style = file_styles[0]
            for other_style in found_same_styles:
                if other_style != main_style and other_style not in mappings_from_sheets:
                    mappings_from_sheets[other_style] = main_style
                    print(f"  从“款式描述”中找到映射: {other_style} -> {main_style}")

        # 存储到数据库
        for style in set(file_styles):  # 去重
            if style and style != 'None':
                process_database[style] = {
                    'data': process_df.copy(),
                    'source_file': file_name
                }
                if sewing_time is not None or sewing_10h_output is not None:
                    sewing_info_database[style] = {
                        '车缝时间': sewing_time,
                        '车缝10H台产': sewing_10h_output
                    }
                print(f"  存储工序信息: {style}")

    print(f"\n成功加载 {len(process_database)} 个款式的工序信息")

    # 3. 构建款号映射
    print("\n构建款号映射关系...")
    style_mapping = build_style_mapping(production_df, process_database, mappings_from_sheets)
    print(f"建立 {len(style_mapping)} 个款号映射 (来自 known_mappings 和 '款式描述')")

    # 4. 合并数据
    print("\n正在合并数据...")

    result_rows = []
    matched_count = 0
    process_matched_count = 0

    # 预处理工序表
    for style_key, process_info in process_database.items():
        if '工序号' in process_info['data'].columns:
            try:
                process_info['data']['工序号_str'] = process_info['data']['工序号'].astype(str).str.replace('.0', '',
                                                                                                            regex=False).str.strip()
            except Exception as e:
                print(f"为 {style_key} 创建工序号_str 时出错: {e}")

    for idx, row in production_df.iterrows():
        if idx % 50000 == 0 and idx > 0:
            print(f"  处理进度: {idx}/{len(production_df)}")

        style_num = row['清洗后款号']
        process_num = row.get('工序序号')
        # <--- MODIFICATION: (Feedback 3) 使用清洗后的工序名称进行匹配 ---
        process_name_cleaned = row.get('工序_清洗后')

        if pd.isna(style_num) or style_num in ['', 'None', 'nan']:
            result_row = create_empty_result_row(row)
            result_rows.append(result_row)
            continue

        # 查找工序信息
        process_info = None
        source_style = style_num
        sewing_info = None

        if style_num in process_database:
            process_info = process_database[style_num]
            if style_num in sewing_info_database:
                sewing_info = sewing_info_database[style_num]
        elif style_num in style_mapping:
            mapped_style = style_mapping[style_num]
            if mapped_style in process_database:
                process_info = process_database[mapped_style]
                source_style = mapped_style
                if mapped_style in sewing_info_database:
                    sewing_info = sewing_info_database[mapped_style]

        if process_info is not None:
            matched_count += 1

        # (重大更新) 查找匹配的工序
        matched_process = None
        if process_info is not None:
            # <--- MODIFICATION: (Feedback 3) 传递清洗后的名称 ---
            matched_process = find_matching_process(process_info, process_num, process_name_cleaned)
            if matched_process is not None:
                process_matched_count += 1

        if matched_process is not None:
            result_row = create_matched_result_row(row, process_info, matched_process, source_style, sewing_info)
            result_rows.append(result_row)
        else:
            result_row = create_partial_result_row(row, process_info, source_style, sewing_info)
            result_rows.append(result_row)

    # 5. 创建最终结果
    final_df = pd.DataFrame(result_rows)

    print(f"\n合并完成:")
    print(f"  款号匹配数: {matched_count}/{len(production_df)} ({matched_count / len(production_df) * 100:.2f}%)")
    print(
        f"  工序匹配数: {process_matched_count}/{len(production_df)} ({process_matched_count / len(production_df) * 100:.2f}%)")

    return final_df


def find_matching_process(process_info, process_num, process_name_cleaned):
    """
    (重大更新) 改进的工序匹配函数
    - 优先匹配 “工序号 + 工序名称”
    """
    process_data = process_info['data']
    matched_row = None

    process_num_str = str(process_num).replace('.0', '').strip() if process_num is not None else None

    # 方法1: (最高优先级) 尝试同时匹配 “工序号” 和 “工序名称”
    if process_num_str and process_name_cleaned and '工序号_str' in process_data.columns and '工序名称_1_清洗后' in process_data.columns:
        try:
            match_series = process_data[
                (process_data['工序号_str'] == process_num_str) &
                (process_data['工序名称_1_清洗后'] == process_name_cleaned)
                ]
            if not match_series.empty:
                matched_row = match_series.iloc[0]
        except Exception as e:
            pass  # 忽略错误

    # 方法2: (回退) 如果方法1失败，尝试只匹配 “工序序号”
    # (这就是导致 "68 -> 吊牌" 的原因, 但现在它是第二优先级)
    if matched_row is None and process_num_str and '工序号_str' in process_data.columns:
        try:
            match_series = process_data[process_data['工序号_str'] == process_num_str]
            if not match_series.empty:
                # 警告：这里可能匹配到多个 (比如 68->吊牌 和 68->平车...)
                # 我们默认取第一个，但现在这个匹配的优先级降低了
                matched_row = match_series.iloc[0]
        except Exception as e:
            pass

    # 方法3: (回退) 如果上述都失败，尝试只匹配 “工序名称” (用于处理工序号缺失或错误)
    if matched_row is None and process_name_cleaned and '工序名称_1_清洗后' in process_data.columns:
        best_match = None
        best_score = 0

        for idx, row in process_data.iterrows():
            table_process_name_cleaned = row.get('工序名称_1_清洗后')
            if pd.notna(table_process_name_cleaned):
                score = similarity(process_name_cleaned, table_process_name_cleaned)

                if score > 0.7 and score > best_score:
                    best_match = row
                    best_score = score

        if best_score > 0.8:  # 相似度阈值
            matched_row = best_match

    # 方法4: (最后的尝试) 工序名称包含关系
    if matched_row is None and process_name_cleaned and '工序名称_1_清洗后' in process_data.columns:
        if process_name_cleaned:
            for idx, row in process_data.iterrows():
                table_process_name_cleaned = row.get('工序名称_1_清洗后')
                if pd.notna(table_process_name_cleaned) and table_process_name_cleaned:
                    len_ratio = min(len(process_name_cleaned), len(table_process_name_cleaned)) / max(
                        len(process_name_cleaned), len(table_process_name_cleaned))
                    if (process_name_cleaned in table_process_name_cleaned or
                        table_process_name_cleaned in process_name_cleaned) and len_ratio > 0.7:
                        matched_row = row
                        break

    return matched_row


def create_empty_result_row(row):
    """创建空的结果行"""
    result_row = row.to_dict()
    result_row.update({
        '工序表来源文件': None,
        '工序表款号': None,
        '工序名称_表1': None,
        '工序名称_表2': None,
        '机器类型': None,
        '基础时间': None,
        'GST时间': None,
        '工序等级': None,
        '小时目标': None,
        '车缝时间': None,
        '车缝10H台产': None,
        '部位': None
    })
    return result_row


def create_matched_result_row(row, process_info, matched_row, source_style, sewing_info=None):
    """(Plan Detail) - 创建匹配成功的结果行 (保留3个工序名称)"""
    result_row = row.to_dict()  # 包含原始'工序' 和 '工序_清洗后'

    process_name1 = matched_row.get('工序名称_1', None)
    process_name2 = matched_row.get('工序名称_2', None)
    machine_type = matched_row.get('机器类型', None)
    base_time = matched_row.get('基础时间', None)
    gst_time = matched_row.get('GST时间', None)
    process_level = matched_row.get('工序等级', None)
    hour_target = matched_row.get('小时目标', None)
    part = matched_row.get('部位', None)

    sewing_time = sewing_info.get('车缝时间', None) if sewing_info else None
    sewing_10h_output = sewing_info.get('车缝10H台产', None) if sewing_info else None

    result_row.update({
        '工序表来源文件': process_info['source_file'],
        '工序表款号': source_style,
        '工序名称_表1': process_name1,  # 来自工序表的名称1
        '工序名称_表2': process_name2,  # 来自工序表的名称2
        '机器类型': machine_type,
        '基础时间': base_time,
        'GST时间': gst_time,
        '工序等级': process_level,
        '小时目标': hour_target,
        '车缝时间': sewing_time,
        '车缝10H台产': sewing_10h_output,
        '部位': part
    })
    return result_row


def create_partial_result_row(row, process_info, source_style, sewing_info=None):
    """创建部分匹配的结果行 (只有款号匹配，工序未匹配)"""
    result_row = row.to_dict()

    sewing_time = sewing_info.get('车缝时间', None) if sewing_info else None
    sewing_10h_output = sewing_info.get('车缝10H台产', None) if sewing_info else None

    result_row.update({
        '工序表来源文件': process_info['source_file'] if process_info else None,
        '工序表款号': source_style if process_info else None,
        '工序名称_表1': None,
        '工序名称_表2': None,
        '机器类型': None,
        '基础时间': None,
        'GST时间': None,
        '工序等级': None,
        '小时目标': None,
        '车缝时间': sewing_time,  # 即使工序没匹配，款号级别的车缝时间也应填上
        '车缝10H台产': sewing_10h_output,  # 同上
        '部位': None
    })
    return result_row


# 使用示例
if __name__ == "__main__":
    print("开始数据合并...")
    merged_data = merge_production_process_data()

    if merged_data is not None:
        # 1. 保存主要合并结果
        output_file = "产量数据_工序表合并.xlsx"
        merged_data.to_excel(output_file, index=False, engine='openpyxl')
        print(f"\n结果已保存到: {output_file}")

        # --- (新增功能) ---
        print("\n正在生成未匹配款式列表...")
        # 2. (根据你的要求) 生成未匹配款式工序列表，以方便人工检查

        # 筛选出所有“工序表来源文件”为空，但“清洗后款号”有效的行
        unmatched_df = merged_data[
            (merged_data['工序表来源文件'].isna()) &
            (merged_data['清洗后款号'].notna()) &
            (merged_data['清洗后款号'] != 'None') &
            (merged_data['清洗后款号'] != '') &
            (merged_data['清洗后款号'] != 'nan')
            ].copy()

        if not unmatched_df.empty:
            # 提取这些款式的独特工序（款号, 序号, 名称）
            unmatched_processes_list = unmatched_df[['清洗后款号', '工序序号', '工序']].drop_duplicates().sort_values(
                by=['清洗后款号', '工序序号']
            )

            unmatched_output_file = "未匹配上的款式工序列表.xlsx"
            unmatched_processes_list.to_excel(unmatched_output_file, index=False, engine='openpyxl')
            print(f"  为方便你手动检查同款，已将未匹配的款式工序列表保存到: {unmatched_output_file}")
            print(f"  共找到 {len(unmatched_processes_list['清洗后款号'].unique())} 个未匹配款式的工序信息。")
        else:
            print("  恭喜！所有有效的产量数据款号都找到了匹配的工序表。")
        # --- (新增功能结束) ---

        # 3. 显示详细统计信息
        total_rows = len(merged_data)
        print(f"\n详细统计信息:")
        print(f"总记录数: {total_rows}")

        cols_to_check = ['工序表来源文件', '工序名称_表1', '工序名称_表2', '机器类型',
                         '基础时间', 'GST时间', '车缝时间', '车缝10H台产', '部位']

        for col in cols_to_check:
            if col in merged_data.columns:
                matched_count = merged_data[col].notna().sum()
                percentage = matched_count / total_rows * 100
                print(f"{col}: {matched_count} ({percentage:.2f}%)")

        # 4. 显示匹配样例
        print(f"\n匹配样例 (来自 {output_file}):")
        matched_samples = merged_data[merged_data['工序表来源文件'].notna()].head(5)
        if len(matched_samples) > 0:
            display_cols = ['款号', '清洗后款号', '工序', '工序表来源文件', '工序名称_表1', '工序名称_表2', '机器类型',
                            '基础时间', '部位']
            display_cols = [col for col in display_cols if col in matched_samples.columns]
            print(matched_samples[display_cols].to_string(index=False))
        else:
            print("没有找到匹配的样例")

    else:
        print("合并失败")