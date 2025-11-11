import pandas as pd
import os
from glob import glob
import re
from collections import Counter


def clean_style_number(style_num):
    """清洗款号,去除多余的符号"""
    if pd.isna(style_num):
        return style_num
    # 去除-Q2, -Q3, -翻单, -网单等后缀
    return str(style_num).split('-')[0]


def is_valid_worker_name(name):
    """验证是否是有效的工人姓名"""
    if pd.isna(name) or name == '':
        return False
    name_str = str(name).strip()

    # 排除明显不是姓名的内容
    invalid_keywords = ['Total', 'total', '合计', '小计', '制单号', '款号', '日期',
                        '工序', '数量', 'NaN', 'nan']
    if any(kw in name_str for kw in invalid_keywords):
        return False

    # 长度检查:中文姓名一般2-4个字
    if len(name_str) > 5:
        return False

    # 检查是否包含中文字符
    has_chinese = any('\u4e00' <= char <= '\u9fff' for char in name_str)

    # 如果包含数字,可能不是姓名(除非是"6组"这种)
    if name_str.isdigit() and '组' not in name_str:
        return False

    return has_chinese or '组' in name_str


def is_valid_process_name(name):
    """验证是否是有效的工序名称 - 放宽验证条件"""
    if pd.isna(name) or name == '':
        return False
    name_str = str(name).strip()

    # 如果为空字符串,返回False
    if name_str == '':
        return False

    # 排除明显不是工序名称的内容
    invalid_keywords = ['Total', 'total', '合计', '小计', '制单号', '款号', '日期',
                        '姓名', '工人', '员工', 'NaN', 'nan']
    if any(kw in name_str for kw in invalid_keywords):
        return False

    # 检查是否包含中文字符
    has_chinese = any('\u4e00' <= char <= '\u9fff' for char in name_str)

    # 只要包含中文就认为可能是工序名称(放宽条件)
    return has_chinese


def find_header_rows(df_raw, data_start_row):
    """智能查找工序行和工人行的位置"""
    possible_combinations = []

    # 在数据开始行之前的10行内查找
    search_range = range(max(0, data_start_row - 10), data_start_row)

    for i in search_range:
        for j in range(i + 1, data_start_row):
            # 检查这两行是否可能是工序行和工人行
            row_i = df_raw.iloc[i]
            row_j = df_raw.iloc[j]

            # 统计每行有多少有效的工人姓名
            valid_names_i = sum(1 for val in row_i[5:50] if is_valid_worker_name(val))
            valid_names_j = sum(1 for val in row_j[5:50] if is_valid_worker_name(val))

            # 统计每行有多少有效的工序名称
            valid_process_i = sum(1 for val in row_i[5:50] if is_valid_process_name(val))
            valid_process_j = sum(1 for val in row_j[5:50] if is_valid_process_name(val))

            # 如果某一行有较多有效姓名,可能是工人行，另一行可能是工序行
            if (valid_names_i >= 3 and valid_process_j >= 1) or (valid_names_j >= 3 and valid_process_i >= 1):
                score = valid_names_i + valid_names_j + valid_process_i + valid_process_j
                # 确定哪一行更可能是工序行，哪一行更可能是工人行
                if valid_process_i > valid_process_j and valid_names_j > valid_names_i:
                    possible_combinations.append((i, j, score))  # i是工序行，j是工人行
                elif valid_process_j > valid_process_i and valid_names_i > valid_names_j:
                    possible_combinations.append((j, i, score))  # j是工序行，i是工人行
                else:
                    # 如果不能明确区分，尝试两种组合
                    possible_combinations.append((i, j, score))
                    possible_combinations.append((j, i, score))

    # 按得分排序,返回最可能的组合
    possible_combinations.sort(key=lambda x: x[2], reverse=True)

    # 返回前5个最可能的组合，去除重复
    unique_combinations = []
    seen = set()
    for combo in possible_combinations:
        key = (combo[0], combo[1])
        if key not in seen:
            seen.add(key)
            unique_combinations.append((combo[0], combo[1]))
        if len(unique_combinations) >= 5:
            break

    return unique_combinations


def extract_rework_data_improved():
    """改进的返工数据提取函数"""
    print("开始改进的返工数据提取...")

    rework_files = [
        '返工数量/返工率—张大丽.xlsx',
        '返工数量/返工率—曾繁利.xlsx',
        '返工数量/返工率—李小萍.xlsx',
        '返工数量/返工率—范丽.xlsx',
        '返工数量/返工率—范嗣惠.xlsx',
        '返工数量/返工率—陈亚梅.xlsx',
        '返工数量/返工率—陈定芬6组.xlsx',
        '返工数量/返工率—陈定芬7组.xlsx'
    ]

    existing_files = [f for f in rework_files if os.path.exists(f)]
    print(f"找到 {len(existing_files)} 个返工文件")

    all_rework_records = []

    for file_path in existing_files:
        try:
            print(f"\n{'=' * 60}")
            print(f"处理文件: {os.path.basename(file_path)}")

            df_raw = pd.read_excel(file_path, header=None)
            print(f"文件形状: {df_raw.shape}")

            # 查找数据开始行
            data_start_row = None
            for i in range(min(15, len(df_raw))):
                style_val = df_raw.iloc[i, 0]
                if pd.notna(style_val) and '制单号' in str(style_val):
                    data_start_row = i + 1
                    break

            if data_start_row is None:
                print("⚠️ 未找到数据开始行,跳过此文件")
                continue

            print(f"数据开始行: {data_start_row}")

            # 智能查找工序行和工人行
            header_combinations = find_header_rows(df_raw, data_start_row)

            if not header_combinations:
                # 如果智能查找失败,使用固定的组合
                header_combinations = [
                    (data_start_row - 3, data_start_row - 2),
                    (data_start_row - 4, data_start_row - 3),
                    (data_start_row - 5, data_start_row - 4),
                    (data_start_row - 2, data_start_row - 1),
                ]

            print(f"尝试 {len(header_combinations)} 种表头组合")

            best_records = []
            best_score = 0
            best_process_count = 0  # 记录有工序名称的数量

            for combo_idx, (process_row, worker_row) in enumerate(header_combinations):
                if process_row < 0 or worker_row < 0:
                    continue

                print(f"\n尝试组合 {combo_idx + 1}: 工序行={process_row}, 工人行={worker_row}")

                current_style = None
                temp_records = []

                # 扫描数据行
                for i in range(data_start_row, min(data_start_row + 200, len(df_raw))):
                    style_val = df_raw.iloc[i, 0]

                    # 更新当前款号
                    if pd.notna(style_val) and 'Total' not in str(style_val):
                        style_str = str(style_val).strip()
                        if len(style_str) > 3 and '制单号' not in style_str:
                            current_style = style_str

                    # 处理日期行
                    date_val = df_raw.iloc[i, 4]
                    if pd.notna(date_val) and current_style:
                        try:
                            date_obj = pd.to_datetime(date_val)
                            date_str = date_obj.strftime('%Y-%m-%d')

                            # 扫描该行的所有列查找返工数据
                            for col in range(5, min(len(df_raw.columns), 200)):
                                rework_value = df_raw.iloc[i, col]

                                if pd.notna(rework_value) and rework_value != '':
                                    try:
                                        rework_qty = float(rework_value)
                                        if rework_qty > 0 and rework_qty < 10000:  # 合理范围
                                            # 获取工序和工人信息
                                            process_name = df_raw.iloc[process_row, col]
                                            worker_name = df_raw.iloc[worker_row, col]

                                            # 关键修改:只验证工人姓名,工序名称即使无效也保存原始值
                                            if is_valid_worker_name(worker_name):
                                                # 清理工序名称但保留原始值
                                                process_str = ''
                                                if pd.notna(process_name):
                                                    process_str = str(process_name).strip()

                                                record = {
                                                    '款号': current_style,
                                                    '日期': date_str,
                                                    '工序名称_返工': process_str,  # 保留原始工序名称
                                                    '工人姓名': str(worker_name).strip(),
                                                    '返工数量': rework_qty,
                                                    '来源文件': os.path.basename(file_path),
                                                    '组合': f"{process_row}-{worker_row}"
                                                }
                                                temp_records.append(record)
                                    except (ValueError, TypeError):
                                        continue
                        except (ValueError, TypeError):
                            continue

                # 评估这个组合的质量
                valid_records = [r for r in temp_records if is_valid_worker_name(r['工人姓名'])]
                score = len(valid_records)

                # 计算有工序名称的记录数
                process_count = sum(1 for r in valid_records if r['工序名称_返工'] and r['工序名称_返工'] != '')

                print(f"✓ 找到 {score} 条有效记录，其中有工序名称的: {process_count} 条")

                # 显示工序名称统计
                process_names = [r['工序名称_返工'] for r in valid_records if r['工序名称_返工']]
                if process_names:
                    unique_processes = set(process_names)
                    print(f"  包含 {len(unique_processes)} 种不同工序: {list(unique_processes)[:5]}...")

                # 优先选择有更多工序名称的组合
                if score > best_score or (score == best_score and process_count > best_process_count):
                    best_score = score
                    best_process_count = process_count
                    best_records = valid_records

            if best_records:
                all_rework_records.extend(best_records)
                print(f"\n✓ 从文件提取了 {len(best_records)} 条返工记录")
                print(
                    f"✓ 其中有工序名称的记录: {sum(1 for r in best_records if r['工序名称_返工'] and r['工序名称_返工'] != '')} 条")

                # 显示部分记录样本
                print("记录样本:")
                for record in best_records[:3]:
                    print(f"  款号={record['款号']}, 日期={record['日期']}, "
                          f"工人={record['工人姓名']}, 返工={record['返工数量']}, 工序={record['工序名称_返工']}")
            else:
                print(f"⚠️ 未能从此文件提取到有效数据")

        except Exception as e:
            print(f"❌ 处理文件出错: {e}")
            import traceback
            traceback.print_exc()
            continue

    if all_rework_records:
        rework_df = pd.DataFrame(all_rework_records)
        rework_df['款号_清洗'] = rework_df['款号'].apply(clean_style_number)

        # 去重
        before_dedup = len(rework_df)
        rework_df = rework_df.drop_duplicates(
            subset=['款号_清洗', '日期', '工人姓名', '返工数量', '工序名称_返工']
        )
        after_dedup = len(rework_df)

        print(f"\n{'=' * 60}")
        print(f"总计提取: {before_dedup} 条记录")
        print(f"去重后: {after_dedup} 条记录 (删除 {before_dedup - after_dedup} 条重复)")

        # 详细统计工序名称情况
        with_process = (rework_df['工序名称_返工'] != '').sum()
        without_process = (rework_df['工序名称_返工'] == '').sum()
        print(f"有工序名称的记录: {with_process} 条 ({with_process / len(rework_df) * 100:.1f}%)")
        print(f"无工序名称的记录: {without_process} 条 ({without_process / len(rework_df) * 100:.1f}%)")

        return rework_df
    else:
        print("\n⚠️ 没有提取到任何返工数据")
        return pd.DataFrame()


def enhance_process_name_matching(output_data, rework_df):
    """增强工序名称匹配 - 专门解决工序名称缺失问题"""
    print(f"\n{'=' * 60}")
    print("开始增强工序名称匹配...")

    # 准备产量数据的工序信息
    output_data['日期'] = pd.to_datetime(output_data['生产时间']).dt.strftime('%Y-%m-%d')
    output_data['款号_清洗'] = output_data['款号'].apply(clean_style_number)

    # 创建工序名称映射
    process_mapping = {}

    # 按工人+款号+日期分组，收集工序名称
    grouped = output_data.groupby(['员工名称', '款号_清洗', '日期'])
    for (worker, style, date), group in grouped:
        key = (worker, style, date)
        processes = group['工序'].dropna().unique()
        if len(processes) > 0:
            process_mapping[key] = list(processes)

    print(f"从产量数据构建了 {len(process_mapping)} 个工序映射组")

    # 应用映射到返工数据
    enhanced_count = 0
    for idx, row in rework_df.iterrows():
        if row['工序名称_返工'] == '' and row['返工数量'] > 0:
            key = (row['工人姓名'], row['款号_清洗'], row['日期'])
            if key in process_mapping:
                processes = process_mapping[key]
                if processes:
                    # 使用该组中最常见的工序名称
                    rework_df.at[idx, '工序名称_返工'] = processes[0]
                    enhanced_count += 1

    print(f"通过产量数据匹配补充了 {enhanced_count} 条工序名称")

    # 基于历史数据的工序名称推测
    print("\n基于历史数据推测工序名称...")

    # 收集每个工人对每个款号最常做的工序
    worker_style_process = {}
    valid_records = rework_df[rework_df['工序名称_返工'] != '']

    if not valid_records.empty:
        for (worker, style), group in valid_records.groupby(['工人姓名', '款号_清洗']):
            processes = group['工序名称_返工'].value_counts()
            if len(processes) > 0:
                worker_style_process[(worker, style)] = processes.index[0]

    print(f"构建了 {len(worker_style_process)} 个工人-款号工序映射")

    # 应用历史映射
    historical_count = 0
    for idx, row in rework_df.iterrows():
        if row['工序名称_返工'] == '' and row['返工数量'] > 0:
            key = (row['工人姓名'], row['款号_清洗'])
            if key in worker_style_process:
                rework_df.at[idx, '工序名称_返工'] = worker_style_process[key]
                historical_count += 1

    print(f"通过历史数据补充了 {historical_count} 条工序名称")

    # 最终统计
    final_with_process = (rework_df['工序名称_返工'] != '').sum()
    final_without_process = (rework_df['工序名称_返工'] == '').sum()

    print(f"\n增强匹配完成:")
    print(f"  有工序名称的记录: {final_with_process} 条 ({final_with_process / len(rework_df) * 100:.1f}%)")
    print(f"  无工序名称的记录: {final_without_process} 条 ({final_without_process / len(rework_df) * 100:.1f}%)")

    return rework_df


def merge_rework_with_output_improved(output_data, rework_df):
    """改进的合并函数,确保工序名称不丢失"""
    print(f"\n{'=' * 60}")
    print("开始合并返工数据...")

    # 准备产量数据
    if '款号_清洗' not in output_data.columns:
        output_data['款号_清洗'] = output_data['款号'].apply(clean_style_number)
    if '日期' not in output_data.columns:
        output_data['日期'] = pd.to_datetime(output_data['生产时间']).dt.strftime('%Y-%m-%d')

    print(f"产量数据记录数: {len(output_data)}")
    print(f"返工数据记录数: {len(rework_df)}")
    print(f"返工数据中有工序名称的记录: {(rework_df['工序名称_返工'] != '').sum()} 条")

    # 第一步：增强工序名称匹配
    rework_df = enhance_process_name_matching(output_data, rework_df)

    # 添加一个标识列用于跟踪匹配过程
    output_data['_merge_id'] = range(len(output_data))

    # 策略1: 精确匹配 (款号+日期+工人)
    print("\n策略1: 款号+日期+工人 精确匹配...")

    # 准备返工数据 - 保留所有记录包括工序名称为空的
    rework_for_merge = rework_df[['款号_清洗', '日期', '工人姓名', '返工数量', '工序名称_返工']].copy()

    merged = pd.merge(
        output_data,
        rework_for_merge,
        left_on=['款号_清洗', '日期', '员工名称'],
        right_on=['款号_清洗', '日期', '工人姓名'],
        how='left',
        suffixes=('', '_rework')
    )
    match1 = merged['返工数量'].notna().sum()
    print(f"  匹配成功: {match1} 条")

    # 统计有工序名称的记录
    process_match1 = (merged['工序名称_返工'].notna() & (merged['工序名称_返工'] != '')).sum()
    print(f"  有工序名称的记录: {process_match1} 条")

    # 策略2: 对于未匹配的,尝试只用款号+日期匹配
    print("\n策略2: 款号+日期 宽松匹配 (用于未匹配的记录)...")

    # 找出未匹配的记录
    unmatched_mask = merged['返工数量'].isna()

    if unmatched_mask.sum() > 0:
        # 对返工数据按款号和日期聚合 - 关键修改:改进工序名称的聚合方式
        def aggregate_process_names(series):
            """聚合工序名称,保留非空值"""
            valid_names = [str(v).strip() for v in series if pd.notna(v) and str(v).strip() != '']
            if valid_names:
                # 返回出现频率最高的工序名称
                from collections import Counter
                counter = Counter(valid_names)
                return counter.most_common(1)[0][0]  # 返回最常见的工序名称
            return ''

        rework_agg = rework_df.groupby(['款号_清洗', '日期']).agg({
            '返工数量': 'sum',
            '工人姓名': lambda x: ','.join(sorted(set(x))),
            '工序名称_返工': aggregate_process_names
        }).reset_index()
        rework_agg.columns = ['款号_清洗', '日期', '返工数量_汇总', '返工工人', '返工工序_汇总']

        # 为未匹配的记录进行第二次匹配
        unmatched_data = merged[unmatched_mask][['_merge_id', '款号_清洗', '日期']].copy()
        matched2 = pd.merge(
            unmatched_data,
            rework_agg,
            on=['款号_清洗', '日期'],
            how='left'
        )

        # 将第二次匹配的结果填充回去
        for idx, row in matched2.iterrows():
            if pd.notna(row['返工数量_汇总']):
                merge_id = row['_merge_id']
                merged.loc[merged['_merge_id'] == merge_id, '返工数量'] = row['返工数量_汇总']
                # 只有当原工序名称为空时才填充汇总的工序名称
                current_process = merged.loc[merged['_merge_id'] == merge_id, '工序名称_返工'].iloc[0]
                if pd.isna(current_process) or current_process == '':
                    merged.loc[merged['_merge_id'] == merge_id, '工序名称_返工'] = row['返工工序_汇总']

        match2 = matched2['返工数量_汇总'].notna().sum()
        process_match2 = (matched2['返工工序_汇总'].notna() & (matched2['返工工序_汇总'] != '')).sum()
        print(f"  额外匹配: {match2} 条")
        print(f"  有工序名称的额外记录: {process_match2} 条")
    else:
        match2 = 0
        process_match2 = 0

    # 策略3: 对于仍然没有工序名称但返工数量>0的记录，尝试从其他匹配记录中获取工序名称
    print("\n策略3: 补充缺失的工序名称...")

    # 找出返工数量>0但工序名称为空的记录
    rework_no_process = merged[(merged['返工数量'] > 0) &
                               (merged['工序名称_返工'].isna() | (merged['工序名称_返工'] == ''))]

    if len(rework_no_process) > 0:
        print(f"  发现 {len(rework_no_process)} 条返工记录缺少工序名称")

        # 为这些记录查找可能的工序名称
        process_filled_count = 0
        for idx, row in rework_no_process.iterrows():
            merge_id = row['_merge_id']

            # 方法1: 从同一款号、日期、工人的其他返工记录中查找
            same_worker_records = rework_df[
                (rework_df['款号_清洗'] == row['款号_清洗']) &
                (rework_df['日期'] == row['日期']) &
                (rework_df['工人姓名'] == row['员工名称']) &
                (rework_df['工序名称_返工'] != '')
                ]

            if len(same_worker_records) > 0:
                # 使用最常见的工序名称
                process_names = same_worker_records['工序名称_返工'].value_counts()
                most_common_process = process_names.index[0]
                merged.loc[merged['_merge_id'] == merge_id, '工序名称_返工'] = most_common_process
                process_filled_count += 1
                continue

            # 方法2: 从同一款号、日期的其他返工记录中查找
            same_style_records = rework_df[
                (rework_df['款号_清洗'] == row['款号_清洗']) &
                (rework_df['日期'] == row['日期']) &
                (rework_df['工序名称_返工'] != '')
                ]

            if len(same_style_records) > 0:
                process_names = same_style_records['工序名称_返工'].value_counts()
                most_common_process = process_names.index[0]
                merged.loc[merged['_merge_id'] == merge_id, '工序名称_返工'] = most_common_process
                process_filled_count += 1

        print(f"  通过策略3补充了 {process_filled_count} 条工序名称")

    # 填充缺失值
    merged['返工数量'] = merged['返工数量'].fillna(0)
    merged['工序名称_返工'] = merged['工序名称_返工'].fillna('')

    # 删除辅助列
    columns_to_drop = ['工人姓名', '_merge_id']
    for col in columns_to_drop:
        if col in merged.columns:
            merged = merged.drop(columns=[col])

    print(f"\n合并完成:")
    print(f"  总记录数: {len(merged)}")
    total_rework = len(merged[merged['返工数量'] > 0])
    print(f"  有返工记录: {total_rework} ({total_rework / len(merged) * 100:.2f}%)")

    with_process = len(merged[(merged['返工数量'] > 0) & (merged['工序名称_返工'] != '')])
    without_process = len(merged[(merged['返工数量'] > 0) & (merged['工序名称_返工'] == '')])
    print(f"  有工序名称的返工记录: {with_process} 条 ({with_process / total_rework * 100:.1f}% of rework)")
    print(f"  无工序名称的返工记录: {without_process} 条 ({without_process / total_rework * 100:.1f}% of rework)")
    print(f"  返工总数量: {merged['返工数量'].sum():.0f}")

    # 显示一些样本数据用于验证
    print(f"\n返工记录样本(前5条):")
    sample = merged[merged['返工数量'] > 0][['款号', '日期', '员工名称', '返工数量', '工序名称_返工']].head()
    if not sample.empty:
        print(sample.to_string(index=False))

    # 显示有工序名称的样本
    process_sample = merged[(merged['返工数量'] > 0) & (merged['工序名称_返工'] != '')][
        ['款号', '日期', '员工名称', '返工数量', '工序名称_返工']].head()
    if not process_sample.empty:
        print(f"\n有工序名称的返工记录样本(前5条):")
        print(process_sample.to_string(index=False))

    # 显示无工序名称的样本
    no_process_sample = merged[(merged['返工数量'] > 0) & (merged['工序名称_返工'] == '')][
        ['款号', '日期', '员工名称', '返工数量', '工序名称_返工']].head()
    if not no_process_sample.empty:
        print(f"\n无工序名称的返工记录样本(前5条):")
        print(no_process_sample.to_string(index=False))

    return merged


def main():
    """主函数"""
    print("=" * 60)
    print("改进版返工数据提取与合并程序")
    print("=" * 60)

    # 1. 加载产量数据
    print("\n加载产量数据...")
    output_data = pd.read_excel('半年产量数据/3-8月车缝数据.xlsx')
    print(f"产量数据: {output_data.shape[0]} 行, {output_data.shape[1]} 列")

    # 2. 提取返工数据
    rework_df = extract_rework_data_improved()

    if not rework_df.empty:
        # 保存提取的返工数据
        rework_df.to_excel('改进提取的返工数据.xlsx', index=False)
        print(f"\n返工数据已保存到: 改进提取的返工数据.xlsx")

        # 显示统计信息
        print(f"\n返工数据统计:")
        print(f"  不同款号数: {rework_df['款号_清洗'].nunique()}")
        print(f"  不同工人数: {rework_df['工人姓名'].nunique()}")
        print(f"  日期范围: {rework_df['日期'].min()} 至 {rework_df['日期'].max()}")
        print(f"  返工总数: {rework_df['返工数量'].sum():.0f}")

        with_process = (rework_df['工序名称_返工'] != '').sum()
        without_process = (rework_df['工序名称_返工'] == '').sum()
        print(f"  有工序名称的记录: {with_process} 条 ({with_process / len(rework_df) * 100:.1f}%)")
        print(f"  无工序名称的记录: {without_process} 条 ({without_process / len(rework_df) * 100:.1f}%)")

        # 显示工序名称统计
        if (rework_df['工序名称_返工'] != '').any():
            process_stats = rework_df[rework_df['工序名称_返工'] != '']['工序名称_返工'].value_counts().head(10)
            print(f"\n最常见的工序名称 TOP 10:")
            for process, count in process_stats.items():
                print(f"  {process}: {count} 条记录")

        # 3. 合并数据
        merged_data = merge_rework_with_output_improved(output_data, rework_df)

        # 4. 保存结果
        print(f"\n保存结果...")
        merged_data.to_excel('产量数据_返工合并结果.xlsx', index=False)
        print(f"结果已保存到: 产量数据_返工合并结果.xlsx")

        # 5. 生成详细报告
        print(f"\n{'=' * 60}")
        print("详细统计报告")
        print(f"{'=' * 60}")

        print(f"\n按文件统计返工记录:")
        file_stats = rework_df.groupby('来源文件').agg({
            '返工数量': ['count', 'sum'],
            '工序名称_返工': lambda x: (x != '').sum()
        }).round(0)
        file_stats.columns = ['记录数', '返工总数', '有工序名称记录数']
        print(file_stats)

        print(f"\n返工最多的工人 TOP 10:")
        worker_stats = rework_df.groupby('工人姓名')['返工数量'].sum().sort_values(ascending=False).head(10)
        for worker, qty in worker_stats.items():
            print(f"  {worker}: {qty:.0f}")

    else:
        print("\n⚠️ 未能提取到返工数据,程序结束")


if __name__ == "__main__":
    main()