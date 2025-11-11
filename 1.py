import pandas as pd
import os
from glob import glob
import re
import time


def clean_style_number(style_num):
    """清洗款号,去除多余的符号如-Q3"""
    if pd.isna(style_num):
        return style_num
    return str(style_num).split('-')[0]


def load_process_tables():
    """加载所有工序表文件 - 优化版"""
    print("正在读取工序表文件...")
    start_time = time.time()

    process_files = glob('工序表/*.xlsx')
    print(f"找到 {len(process_files)} 个工序表文件")

    all_process_dfs = []

    for file_path in process_files:
        try:
            file_name = os.path.basename(file_path)
            print(f"处理文件: {file_name}")

            # 从文件名提取同款信息
            style_pattern = r'(\d+[A-Z]\d+|\d+[A-Z][A-Z]\d+)'
            style_numbers = re.findall(style_pattern, file_name)

            # 【优化1】使用ExcelFile对象,避免重复读取
            xl = pd.ExcelFile(file_path)

            for sheet_name in xl.sheet_names:
                # 【优化2】使用parse()而不是read_excel()
                df = xl.parse(sheet_name)

                # 添加文件来源信息
                df['来源文件'] = file_name
                df['来源sheet'] = sheet_name

                # 如果文件中包含多个款号,为每个款号创建记录
                if style_numbers:
                    df['文件同款'] = ','.join(style_numbers)

                all_process_dfs.append(df)

        except Exception as e:
            print(f"读取文件 {file_path} 时出错: {e}")

    if all_process_dfs:
        process_df = pd.concat(all_process_dfs, ignore_index=True)
        print(f"合并后的工序表数据形状: {process_df.shape}")
        print(f"读取耗时: {time.time() - start_time:.2f}秒")
        return process_df
    else:
        print("没有成功读取到任何工序表数据")
        return pd.DataFrame()


def filter_process_data(process_df):
    """过滤工序表数据,去除裁床和尾部工序 - 优化版"""
    print("正在过滤工序表数据...")
    start_time = time.time()

    # 查找工序类别列
    category_col = None
    for col in process_df.columns:
        if any(keyword in str(col) for keyword in ['工序类别', '类别', '类型', '工序类型']):
            category_col = col
            break

    if category_col:
        print(f"使用列 '{category_col}' 作为工序类别列")

        # 【优化3】先转换一次,避免重复转换
        category_str = process_df[category_col].astype(str)

        # 过滤掉裁床和尾部工序
        original_count = len(process_df)
        mask = ~category_str.str.contains('裁床|尾部', na=False, regex=True)
        process_df = process_df[mask].copy()
        filtered_count = len(process_df)
        print(f"过滤掉 {original_count - filtered_count} 条裁床/尾部工序记录")

        # 【优化4】使用向量化操作提取部位信息
        print("正在提取部位信息...")
        location_keywords = ['里布', '腰头', '小部件', '门筒', '袖口', '下摆', '帽子', '面布', '前幅', '后幅', '领子',
                             '袖子']

        # 重新获取category_str(因为已经过滤了)
        category_str = process_df[category_col].astype(str)

        # 使用向量化的方式查找关键词
        process_df['部位'] = '其他'
        for keyword in location_keywords:
            mask = category_str.str.contains(keyword, na=False)
            process_df.loc[mask, '部位'] = keyword

        print("部位分布:")
        print(process_df['部位'].value_counts())
        print(f"过滤耗时: {time.time() - start_time:.2f}秒")

    return process_df


def create_style_mapping(process_df):
    """创建同款款式映射 - 优化版"""
    print("\n正在创建同款款式映射...")
    start_time = time.time()

    same_style_map = {}

    # 1. 从文件名提取同款信息
    for file_name in process_df['来源文件'].unique():
        style_pattern = r'(\d+[A-Z]\d+|\d+[A-Z][A-Z]\d+)'
        style_numbers = re.findall(style_pattern, file_name)

        if len(style_numbers) > 1:
            main_style = style_numbers[0]
            for style in style_numbers[1:]:
                same_style_map[style] = main_style
                print(f"从文件名识别同款: {style} -> {main_style}")

    # 【优化5】使用groupby优化同款查找,避免嵌套循环
    if '款式描述' in process_df.columns and '款号' in process_df.columns:
        style_descriptions = process_df[['款号', '款式描述']].dropna().drop_duplicates()

        # 按款式描述分组
        grouped = style_descriptions.groupby('款式描述')['款号'].apply(list)

        # 对于有多个款号的描述,建立映射关系
        for desc, styles in grouped.items():
            if len(styles) > 1:
                main_style = styles[0]
                for style in styles[1:]:
                    same_style_map[style] = main_style
                    print(f"从款式描述识别同款: {style} -> {main_style} (描述: {desc})")

    # 3. 手动添加已知同款
    manual_mapping = {
        '12C1469': '12C1450',
        '12C1451': '12C1450',
        '12C1505': '12C1450',
        '71G0002': '71G0009',
        '12C1572': '12C1450',
        '67C0015': '12C1470',
        '17A0025': '16A0509',
        '18A0002': '16A0509'
    }

    same_style_map.update(manual_mapping)

    print(f"总共创建了 {len(same_style_map)} 个同款映射")
    print(f"映射创建耗时: {time.time() - start_time:.2f}秒")
    return same_style_map


def merge_data(output_data, process_df, same_style_map):
    """合并产量数据和工序表数据 - 优化版"""
    print("\n正在合并数据...")
    start_time = time.time()

    # 【优化6】使用向量化操作清洗款号
    print("清洗产量数据款号...")
    if '款号' in output_data.columns:
        # 向量化操作:先转字符串,再用str.split()
        output_data['款号_清洗'] = output_data['款号'].astype(str).str.split('-').str[0]
        print(f"款号清洗示例:")
        print(output_data[['款号', '款号_清洗']].head())
    else:
        print("警告: 产量数据中未找到'款号'列")
        print("可用列:", output_data.columns.tolist())
        return output_data

    # 在工序表中应用同款映射
    if '款号' in process_df.columns:
        process_df['款号_主款'] = process_df['款号'].replace(same_style_map)
        process_df['款号_主款'] = process_df['款号_主款'].fillna(process_df['款号'])
    else:
        print("警告: 工序表中未找到'款号'列")
        print("可用列:", process_df.columns.tolist())
        return output_data

    # 准备合并键
    left_on = ['款号_清洗']
    right_on = ['款号_主款']

    # 如果存在工序号,也加入合并键
    if '工序号' in output_data.columns and '工序号' in process_df.columns:
        left_on.append('工序号')
        right_on.append('工序号')
        print("使用款号+工序号作为合并键")
    else:
        print("使用款号作为合并键")

    # 执行合并
    merged_data = pd.merge(
        output_data,
        process_df,
        left_on=left_on,
        right_on=right_on,
        how='left',
        suffixes=('_产量', '_工序')
    )

    print(f"合并完成: {len(output_data)} -> {len(merged_data)} 条记录")
    print(f"合并耗时: {time.time() - start_time:.2f}秒")

    return merged_data


def handle_process_names(merged_data):
    """处理工序名称列,确保有三列工序名称"""
    print("\n正在处理工序名称列...")

    # 查找所有工序名称列
    process_name_cols = [col for col in merged_data.columns if '工序名称' in col]
    print(f"找到的工序名称列: {process_name_cols}")

    # 保留产量数据中的原始工序名称
    output_process_col = None
    for col in process_name_cols:
        if '_产量' in col or col == '工序名称':
            output_process_col = col
            break

    # 从工序表中找出两个工序名称列
    process_cols = [col for col in process_name_cols if col != output_process_col]

    # 确保有三列工序名称
    final_process_cols = []

    # 第一列:产量数据中的工序名称
    if output_process_col:
        final_process_cols.append(output_process_col)
    else:
        merged_data['工序名称_产量'] = ''
        final_process_cols.append('工序名称_产量')

    # 第二、三列:工序表中的两个工序名称列
    for i, col in enumerate(process_cols[:2]):
        final_process_cols.append(col)

    # 如果不足三列,补充空列
    while len(final_process_cols) < 3:
        new_col_name = f'工序名称_补充{len(final_process_cols) - 1}'
        merged_data[new_col_name] = ''
        final_process_cols.append(new_col_name)

    print(f"最终保留的三列工序名称: {final_process_cols}")

    # 重命名以便识别
    rename_map = {}
    if len(final_process_cols) >= 1:
        rename_map[final_process_cols[0]] = '工序名称_产量'
    if len(final_process_cols) >= 2:
        rename_map[final_process_cols[1]] = '工序名称_工序表1'
    if len(final_process_cols) >= 3:
        rename_map[final_process_cols[2]] = '工序名称_工序表2'

    merged_data = merged_data.rename(columns=rename_map)

    return merged_data


def save_large_dataframe(df, base_filename, max_rows=1000000):
    """将大型DataFrame分割成多个文件保存,避免Excel行数限制"""
    print(f"\n正在保存数据...")
    start_time = time.time()

    total_rows = len(df)
    if total_rows <= max_rows:
        df.to_excel(f"{base_filename}.xlsx", index=False, engine='openpyxl')
        print(f"数据已保存至: {base_filename}.xlsx")
        print(f"保存耗时: {time.time() - start_time:.2f}秒")
        return

    # 计算需要分成几个文件
    num_files = (total_rows // max_rows) + 1
    print(f"数据量过大 ({total_rows} 行),将分割成 {num_files} 个文件")

    for i in range(num_files):
        start_idx = i * max_rows
        end_idx = min((i + 1) * max_rows, total_rows)

        filename = f"{base_filename}_part{i + 1}.xlsx"
        df.iloc[start_idx:end_idx].to_excel(filename, index=False, engine='openpyxl')
        print(f"部分 {i + 1} 已保存至: {filename} (行 {start_idx + 1}-{end_idx})")

    print(f"总保存耗时: {time.time() - start_time:.2f}秒")


def main():
    """主函数"""
    print("=" * 60)
    print("开始处理产量数据与工序表合并...")
    print("=" * 60)
    total_start = time.time()

    # 1. 读取产量数据
    print("\n1. 读取产量数据...")
    try:
        output_data = pd.read_excel('半年产量数据/3-8月车缝数据.xlsx')
        print(f"产量数据形状: {output_data.shape}")
        print("产量数据列名:", output_data.columns.tolist())
    except Exception as e:
        print(f"读取产量数据失败: {e}")
        return

    # 2. 加载和处理工序表
    process_df = load_process_tables()
    if process_df.empty:
        print("无法继续处理:工序表数据为空")
        return

    print("\n工序表列名:", process_df.columns.tolist())

    # 3. 过滤工序表数据
    process_df = filter_process_data(process_df)

    # 4. 创建同款映射
    same_style_map = create_style_mapping(process_df)

    # 5. 合并数据
    merged_data = merge_data(output_data, process_df, same_style_map)

    # 6. 处理工序名称
    merged_data = handle_process_names(merged_data)

    # 7. 保存结果
    print("\n正在保存结果...")
    save_large_dataframe(merged_data, '产量数据_工序表合并结果')

    # 保存未匹配的记录
    unmatched_mask = merged_data['款号_主款'].isna()
    if unmatched_mask.any():
        unmatched_data = merged_data[unmatched_mask]
        save_large_dataframe(unmatched_data, '未匹配的产量记录', max_rows=1000000)

        # 分析未匹配的款号
        unmatched_styles = unmatched_data['款号_清洗'].unique()
        print(f"\n未匹配的款号数量: {len(unmatched_styles)}")
        print(f"未匹配的款号: {list(unmatched_styles)}")

    # 生成报告
    total_records = len(merged_data)
    matched_records = len(merged_data[~merged_data['款号_主款'].isna()])
    match_rate = matched_records / total_records * 100

    print("\n" + "=" * 60)
    print("=== 处理完成 ===")
    print("=" * 60)
    print(f"总记录数: {total_records:,}")
    print(f"成功匹配: {matched_records:,} ({match_rate:.2f}%)")
    print(f"未匹配: {total_records - matched_records:,}")
    print(f"同款映射数量: {len(same_style_map)}")
    print(f"\n总耗时: {time.time() - total_start:.2f}秒")
    print("=" * 60)


if __name__ == "__main__":
    main()