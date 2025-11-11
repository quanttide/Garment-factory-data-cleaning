import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime, timedelta
import re

# 设置日志输出
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def read_attendance_data(file_path):
    """读取考勤数据Excel文件"""
    try:
        logging.info(f"开始读取考勤数据文件: {file_path}")
        xl = pd.ExcelFile(file_path)
        logging.info(f"文件包含的sheet: {xl.sheet_names}")

        all_attendance_data = {}

        for sheet_name in xl.sheet_names:
            logging.info(f"正在处理sheet: {sheet_name}")
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
            logging.info(f"sheet '{sheet_name}' 的形状: {df.shape}")

            processed_sheet = process_attendance_sheet(df, sheet_name)
            if processed_sheet:
                all_attendance_data[sheet_name] = processed_sheet

        return all_attendance_data

    except Exception as e:
        logging.error(f"读取考勤数据时出错: {e}")
        return None


def process_attendance_sheet(df, sheet_name):
    """处理单个考勤表sheet"""
    try:
        start_row = find_employee_data_start(df)
        if start_row is None:
            logging.warning(f"在sheet '{sheet_name}' 中未找到员工数据起始行")
            return None

        logging.info(f"员工数据起始行: {start_row}")

        dates = extract_dates_from_sheet(df, sheet_name)
        logging.info(f"提取到的日期范围: {len(dates)} 天")

        employees = extract_employee_attendance_from_sheet(df, start_row, dates)
        logging.info(f"从sheet '{sheet_name}' 中提取到 {len(employees)} 名员工数据")

        return {
            'dates': dates,
            'employees': employees,
            'start_row': start_row
        }

    except Exception as e:
        logging.error(f"处理考勤表sheet时出错: {e}")
        return None


def find_employee_data_start(df):
    """查找员工数据的起始行"""
    for i in range(len(df)):
        row = df.iloc[i]
        for j in range(min(5, len(row))):
            if pd.notna(row.iloc[j]):
                cell_str = str(row.iloc[j]).strip()
                if cell_str.startswith('RF') and len(cell_str) >= 6:
                    logging.info(f"在第 {i} 行第 {j} 列找到员工编号: {cell_str}")
                    return i
    return None


def extract_dates_from_sheet(df, sheet_name):
    """从考勤表sheet中提取日期信息"""
    try:
        month_match = re.search(r'(\d+)月', sheet_name)

        if month_match:
            month = int(month_match.group(1))
            year = 2025

            start_date = datetime(year, month, 11)
            if month == 12:
                end_date = datetime(year + 1, 1, 10)
            else:
                end_date = datetime(year, month + 1, 10)

            date_range = []
            current_date = start_date
            while current_date <= end_date:
                date_range.append(current_date)
                current_date += timedelta(days=1)

            logging.info(f"生成日期范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
            return date_range

        return []

    except Exception as e:
        logging.error(f"提取日期时出错: {e}")
        return []


def extract_employee_attendance_from_sheet(df, start_row, dates):
    """从考勤表中提取员工出勤信息"""
    employees = []

    try:
        i = start_row
        while i < len(df):
            row = df.iloc[i]

            # 查找员工编号（应该在第2列，索引为1）
            is_employee_row = False
            employee_id = None
            employee_name = None

            # 检查第2列是否有RF开头的员工编号
            if len(row) > 1 and pd.notna(row.iloc[1]):
                cell_str = str(row.iloc[1]).strip()
                if cell_str.startswith('RF') and len(cell_str) >= 6:
                    is_employee_row = True
                    employee_id = cell_str

                    # 员工姓名应该在第3列（索引为2）
                    if len(row) > 2 and pd.notna(row.iloc[2]):
                        employee_name = str(row.iloc[2]).strip()

            if is_employee_row and employee_name:
                employee_data = {
                    'employee_id': employee_id,
                    'name': employee_name,
                    'attendance': {}
                }

                # 出勤数据从第7列开始（索引为6）
                date_start_col = 6

                date_index = 0
                for j in range(date_start_col, min(date_start_col + len(dates), len(row))):
                    if date_index < len(dates):
                        date_key = dates[date_index]
                        if pd.notna(row.iloc[j]):
                            attendance_value = str(row.iloc[j]).strip()
                            # 保留原始文本，只去除不必要的空格
                            attendance_value = re.sub(r'\s+', '', attendance_value)
                            employee_data['attendance'][date_key] = attendance_value
                        date_index += 1

                employees.append(employee_data)
                logging.info(f"提取员工: {employee_data['employee_id']} - {employee_data['name']}")
            else:
                break

            i += 1

        return employees

    except Exception as e:
        logging.error(f"提取员工考勤数据时出错: {e}")
        return []


def read_production_data(file_path):
    """读取产量数据Excel文件"""
    try:
        logging.info(f"开始读取产量数据文件: {file_path}")
        df = pd.read_excel(file_path)
        logging.info(f"产量数据形状: {df.shape}")
        logging.info(f"产量数据列名: {df.columns.tolist()}")

        if len(df) > 0:
            logging.info(f"产量数据前3行:\n{df.head(3)}")

        return df

    except Exception as e:
        logging.error(f"读取产量数据时出错: {e}")
        return None


def match_production_attendance(production_df, attendance_data):
    """将产量数据与考勤数据进行匹配"""
    try:
        logging.info("开始匹配产量数据和考勤数据")

        matched_data = production_df.copy()
        matched_data['出勤时间'] = ''
        matched_data['matched_date'] = ''
        matched_data['attendance_source'] = ''

        match_count = 0
        no_match_count = 0

        # 收集所有员工姓名用于调试
        attendance_names = set()
        production_names = set()

        for sheet_name, sheet_data in attendance_data.items():
            for employee in sheet_data.get('employees', []):
                if employee['name']:
                    attendance_names.add(employee['name'].strip())

        for idx, prod_row in production_df.iterrows():
            if '员工名称' in prod_row.index and pd.notna(prod_row['员工名称']):
                production_names.add(str(prod_row['员工名称']).strip())

        logging.info(f"考勤数据中员工姓名数量: {len(attendance_names)}")
        logging.info(f"产量数据中员工姓名数量: {len(production_names)}")

        common_names = attendance_names.intersection(production_names)
        logging.info(f"共同的员工姓名数量: {len(common_names)}")

        if len(common_names) > 0:
            logging.info(f"前10个共同员工姓名: {list(common_names)[:10]}")
        else:
            logging.warning("没有找到共同的员工姓名！")
            logging.info(f"考勤数据中的前10个姓名: {list(attendance_names)[:10]}")
            logging.info(f"产量数据中的前10个姓名: {list(production_names)[:10]}")

        # 遍历产量数据进行匹配
        for idx, prod_row in production_df.iterrows():
            employee_name = None
            production_date = None

            if '员工名称' in prod_row.index and pd.notna(prod_row['员工名称']):
                employee_name = str(prod_row['员工名称']).strip()
                employee_name = re.sub(r'\s+', '', employee_name)

            if '生产时间' in prod_row.index and pd.notna(prod_row['生产时间']):
                production_date = prod_row['生产时间']

            if employee_name and production_date is not None:
                attendance_info = find_attendance_for_employee(employee_name, production_date, attendance_data)

                if attendance_info:
                    matched_data.at[idx, '出勤时间'] = attendance_info.get('attendance_value', '')
                    matched_data.at[idx, 'matched_date'] = attendance_info.get('date', '')
                    matched_data.at[idx, 'attendance_source'] = attendance_info.get('source', '')
                    match_count += 1
                else:
                    # 没有匹配到考勤数据，保持空白
                    no_match_count += 1

        logging.info(f"成功匹配 {match_count} 条记录，无法匹配 {no_match_count} 条记录")

        # 分析未匹配的原因
        analyze_unmatched_records(matched_data, production_df, attendance_data)

        if len(production_df) > 0:
            match_rate = (match_count / len(production_df)) * 100
            logging.info(f"匹配率: {match_rate:.2f}%")

        return matched_data

    except Exception as e:
        logging.error(f"匹配数据时出错: {e}")
        return production_df


def find_attendance_for_employee(employee_name, production_date, attendance_data):
    """在考勤数据中查找指定员工和日期的出勤信息"""
    try:
        prod_date = parse_date(production_date)
        if prod_date is None:
            return None

        clean_employee_name = re.sub(r'\s+', '', employee_name)

        for sheet_name, sheet_data in attendance_data.items():
            for employee in sheet_data.get('employees', []):
                clean_attendance_name = re.sub(r'\s+', '', employee['name']) if employee['name'] else ''

                # 使用更宽松的匹配策略
                if (clean_attendance_name and
                        (clean_attendance_name == clean_employee_name or
                         clean_employee_name in clean_attendance_name or
                         clean_attendance_name in clean_employee_name)):

                    for date_key, attendance_value in employee['attendance'].items():
                        if date_key.date() == prod_date.date():
                            return {
                                'attendance_value': attendance_value,
                                'date': date_key.strftime('%Y-%m-%d'),
                                'source': sheet_name
                            }

        return None

    except Exception as e:
        logging.error(f"查找员工考勤信息时出错: {e}")
        return None


def analyze_unmatched_records(matched_data, production_df, attendance_data):
    """分析未匹配的记录"""
    try:
        unmatched_records = matched_data[matched_data['出勤时间'] == '']

        if len(unmatched_records) > 0:
            logging.info(f"分析 {len(unmatched_records)} 条未匹配记录:")

            # 分析未匹配的员工
            unmatched_employees = unmatched_records['员工名称'].value_counts().head(10)
            logging.info(f"未匹配次数最多的前10名员工:\n{unmatched_employees}")

            # 分析未匹配的日期范围
            unmatched_dates = unmatched_records['生产时间'].apply(
                lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else '未知').value_counts().head(10)
            logging.info(f"未匹配次数最多的前10个日期:\n{unmatched_dates}")

            # 检查考勤数据中是否存在这些员工
            attendance_employees = set()
            for sheet_name, sheet_data in attendance_data.items():
                for employee in sheet_data.get('employees', []):
                    if employee['name']:
                        attendance_employees.add(re.sub(r'\s+', '', employee['name']))

            unmatched_employee_set = set()
            for employee in unmatched_records['员工名称'].unique():
                if pd.notna(employee):
                    unmatched_employee_set.add(re.sub(r'\s+', '', employee))

            missing_in_attendance = unmatched_employee_set - attendance_employees
            if missing_in_attendance:
                logging.info(f"以下员工在考勤数据中完全不存在: {list(missing_in_attendance)[:10]}")

    except Exception as e:
        logging.error(f"分析未匹配记录时出错: {e}")


def parse_date(date_value):
    """解析日期值"""
    try:
        if isinstance(date_value, datetime):
            return date_value
        elif isinstance(date_value, str):
            for fmt in ['%Y/%m/%d', '%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y', '%Y.%m.%d']:
                try:
                    return datetime.strptime(date_value, fmt)
                except:
                    continue

        try:
            parsed_date = pd.to_datetime(date_value)
            if pd.notna(parsed_date):
                return parsed_date.to_pydatetime()
        except:
            pass

        return None

    except Exception as e:
        logging.debug(f"解析日期时出错: {e}")
        return None


def main():
    """主函数"""
    logging.info("开始处理产量数据和考勤数据匹配 - 保留原始文本版本")

    # 文件路径
    production_file = r"C:\Users\33163\PycharmProjects\PythonProject1\半年产量数据\3-8月车缝数据.xlsx"
    attendance_file = r"C:\Users\33163\PycharmProjects\PythonProject1\考勤\缝制考勤数据_2025年9月.xlsx"

    # 检查文件是否存在
    if not os.path.exists(attendance_file):
        logging.error(f"考勤文件不存在: {attendance_file}")
        return

    if not os.path.exists(production_file):
        logging.error(f"产量文件不存在: {production_file}")
        return

    # 读取数据
    attendance_data = read_attendance_data(attendance_file)
    production_data = read_production_data(production_file)

    if attendance_data is None or production_data is None:
        logging.error("数据读取失败，程序终止")
        return

    # 匹配数据
    matched_data = match_production_attendance(production_data, attendance_data)

    # 保存结果
    output_file = "产量数据_考勤合并数据.xlsx"
    try:
        matched_data.to_excel(output_file, index=False)
        logging.info(f"匹配结果已保存到: {output_file}")

        # 统计信息
        total_records = len(matched_data)
        matched_records = len(matched_data[matched_data['出勤时间'] != ''])
        logging.info(f"匹配统计: {matched_records}/{total_records} 条记录成功匹配")

        if matched_records > 0:
            # 显示出勤时间的分布情况
            attendance_counts = matched_data['出勤时间'].value_counts()
            logging.info(f"出勤时间分布:\n{attendance_counts}")

    except Exception as e:
        logging.error(f"保存结果时出错: {e}")

    logging.info("程序执行完成")


if __name__ == "__main__":
    main()