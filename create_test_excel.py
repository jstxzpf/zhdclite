#!/usr/bin/env python3
"""
创建测试用的地方点数据Excel文件
"""

import pandas as pd
import os

def create_test_excel():
    """创建测试Excel文件"""
    
    # 创建测试数据，包含可能出现.0问题的数据
    test_data = {
        '户代码': [
            '3212830010021154',  # 正常户代码
            '3212830010021083',  # 正常户代码
            '3212830010050524'   # 正常户代码
        ],
        '编码': [
            '310101',  # 食品编码
            '310102',  # 食品编码
            '310103'   # 食品编码
        ],
        '数量': [
            '1',
            '2.5',
            '3'
        ],
        '金额': [
            '100.50',
            '200.75',
            '300.25'
        ],
        '年': [
            '2024',
            '2024',
            '2024'
        ],
        '月': [
            '12',
            '12',
            '12'
        ],
        '日': [
            '15',    # 有效日期
            '',      # 空日期，应该使用默认值
            '25'     # 有效日期
        ]
    }
    
    df = pd.DataFrame(test_data)
    
    # 保存到uploads目录
    uploads_dir = 'uploads'
    if not os.path.exists(uploads_dir):
        os.makedirs(uploads_dir)
    
    file_path = os.path.join(uploads_dir, 'test_local_data.xlsx')
    df.to_excel(file_path, index=False)
    
    print(f"测试Excel文件已创建: {file_path}")
    print("\n文件内容:")
    print(df)
    
    return file_path

def create_problematic_excel():
    """创建包含.0问题的Excel文件（模拟从其他系统导出的数据）"""
    
    # 创建包含浮点数的数据（会产生.0后缀）
    test_data = {
        '户代码': [
            3212830010021154.0,  # 数字格式，会产生.0
            3212830010021083.0,  # 数字格式，会产生.0
            3212830010050524.0   # 数字格式，会产生.0
        ],
        '编码': [
            310101.0,  # 数字格式，会产生.0
            310102.0,  # 数字格式，会产生.0
            310103.0   # 数字格式，会产生.0
        ],
        '数量': [
            1.0,
            2.5,
            3.0
        ],
        '金额': [
            100.50,
            200.75,
            300.25
        ],
        '年': [
            2024.0,
            2024.0,
            2024.0
        ],
        '月': [
            12.0,
            12.0,
            12.0
        ],
        '日': [
            15.0,    # 有效日期
            None,    # 空日期，会变成NaN
            25.0     # 有效日期
        ]
    }
    
    df = pd.DataFrame(test_data)
    
    # 保存到uploads目录
    uploads_dir = 'uploads'
    if not os.path.exists(uploads_dir):
        os.makedirs(uploads_dir)
    
    file_path = os.path.join(uploads_dir, 'test_problematic_data.xlsx')
    df.to_excel(file_path, index=False)
    
    print(f"\n问题Excel文件已创建: {file_path}")
    print("\n文件内容（包含.0后缀问题）:")
    print(df)
    
    # 验证读取后的效果
    df_read = pd.read_excel(file_path)
    print("\n直接读取的效果（会有.0问题）:")
    print(df_read)
    
    return file_path

def main():
    """主函数"""
    print("创建测试用的地方点数据Excel文件...")
    
    # 创建正常的测试文件
    normal_file = create_test_excel()
    
    # 创建有问题的测试文件
    problematic_file = create_problematic_excel()
    
    print(f"\n✅ 测试文件创建完成:")
    print(f"  正常文件: {normal_file}")
    print(f"  问题文件: {problematic_file}")
    
    print(f"\n📝 测试说明:")
    print(f"  1. 使用 {normal_file} 测试正常情况")
    print(f"  2. 使用 {problematic_file} 测试修复效果")
    print(f"  3. 通过Web界面导入这些文件，观察是否还有.0后缀问题")
    print(f"  4. 检查ri列数据是否正确导入到date字段")

if __name__ == "__main__":
    main()
