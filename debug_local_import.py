#!/usr/bin/env python3
"""
调试地方点数据导入问题
"""

import sys
import os
import pandas as pd
import tempfile

# 添加src目录到路径
sys.path.insert(0, 'src')

# 修复相对导入问题
import database_pool
from database_pool import get_connection_pool, get_external_connection_pool

# 现在可以导入Database类
from database import Database
from excel_operations import ExcelOperations

def create_test_excel():
    """创建测试Excel文件"""
    test_data = {
        '户代码': ['3212830010021154', '3212830010021083'],
        '编码': ['310101', '310102'],
        '数量': ['1', '2'],
        '金额': ['100', '200'],
        '年': ['2024', '2024'],
        '月': ['12', '12'],
        '日': ['15', '16']
    }
    
    df = pd.DataFrame(test_data)
    
    # 创建临时Excel文件
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
        df.to_excel(tmp_file.name, index=False)
        return tmp_file.name

def test_database_connection():
    """测试数据库连接"""
    try:
        db = Database()
        # 测试简单查询
        result = db.execute_query_safe("SELECT 1 as test")
        print(f"✅ 数据库连接成功: {result}")
        return True
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        return False

def test_table_exists():
    """测试表是否存在"""
    try:
        db = Database()
        
        # 检查地方点待导入表是否存在
        result = db.execute_query_safe("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = '地方点待导入'
        """)
        
        if result and result[0][0] > 0:
            print("✅ 地方点待导入表存在")
        else:
            print("❌ 地方点待导入表不存在")
            return False
        
        # 检查调查点台账合并表是否存在
        result = db.execute_query_safe("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = '调查点台账合并'
        """)
        
        if result and result[0][0] > 0:
            print("✅ 调查点台账合并表存在")
            return True
        else:
            print("❌ 调查点台账合并表不存在")
            return False
            
    except Exception as e:
        print(f"❌ 检查表存在性失败: {e}")
        return False

def test_simple_insert():
    """测试简单的INSERT语句"""
    try:
        db = Database()
        
        # 创建测试数据
        test_data = {
            'hudaima': ['test123'],
            'bianma': ['310101'],
            'shuliang': ['1'],
            'jine': ['100'],
            'nian': ['2024'],
            'yue': ['12'],
            'ri': ['15']
        }
        
        df = pd.DataFrame(test_data)
        
        # 导入到临时表
        import_result = db.import_data(df, '地方点待导入')
        print(f"✅ 测试数据导入成功: {import_result}")
        
        # 测试简单的SELECT
        result = db.execute_query_safe("SELECT TOP 1 * FROM 地方点待导入")
        print(f"✅ 查询测试数据成功: {len(result)} 行")
        
        # 测试简单的INSERT INTO ... SELECT
        simple_insert = """
        INSERT INTO [调查点台账合并] (hudm, code, amount, money, year, month, z_guid, id, wton, ntow)
        SELECT 
            cast([hudaima] as varchar(50)) as hudm,
            cast([bianma] as varchar(50)) as code,
            1.0 as amount,
            100.0 as money,
            cast([nian] as varchar(4)) as year,
            cast([yue] as varchar(2)) as month,
            NEWID() as z_guid,
            2000000001 as id,
            '1' as wton,
            '0' as ntow
        FROM 地方点待导入
        WHERE hudaima = 'test123'
        """
        
        with db.pool.get_cursor() as cursor:
            cursor.execute(simple_insert)
            inserted_count = cursor.rowcount
            print(f"✅ 简单INSERT成功: {inserted_count} 行")
        
        return True
        
    except Exception as e:
        print(f"❌ 简单INSERT测试失败: {e}")
        return False

def main():
    """主测试函数"""
    print("开始调试地方点数据导入问题...")
    
    tests = [
        ("数据库连接", test_database_connection),
        ("表存在性检查", test_table_exists),
        ("简单INSERT测试", test_simple_insert),
    ]
    
    for test_name, test_func in tests:
        print(f"\n{'='*50}")
        print(f"测试: {test_name}")
        print('='*50)
        
        try:
            success = test_func()
            if not success:
                print(f"❌ {test_name} 失败，停止后续测试")
                return False
        except Exception as e:
            print(f"❌ {test_name} 异常: {e}")
            return False
    
    print(f"\n{'='*50}")
    print("所有测试完成")
    print('='*50)
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
