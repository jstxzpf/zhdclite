#!/usr/bin/env python3
"""
SQL Server到SQLite的SQL语句转换工具
"""

import re
import logging

logger = logging.getLogger(__name__)

class SQLConverter:
    """SQL Server到SQLite的转换器"""
    
    @staticmethod
    def convert_sql(sql):
        """
        将SQL Server语法转换为SQLite兼容语法
        
        Args:
            sql (str): 原始SQL语句
            
        Returns:
            str: 转换后的SQLite兼容SQL语句
        """
        if not sql:
            return sql
            
        # 转换后的SQL
        converted_sql = sql
        
        # 1. ISNULL() -> COALESCE() 或 IFNULL()
        converted_sql = re.sub(
            r'\bISNULL\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
            r'COALESCE(\1, \2)',
            converted_sql,
            flags=re.IGNORECASE
        )
        
        # 2. NEWID() -> 使用随机字符串或UUID
        converted_sql = re.sub(
            r'\bNEWID\s*\(\s*\)',
            r"lower(hex(randomblob(16)))",
            converted_sql,
            flags=re.IGNORECASE
        )
        
        # 3. TRY_CAST() -> CAST() (需要在应用层处理错误)
        converted_sql = re.sub(
            r'\bTRY_CAST\s*\(\s*([^)]+)\s+AS\s+([^)]+)\s*\)',
            r'CAST(\1 AS \2)',
            converted_sql,
            flags=re.IGNORECASE
        )
        
        # 4. TRY_CONVERT() -> CAST()
        converted_sql = re.sub(
            r'\bTRY_CONVERT\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
            r'CAST(\2 AS \1)',
            converted_sql,
            flags=re.IGNORECASE
        )
        
        # 5. 移除COLLATE Chinese_PRC_CI_AS
        converted_sql = re.sub(
            r'\s+COLLATE\s+Chinese_PRC_CI_AS',
            '',
            converted_sql,
            flags=re.IGNORECASE
        )
        
        # 6. SMALLDATETIME -> DATETIME
        converted_sql = re.sub(
            r'\bSMALLDATETIME\b',
            'DATETIME',
            converted_sql,
            flags=re.IGNORECASE
        )
        
        # 7. 转换数据类型
        type_mappings = {
            r'\bVARCHAR\s*\(\s*MAX\s*\)': 'TEXT',
            r'\bNVARCHAR\s*\(\s*MAX\s*\)': 'TEXT',
            r'\bTEXT\b': 'TEXT',
            r'\bNTEXT\b': 'TEXT',
            r'\bBIT\b': 'INTEGER',
            r'\bTINYINT\b': 'INTEGER',
            r'\bSMALLINT\b': 'INTEGER',
            r'\bBIGINT\b': 'INTEGER',
            r'\bFLOAT\b': 'REAL',
            r'\bREAL\b': 'REAL',
            r'\bMONEY\b': 'REAL',
            r'\bSMALLMONEY\b': 'REAL',
            r'\bDECIMAL\b': 'REAL',
            r'\bNUMERIC\b': 'REAL',
        }
        
        for pattern, replacement in type_mappings.items():
            converted_sql = re.sub(pattern, replacement, converted_sql, flags=re.IGNORECASE)
        
        # 8. 处理TOP子句 - 转换为LIMIT
        converted_sql = re.sub(
            r'\bSELECT\s+TOP\s+(\d+)\s+',
            r'SELECT ',
            converted_sql,
            flags=re.IGNORECASE
        )
        
        # 如果有TOP子句，需要在末尾添加LIMIT
        top_match = re.search(r'\bSELECT\s+TOP\s+(\d+)\s+', sql, flags=re.IGNORECASE)
        if top_match:
            limit_value = top_match.group(1)
            if 'LIMIT' not in converted_sql.upper():
                converted_sql += f' LIMIT {limit_value}'
        
        # 9. 处理IDENTITY列 - 转换为AUTOINCREMENT
        converted_sql = re.sub(
            r'\bIDENTITY\s*\(\s*\d+\s*,\s*\d+\s*\)',
            'AUTOINCREMENT',
            converted_sql,
            flags=re.IGNORECASE
        )
        
        # 10. 处理方括号表名和列名
        converted_sql = re.sub(r'\[([^\]]+)\]', r'`\1`', converted_sql)
        
        # 11. 处理NONCLUSTERED INDEX
        converted_sql = re.sub(
            r'\bCREATE\s+NONCLUSTERED\s+INDEX\b',
            'CREATE INDEX',
            converted_sql,
            flags=re.IGNORECASE
        )
        
        # 12. 处理WITH (NOLOCK)
        converted_sql = re.sub(
            r'\s+WITH\s*\(\s*NOLOCK\s*\)',
            '',
            converted_sql,
            flags=re.IGNORECASE
        )
        
        return converted_sql
    
    @staticmethod
    def convert_date_functions(sql):
        """转换日期函数"""
        # GETDATE() -> datetime('now')
        sql = re.sub(
            r'\bGETDATE\s*\(\s*\)',
            "datetime('now')",
            sql,
            flags=re.IGNORECASE
        )
        
        # DATEADD() -> date() 函数
        sql = re.sub(
            r'\bDATEADD\s*\(\s*day\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)',
            r"date(\2, '+\1 days')",
            sql,
            flags=re.IGNORECASE
        )
        
        # DATEDIFF() -> julianday() 函数
        sql = re.sub(
            r'\bDATEDIFF\s*\(\s*day\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)',
            r'CAST(julianday(\2) - julianday(\1) AS INTEGER)',
            sql,
            flags=re.IGNORECASE
        )
        
        return sql
    
    @staticmethod
    def convert_string_functions(sql):
        """转换字符串函数"""
        # LEN() -> LENGTH()
        sql = re.sub(
            r'\bLEN\s*\(',
            'LENGTH(',
            sql,
            flags=re.IGNORECASE
        )
        
        # LTRIM() 和 RTRIM() -> TRIM()
        sql = re.sub(
            r'\bLTRIM\s*\(\s*RTRIM\s*\(\s*([^)]+)\s*\)\s*\)',
            r'TRIM(\1)',
            sql,
            flags=re.IGNORECASE
        )
        
        sql = re.sub(
            r'\bRTRIM\s*\(\s*LTRIM\s*\(\s*([^)]+)\s*\)\s*\)',
            r'TRIM(\1)',
            sql,
            flags=re.IGNORECASE
        )
        
        # CHARINDEX() -> INSTR()
        sql = re.sub(
            r'\bCHARINDEX\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
            r'INSTR(\2, \1)',
            sql,
            flags=re.IGNORECASE
        )
        
        return sql

def convert_file_sql(file_path):
    """转换文件中的所有SQL语句"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 查找SQL语句模式
        sql_patterns = [
            r'"""[\s\S]*?"""',  # 三引号字符串
            r"'''[\s\S]*?'''",  # 三引号字符串
            r'"[^"]*SELECT[^"]*"',  # 双引号中的SELECT
            r"'[^']*SELECT[^']*'",  # 单引号中的SELECT
        ]
        
        converted_content = content
        
        for pattern in sql_patterns:
            matches = re.finditer(pattern, content, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                original_sql = match.group(0)
                if 'SELECT' in original_sql.upper() or 'INSERT' in original_sql.upper() or 'UPDATE' in original_sql.upper():
                    converted_sql = SQLConverter.convert_sql(original_sql)
                    converted_sql = SQLConverter.convert_date_functions(converted_sql)
                    converted_sql = SQLConverter.convert_string_functions(converted_sql)
                    converted_content = converted_content.replace(original_sql, converted_sql)
        
        return converted_content
    
    except Exception as e:
        logger.error(f"转换文件 {file_path} 失败: {e}")
        return None

if __name__ == "__main__":
    # 测试转换器
    test_sql = """
    SELECT TOP 10 ISNULL(name, 'Unknown') as name, 
           TRY_CAST(age AS INT) as age,
           NEWID() as id
    FROM [users] WITH (NOLOCK)
    WHERE name COLLATE Chinese_PRC_CI_AS LIKE '%test%'
    """
    
    converter = SQLConverter()
    converted = converter.convert_sql(test_sql)
    print("原始SQL:")
    print(test_sql)
    print("\n转换后SQL:")
    print(converted)
