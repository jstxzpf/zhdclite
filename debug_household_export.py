#!/usr/bin/env python3
"""
调试调查点户名单导出问题：检查数据库中的字段值和视图映射
"""
import sqlite3

def main():
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    
    print("=== 问题1：调查点户名单表中这两个字段是否为空 ===")
    
    # 检查示例户代码的字段值
    test_codes = ['321283001002012', '321283001002016', '321283001002020', '321283001002031', '321283001002032']
    
    for code in test_codes:
        cur.execute("""
            SELECT 户代码, 户主姓名, 所在乡镇街道, 村居名称, 调查小区名称, 住宅地址
            FROM 调查点户名单 
            WHERE 户代码 = ?
        """, (code,))
        row = cur.fetchone()
        if row:
            print(f"户代码: {row[0]}")
            print(f"  户主姓名: {row[1]}")
            print(f"  所在乡镇街道: '{row[2]}' (类型: {type(row[2])}, 长度: {len(str(row[2])) if row[2] else 0})")
            print(f"  村居名称: '{row[3]}' (类型: {type(row[3])}, 长度: {len(str(row[3])) if row[3] else 0})")
            print(f"  调查小区名称: '{row[4]}'")
            print(f"  住宅地址: '{row[5]}'")
            print()
    
    print("=== 问题2：v_town_village_list视图中是否有对应映射 ===")
    
    # 检查户代码前12位在视图中的映射
    village_codes = set(code[:12] for code in test_codes)
    
    for village_code in village_codes:
        print(f"村代码: {village_code}")
        cur.execute("""
            SELECT 村代码, 所在乡镇街道, 村居名称
            FROM v_town_village_list 
            WHERE 村代码 = ?
        """, (village_code,))
        row = cur.fetchone()
        if row:
            print(f"  视图中找到: 所在乡镇街道='{row[1]}', 村居名称='{row[2]}'")
        else:
            print(f"  视图中未找到对应记录")
        print()
    
    print("=== 补充：检查调查点村名单表 ===")
    
    for village_code in village_codes:
        cur.execute("""
            SELECT 户代码前12位, 所在乡镇街道, 村居名称
            FROM 调查点村名单 
            WHERE 户代码前12位 = ?
        """, (village_code,))
        row = cur.fetchone()
        if row:
            print(f"村代码 {village_code}: 调查点村名单中找到: 所在乡镇街道='{row[1]}', 村居名称='{row[2]}'")
        else:
            print(f"村代码 {village_code}: 调查点村名单中未找到")
    
    print("\n=== 统计信息 ===")
    
    # 统计调查点户名单表中这两个字段的空值情况
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN 所在乡镇街道 IS NULL OR TRIM(所在乡镇街道) = '' THEN 1 ELSE 0 END) as town_empty,
            SUM(CASE WHEN 村居名称 IS NULL OR TRIM(村居名称) = '' THEN 1 ELSE 0 END) as village_empty
        FROM 调查点户名单
    """)
    stats = cur.fetchone()
    print(f"调查点户名单总记录数: {stats[0]}")
    print(f"所在乡镇街道为空的记录数: {stats[1]} ({stats[1]/stats[0]*100:.1f}%)")
    print(f"村居名称为空的记录数: {stats[2]} ({stats[2]/stats[0]*100:.1f}%)")
    
    # 统计视图中的记录数
    cur.execute("SELECT COUNT(*) FROM v_town_village_list")
    view_count = cur.fetchone()[0]
    print(f"v_town_village_list视图记录数: {view_count}")
    
    # 统计调查点村名单表中的记录数
    cur.execute("SELECT COUNT(*) FROM 调查点村名单")
    village_table_count = cur.fetchone()[0]
    print(f"调查点村名单表记录数: {village_table_count}")
    
    conn.close()

if __name__ == '__main__':
    main()
