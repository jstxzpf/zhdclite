"""
直接匹配编码处理器
用于基于type_name进行直接匹配编码，替代AI编码功能
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


class DirectMatchProcessor:
    """直接匹配编码处理器"""
    
    def __init__(self, db):
        """
        初始化直接匹配处理器
        
        Args:
            db: 数据库连接对象
        """
        self.db = db
        self.mapping_cache = {}
        self.cache_loaded = False
        
    def load_mapping_cache(self) -> bool:
        """
        加载type_name到code的映射缓存

        Returns:
            bool: 是否成功加载缓存
        """
        try:
            # 查询已编码的记录，构建映射缓存
            query = """
            SELECT type_name, code, COUNT(*) as frequency
            FROM 调查点台账合并
            WHERE code IS NOT NULL
            AND code COLLATE Chinese_PRC_CI_AS != ''
            AND type_name IS NOT NULL
            AND type_name COLLATE Chinese_PRC_CI_AS != ''
            GROUP BY type_name, code
            ORDER BY type_name, frequency DESC
            """

            results = self.db.execute_query_safe(query)
            
            # 构建映射缓存，每个type_name对应频次最高的code
            for row in results:
                type_name = row[0].strip()
                code = row[1].strip()
                frequency = row[2]
                
                if type_name not in self.mapping_cache:
                    # 第一次遇到这个type_name，记录频次最高的code
                    self.mapping_cache[type_name] = {
                        'code': code,
                        'frequency': frequency,
                        'total_frequency': frequency
                    }
                else:
                    # 累加总频次
                    self.mapping_cache[type_name]['total_frequency'] += frequency
            
            self.cache_loaded = True
            logger.info(f"映射缓存加载完成，共 {len(self.mapping_cache)} 个映射")
            return True
            
        except Exception as e:
            logger.error(f"加载映射缓存失败: {str(e)}")
            return False
    
    def get_mapping_statistics(self) -> Dict:
        """
        获取映射统计信息
        
        Returns:
            Dict: 映射统计信息
        """
        if not self.cache_loaded:
            self.load_mapping_cache()
        
        total_mappings = len(self.mapping_cache)
        total_frequency = sum(mapping['total_frequency'] for mapping in self.mapping_cache.values())
        avg_frequency = total_frequency / total_mappings if total_mappings > 0 else 0
        unique_codes = len(set(mapping['code'] for mapping in self.mapping_cache.values()))
        
        return {
            'total_mappings': total_mappings,
            'total_frequency': total_frequency,
            'avg_frequency': avg_frequency,
            'unique_codes': unique_codes,
            'cache_loaded': self.cache_loaded
        }
    
    def find_direct_match(self, type_name: str) -> Optional[str]:
        """
        查找type_name的直接匹配编码
        
        Args:
            type_name: 要匹配的type_name
            
        Returns:
            Optional[str]: 匹配的编码，如果没有匹配则返回None
        """
        if not self.cache_loaded:
            self.load_mapping_cache()
        
        type_name = type_name.strip()
        
        # 精确匹配
        if type_name in self.mapping_cache:
            return self.mapping_cache[type_name]['code']
        
        # 模糊匹配（可选）
        for cached_type_name, mapping in self.mapping_cache.items():
            if type_name in cached_type_name or cached_type_name in type_name:
                logger.info(f"模糊匹配: '{type_name}' -> '{cached_type_name}' -> {mapping['code']}")
                return mapping['code']
        
        return None
    
    def process_uncoded_records(self, limit: int = 1000) -> Dict:
        """
        处理未编码记录

        Args:
            limit: 处理记录数限制

        Returns:
            Dict: 处理结果统计
        """
        try:
            # 获取未编码记录
            query = """
            SELECT id, type_name
            FROM 调查点台账合并
            WHERE (code IS NULL OR code COLLATE Chinese_PRC_CI_AS = '')
            AND type_name IS NOT NULL
            AND type_name COLLATE Chinese_PRC_CI_AS != ''
            ORDER BY id
            OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
            """

            uncoded_records = self.db.execute_query_safe(query, (limit,))
            
            if not uncoded_records:
                return {
                    'success': True,
                    'total_records': 0,
                    'matched_records': 0,
                    'unmatched_records': 0,
                    'message': '没有找到未编码记录'
                }
            
            matched_count = 0
            unmatched_count = 0
            
            # 处理每条记录
            # 使用连接池的上下文管理器确保事务一致性
            with self.db.pool.get_cursor() as cursor:
                for record in uncoded_records:
                    record_id = record[0]
                    type_name = record[1]
                    
                    # 查找直接匹配
                    matched_code = self.find_direct_match(type_name)
                    
                    if matched_code:
                        # 更新记录
                        update_query = """
                        UPDATE 调查点台账合并
                        SET code = ?
                        WHERE id = ?
                        """
                        cursor.execute(update_query, (matched_code, record_id))
                        matched_count += 1
                    else:
                        unmatched_count += 1
                # 事务会在上下文管理器退出时自动提交
            
            return {
                'success': True,
                'total_records': len(uncoded_records),
                'matched_records': matched_count,
                'unmatched_records': unmatched_count,
                'match_rate': (matched_count / len(uncoded_records)) * 100 if uncoded_records else 0,
                'message': f'处理完成：匹配 {matched_count} 条，未匹配 {unmatched_count} 条'
            }
            
        except Exception as e:
            logger.error(f"处理未编码记录失败: {str(e)}")
            # 注意：连接池会自动处理事务回滚
            
            return {
                'success': False,
                'error': str(e),
                'message': '处理未编码记录失败'
            }
    
    def get_uncoded_count(self) -> int:
        """
        获取未编码记录数量

        Returns:
            int: 未编码记录数量
        """
        try:
            query = """
            SELECT COUNT(*)
            FROM 调查点台账合并
            WHERE (code IS NULL OR code COLLATE Chinese_PRC_CI_AS = '')
            AND type_name IS NOT NULL
            AND type_name COLLATE Chinese_PRC_CI_AS != ''
            """

            result = self.db.execute_query_safe(query)
            return result[0][0] if result else 0

        except Exception as e:
            logger.error(f"获取未编码记录数量失败: {str(e)}")
            return 0
    
    def get_mapping_preview(self, limit: int = 20) -> List[Dict]:
        """
        获取映射预览
        
        Args:
            limit: 返回记录数限制
            
        Returns:
            List[Dict]: 映射预览列表
        """
        if not self.cache_loaded:
            self.load_mapping_cache()
        
        # 按频次排序，返回前N个映射
        sorted_mappings = sorted(
            self.mapping_cache.items(),
            key=lambda x: x[1]['total_frequency'],
            reverse=True
        )
        
        preview = []
        for type_name, mapping in sorted_mappings[:limit]:
            preview.append({
                'type_name': type_name,
                'code': mapping['code'],
                'frequency': mapping['total_frequency'],
                'confidence': 1.0  # 直接匹配的置信度为1.0
            })
        
        return preview
    
    def refresh_cache(self) -> bool:
        """
        刷新映射缓存
        
        Returns:
            bool: 是否成功刷新
        """
        self.mapping_cache.clear()
        self.cache_loaded = False
        return self.load_mapping_cache()
