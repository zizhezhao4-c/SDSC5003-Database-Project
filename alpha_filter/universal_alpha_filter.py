#!/usr/bin/env python3
"""
通用Alpha筛选系统 - 主程序
支持通过JSON配置文件灵活筛选符合条件的alpha
"""

from pymongo import MongoClient
import json
from datetime import datetime
import sys
import os
import gc
from typing import Dict, List, Any, Optional


class AlphaFilter:
    """Alpha筛选器类"""
    
    def __init__(self, config_file: str = 'alpha_filter_config.json'):
        """初始化筛选器"""
        self.config = self._load_config(config_file)
        self.client = None
        self.db = None
        self.collection = None
        
    def _load_config(self, config_file: str) -> Dict:
        """加载配置文件"""
        # 如果配置文件路径不是绝对路径，则在脚本所在目录查找
        if not os.path.isabs(config_file):
            script_dir = os.path.dirname(os.path.abspath(__file__))
            config_file = os.path.join(script_dir, config_file)
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            print(f"配置加载成功: {config.get('filter_name', '未命名配置')}")
            print(f"配置文件路径: {config_file}")
            return config
        except FileNotFoundError:
            print(f"错误: 配置文件 {config_file} 不存在")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"错误: 配置文件格式错误 - {e}")
            sys.exit(1)
    
    def connect(self) -> None:
        """连接MongoDB数据库"""
        uri = 'mongodb://localhost:27017'
        self.client = MongoClient(uri)
        db_name = self.config.get('database', 'regular_alphas')
        collection_name = self.config.get('collection', 'zzz')
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        print(f'已连接到MongoDB: {db_name}.{collection_name}')
    
    def disconnect(self) -> None:
        """断开数据库连接"""
        if self.client:
            self.client.close()
            print('已断开MongoDB连接')
    
    def _build_mongodb_query(self) -> Dict:
        """构建MongoDB查询条件"""
        query = {}
        filters = self.config.get('filters', {})
        
        # ---------------------------------------------------------
        # 1. 基础字段筛选 (Pyramid, Status, Region, Universe, Author, Delay)
        # ---------------------------------------------------------
        
        # Pyramid筛选
        if filters.get('pyramids', {}).get('enabled'):
            pyramid_config = filters['pyramids']
            pyramid_values = pyramid_config.get('values', [])
            if pyramid_values:
                if pyramid_config.get('operator') == 'contains_any':
                    query['is.checks'] = {
                        '$elemMatch': {
                            'name': 'MATCHES_PYRAMID',
                            'pyramids.name': {'$in': pyramid_values}
                        }
                    }
        
        # Status筛选
        if filters.get('status', {}).get('enabled'):
            status_config = filters['status']
            status_values = status_config.get('values', [])
            operator = status_config.get('operator', 'exclude')
            
            if operator == 'exclude':
                if len(status_values) == 1:
                    query['status'] = {'$ne': status_values[0]}
                else:
                    query['status'] = {'$nin': status_values}
            elif operator == 'in':
                query['status'] = {'$in': status_values}
            elif operator == 'equals':
                query['status'] = status_values[0] if status_values else None
        
        # Region筛选
        if filters.get('region', {}).get('enabled'):
            region_config = filters['region']
            region_values = region_config.get('values', [])
            operator = region_config.get('operator', 'in')
            
            if operator == 'in':
                query['settings.region'] = {'$in': region_values}
            elif operator == 'equals':
                query['settings.region'] = region_config.get('value')
        
        # Universe筛选
        if filters.get('universe', {}).get('enabled'):
            universe_config = filters['universe']
            universe_values = universe_config.get('values', [])
            operator = universe_config.get('operator', 'in')
            
            if operator == 'in':
                query['settings.universe'] = {'$in': universe_values}
            elif operator == 'equals':
                query['settings.universe'] = universe_config.get('value')
        
        # Author筛选
        if filters.get('author', {}).get('enabled'):
            author_config = filters['author']
            query['author'] = author_config.get('value')
        
        # Delay筛选
        if filters.get('delay', {}).get('enabled'):
            delay_config = filters['delay']
            operator = delay_config.get('operator', 'equals')
            value = delay_config.get('value')
            
            if operator == 'equals':
                query['settings.delay'] = value
            elif operator == 'gt':
                query['settings.delay'] = {'$gt': value}
            elif operator == 'lt':
                query['settings.delay'] = {'$lt': value}

        # ---------------------------------------------------------
        # 2. Sharpe 筛选 (下推到 MongoDB)
        # ---------------------------------------------------------
        sharpe_config = filters.get('sharpe', {})
        
        # is_sharpe: 直接在 is.sharpe 字段
        if sharpe_config.get('is_sharpe', {}).get('enabled', False):
            config = sharpe_config['is_sharpe']
            value = config.get('value')
            operator = config.get('operator', 'gt')
            op_map = {'gt': '$gt', 'gte': '$gte', 'lt': '$lt', 'lte': '$lte'}
            if operator in op_map:
                query['is.sharpe'] = {op_map[operator]: value}

        # any_sharpe_gt: 2Y或Ladder任一满足即可
        # 使用 $or 查询优化
        if sharpe_config.get('any_sharpe_gt', {}).get('enabled', False):
            threshold = sharpe_config['any_sharpe_gt'].get('value', 1.5)
            
            # 构建针对 checks 数组的查询
            # 只要数组中存在满足条件的元素即可
            or_conditions = [
                # 检查 LOW_2Y_SHARPE > threshold
                {'is.checks': {'$elemMatch': {'name': 'LOW_2Y_SHARPE', 'value': {'$gt': threshold}}}},
                # 检查 IS_LADDER_SHARPE > threshold
                {'is.checks': {'$elemMatch': {'name': 'IS_LADDER_SHARPE', 'value': {'$gt': threshold}}}}
            ]
            
            # 如果已有 $or 条件（虽然目前没有），则使用 $and 合并
            if '$or' in query:
                query['$and'] = [
                    {'$or': query.pop('$or')},
                    {'$or': or_conditions}
                ]
            else:
                query['$or'] = or_conditions

        # 单独的 Sharpe 2Y 和 Ladder Sharpe 筛选
        # 注意：如果启用了 any_sharpe_gt，这些通常不应该同时启用，否则逻辑是 AND 关系
        if sharpe_config.get('sharpe_2y', {}).get('enabled', False):
            config = sharpe_config['sharpe_2y']
            value = config.get('value')
            operator = config.get('operator', 'gt')
            op_map = {'gt': '$gt', 'gte': '$gte', 'lt': '$lt', 'lte': '$lte'}
            
            if operator in op_map:
                # 使用 $elemMatch 确保是同一个对象的 name 和 value
                condition = {'$elemMatch': {'name': 'LOW_2Y_SHARPE', 'value': {op_map[operator]: value}}}
                
                # 合并到 is.checks 查询中
                if 'is.checks' in query:
                    # 如果已经有 is.checks 查询（例如 Pyramid），使用 $and
                    if '$and' not in query:
                        query['$and'] = []
                    # 将现有的 is.checks 移入 $and
                    if 'is.checks' in query:
                        query['$and'].append({'is.checks': query.pop('is.checks')})
                    query['$and'].append({'is.checks': condition})
                else:
                    query['is.checks'] = condition

        if sharpe_config.get('ladder_sharpe', {}).get('enabled', False):
            config = sharpe_config['ladder_sharpe']
            value = config.get('value')
            operator = config.get('operator', 'gt')
            op_map = {'gt': '$gt', 'gte': '$gte', 'lt': '$lt', 'lte': '$lte'}
            
            if operator in op_map:
                condition = {'$elemMatch': {'name': 'IS_LADDER_SHARPE', 'value': {op_map[operator]: value}}}
                
                if 'is.checks' in query:
                    if '$and' not in query:
                        query['$and'] = []
                    if 'is.checks' in query:
                        query['$and'].append({'is.checks': query.pop('is.checks')})
                    query['$and'].append({'is.checks': condition})
                elif '$and' in query:
                    query['$and'].append({'is.checks': condition})
                else:
                    query['is.checks'] = condition

        # ---------------------------------------------------------
        # 3. Red Tagged 筛选 (下推到 MongoDB)
        # ---------------------------------------------------------
        red_tagged_config = filters.get('exclude_red_tagged', {})
        if red_tagged_config.get('enabled', False):
            # 逻辑：排除 (Color=RED AND (Name匹配规则 OR Tags包含关键词))
            # 等价于：保留 (Color != RED) OR (Color=RED AND NOT (Name匹配规则 OR Tags包含关键词))
            # 但 MongoDB 的 $nor 或 $not 逻辑较复杂，通常用 $ne 排除特定模式更高效
            
            # 这里的逻辑是：如果 Color 是 RED，则必须不满足排除条件
            # 这是一个“排除”逻辑，在 MongoDB 中可以用 $nor 实现
            
            exclude_conditions = []
            
            # 1. 精确匹配规则
            rules = red_tagged_config.get('rules', [])
            for rule in rules:
                rule_name = rule.get('name')
                rule_tag = rule.get('tag')
                if rule_name and rule_tag:
                    exclude_conditions.append({
                        'name': rule_name,
                        'tags': rule_tag
                    })
            
            # 2. 关键词包含规则
            keywords = red_tagged_config.get('keywords', [])
            for keyword in keywords:
                # Name 包含关键词
                exclude_conditions.append({'name': {'$regex': keyword, '$options': 'i'}})
                # Tags 包含关键词
                exclude_conditions.append({'tags': {'$regex': keyword, '$options': 'i'}})
            
            if exclude_conditions:
                # 只有当颜色为 RED 时才检查这些条件
                # 查询逻辑：
                #   要么 color != RED
                #   要么 color == RED 且 不满足任何 exclude_conditions
                
                red_condition = {'color': 'RED'}
                # $or: [ {规则1}, {规则2}, ... ]
                match_any_rule = {'$or': exclude_conditions}
                
                # 排除掉 (RED 且 匹配任意规则)
                # MongoDB: $nor: [ { color: RED, $or: [...] } ]
                query['$nor'] = [
                    {
                        '$and': [
                            red_condition,
                            match_any_rule
                        ]
                    }
                ]
        
        return query
    
    def _filter_by_checks(self, alpha: Dict) -> bool:
        """根据检查项筛选alpha"""
        filters = self.config.get('filters', {})
        checks_config = filters.get('checks', {})
        
        if not ('is' in alpha and 'checks' in alpha['is']):
            return False
        
        checks = alpha['is']['checks']
        
        # 检查是否有FAIL项
        if checks_config.get('no_fail', {}).get('enabled', False):
            for check in checks:
                if check.get('result') == 'FAIL':
                    return False
        
        # 检查特定检查项
        if checks_config.get('specific_checks', {}).get('enabled', False):
            specific_config = checks_config['specific_checks']
            check_names = specific_config.get('check_names', [])
            required_result = specific_config.get('required_result', 'PASS')
            
            for check_name in check_names:
                check_found = False
                for check in checks:
                    if check.get('name') == check_name:
                        check_found = True
                        if check.get('result') != required_result:
                            return False
                        break
                
                if not check_found:
                    return False
        
        return True
    
    def _filter_by_sharpe(self, alpha: Dict) -> bool:
        """根据Sharpe比率筛选alpha"""
        filters = self.config.get('filters', {})
        sharpe_config = filters.get('sharpe', {})
        
        if not ('is' in alpha and 'checks' in alpha['is']):
            return False
        
        checks = alpha['is']['checks']
        
        # 提取Sharpe值
        sharpe_2y = None
        ladder_sharpe = None
        is_sharpe = alpha.get('is', {}).get('sharpe')
        
        for check in checks:
            if check.get('name') == 'LOW_2Y_SHARPE':
                sharpe_2y = check.get('value')
            elif check.get('name') == 'IS_LADDER_SHARPE':
                ladder_sharpe = check.get('value')
        
        # any_sharpe_gt: 2Y或Ladder任一满足即可
        if sharpe_config.get('any_sharpe_gt', {}).get('enabled', False):
            threshold = sharpe_config['any_sharpe_gt'].get('value', 1.5)
            if (sharpe_2y and sharpe_2y > threshold) or (ladder_sharpe and ladder_sharpe > threshold):
                return True
            return False
        
        # 单独检查各项Sharpe
        passed = True
        
        if sharpe_config.get('sharpe_2y', {}).get('enabled', False):
            config = sharpe_config['sharpe_2y']
            value = config.get('value')
            operator = config.get('operator', 'gt')
            
            if sharpe_2y is None:
                return False
            
            if operator == 'gt' and not (sharpe_2y > value):
                passed = False
            elif operator == 'gte' and not (sharpe_2y >= value):
                passed = False
            elif operator == 'lt' and not (sharpe_2y < value):
                passed = False
        
        if sharpe_config.get('ladder_sharpe', {}).get('enabled', False):
            config = sharpe_config['ladder_sharpe']
            value = config.get('value')
            operator = config.get('operator', 'gt')
            
            if ladder_sharpe is None:
                return False
            
            if operator == 'gt' and not (ladder_sharpe > value):
                passed = False
            elif operator == 'gte' and not (ladder_sharpe >= value):
                passed = False
        
        if sharpe_config.get('is_sharpe', {}).get('enabled', False):
            config = sharpe_config['is_sharpe']
            value = config.get('value')
            operator = config.get('operator', 'gt')
            
            if is_sharpe is None:
                return False
            
            if operator == 'gt' and not (is_sharpe > value):
                passed = False
            elif operator == 'gte' and not (is_sharpe >= value):
                passed = False
        
        return passed
    
    def _filter_by_red_tagged(self, alpha: Dict) -> bool:
        """根据red标记筛选alpha"""
        filters = self.config.get('filters', {})
        red_tagged_config = filters.get('exclude_red_tagged', {})
        
        # 如果未启用，直接通过
        if not red_tagged_config.get('enabled', False):
            return True
        
        # 检查color是否为RED
        color = alpha.get('color')
        if color != 'RED':
            return True
        
        # 获取name和tags
        alpha_name = alpha.get('name')
        alpha_tags = alpha.get('tags', [])
        
        # 1. 检查精确匹配规则
        rules = red_tagged_config.get('rules', [])
        for rule in rules:
            rule_name = rule.get('name')
            rule_tag = rule.get('tag')
            
            # 如果name和tag都匹配，则排除此alpha
            if rule_name and rule_tag:
                if alpha_name == rule_name and rule_tag in alpha_tags:
                    return False
        
        # 2. 检查关键词包含规则 (只要name或任意tag中包含关键词，即排除)
        keywords = red_tagged_config.get('keywords', [])
        for keyword in keywords:
            # 检查name是否包含关键词
            if alpha_name and keyword in alpha_name:
                return False
            # 检查tags中是否有包含关键词的项
            for tag in alpha_tags:
                if keyword in tag:
                    return False
        
        return True
    
    def _extract_alpha_data(self, alpha: Dict) -> Dict:
        """提取alpha的关键数据"""
        output_config = self.config.get('output', {})
        include_fields = output_config.get('include_fields', [])
        
        result = {}
        
        # 提取Sharpe值
        sharpe_2y = None
        ladder_sharpe = None
        
        if 'is' in alpha and 'checks' in alpha['is']:
            for check in alpha['is']['checks']:
                if check.get('name') == 'LOW_2Y_SHARPE':
                    sharpe_2y = check.get('value')
                elif check.get('name') == 'IS_LADDER_SHARPE':
                    ladder_sharpe = check.get('value')
        
        # 根据配置提取字段
        for field in include_fields:
            if field == 'id':
                result['id'] = alpha.get('id')
            elif field == 'author':
                result['author'] = alpha.get('author')
            elif field == 'status':
                result['status'] = alpha.get('status')
            elif field == 'settings':
                result['settings'] = alpha.get('settings')
            elif field == 'is_sharpe':
                result['is_sharpe'] = alpha.get('is', {}).get('sharpe')
            elif field == 'sharpe_2y':
                result['sharpe_2y'] = sharpe_2y
            elif field == 'ladder_sharpe':
                result['ladder_sharpe'] = ladder_sharpe
            elif field == 'checks':
                result['checks'] = alpha.get('is', {}).get('checks')
        
        return result
    
    def filter_alphas(self) -> List[Dict]:
        """执行筛选"""
        print('\n开始筛选alpha...')
        
        # 构建MongoDB查询
        query = self._build_mongodb_query()
        print(f'MongoDB查询条件: {json.dumps(query, indent=2, ensure_ascii=False)}')
        
        # 执行查询
        cursor = self.collection.find(query)
        
        # 后续过滤
        results = []
        for alpha in cursor:
            # 检查项筛选
            if not self._filter_by_checks(alpha):
                continue
            
            # Sharpe筛选
            if not self._filter_by_sharpe(alpha):
                continue
            
            # 排除RED标记的alpha
            if not self._filter_by_red_tagged(alpha):
                continue
            
            # 提取数据
            result = self._extract_alpha_data(alpha)
            results.append(result)
        
        print(f'筛选完成，找到 {len(results)} 个符合条件的alpha')
        return results
    
    def display_results(self, results: List[Dict]) -> None:
        """显示结果"""
        display_config = self.config.get('display', {})
        
        if not display_config.get('show_summary', True):
            return
        
        print('\n' + '=' * 60)
        print(f"{self.config.get('filter_name', 'Alpha筛选结果')}")
        print('=' * 60)
        
        # Alpha IDs
        alpha_ids = [r['id'] for r in results]
        print(f'\n找到 {len(alpha_ids)} 个Alpha:')
        print(json.dumps(alpha_ids, indent=2))
        
        # 统计信息
        if display_config.get('show_statistics', True):
            self._show_statistics(results)
        
        # 详细信息
        max_display = display_config.get('max_detail_display', 10)
        if max_display > 0:
            print(f'\n前 {min(max_display, len(results))} 个Alpha详细信息:')
            print('-' * 60)
            
            for i, alpha in enumerate(results[:max_display], 1):
                print(f"\n[{i}] ID: {alpha.get('id')}")
                print(f"    作者: {alpha.get('author')}")
                print(f"    状态: {alpha.get('status', 'N/A')}")
                if alpha.get('settings'):
                    print(f"    区域: {alpha['settings'].get('region')}")
                    print(f"    Universe: {alpha['settings'].get('universe')}")
                    print(f"    Delay: {alpha['settings'].get('delay')}")
                print(f"    IS Sharpe: {alpha.get('is_sharpe')}")
                print(f"    2Y Sharpe: {alpha.get('sharpe_2y') or 'N/A'}")
                print(f"    Ladder Sharpe: {alpha.get('ladder_sharpe') or 'N/A'}")
    
    def _show_statistics(self, results: List[Dict]) -> None:
        """显示统计信息"""
        print(f'\n统计信息:')
        print('-' * 60)
        
        # 区域分布
        regions = {}
        authors = {}
        universes = {}
        sharpe_2y_count = 0
        ladder_sharpe_count = 0
        total_is_sharpe = 0
        
        for alpha in results:
            # 区域
            if alpha.get('settings'):
                region = alpha['settings'].get('region', 'Unknown')
                regions[region] = regions.get(region, 0) + 1
                
                # Universe
                universe = alpha['settings'].get('universe', 'Unknown')
                universes[universe] = universes.get(universe, 0) + 1
            
            # 作者
            author = alpha.get('author', 'Unknown')
            authors[author] = authors.get(author, 0) + 1
            
            # Sharpe
            if alpha.get('sharpe_2y'):
                sharpe_2y_count += 1
            if alpha.get('ladder_sharpe'):
                ladder_sharpe_count += 1
            if alpha.get('is_sharpe'):
                total_is_sharpe += alpha['is_sharpe']
        
        print(f"区域分布: {regions}")
        print(f"Universe分布: {universes}")
        print(f"作者分布: {authors}")
        print(f"有2Y Sharpe的: {sharpe_2y_count} 个")
        print(f"有Ladder Sharpe的: {ladder_sharpe_count} 个")
        if len(results) > 0:
            print(f"平均IS Sharpe: {total_is_sharpe / len(results):.2f}")
    
    def save_results(self, results: List[Dict]) -> Optional[str]:
        """保存结果到文件"""
        output_config = self.config.get('output', {})
        
        if not output_config.get('save_to_file', True):
            return None
        
        # 构建文件名，包含筛选条件信息
        filename_parts = []
        
        # 1. collection
        collection = self.config.get('collection', 'zzz')
        filename_parts.append(collection)
        
        # 2. 固定标识
        filename_parts.append('filtered_alphas')
        
        # 3. database
        database = self.config.get('database', 'regular_alphas')
        filename_parts.append(database)
        
        # 4. pyramids (处理多个pyramid的情况)
        filters = self.config.get('filters', {})
        if filters.get('pyramids', {}).get('enabled'):
            pyramid_values = filters['pyramids'].get('values', [])
            if pyramid_values:
                # 将pyramid名称中的斜杠替换为下划线，避免文件名问题
                pyramid_names = [p.replace('/', '_') for p in pyramid_values]
                if len(pyramid_names) == 1:
                    filename_parts.append(pyramid_names[0])
                else:
                    # 多个pyramid用+连接
                    filename_parts.append('+'.join(pyramid_names))
        
        # 5. status values
        if filters.get('status', {}).get('enabled'):
            status_config = filters['status']
            status_values = status_config.get('values', [])
            operator = status_config.get('operator', 'exclude')
            if status_values:
                if operator == 'exclude':
                    status_str = 'exclude_' + '_'.join(status_values)
                elif operator == 'in':
                    status_str = 'include_' + '_'.join(status_values)
                else:
                    status_str = '_'.join(status_values)
                filename_parts.append(status_str)
        
        # 6. 时间戳
        if output_config.get('include_timestamp', True):
            timestamp = datetime.now().strftime('%Y%m%dT%H%M%S')
            filename_parts.append(timestamp)
        
        # 组合文件名
        filename = '_'.join(filename_parts) + '.json'
        
        # 确保文件保存到脚本所在目录
        script_dir = os.path.dirname(os.path.abspath(__file__))
        full_path = os.path.join(script_dir, filename)
        
        # 准备输出数据
        alpha_ids = [r['id'] for r in results]
        
        # 根据配置决定是否只保存ID
        if output_config.get('save_ids_only', False):
            output_data = {
                'filter_name': self.config.get('filter_name', '未命名筛选'),
                'query_time': datetime.now().isoformat(),
                'total_count': len(results),
                'alpha_ids': alpha_ids
            }
        else:
            output_data = {
                'filter_name': self.config.get('filter_name', '未命名筛选'),
                'query_time': datetime.now().isoformat(),
                'total_count': len(results),
                'alpha_ids': alpha_ids,
                'details': results
            }
        
        # 保存文件
        with open(full_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        file_size = os.path.getsize(full_path) / 1024  # KB
        print(f'\n结果已保存到: {full_path} ({file_size:.1f} KB)')
        return full_path
    
    def _cleanup_old_files(self) -> None:
        """清理旧的结果文件，只保留最近的N个"""
        output_config = self.config.get('output', {})
        max_keep = output_config.get('max_keep_files', 10)  # 默认保留10个
        
        if max_keep <= 0:
            return

        collection = self.config.get('collection', 'zzz')
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # 查找匹配的文件
        prefix = f"{collection}_filtered_alphas_"
        files = []
        
        try:
            for filename in os.listdir(script_dir):
                if filename.startswith(prefix) and filename.endswith('.json'):
                    full_path = os.path.join(script_dir, filename)
                    files.append(full_path)
            
            # 按修改时间排序（新的在前）
            files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            
            # 删除多余的文件
            if len(files) > max_keep:
                files_to_delete = files[max_keep:]
                print(f'\n正在清理旧文件 (配置保留 {max_keep} 个)...')
                for f in files_to_delete:
                    try:
                        os.remove(f)
                        print(f"已删除: {os.path.basename(f)}")
                    except OSError as e:
                        print(f"删除失败 {os.path.basename(f)}: {e}")
                print(f"清理完成，共删除 {len(files_to_delete)} 个文件")
                
        except Exception as e:
            print(f"清理文件时出错: {e}")
    
    def _cleanup_memory(self) -> None:
        """清理内存"""
        print('\n正在执行内存清理...')
        
        # 1. 清理Python垃圾回收
        gc.collect()
        print("Python垃圾回收完成")
        
        # 2. 尝试清理MongoDB缓存 (Plan Cache)
        # 注意: 这不会释放WiredTiger占用的物理内存，那是MongoDB的正常行为
        try:
            if self.db:
                # 清除当前集合的查询计划缓存
                collection_name = self.config.get('collection', 'zzz')
                self.db.command('planCacheClear', collection_name)
                print(f"MongoDB查询计划缓存已清理 ({collection_name})")
        except Exception as e:
            print(f"MongoDB缓存清理警告: {e}")
            
    def run(self) -> List[Dict]:
        """运行筛选流程"""
        try:
            self.connect()
            results = self.filter_alphas()
            self.display_results(results)
            self.save_results(results)
            self._cleanup_old_files()
            return results
        finally:
            self._cleanup_memory()
            self.disconnect()


def main():
    """主函数"""
    # 检查命令行参数
    config_file = 'alpha_filter_config.json'
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    
    # 运行筛选
    filter_engine = AlphaFilter(config_file)
    results = filter_engine.run()
    
    print(f'\n筛选完成，共找到 {len(results)} 个符合条件的alpha')


if __name__ == '__main__':
    main()
