/**
 * Google Sheets SQL
 * 一个基于 Google Apps Scripts + AlaSQL 的工具
 * 支持查询 Google Drive 下的所有 sheets 表的数据
 * 支持联表查询，JSON 解析等功能
 */

let alasql;
/**
 * 初始化 AlaSQL 库
 */
function initAlaSQL() {
    // 检查 AlaSQLGS 库是否已添加到脚本中
    if (typeof AlaSQLGS === 'undefined') {
      Logger.log('错误: AlaSQLGS 库未添加到项目中');
      return false;
    }
    
    try {
      // 使用 AlaSQLGS.load() 获取 alasql 实例
      const { load } = AlaSQLGS;
      alasql = load();
      
      // 验证 alasql 对象
      if (!alasql) {
        Logger.log('AlaSQLGS 加载失败: alasql 对象为空');
        return false;
      }
      
      // 确保 tables 对象存在
      if (!alasql.tables) {
        alasql.tables = {};
        Logger.log('已创建 alasql.tables 对象');
      }
      
      // 简单验证 - 确保 alasql 是函数
      if (typeof alasql !== 'function') {
        Logger.log('AlaSQLGS 加载不正确: alasql 不是函数');
        return false;
      }
      
      // 注册自定义 JSON 解析函数
      registerJSONFunctions(alasql);
      registerCustomSQLFunctions(alasql);
      
      Logger.log('AlaSQLGS 库加载成功');
      return true;
    } catch (e) {
      Logger.log('初始化 AlaSQL 时出错: ' + e.toString() + '\n堆栈: ' + e.stack);
      return false;
    }
}

/**
 * 解析 SQL 语句中的表引用
 * @param {string} sql - SQL 查询语句
 * @returns {Set<string>} - 表引用集合
 */
function extractTableReferences(sql) {
  // 修改正则表达式以支持数组字段引用
  const regex = /(?:FROM|(?:CROSS\s+)?JOIN)\s+([a-zA-Z0-9_\.\u4e00-\u9fa5]+)(?:\s+(?:AS\s+)?([a-zA-Z0-9_\u4e00-\u9fa5]+))?/gi;
  const matches = [...sql.matchAll(regex)];
  const tableReferences = new Set();
  const aliasMap = new Map();
  const arrayFields = new Map(); // 新增：存储数组字段引用
  
  matches.forEach(match => {
    const tableRef = match[1];
    const alias = match[2];
    
    // 检查是否是数组字段引用（包含两个点的情况）
    if (tableRef.includes('.')) {
      const parts = tableRef.split('.');
      if (parts.length === 3) { // 形如 a.field.array 的情况
        const parentAlias = parts[0];
        const arrayField = parts[2];
        arrayFields.set(alias || arrayField, { parentAlias, arrayField });
      } else {
        tableReferences.add(tableRef.trim());
      }
    } else {
      tableReferences.add(tableRef.trim());
    }
    
    if (alias) {
      aliasMap.set(alias.trim(), tableRef.trim());
    }
  });
  
  return { tableReferences, aliasMap, arrayFields };
}

/**
 * 执行 SQL 查询（简化版）
 * @param {string} sql - SQL 查询语句
 * @param {Object} params - 查询参数
 * @returns {Object} - 查询结果
 */
function executeSQL(sql, params = {}) {
  const startTime = new Date().getTime();
  
  try {
    // 确保 AlaSQL 已正确初始化
    if (!alasql || typeof alasql !== 'function') {
      const initSuccess = initAlaSQL();
      if (!initSuccess) {
        throw new Error('无法初始化 AlaSQLGS 库');
      }
    }

    // 检查是否存在 UNNEST 操作
    const arrayJoinMatch = sql.match(/FROM\s+([a-zA-Z0-9_\.]+)\s+([a-zA-Z0-9_]+)\s+CROSS\s+JOIN\s+UNNEST\(\s*(\2\.[a-zA-Z0-9_]+)\s*\)(?:\s+AS)?\s+([a-zA-Z0-9_]+)/i);
    
    let modifiedSql = sql;
    let expansionStats = null;
    
    // 如果存在 UNNEST 操作，进行预处理
    if (arrayJoinMatch) {
      const fieldParts = arrayJoinMatch[3].split('.');
      const arrayField = fieldParts[1];
      
      const modifiedMatch = [
        arrayJoinMatch[0], 
        arrayJoinMatch[1], 
        arrayJoinMatch[2], 
        arrayField, 
        arrayJoinMatch[4]
      ];
      
      // 预处理 UNNEST 操作，返回临时表名和修改后的SQL
      const { tempTableName, newSql, stats } = preprocessArrayExpansion(sql, modifiedMatch);
      modifiedSql = newSql;
      expansionStats = stats;
    }
    
    // 执行处理后的SQL查询（无论是否包含UNNEST操作）
    const result = executeRegularSQL(modifiedSql, params, startTime);
    
    // 如果有数组展开的统计信息，添加到结果中
    if (expansionStats && result.stats) {
      result.stats.arrayExpansion = expansionStats;
    }
    
    return result;

  } catch (e) {
    Logger.log("查询错误: " + e.toString());
    return {
      error: e.toString(),
      stats: {
        executionTime: new Date().getTime() - startTime
      }
    };
  }
}

/**
 * 预处理包含 UNNEST 操作的 SQL
 * @param {string} sql - 原始SQL
 * @param {Array} arrayJoinMatch - UNNEST匹配信息
 * @returns {Object} - 处理后的SQL和统计信息
 */
function preprocessArrayExpansion(sql, arrayJoinMatch) {
  const tableName = arrayJoinMatch[1];
  const tableAlias = arrayJoinMatch[2];  // 原始表的别名，例如 't'
  const arrayField = arrayJoinMatch[3];
  const arrayAlias = arrayJoinMatch[4];
  
  // 解析查询组件
  const queryParts = parseQueryComponents(sql);
  
  // 记录原始查询信息
  Logger.log(`处理UNNEST操作: 表=${tableName}, 别名=${tableAlias}, 数组字段=${arrayField}, 数组别名=${arrayAlias}`);
  Logger.log(`选择的字段: ${JSON.stringify(queryParts.selectedFields)}`);
  
  // 提取主表过滤条件
  const primaryTableFilters = extractPrimaryTableFilters(queryParts.whereClause, tableAlias);
  
  // 加载数据并应用主表过滤
  const { data, headers, filtered } = loadFilteredTableData(tableName, primaryTableFilters);
  if (!data) {
    throw new Error(`无法从 ${tableName} 加载数据`);
  }
  
  // 记录统计信息
  const preFilterCount = data.length - 1;
  const postFilterCount = filtered ? filtered : preFilterCount;
  
  // 转换原始数据为行对象
  const rows = convertToRowObjects(data, headers);
  
  // 记录第一行原始数据，帮助调试
  if (rows.length > 0) {
    Logger.log(`第一行原始数据: ${JSON.stringify(rows[0])}`);
    if (arrayField in rows[0]) {
      const fieldValue = rows[0][arrayField];
      Logger.log(`数组字段 ${arrayField} 类型: ${typeof fieldValue}, 是数组: ${Array.isArray(fieldValue)}`);
      if (Array.isArray(fieldValue) && fieldValue.length > 0) {
        Logger.log(`数组第一项: ${JSON.stringify(fieldValue[0])}`);
      }
    } else {
      Logger.log(`警告: 数组字段 ${arrayField} 在数据中不存在`);
    }
  }
  
  // 执行数组展开
  const expandedRows = expandArrayData(rows, tableAlias, arrayField, arrayAlias, queryParts.selectedFields);
  
  // 创建临时表存放展开后的数据
  const tempTableName = 'expanded_' + Math.random().toString(36).substring(2, 8);
  alasql(`CREATE TABLE ${tempTableName}`);
  alasql(`INSERT INTO ${tempTableName} SELECT * FROM ?`, [expandedRows]);
  
  // 查看临时表数据
  const sampleData = alasql(`SELECT TOP 3 * FROM ${tempTableName}`);
  Logger.log(`${tempTableName} 表的前3条数据: ${JSON.stringify(sampleData)}`);
  
  // 查看临时表结构
  const columns = [];
  if (sampleData.length > 0) {
    for (const key in sampleData[0]) {
      columns.push(key);
    }
    Logger.log(`临时表列名: ${columns.join(', ')}`);
  }
  
  // 查找SQL中的所有JOIN类型
  const joinTypes = [];
  const joinRegex = /(LEFT|RIGHT|INNER|OUTER|CROSS|FULL)?\s+JOIN/gi;
  let joinMatch;
  while ((joinMatch = joinRegex.exec(sql)) !== null) {
    if (joinMatch[1]) {
      joinTypes.push(joinMatch[1].toUpperCase());
    } else {
      joinTypes.push("INNER"); // 默认JOIN类型是INNER
    }
  }
  Logger.log(`SQL中的JOIN类型: ${joinTypes.join(', ')}`);
  
  // 检测SQL中是否包含LEFT JOIN等需要特殊处理的JOIN类型
  const hasLeftJoin = /LEFT\s+JOIN/i.test(sql);
  const hasRightJoin = /RIGHT\s+JOIN/i.test(sql);
  const hasFullJoin = /FULL\s+JOIN/i.test(sql);
  
  // 特殊处理LEFT JOIN和其他外连接的情况
  let newSql = sql;
  if (hasLeftJoin || hasRightJoin || hasFullJoin) {
    Logger.log("Detected outer join (LEFT/RIGHT/FULL JOIN), using special handling");
    
    // Step 1: Replace CROSS JOIN UNNEST part while preserving original table alias
    newSql = sql.replace(
      arrayJoinMatch[0], 
      `FROM ${tempTableName} ${tableAlias}`
    );
    
    // Step 3: Fix array field references throughout the entire SQL query
    const arrayFieldRefRegex = new RegExp(`${arrayAlias}\\.([a-zA-Z0-9_]+)`, 'g');
    newSql = newSql.replace(arrayFieldRefRegex, (match, fieldName) => {
      // Preserve the original reference format to ensure JOIN conditions work correctly
      return `${arrayAlias}.${fieldName}`;
    });
    
    // Additional fix for JOIN conditions that might have been altered
    // Look for JOIN conditions with the array alias
    const joinConditionRegex = new RegExp(`(LEFT|RIGHT|FULL|INNER)\\s+JOIN\\s+[^\\s]+\\s+(?:AS\\s+)?([^\\s]+)\\s+ON\\s+([^\\s]+)\\s*=\\s*${arrayAlias}\\.([a-zA-Z0-9_]+)`, 'gi');
    newSql = newSql.replace(joinConditionRegex, (match, joinType, joinAlias, leftExpr, rightField) => {
      // Ensure the ON condition correctly references the array field
      return `${joinType} JOIN ${joinAlias} ON ${leftExpr} = ${arrayAlias}.${rightField}`;
    });
    
    Logger.log(`After fixing array references: ${newSql}`);
  } else {
    // 对于标准JOIN或无JOIN的查询，简单替换FROM子句
    newSql = sql.replace(
      arrayJoinMatch[0], 
      `FROM ${tempTableName} ${tableAlias}`
    );
  }
  
  // 记录SQL变化
  Logger.log(`原始SQL: ${sql}`);
  Logger.log(`修改后SQL: ${newSql}`);
  
  // In preprocessArrayExpansion, add special handling for JSON functions in JOIN conditions
  if (sql.includes('JSON_EXTRACT') || sql.includes('JSON_VALUE')) {
    Logger.log("检测到 JSON 函数在 JOIN 条件中，使用特殊处理");
    
    // Preserve JSON function calls in ON clauses
    const jsonFuncRegex = /(JSON_EXTRACT|JSON_VALUE)\s*\(\s*([^,]+),\s*['"]([^'"]+)['"]\s*\)/gi;
    newSql = newSql.replace(jsonFuncRegex, (match) => {
      Logger.log(`保留 JSON 函数调用: ${match}`);
      return match; // Preserve the original function call
    });
  }
  
  return {
    tempTableName,
    newSql,
    stats: {
      originalRowCount: preFilterCount,
      filteredRowCount: postFilterCount,
      expandedRowCount: expandedRows.length,
      tempTable: tempTableName
    }
  };
}

/**
 * Expands array data to create new rows
 * @param {Array} rows - Original data rows
 * @param {string} tableAlias - Table alias
 * @param {string} arrayField - Array field name
 * @param {string} arrayAlias - Array alias
 * @param {Array} selectedFields - Selected fields from query
 * @returns {Array} - Expanded rows
 */
function expandArrayData(rows, tableAlias, arrayField, arrayAlias, selectedFields) {
  let resultRows = [];
  
  rows.forEach((row, index) => {
    // 检查字段是否存在
    if (arrayField in row) {
      const arrayData = row[arrayField];
      
      // 检查是否为数组
      if (Array.isArray(arrayData)) {
        // 检查数组是否为空
        if (arrayData.length > 0) {
          // 对每个数组元素创建一个新行
          arrayData.forEach((item) => {
            const expandedRow = createExpandedRow(row, item, tableAlias, arrayField, arrayAlias, selectedFields);
            resultRows.push(expandedRow);
          });
        }
      } else {
        // 尝试解析JSON字符串
        if (typeof arrayData === 'string' && (arrayData.startsWith('[') || arrayData.startsWith('{'))) {
          try {
            const parsedData = JSON.parse(arrayData);
            if (Array.isArray(parsedData) && parsedData.length > 0) {
              parsedData.forEach((item) => {
                const expandedRow = createExpandedRow(row, item, tableAlias, arrayField, arrayAlias, selectedFields);
                resultRows.push(expandedRow);
              });
            }
          } catch (e) {
            // 忽略解析错误
          }
        }
      }
    }
  });
  
  // 如果没有展开任何行，创建一个空结果行
  if (resultRows.length === 0) {
    // 创建一个空行，保留基本字段
    const emptyRow = { __empty_result: true };
    
    // 添加表字段的占位符
    if (rows.length > 0) {
      const firstRow = rows[0];
      for (const key in firstRow) {
        if (key !== arrayField) {
          emptyRow[key] = null;
        }
      }
    }
    
    // 添加数组别名字段的占位符
    emptyRow[arrayAlias] = null;
    
    resultRows.push(emptyRow);
  }
  
  return resultRows;
}

/**
 * Creates an expanded row from an array item
 * @param {Object} row - Original data row
 * @param {*} item - Array item
 * @param {string} tableAlias - Table alias
 * @param {string} arrayField - Array field name
 * @param {string} arrayAlias - Array alias
 * @param {Array} selectedFields - Selected fields from query
 * @returns {Object} - Expanded row
 */
function createExpandedRow(row, item, tableAlias, arrayField, arrayAlias, selectedFields) {
  const expandedRow = {};
  
  // 保留所有原始行数据，保持原始字段名
  for (const key in row) {
    if (key !== arrayField) { // 排除数组字段本身
      expandedRow[key] = row[key];
    }
  }
  
  // 添加数组项字段
  if (typeof item === 'object' && item !== null && !Array.isArray(item)) {
    // 对象类型的数组项 - 展开其属性
    expandedRow[arrayAlias] = item; // 保留完整对象
    
    // 也添加每个属性作为单独的字段
    for (const key in item) {
      expandedRow[`${arrayAlias}.${key}`] = item[key]; // 使用点号语法而不是下划线
    }
  } else {
    // 简单值类型的数组项
    expandedRow[arrayAlias] = item;
  }
  
  // 如果提供了选定字段列表，确保它们都存在（即使为null）
  if (selectedFields && selectedFields.length > 0) {
    selectedFields.forEach(fieldExpr => {
      // 处理 "alias.field" 格式的字段引用
      if (fieldExpr.includes('.')) {
        const [alias, field] = fieldExpr.split('.');
        // 只处理与当前表别名或数组别名匹配的字段
        if (alias === tableAlias || alias === arrayAlias) {
          if (!(fieldExpr in expandedRow)) {
            expandedRow[fieldExpr] = null;
          }
        }
      }
      // 处理可能的AS别名
      else if (fieldExpr.toLowerCase().includes(' as ')) {
        const parts = fieldExpr.split(/\s+as\s+/i);
        const aliasName = parts[1].trim();
        // 为别名创建字段，如果需要
        if (!(aliasName in expandedRow)) {
          expandedRow[aliasName] = null;
        }
      }
    });
  }
  
  return expandedRow;
}

/**
 * 转义正则表达式中的特殊字符
 * @param {string} string - 需要转义的字符串
 * @returns {string} - 转义后的字符串
 */
function escapeRegExp(string) {
  return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * 提取针对主表的过滤条件
 * @param {string} whereClause - 完整的WHERE子句
 * @param {string} tableAlias - 表别名
 * @returns {Object|null} - 主表过滤条件
 */
function extractPrimaryTableFilters(whereClause, tableAlias) {
  if (!whereClause) return null;
  
  // 寻找针对主表的简单过滤条件 (如 t.id = 123)
  const tableFilterRegex = new RegExp(`${tableAlias}\\.(\\w+)\\s*(=|>|<|>=|<=|!=|<>|IN|LIKE)\\s*(.+?)(?:\\s+(?:AND|OR)\\s+|$)`, 'gi');
  const matches = [...whereClause.matchAll(tableFilterRegex)];
  
  if (matches.length === 0) return null;
  
  const filters = {};
  matches.forEach(match => {
    const fieldName = match[1];
    const operator = match[2].toUpperCase();
    let value = match[3].trim();
    
    // 处理引号和数字
    if ((value.startsWith("'") && value.endsWith("'")) || 
        (value.startsWith('"') && value.endsWith('"'))) {
      value = value.substring(1, value.length - 1);
    } else if (!isNaN(value)) {
      value = Number(value);
    }
    
    filters[fieldName] = { operator, value };
  });
  
  return filters;
}

/**
 * 加载并过滤表数据
 * @param {string} tableName - 表名（格式为"fileName.sheetName"）
 * @param {Object|null} filters - 过滤条件
 * @returns {Object} - 数据、表头和过滤后的行数
 */
function loadFilteredTableData(tableName, filters) {
  const parts = tableName.split('.');
  if (parts.length !== 2) {
    throw new Error(`无效的表名格式: ${tableName}`);
  }

  const fileName = parts[0];
  const sheetName = parts[1];
  const files = findSheetByName(fileName);
  
  if (files.length === 0) {
    throw new Error(`找不到文件: ${fileName}`);
  }

  const fileId = files[0].id;
  const spreadsheet = SpreadsheetApp.openById(fileId);
  const sheet = spreadsheet.getSheetByName(sheetName);

  if (!sheet) {
    throw new Error(`找不到工作表: ${sheetName}`);
  }

  // 获取数据
  const allData = sheet.getDataRange().getValues();
  const headers = allData[0];
  
  // 如果没有过滤条件，返回所有数据
  if (!filters) {
    return { data: allData, headers };
  }
  
  // 应用过滤条件
  const filteredData = [headers]; // 保留表头行
  
  // 对每行应用过滤器
  for (let i = 1; i < allData.length; i++) {
    const row = allData[i];
    let includeRow = true;
    
    // 检查行是否符合所有过滤条件
    for (const field in filters) {
      const columnIndex = headers.indexOf(field);
      if (columnIndex === -1) continue;
      
      const cellValue = row[columnIndex];
      const filter = filters[field];
      
      // 根据操作符进行比较
      switch (filter.operator) {
        case '=':
          if (cellValue != filter.value) includeRow = false;
          break;
        case '>':
          if (cellValue <= filter.value) includeRow = false;
          break;
        case '<':
          if (cellValue >= filter.value) includeRow = false;
          break;
        case '>=':
          if (cellValue < filter.value) includeRow = false;
          break;
        case '<=':
          if (cellValue > filter.value) includeRow = false;
          break;
        case '!=':
        case '<>':
          if (cellValue == filter.value) includeRow = false;
          break;
        case 'IN':
          // 简单处理IN，假设值是逗号分隔的列表
          const inValues = filter.value.replace(/[\(\)]/g, '').split(',').map(v => v.trim());
          if (!inValues.includes(String(cellValue))) includeRow = false;
          break;
        case 'LIKE':
          // 简单处理LIKE，将SQL通配符转换为正则表达式
          const pattern = filter.value.replace(/%/g, '.*').replace(/_/g, '.');
          const regex = new RegExp(`^${pattern}$`, 'i');
          if (!regex.test(String(cellValue))) includeRow = false;
          break;
      }
      
      if (!includeRow) break;
    }
    
    if (includeRow) {
      filteredData.push(row);
    }
  }
  
  return { 
    data: filteredData, 
    headers, 
    filtered: filteredData.length - 1 // 过滤后的行数（不包括表头）
  };
}

/**
 * 提取用于展开后数据的次级过滤条件
 * @param {string} whereClause - 完整的WHERE子句
 * @param {string} tableAlias - 表别名
 * @param {string} arrayAlias - 数组别名
 * @returns {string|null} - 次级过滤条件
 */
function extractSecondaryFilters(whereClause, tableAlias, arrayAlias) {
  if (!whereClause) return null;
  
  // 提取数组相关的过滤条件或其他未处理的条件
  // 由于复杂性，这里可能需要根据实际情况调整
  // 简单实现：如果有主表过滤条件，就保留原始WHERE子句
  return whereClause;
}

/**
 * Loads table data from a Google Sheet
 * @param {string} tableName - Table name in format "fileName.sheetName"
 * @returns {Object} - Data and headers from the sheet
 */
function loadTableData(tableName) {
  const parts = tableName.split('.');
  if (parts.length !== 2) {
    throw new Error(`Invalid table name format: ${tableName}`);
  }

  const fileName = parts[0];
  const sheetName = parts[1];
  const files = findSheetByName(fileName);
  
  if (files.length === 0) {
    throw new Error(`File not found: ${fileName}`);
  }

  const fileId = files[0].id;
  const spreadsheet = SpreadsheetApp.openById(fileId);
  const sheet = spreadsheet.getSheetByName(sheetName);

  if (!sheet) {
    throw new Error(`Sheet not found: ${sheetName}`);
  }

  // Get data
  const data = sheet.getDataRange().getValues();
  const headers = data[0];
  
  return { data, headers };
}

/**
 * Parses SQL query components (SELECT, WHERE, ORDER BY, LIMIT clauses)
 * @param {string} sql - SQL query string
 * @returns {Object} - Parsed query components
 */
function parseQueryComponents(sql) {
  // 改进：使用更健壮的SELECT子句提取方法，处理嵌套查询和子查询
  let selectedFields = [];
  
  try {
    // 提取SELECT和FROM之间的内容，但要考虑嵌套的子查询
    const selectRegex = /SELECT\s+([\s\S]+?)\s+FROM/i;
    const selectMatch = sql.match(selectRegex);
    
    if (selectMatch) {
      const selectClause = selectMatch[1].trim();
      
      if (selectClause !== '*') {
        // 改进：使用更复杂的逻辑处理逗号分隔列表，考虑嵌套括号和引号
        selectedFields = splitSelectFields(selectClause);
      }
    }
  } catch (e) {
    Logger.log(`解析SELECT子句出错: ${e}`);
    // 返回空数组表示无法解析字段，将使用 SELECT *
  }
  
  // 提取WHERE、ORDER BY、LIMIT子句的代码保持不变
  const whereMatch = sql.match(/WHERE\s+(.+?)(?:\s+ORDER\s+BY|\s+LIMIT|\s*$)/i);
  const whereClause = whereMatch ? whereMatch[1] : null;
  
  const orderByMatch = sql.match(/ORDER\s+BY\s+(.+?)(?:\s+LIMIT|\s*$)/i);
  const orderByClause = orderByMatch ? orderByMatch[1] : null;
  
  const limitMatch = sql.match(/LIMIT\s+(\d+)/i);
  const limitValue = limitMatch ? parseInt(limitMatch[1]) : null;
  
  return {
    selectedFields,
    whereClause,
    orderByClause, 
    limitValue
  };
}

/**
 * 智能拆分SELECT子句中的字段，考虑嵌套括号和函数调用
 * @param {string} selectClause - SELECT子句内容
 * @returns {Array} - 分离后的字段列表
 */
function splitSelectFields(selectClause) {
  const fields = [];
  let currentField = '';
  let bracketCount = 0;
  let inQuote = false;
  let quoteChar = '';
  
  // 逐字符分析SELECT子句
  for (let i = 0; i < selectClause.length; i++) {
    const char = selectClause[i];
    
    // 处理引号
    if ((char === "'" || char === '"') && (i === 0 || selectClause[i-1] !== '\\')) {
      if (!inQuote) {
        inQuote = true;
        quoteChar = char;
      } else if (char === quoteChar) {
        inQuote = false;
      }
    }
    
    // 处理括号 (只在不在引号内时计数)
    if (!inQuote) {
      if (char === '(' || char === '[' || char === '{') {
        bracketCount++;
      } else if (char === ')' || char === ']' || char === '}') {
        bracketCount--;
      }
    }
    
    // 处理字段分隔符逗号 (只在不在引号内且没有未闭合的括号时)
    if (char === ',' && !inQuote && bracketCount === 0) {
      fields.push(currentField.trim());
      currentField = '';
    } else {
      currentField += char;
    }
  }
  
  // 添加最后一个字段
  if (currentField.trim()) {
    fields.push(currentField.trim());
  }
  
  return fields;
}

/**
 * Converts sheet data to row objects
 * @param {Array} data - Raw sheet data
 * @param {Array} headers - Sheet headers
 * @returns {Array} - Array of row objects
 */
function convertToRowObjects(data, headers) {
  return data.slice(1).map(row => {
    const obj = {};
    headers.forEach((header, index) => {
      let value = row[index];
      // Try to parse JSON strings
      if (typeof value === 'string' && (value.startsWith('[') || value.startsWith('{'))) {
        try {
          value = JSON.parse(value);
        } catch (e) {
          // Keep original value if parsing fails
        }
      }
      obj[header] = value;
    });
    return obj;
  });
}

/**
 * Applies WHERE condition filtering
 * @param {Array} rows - Data rows
 * @param {string} whereClause - WHERE clause
 * @returns {Array} - Filtered rows
 */
function applyWhereCondition(rows, whereClause) {
  if (!whereClause || rows.length === 0) return rows;
  
  try {
    // Create temporary table and apply filtering
    const tempTableName = 'temp_' + Math.random().toString(36).substring(2, 8);
    alasql(`CREATE TABLE ${tempTableName}`);
    alasql(`INSERT INTO ${tempTableName} SELECT * FROM ?`, [rows]);
    
    // Execute filtering query
    const filteredRows = alasql(`SELECT * FROM ${tempTableName} WHERE ${whereClause}`);
    
    // Clean up temporary table
    alasql(`DROP TABLE ${tempTableName}`);
    
    return filteredRows;
  } catch (e) {
    Logger.log(`Error applying WHERE condition: ${e}`);
    return rows; // Return original rows if filtering fails
  }
}

/**
 * Applies ORDER BY sorting
 * @param {Array} rows - Data rows
 * @param {string} orderByClause - ORDER BY clause
 * @returns {Array} - Sorted rows
 */
function applySorting(rows, orderByClause) {
  if (!orderByClause || rows.length === 0) return rows;
  
  try {
    // Create temporary table and apply sorting
    const tempTableName = 'temp_' + Math.random().toString(36).substring(2, 8);
    alasql(`CREATE TABLE ${tempTableName}`);
    alasql(`INSERT INTO ${tempTableName} SELECT * FROM ?`, [rows]);
    
    // Execute sorting query
    const sortedRows = alasql(`SELECT * FROM ${tempTableName} ORDER BY ${orderByClause}`);
    
    // Clean up temporary table
    alasql(`DROP TABLE ${tempTableName}`);
    
    return sortedRows;
  } catch (e) {
    Logger.log(`Error applying ORDER BY: ${e}`);
    return rows; // Return original rows if sorting fails
  }
}

/**
 * Applies LIMIT restriction
 * @param {Array} rows - Data rows
 * @param {number} limit - Limit value
 * @returns {Array} - Limited rows
 */
function applyLimit(rows, limit) {
  if (!limit || limit <= 0 || rows.length === 0) return rows;
  return rows.slice(0, limit);
}

function extractRegularTableReferences(sql) {
  // Get all direct table references from FROM and JOIN clauses
  const regex = /(?:FROM|JOIN)\s+([a-zA-Z0-9_\.\u4e00-\u9fa5]+)(?:\s+(?:AS\s+)?([a-zA-Z0-9_\u4e00-\u9fa5]+))?/gi;
  const matches = [...sql.matchAll(regex)];
  const tableReferences = new Set();
  
  matches.forEach(match => {
    const table = match[1];
    if (table) {
      // Check if this is a temporary table or a regular table
      const tableName = table.trim();
      if (!tableName.startsWith('expanded_')) {
        tableReferences.add(tableName);
      } else {
        Logger.log(`检测到临时表引用: ${tableName}`);
      }
    }
  });
  
  // Additional regex to catch tables in LEFT JOIN specifically
  // This catches references like "LEFT JOIN table" anywhere in the query
  const leftJoinRegex = /LEFT\s+JOIN\s+([a-zA-Z0-9_\.\u4e00-\u9fa5]+)/gi;
  const leftJoinMatches = [...sql.matchAll(leftJoinRegex)];
  
  leftJoinMatches.forEach(match => {
    const tableName = match[1].trim();
    if (!tableName.startsWith('expanded_')) {
      tableReferences.add(tableName);
      Logger.log(`检测到LEFT JOIN表引用: ${tableName}`);
    }
  });
  
  // Add specific check for tables that might be used with CAST or JSON functions in LEFT JOIN conditions
  if (sql.includes('LEFT JOIN') && (sql.includes('JSON_EXTRACT') || sql.includes('CAST'))) {
    Logger.log("检测到复杂LEFT JOIN查询，查找额外表引用");
    // Extract all potential table aliases from ON clauses
    const onClauseRegex = /ON\s+(?:CAST\s*\()?\s*([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)/gi;
    const onMatches = [...sql.matchAll(onClauseRegex)];
    
    onMatches.forEach(match => {
      const alias = match[1];
      // Find the actual table for this alias
      const aliasDefRegex = new RegExp(`(FROM|JOIN)\\s+([a-zA-Z0-9_\\.\\u4e00-\\u9fa5]+)(?:\\s+(?:AS\\s+)?${alias})`, 'i');
      const aliasMatch = sql.match(aliasDefRegex);
      if (aliasMatch && aliasMatch[2]) {
        const actualTable = aliasMatch[2].trim();
        if (!actualTable.startsWith('expanded_')) {
          tableReferences.add(actualTable);
          Logger.log(`从ON子句别名 ${alias} 检测到表引用: ${actualTable}`);
        }
      }
    });
  }
  
  return tableReferences;
}

// 处理普通SQL查询
function executeRegularSQL(sql, params, startTime) {
  try {
    // 记录不同阶段的时间统计
    const timings = {
      initialization: new Date().getTime() - startTime,
      tableLoading: 0,
      execution: 0,
      formatting: 0
    };
    // 提取表引用
    const tableReferences = extractRegularTableReferences(sql);
    Logger.log("查询引用的表: " + JSON.stringify(Array.from(tableReferences)));
    
    // 创建替换表映射和修改后的SQL
    const tableMapping = {};
    let modifiedSql = sql;
    
    // 存储原表的列顺序
    const tableColumnOrders = {};
    
    // 加载表数据的开始时间
    const loadingStartTime = new Date().getTime();
    
    // 加载表数据 
    for (const tableRef of tableReferences) {
      const parts = tableRef.split('.');
      
      // 新增：检查表是否已存在于 alasql 中（如临时表）
      if (alasql.tables && alasql.tables[tableRef]) {
        Logger.log(`表 ${tableRef} 已存在，无需加载`);
        continue; // 如果表已存在，跳过后续处理
      }
      
      // 修改: 处理所有表引用类型
      if (parts.length === 2) {
        // 原始代码：处理 fileName.sheetName 格式的表引用
        const fileName = parts[0];
        const sheetName = parts[1];
        
        // 创建表名
        const randomPrefix = 'tbl' + Math.random().toString(36).substring(2, 8);
        const alasqlTableName = `${randomPrefix}`;
        
        // 更新SQL中的表引用和记录映射 - 这部分不变
        let matchPattern = tableRef.replace(/\./g, '\\.').replace(/[\[\]\(\)\{\}\*\+\?\|\^\$]/g, '\\$&');
        const tableAliasRegex = new RegExp(`${matchPattern}\\s+(?:AS\\s+)?([a-zA-Z0-9_\u4e00-\u9fa5]+)`, 'gi');
        const tableAliasMatches = [...modifiedSql.matchAll(tableAliasRegex)];
        
        if (tableAliasMatches.length > 0) {
          for (const match of tableAliasMatches) {
            const fullMatch = match[0];
            const alias = match[1];
            modifiedSql = modifiedSql.replace(fullMatch, `${alasqlTableName} ${alias}`);
          }
        } else {
          const tableRegex = new RegExp(matchPattern, 'g');
          modifiedSql = modifiedSql.replace(tableRegex, alasqlTableName);
        }
        
        // 记录表映射关系
        tableMapping[tableRef] = alasqlTableName;
        Logger.log(`表映射: ${tableRef} -> ${alasqlTableName}`);
        const files = findSheetByName(fileName);
        if (files.length > 0) {
          const fileId = files[0].id;
          const spreadsheet = SpreadsheetApp.openById(fileId);
          const sheet = spreadsheet.getSheetByName(sheetName);
          
          if (sheet) {
            // 直接从表单中获取数据并创建表
            const data = sheet.getDataRange().getValues();
            
            // 将数据转换为对象数组（表头作为列名）
            const headers = data[0];
            const rows = [];
            
            // 存储原始列顺序
            const filteredHeaders = [...headers].filter(header => !isExcludedColumn(header));
            tableColumnOrders[alasqlTableName] = filteredHeaders;
            
            for (let i = 1; i < data.length; i++) {
              const row = {};
              for (let j = 0; j < headers.length; j++) {
                // 跳过被排除的列名
                if (!isExcludedColumn(headers[j])) {
                  row[headers[j]] = data[i][j];
                }
              }
              rows.push(row);
            }
            
            // 使用修改后的表名创建表
            Logger.log(`创建表 ${alasqlTableName} (原始: ${tableRef})`);
            alasql(`CREATE TABLE ${alasqlTableName}`);
            
            // 一次性插入所有数据以提高性能
            if (rows.length > 0) {
              alasql(`INSERT INTO ${alasqlTableName} SELECT * FROM ?`, [rows]);
              Logger.log(`已加载表 ${alasqlTableName}，共 ${rows.length} 条记录`);
            }
          } else {
            // 创建空表
            alasql(`CREATE TABLE ${alasqlTableName} (dummy INT)`);
            Logger.log(`警告: 表 ${tableRef} 不存在或没有数据，创建了空表 ${alasqlTableName}`);
          }
        } else {
          return { error: `文件 "${fileName}" 未找到` };
        }
      } else {
        // 新增：处理单一名称的表引用（如临时表）
        Logger.log(`处理单一名称表: ${tableRef}`);
        // 这种情况不需要加载数据，但我们需要确保 SQL 中的引用是正确的
        // 在一些情况下可能需要加入其他逻辑，如检查表是否存在等
      }
    }
    
    // 记录表加载时间
    timings.tableLoading = new Date().getTime() - loadingStartTime;
    
    // 执行修改后的查询
    Logger.log("原始查询: " + sql);
    Logger.log("修改后查询: " + modifiedSql);
    
    // 执行查询的开始时间
    const executionStartTime = new Date().getTime();
    
    // 添加调试信息
    Logger.log(`即将执行 SQL: ${modifiedSql}`);
    
    // Log available tables before executing
    const availableTables = Object.keys(alasql.tables || {});
    Logger.log(`可用的表有: ${availableTables.join(', ')}`);
    
    // IMPORTANT: Define result variable here, outside the inner try/catch
    let result;
    
    try {
      // Sample data for debugging
      if (tableReferences.size > 0) {
        try {
          const firstTableRef = Array.from(tableReferences)[0];
          const sampleData = alasql(`SELECT TOP 3 * FROM ${firstTableRef}`);
          Logger.log(`${firstTableRef} 表的前3条数据: ${JSON.stringify(sampleData)}`);
        } catch (e) {
          Logger.log(`获取表数据示例失败: ${e}`);
        }
      }
      
      // Execute the query
      result = alasql(modifiedSql, params);
    } catch (e) {
      Logger.log(`执行查询错误: ${e.toString()}\nSQL: ${modifiedSql}`);
      throw e; // Re-throw to outer catch
    }
    
    // 记录执行时间
    timings.execution = new Date().getTime() - executionStartTime;
    
    // 格式化结果的开始时间 
    const formattingStartTime = new Date().getTime();
    
    // 如果是 SELECT * 查询并且结果是数组，尝试保持原表的列顺序
    if (Array.isArray(result) && result.length > 0) {
      // 检查是否是简单的 SELECT * 查询
      const isSelectStar = /SELECT\s+\*\s+FROM\s+([^\s;]+)/i.test(sql);
      
      if (isSelectStar) {
        // 尝试找到表名
        const tableMatch = sql.match(/FROM\s+([^\s;]+)/i);
        if (tableMatch && tableMatch[1]) {
          const originalTableRef = tableMatch[1].trim();
          const aliasedTableName = tableMapping[originalTableRef];
          
          // 如果有这个表的列顺序记录，按照它重新排序结果
          if (aliasedTableName && tableColumnOrders[aliasedTableName]) {
            const orderedColumns = tableColumnOrders[aliasedTableName];
            
            // 重新排序结果的列
            result = result.map(row => {
              const orderedRow = {};
              orderedColumns.forEach(column => {
                if (column in row) {
                  orderedRow[column] = row[column];
                }
              });
              
              // 添加可能的其他列（如计算列）
              Object.keys(row).forEach(key => {
                if (!orderedColumns.includes(key)) {
                  orderedRow[key] = row[key];
                }
              });
              
              return orderedRow;
            });
          }
        }
      }
    }
    
    // 记录格式化时间
    timings.formatting = new Date().getTime() - formattingStartTime;
    
    // 计算总时间
    const totalTime = new Date().getTime() - startTime;
    
    // 将结果包装在一个对象中，包含执行时间信息
    return {
      data: result,
      stats: {
        rowCount: Array.isArray(result) ? result.length : 0,
        executionTime: totalTime,
        timings: timings
      }
    };
  } catch (e) {
    Logger.log("查询错误: " + e.toString() + "\n堆栈: " + e.stack);
    const totalTime = new Date().getTime() - startTime;
    return { 
      error: '执行 SQL 查询失败: ' + e.toString(),
      stats: {
        executionTime: totalTime
      }
    };
  }
}

/**
 * 创建 Web App UI
 * @returns {HtmlOutput} - HTML 界面
 */
function doGet(e) {
  // 检查是否请求配置检查页面
  Logger.log('doget page', e) 
  if (e && e.parameter && e.parameter.page === 'check') {
    Logger.log('check page') 
    return showConfigCheckPage();
  }
  
  // 检查是否请求帮助页面
  if (e && e.parameter && e.parameter.page === 'help') {
    return HtmlService.createHtmlOutputFromFile('Help')
      .setTitle('Google Sheets SQL - 帮助文档');
  }
  
  const config = getConfig();
  
  // 返回主页面
  return HtmlService.createHtmlOutputFromFile('Index')
    .setTitle(config.app.name)
    .setFaviconUrl(config.app.faviconUrl);
}

// 暴露给前端的 API 函数
function getAllSheetsForClient() {
  return getAllSheets();
}

function executeSQLForClient(sql, params = {}) {
  const result = executeSQL(sql, params);
  
  // 添加查询 SQL 到结果中，便于调试
  if (result.stats) {
    result.stats.sql = sql;
  }
  
  return result;
}

/**
 * 获取工作表的基础信息（页签列表和第一个页签的表头）
 * @param {string} fileId - Sheet 文件的 ID
 * @returns {Object} - 包含页签列表和表头信息的对象
 */
function getSheetMetadata(fileId) {
  try {
    const spreadsheet = SpreadsheetApp.openById(fileId);
    const sheets = spreadsheet.getSheets();
    const result = {
      sheets: [],      // 所有页签信息
      firstHeaders: [] // 第一个页签的表头
    };
    
    // 获取所有页签名称
    for (let i = 0; i < sheets.length; i++) {
      const sheet = sheets[i];
      result.sheets.push({
        name: sheet.getName(),
        index: i
      });
      
      // 获取第一个页签的表头
      if (i === 0 && sheet.getLastRow() > 0) {
        const headerRange = sheet.getRange(1, 1, 1, sheet.getLastColumn());
        result.firstHeaders = headerRange.getValues()[0];
      }
    }
    
    return result;
  } catch (e) {
    Logger.log('获取 Sheet 元数据失败: ' + e);
    return {
      sheets: [],
      firstHeaders: [],
      error: e.toString()
    };
  }
}

/**
 * 检查列名是否在排除列表中
 * @param {string} columnName - 列名
 * @returns {boolean} - 是否被排除
 */
function isExcludedColumn(columnName) {
  const config = getConfig();
  const excludedColumns = config.data && config.data.excludedColumns 
    ? config.data.excludedColumns 
    : [];
  return excludedColumns.includes(columnName);
}

/**
 * 获取当前配置的排除列名列表
 * @returns {string[]} - 排除的列名数组
 */
function getExcludedColumns() {
  const config = getConfig();
  return config.data && config.data.excludedColumns 
    ? config.data.excludedColumns 
    : [];
}

/**
 * 设置排除列名列表
 * @param {string[]} columns - 要排除的列名数组
 * @returns {Object} - 操作结果
 */
function setExcludedColumns(columns) {
  try {
    // 这里需要根据配置存储机制来更新配置
    // 如果使用 Properties Service 来存储配置，代码可能类似：
    const userProperties = PropertiesService.getUserProperties();
    const configStr = userProperties.getProperty('gsql_config') || '{}';
    const config = JSON.parse(configStr);
    
    if (!config.data) config.data = {};
    config.data.excludedColumns = Array.isArray(columns) ? columns : [];
    
    userProperties.setProperty('gsql_config', JSON.stringify(config));
    
    return {
      success: true,
      message: `已更新排除列名列表，共 ${config.data.excludedColumns.length} 个列名`
    };
  } catch (e) {
    return {
      success: false,
      error: e.toString()
    };
  }
}

// 为客户端暴露 API
function getExcludedColumnsForClient() {
  return getExcludedColumns();
}

function setExcludedColumnsForClient(columns) {
  return setExcludedColumns(columns);
}
