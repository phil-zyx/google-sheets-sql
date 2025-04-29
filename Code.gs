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
    // Ensure AlaSQL is properly initialized
    if (!alasql || typeof alasql !== 'function') {
      const initSuccess = initAlaSQL();
      if (!initSuccess) {
        throw new Error('无法初始化 AlaSQLGS 库');
      }
    }

    // Check for UNNEST operations
    const arrayJoinMatch = sql.match(/FROM\s+([a-zA-Z0-9_\.]+)\s+([a-zA-Z0-9_]+)\s+CROSS\s+JOIN\s+UNNEST\(\s*(\2\.[a-zA-Z0-9_]+)\s*\)(?:\s+AS)?\s+([a-zA-Z0-9_]+)/i);
    
    let modifiedSql = sql;
    let expansionStats = null;
    
    // If UNNEST operation exists, preprocess it
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
      
      // Preprocess UNNEST operation, return temp table name and modified SQL
      const { tempTableName, newSql, stats } = preprocessArrayExpansion(sql, modifiedMatch);
      modifiedSql = newSql;
      expansionStats = stats;
    }
    
    // Execute the processed SQL query
    const result = executeRegularSQL(modifiedSql, params, startTime);
    
    // If array expansion stats exist, add to result
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
  const tableAlias = arrayJoinMatch[2];  // Original table alias, e.g., 't'
  const arrayField = arrayJoinMatch[3];
  const arrayAlias = arrayJoinMatch[4];
  
  // Parse query components
  const queryParts = parseQueryComponents(sql);
  
  // Log original query info
  Logger.log(`处理UNNEST操作: 表=${tableName}, 别名=${tableAlias}, 数组字段=${arrayField}, 数组别名=${arrayAlias}`);
  Logger.log(`选择的字段: ${JSON.stringify(queryParts.selectedFields)}`);
  
  // Extract primary table filters
  const primaryTableFilters = extractPrimaryTableFilters(queryParts.whereClause, tableAlias);
  
  // Load data and apply primary table filters
  const { data, headers, filtered } = loadFilteredTableData(tableName, primaryTableFilters);
  if (!data) {
    throw new Error(`无法从 ${tableName} 加载数据`);
  }
  
  // Record stats
  const preFilterCount = data.length - 1;
  const postFilterCount = filtered ? filtered : preFilterCount;
  
  // Convert raw data to row objects
  const rows = convertToRowObjects(data, headers);
  
  // Record first row original data for debugging
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
  
  // Execute array expansion
  const expandedRows = expandArrayData(rows, tableAlias, arrayField, arrayAlias, queryParts.selectedFields);
  
  // Create temp table to store expanded data
  const tempTableName = 'expanded_' + Math.random().toString(36).substring(2, 8);
  alasql(`CREATE TABLE ${tempTableName}`);
  alasql(`INSERT INTO ${tempTableName} SELECT * FROM ?`, [expandedRows]);
  
  // View temp table data
  const sampleData = alasql(`SELECT TOP 3 * FROM ${tempTableName}`);
  Logger.log(`${tempTableName} 表的前3条数据: ${JSON.stringify(sampleData)}`);
  
  // View temp table structure
  const columns = [];
  if (sampleData.length > 0) {
    for (const key in sampleData[0]) {
      columns.push(key);
    }
    Logger.log(`临时表列名: ${columns.join(', ')}`);
  }
  
  // Find all JOIN types in SQL
  const joinTypes = [];
  const joinRegex = /(LEFT|RIGHT|INNER|OUTER|CROSS|FULL)?\s+JOIN/gi;
  let joinMatch;
  while ((joinMatch = joinRegex.exec(sql)) !== null) {
    if (joinMatch[1]) {
      joinTypes.push(joinMatch[1].toUpperCase());
    } else {
      joinTypes.push("INNER"); // Default JOIN type is INNER
    }
  }
  Logger.log(`SQL中的JOIN类型: ${joinTypes.join(', ')}`);
  
  // Detect SQL for LEFT JOIN etc. that need special handling
  const hasLeftJoin = /LEFT\s+JOIN/i.test(sql);
  const hasRightJoin = /RIGHT\s+JOIN/i.test(sql);
  const hasFullJoin = /FULL\s+JOIN/i.test(sql);
  
  // Special handling for LEFT JOIN and other outer joins
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
    // For standard JOIN or no JOIN queries, simply replace FROM clause
    newSql = sql.replace(
      arrayJoinMatch[0], 
      `FROM ${tempTableName} ${tableAlias}`
    );
  }
  
  // Record SQL changes
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
    // Check if field exists
    if (arrayField in row) {
      const arrayData = row[arrayField];
      
      // Check if it's an array
      if (Array.isArray(arrayData)) {
        // Check if array is empty
        if (arrayData.length > 0) {
          // Create a new row for each array element
          arrayData.forEach((item) => {
            const expandedRow = createExpandedRow(row, item, tableAlias, arrayField, arrayAlias, selectedFields);
            resultRows.push(expandedRow);
          });
        }
      } else {
        // Try to parse JSON string
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
            // Ignore parsing error
          }
        }
      }
    }
  });
  
  // If no rows were expanded, create an empty result row
  if (resultRows.length === 0) {
    // Create an empty row, preserving basic fields
    const emptyRow = { __empty_result: true };
    
    // Add placeholder for table fields
    if (rows.length > 0) {
      const firstRow = rows[0];
      for (const key in firstRow) {
        if (key !== arrayField) {
          emptyRow[key] = null;
        }
      }
    }
    
    // Add placeholder for array alias field
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
  
  // Preserve all original row data, keeping original field names
  for (const key in row) {
    if (key !== arrayField) { // Exclude array field itself
      expandedRow[key] = row[key];
    }
  }
  
  // Add array item field
  if (typeof item === 'object' && item !== null && !Array.isArray(item)) {
    // Object type array items - expand their properties
    expandedRow[arrayAlias] = item; // Preserve full object
    
    // Also add each property as a separate field
    for (const key in item) {
      expandedRow[`${arrayAlias}.${key}`] = item[key]; // Use dot syntax instead of underscore
    }
  } else {
    // Simple value type array items
    expandedRow[arrayAlias] = item;
  }
  
  // If selected fields list is provided, ensure they exist (even as null)
  if (selectedFields && selectedFields.length > 0) {
    selectedFields.forEach(fieldExpr => {
      // Handle "alias.field" format field references
      if (fieldExpr.includes('.')) {
        const [alias, field] = fieldExpr.split('.');
        // Only handle fields that match current table alias or array alias
        if (alias === tableAlias || alias === arrayAlias) {
          if (!(fieldExpr in expandedRow)) {
            expandedRow[fieldExpr] = null;
          }
        }
      }
      // Handle possible AS aliases
      else if (fieldExpr.toLowerCase().includes(' as ')) {
        const parts = fieldExpr.split(/\s+as\s+/i);
        const aliasName = parts[1].trim();
        // Create field for alias if needed
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
  
  // Find simple filter conditions for the main table (e.g., t.id = 123)
  const tableFilterRegex = new RegExp(`${tableAlias}\\.(\\w+)\\s*(=|>|<|>=|<=|!=|<>|IN|LIKE)\\s*(.+?)(?:\\s+(?:AND|OR)\\s+|$)`, 'gi');
  const matches = [...whereClause.matchAll(tableFilterRegex)];
  
  if (matches.length === 0) return null;
  
  const filters = {};
  matches.forEach(match => {
    const fieldName = match[1];
    const operator = match[2].toUpperCase();
    let value = match[3].trim();
    
    // Handle quotes and numbers
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
          // Simple handling for IN, assuming value is comma-separated list
          const inValues = filter.value.replace(/[\(\)]/g, '').split(',').map(v => v.trim());
          if (!inValues.includes(String(cellValue))) includeRow = false;
          break;
        case 'LIKE':
          // Simple handling for LIKE, converting SQL wildcards to regular expression
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
    filtered: filteredData.length - 1 // Filtered row count (excluding header)
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
  
  // Extract array-related filters or other unprocessed conditions
  // Due to complexity, this might need to be adjusted based on actual situation
  // Simple implementation: If there's main table filter, keep original WHERE clause
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
  // Improved: Use more robust SELECT clause extraction method, handling nested queries and subqueries
  let selectedFields = [];
  
  try {
    // Extract SELECT and FROM content, but consider nested subqueries
    const selectRegex = /SELECT\s+([\s\S]+?)\s+FROM/i;
    const selectMatch = sql.match(selectRegex);
    
    if (selectMatch) {
      const selectClause = selectMatch[1].trim();
      
      if (selectClause !== '*') {
        // Improved: Use more complex logic to handle comma-separated lists, considering nested parentheses and quotes
        selectedFields = splitSelectFields(selectClause);
      }
    }
  } catch (e) {
    Logger.log(`解析SELECT子句出错: ${e}`);
    // Return empty array indicating unable to parse fields, will use SELECT *
  }
  
  // Extract WHERE, ORDER BY, LIMIT clauses
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
  
  // Analyze SELECT clause character by character
  for (let i = 0; i < selectClause.length; i++) {
    const char = selectClause[i];
    
    // Handle quotes
    if ((char === "'" || char === '"') && (i === 0 || selectClause[i-1] !== '\\')) {
      if (!inQuote) {
        inQuote = true;
        quoteChar = char;
      } else if (char === quoteChar) {
        inQuote = false;
      }
    }
    
    // Handle parentheses (only count when not in quotes)
    if (!inQuote) {
      if (char === '(' || char === '[' || char === '{') {
        bracketCount++;
      } else if (char === ')' || char === ']' || char === '}') {
        bracketCount--;
      }
    }
    
    // Handle field separator comma (only when not in quotes and no unclosed parentheses)
    if (char === ',' && !inQuote && bracketCount === 0) {
      fields.push(currentField.trim());
      currentField = '';
    } else {
      currentField += char;
    }
  }
  
  // Add last field
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
  const regex = /(?:FROM|(?:LEFT|RIGHT|INNER|OUTER|CROSS|FULL)?\s*JOIN)\s+([a-zA-Z0-9_\.\u4e00-\u9fa5]+)(?:\s+(?:AS\s+)?([a-zA-Z0-9_\u4e00-\u9fa5]+))?/gi;
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
  
  // Enhance JSON function detection in JOIN conditions
  if (sql.includes('JOIN') && sql.includes('JSON_EXTRACT')) {
    Logger.log("检测到带有 JSON_EXTRACT 的 JOIN 查询，特殊处理");
    
    // Add special handling for JSON_EXTRACT in ON clauses
    const jsonExtractJoinRegex = /ON\s+([a-zA-Z0-9_\.]+)\s*=\s*JSON_EXTRACT\s*\(\s*([a-zA-Z0-9_\.]+)\s*,\s*['"]\$\.([a-zA-Z0-9_]+)['"]\s*\)/gi;
    const jsonExtractMatches = [...sql.matchAll(jsonExtractJoinRegex)];
    
    jsonExtractMatches.forEach(match => {
      const leftField = match[1];
      const rightField = match[2];
      
      // Extract table aliases from field references
      const leftAlias = leftField.split('.')[0];
      const rightAlias = rightField.split('.')[0];
      
      // Find actual tables for these aliases
      const leftTableMatch = sql.match(new RegExp(`(FROM|JOIN)\\s+([a-zA-Z0-9_\\.\\u4e00-\\u9fa5]+)(?:\\s+(?:AS\\s+)?${leftAlias})`, 'i'));
      const rightTableMatch = sql.match(new RegExp(`(FROM|JOIN)\\s+([a-zA-Z0-9_\\.\\u4e00-\\u9fa5]+)(?:\\s+(?:AS\\s+)?${rightAlias})`, 'i'));
      
      if (leftTableMatch && leftTableMatch[2]) {
        tableReferences.add(leftTableMatch[2].trim());
        Logger.log(`从JSON_EXTRACT JOIN条件左侧检测到表: ${leftTableMatch[2].trim()}`);
      }
      
      if (rightTableMatch && rightTableMatch[2]) {
        tableReferences.add(rightTableMatch[2].trim());
        Logger.log(`从JSON_EXTRACT JOIN条件右侧检测到表: ${rightTableMatch[2].trim()}`);
      }
    });
  }
  
  return tableReferences;
}

// 处理普通SQL查询
function executeRegularSQL(sql, params, startTime) {
  try {
    // Record different stage timings
    const timings = {
      initialization: new Date().getTime() - startTime,
      tableLoading: 0,
      execution: 0,
      formatting: 0
    };
    // Extract table references
    const tableReferences = extractRegularTableReferences(sql);
    Logger.log("查询引用的表: " + JSON.stringify(Array.from(tableReferences)));
    
    // Create replacement table mapping and modified SQL
    const tableMapping = {};
    let modifiedSql = sql;
    
    // Store original table column order
    const tableColumnOrders = {};
    
    // Load table data start time
    const loadingStartTime = new Date().getTime();
    
    // Load table data 
    for (const tableRef of tableReferences) {
      const parts = tableRef.split('.');
      
      // New: Check if table already exists in alasql (e.g., temp tables)
      if (alasql.tables && alasql.tables[tableRef]) {
        Logger.log(`表 ${tableRef} 已存在，无需加载`);
        continue; // If table exists, skip subsequent processing
      }
      
      // New: Check if table already exists in alasql (e.g., temp tables)
      if (alasql.tables && alasql.tables[tableRef]) {
        Logger.log(`表 ${tableRef} 已存在，无需加载`);
        continue; // If table exists, skip subsequent processing
      }
      
      // New: Handle all table reference types
      if (parts.length === 2) {
        // Process fileName.sheetName format table references
        const fileName = parts[0];
        const sheetName = parts[1];
        
        // Create table name
        const randomPrefix = 'tbl' + Math.random().toString(36).substring(2, 8);
        const alasqlTableName = `${randomPrefix}`;
        
        // Update SQL references and record mapping - this part remains unchanged
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
        
        // Record table mapping relationship
        tableMapping[tableRef] = alasqlTableName;
        Logger.log(`表映射: ${tableRef} -> ${alasqlTableName}`);
        const files = findSheetByName(fileName);
        if (files.length > 0) {
          const fileId = files[0].id;
          const spreadsheet = SpreadsheetApp.openById(fileId);
          const sheet = spreadsheet.getSheetByName(sheetName);
          
          if (sheet) {
            // Get data directly from sheet and create table
            const data = sheet.getDataRange().getValues();
            
            // Convert data to array of objects (header as column names)
            const headers = data[0];
            const rows = [];
            
            // Store original column order
            const filteredHeaders = [...headers].filter(header => !isExcludedColumn(header));
            tableColumnOrders[alasqlTableName] = filteredHeaders;
            
            for (let i = 1; i < data.length; i++) {
              const row = {};
              for (let j = 0; j < headers.length; j++) {
                // Skip excluded column names
                if (!isExcludedColumn(headers[j])) {
                  row[headers[j]] = data[i][j];
                }
              }
              rows.push(row);
            }
            
            // Use modified table name to create table
            Logger.log(`创建表 ${alasqlTableName} (原始: ${tableRef})`);
            alasql(`CREATE TABLE ${alasqlTableName}`);
            
            // Insert all data at once for performance
            if (rows.length > 0) {
              alasql(`INSERT INTO ${alasqlTableName} SELECT * FROM ?`, [rows]);
              Logger.log(`已加载表 ${alasqlTableName}，共 ${rows.length} 条记录`);
            }
          } else {
            // Create empty table
            alasql(`CREATE TABLE ${alasqlTableName} (dummy INT)`);
            Logger.log(`警告: 表 ${tableRef} 不存在或没有数据，创建了空表 ${alasqlTableName}`);
          }
        } else {
          return { error: `文件 "${fileName}" 未找到` };
        }
      } else {
        // New: Handle single name table references (e.g., temp tables)
        Logger.log(`处理单一名称表: ${tableRef}`);
        // This case doesn't need to load data, but we need to ensure references in SQL are correct
      }
    }
    
    // Record table loading time
    timings.tableLoading = new Date().getTime() - loadingStartTime;
    
    // Execute modified query
    Logger.log("原始查询: " + sql);
    Logger.log("修改后查询: " + modifiedSql);
    
    // Query execution start time
    const executionStartTime = new Date().getTime();
    
    // 在SQL执行前，打印每个表的表头和第一行数据
    Logger.log("===== 表数据调试信息 =====");
    for (const tableName in alasql.tables) {
      Logger.log(`表: ${tableName}`);
      
      // 获取表结构
      if (alasql.tables[tableName].data && alasql.tables[tableName].data.length > 0) {
        // 打印表头 (列名)
        const firstRow = alasql.tables[tableName].data[0];
        const columns = Object.keys(firstRow);
        Logger.log(`表头: ${JSON.stringify(columns)}`);
        
        // 打印第一行数据
        Logger.log(`第一行数据: ${JSON.stringify(firstRow)}`);
        
        // 打印表大小
        Logger.log(`总行数: ${alasql.tables[tableName].data.length}`);
      } else {
        Logger.log("表为空或无数据");
      }
      Logger.log("-----------------");
    }
    Logger.log(`即将执行SQL: ${modifiedSql}`);
    
    // IMPORTANT: Define result variable here, outside the inner try/catch
    let result;
    
    try {
      // Execute the query
      result = alasql(modifiedSql, params);
    } catch (e) {
      Logger.log(`执行查询错误: ${e.toString()}\nSQL: ${modifiedSql}`);
      throw e; // Re-throw to outer catch
    }
    
    // Record execution time
    timings.execution = new Date().getTime() - executionStartTime;
    
    // Formatting result start time 
    const formattingStartTime = new Date().getTime();
    
    // If SELECT * query and result is array, try to maintain original table column order
    if (Array.isArray(result) && result.length > 0) {
      // Check if it's a simple SELECT * query
      const isSelectStar = /SELECT\s+\*\s+FROM\s+([^\s;]+)/i.test(sql);
      
      if (isSelectStar) {
        // Try to find table name
        const tableMatch = sql.match(/FROM\s+([^\s;]+)/i);
        if (tableMatch && tableMatch[1]) {
          const originalTableRef = tableMatch[1].trim();
          const aliasedTableName = tableMapping[originalTableRef];
          
          // If there's column order record for this table, reorder result based on it
          if (aliasedTableName && tableColumnOrders[aliasedTableName]) {
            const orderedColumns = tableColumnOrders[aliasedTableName];
            
            // Reorder result columns
            result = result.map(row => {
              const orderedRow = {};
              orderedColumns.forEach(column => {
                if (column in row) {
                  orderedRow[column] = row[column];
                }
              });
              
              // Add possible other columns (e.g., calculated columns)
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
    
    // Record formatting time
    timings.formatting = new Date().getTime() - formattingStartTime;
    
    // Calculate total time
    const totalTime = new Date().getTime() - startTime;
    
    // Wrap result in an object, including execution time information
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
  var page = e.parameter.page || 'index';
  
  switch(page) {
    case 'help':
      return HtmlService.createHtmlOutputFromFile('HelpPage')
        .setTitle('Google Sheets SQL - 帮助');
    case 'check':
      return HtmlService.createHtmlOutputFromFile('CheckPage')
        .setTitle('Google Sheets SQL - 配置检查');
    default:
      return HtmlService.createHtmlOutputFromFile('Index')
        .setTitle('Google Sheets SQL');
  }
}

// Expose API functions to client
function getAllSheetsForClient() {
  return getAllSheets();
}

function executeSQLForClient(sql, params = {}) {
  const result = executeSQL(sql, params);
  
  // Add query SQL to result for debugging
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
    
    // Get all sheet names
    for (let i = 0; i < sheets.length; i++) {
      const sheet = sheets[i];
      result.sheets.push({
        name: sheet.getName(),
        index: i
      });
      
      // Get first sheet header
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
    // Here we need to update configuration based on storage mechanism
    // If using Properties Service to store configuration, code might be similar:
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

// Expose API functions to client
function getExcludedColumnsForClient() {
  return getExcludedColumns();
}

function setExcludedColumnsForClient(columns) {
  return setExcludedColumns(columns);
}

 function getScriptUrl() {
    return ScriptApp.getService().getUrl();
}

// Server-side functions in Code.gs
function getPageUrl(page) {
  var scriptId = ScriptApp.getScriptId();
  return 'https://script.google.com/macros/s/' + scriptId + '/dev?page=' + page;
}

function loadPageHtml(page) {
  switch(page) {
    case 'help':
      return HtmlService.createHtmlOutputFromFile('HelpPage').getContent();
    case 'check':
      return HtmlService.createHtmlOutputFromFile('CheckPage').getContent();
    default:
      return HtmlService.createHtmlOutputFromFile('Index').getContent();
  }
}

function getDeploymentUrl(page) {
  // 获取当前部署的URL
  var url = ScriptApp.getService().getUrl();
  if (url) {
    return url + '?page=' + page;
  }
  return null;
}

function navigateToPage(page) {
  try {
    // 记录当前请求的页面，并返回成功响应
    return {
      success: true,
      url: ScriptApp.getService().getUrl() + '?page=' + page
    };
  } catch(e) {
    return {
      success: false,
      error: e.toString()
    };
  }
}