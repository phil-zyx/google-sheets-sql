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
 * 获取 Google Drive 中所有可访问的 Sheet 文件
 * @param {string} [folderId] - 可选的 Google Drive 文件夹 ID，如果提供则只获取该文件夹下的文件
 * @returns {Object[]} - 包含 id, name 的 Sheet 文件列表
 */
function getAllSheets() {
  try {
    const result = [];
    const rootFolder = DriveApp.getFolderById("18fRVYlkXD7hhfQbeYhePDd-upXPF5EyJ");
    
    // 递归获取文件夹中的所有 Sheet 文件
    function processFolder(folder) {
      // 获取当前文件夹中的所有 Sheet 文件
      const files = folder.getFilesByType(MimeType.GOOGLE_SHEETS);
      while (files.hasNext()) {
        const file = files.next();
        result.push({
          id: file.getId(),
          name: file.getName(),
          lastUpdated: file.getLastUpdated().toISOString(),
          url: file.getUrl()
        });
      }
      
      // 递归处理子文件夹
      const subFolders = folder.getFolders();
      while (subFolders.hasNext()) {
        const subFolder = subFolders.next();
        processFolder(subFolder);
      }
    }
    
    // 开始处理根文件夹
    processFolder(rootFolder);
    return result;
    
  } catch (e) {
    Logger.log('获取 Sheet 文件失败: ' + e.toString());
    return [];
  }
}

/**
 * 解析 SQL 语句中的表引用
 * @param {string} sql - SQL 查询语句
 * @returns {Set<string>} - 表引用集合
 */
function extractTableReferences(sql) {
  // 匹配 FROM 和 JOIN 子句中的表名，现在支持中文和其他特殊字符
  const regex = /(?:FROM|JOIN)\s+([a-zA-Z0-9_\.\u4e00-\u9fa5]+)(?:\s+(?:AS\s+)?([a-zA-Z0-9_\u4e00-\u9fa5]+))?/gi;
  const matches = [...sql.matchAll(regex)];
  const tableReferences = new Set();
  
  matches.forEach(match => {
    const table = match[1];
    if (table) {
      tableReferences.add(table.trim());
    }
  });
  
  return tableReferences;
}

/**
 * 在所有文件中搜索特定名称的Sheet文件
 * @param {string} fileName - 要搜索的Sheet文件的名称
 * @returns {Object[]} - 包含 id, name, lastUpdated, url 的 Sheet 文件列表
 */
function findSheetByName(fileName) {
  try {
    // 使用Drive API直接按名称搜索文件
    const files = DriveApp.getFilesByName(fileName);
    const results = [];
    
    while (files.hasNext()) {
      const file = files.next();
      // 只返回Google Sheets类型的文件
      if (file.getMimeType() === MimeType.GOOGLE_SHEETS) {
        results.push({
          id: file.getId(),
          name: file.getName(),
          lastUpdated: file.getLastUpdated().toISOString(),
          url: file.getUrl()
        });
      }
    }
    
    return results;
  } catch (e) {
    Logger.log('根据名称查找Sheet文件失败: ' + e.toString());
    return [];
  }
}

/**
 * 执行 SQL 查询（简化版）
 * @param {string} sql - SQL 查询语句
 * @param {Object} params - 查询参数
 * @returns {Object[]} - 查询结果
 */
function executeSQL(sql, params = {}) {
  const startTime = new Date().getTime();
  
  try {
    // 确保 AlaSQL 已正确初始化
    if (!alasql || typeof alasql !== 'function' || !alasql.tables) {
      const initSuccess = initAlaSQL();
      if (!initSuccess) {
        throw new Error('无法初始化 AlaSQLGS 库');
      }
      
      // 二次检查确保初始化成功
      if (!alasql || typeof alasql !== 'function') {
        throw new Error('AlaSQL 初始化后仍然无效');
      }
    }
    
    // 记录不同阶段的时间统计
    const timings = {
      initialization: new Date().getTime() - startTime,
      tableLoading: 0,
      execution: 0,
      formatting: 0
    };
    
    // 提取表引用
    const tableReferences = extractTableReferences(sql);
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
      
      if (parts.length === 2) {
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
            tableColumnOrders[alasqlTableName] = [...headers];
            
            for (let i = 1; i < data.length; i++) {
              const row = {};
              for (let j = 0; j < headers.length; j++) {
                row[headers[j]] = data[i][j];
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
      }
    }
    
    // 记录表加载时间
    timings.tableLoading = new Date().getTime() - loadingStartTime;
    
    // 执行修改后的查询
    Logger.log("原始查询: " + sql);
    Logger.log("修改后查询: " + modifiedSql);
    
    // 执行查询的开始时间
    const executionStartTime = new Date().getTime();
    let result = alasql(modifiedSql, params);
    
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
  // 检查是否请求帮助页面
  if (e && e.parameter && e.parameter.page === 'help') {
    return HtmlService.createHtmlOutputFromFile('Help')
      .setTitle('Google Sheets SQL - 帮助文档');
  }
  
  // 返回主页面
  return HtmlService.createHtmlOutputFromFile('Index')
    .setTitle('Google Sheets SQL')
    .setFaviconUrl('https://www.google.com/images/favicon.ico');
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
 * 测试 AlaSQLGS 是否正常工作
 */
function testAlaSQLGS() {
  try {
    if (typeof AlaSQLGS === 'undefined') {
      return { success: false, error: "AlaSQLGS 库未添加到项目中" };
    }
    
    // 加载 AlaSQLGS
    const { load } = AlaSQLGS;
    const alasql = load();
    
    if (!alasql) {
      return { success: false, error: "AlaSQLGS.load() 未返回有效对象" };
    }
    alasql('SELECT * FROM sheetsql.test1 Limit 10');
    
    // 创建测试表和数据
    alasql('CREATE TABLE test (id INT, name STRING)');
    alasql('INSERT INTO test VALUES (1, "测试1"), (2, "测试2")');
    
    // 执行测试查询
    const result = alasql('SELECT * FROM test WHERE id = 1');
    Logger.log('success')
    return {
      success: true,
      result: result,
      message: "AlaSQLGS 测试成功"
    };
  } catch (e) {
    Logger.log(e.stack)
    return {
      success: false,
      error: e.toString(),
      stack: e.stack
    };
  }
}

/**
 * 保存SQL模板
 * @param {string} name - 模板名称
 * @param {string} description - 模板描述
 * @param {string} sql - SQL查询语句
 * @param {string[]} validationRules - 验证规则数组
 * @returns {Object} - 保存结果
 */
function saveSQLTemplate(name, description, sql, validationRules = []) {
  try {
    const userProperties = PropertiesService.getUserProperties();
    const templatesStr = userProperties.getProperty('sql_templates') || '[]';
    const templates = JSON.parse(templatesStr);
    
    // 检查是否存在同名模板
    const existingIndex = templates.findIndex(t => t.name === name);
    
    if (existingIndex >= 0) {
      // 更新现有模板
      templates[existingIndex] = {
        name: name,
        description: description || '',
        sql: sql,
        validationRules: validationRules || [], // 添加验证规则
        created: templates[existingIndex].created,
        updated: new Date().toISOString()
      };
    } else {
      // 添加新模板
      templates.push({
        name: name,
        description: description || '',
        sql: sql,
        validationRules: validationRules || [], // 添加验证规则
        created: new Date().toISOString(),
        updated: new Date().toISOString()
      });
    }
    
    // 保存回 Properties
    userProperties.setProperty('sql_templates', JSON.stringify(templates));
    
    return {
      success: true,
      message: existingIndex >= 0 ? '模板已更新' : '模板已保存',
      template: templates[existingIndex >= 0 ? existingIndex : templates.length - 1]
    };
  } catch (e) {
    Logger.log('保存SQL模板失败: ' + e);
    return {
      success: false,
      error: e.toString()
    };
  }
}

/**
 * 获取所有SQL模板
 * @returns {Object[]} - 模板列表
 */
function getSQLTemplates() {
  try {
    const userProperties = PropertiesService.getUserProperties();
    const templatesStr = userProperties.getProperty('sql_templates') || '[]';
    return JSON.parse(templatesStr);
  } catch (e) {
    Logger.log('获取SQL模板失败: ' + e);
    return [];
  }
}

/**
 * 删除SQL模板
 * @param {string} name - 模板名称
 * @returns {Object} - 删除结果
 */
function deleteSQLTemplate(name) {
  try {
    const userProperties = PropertiesService.getUserProperties();
    const templatesStr = userProperties.getProperty('sql_templates') || '[]';
    let templates = JSON.parse(templatesStr);
    
    // 查找并删除模板
    const initialLength = templates.length;
    templates = templates.filter(t => t.name !== name);
    
    // 如果长度没有变化，说明没有找到模板
    if (templates.length === initialLength) {
      return {
        success: false,
        error: '模板不存在'
      };
    }
    
    // 保存回 Properties
    userProperties.setProperty('sql_templates', JSON.stringify(templates));
    
    return {
      success: true,
      message: '模板已删除'
    };
  } catch (e) {
    Logger.log('删除SQL模板失败: ' + e);
    return {
      success: false,
      error: e.toString()
    };
  }
}

function saveSQLTemplateForClient(name, description, sql, validationRules = []) {
  return saveSQLTemplate(name, description, sql, validationRules);
}

function getSQLTemplatesForClient() {
  return getSQLTemplates();
}

function deleteSQLTemplateForClient(name) {
  return deleteSQLTemplate(name);
}

// 添加导出和导入功能，允许用户备份模板
function exportSQLTemplates() {
  const templates = getSQLTemplates();
  return JSON.stringify(templates);
}

function importSQLTemplates(jsonData) {
  try {
    const templates = JSON.parse(jsonData);
    const userProperties = PropertiesService.getUserProperties();
    userProperties.setProperty('sql_templates', JSON.stringify(templates));
    return {
      success: true,
      message: `已导入 ${templates.length} 个模板`
    };
  } catch (e) {
    return {
      success: false,
      error: '导入失败: ' + e.toString()
    };
  }
}

// 检查存储使用情况
function checkStorageUsage() {
  const userProperties = PropertiesService.getUserProperties();
  const templatesStr = userProperties.getProperty('sql_templates') || '[]';
  
  return {
    templatesCount: JSON.parse(templatesStr).length,
    templatesSize: templatesStr.length,
    totalSize: new Blob([templatesStr]).size,
    limit: 50 * 1024  // 50 KB in bytes
  };
}

/**
 * 执行 SQL 查询并应用验证规则的客户端接口
 * @param {string} sql - SQL 查询语句
 * @param {Object} params - 查询参数
 * @param {string[]} validationRules - 验证规则数组
 * @returns {Object} - 包含查询结果和验证信息的对象
 */
function executeSQLWithValidation(sql, params = {}, validationRules = []) {
  // 预处理规则（去除空规则）
  const rules = (validationRules || []).filter(rule => rule && rule.trim().length > 0);
  
  try {
    // 执行原有的SQL查询
    const result = executeSQL(sql, params);
    
    // 如果查询返回错误，直接返回
    if (result.error) {
      return result;
    }
    
    // 验证结果对象
    const validationResults = {
      totalRows: Array.isArray(result.data) ? result.data.length : 0,
      errorRows: 0,
      errors: []
    };
    
    // 如果有验证规则和有效数据，应用验证
    if (rules.length > 0 && Array.isArray(result.data) && result.data.length > 0) {
      // 处理每一行数据
      result.data.forEach((row, rowIndex) => {
        // 初始化验证状态
        row._validationStatus = 'valid';
        row._validationErrors = [];
        
        // 检查每条规则
        rules.forEach((rule, ruleIndex) => {
          const isValid = evaluateExpression(rule, row);
          
          if (!isValid) {
            // 获取左右值用于错误消息
            const parts = rule.split('=').map(p => p.trim());
            let leftValue = '(未知)';
            let rightValue = '(未知)';
            
            if (parts.length === 2) {
              if (parts[0] in row) leftValue = JSON.stringify(row[parts[0]]);
              if (parts[1] in row) rightValue = JSON.stringify(row[parts[1]]);
            }
            
            // 标记不符合条件的行
            row._validationStatus = 'invalid';
            row._validationErrors.push({
              rule: rule,
              message: `规则 #${ruleIndex + 1} 验证失败: ${rule}
                左侧值: ${leftValue}
                右侧值: ${rightValue}`
            });
            
            // 添加到错误汇总
            validationResults.errors.push({
              rowIndex: rowIndex,
              rule: rule,
              data: {...row}
            });
            
            // 统计错误行数
            if (row._validationStatus === 'invalid') {
              validationResults.errorRows++;
            }
          }
        });
      });
    }
    
    // 将验证结果添加到返回对象中
    result.validation = validationResults;
    
    // 添加验证相关信息到统计中
    if (result.stats) {
      result.stats.sql = sql;
      result.stats.validationRules = rules;
    }
    
    return result;
  } catch (e) {
    Logger.log('验证规则执行错误: ' + e);
    return { 
      error: '执行验证规则失败: ' + e.toString(),
      stats: {
        executionTime: 0
      }
    };
  }
}

/**
 * 评估验证表达式
 * @param {string} expression - 条件表达式，如 "col1 = col2" 或 "JSON_EXTRACT(jsonColumn, '$.type') = 'product'"
 * @param {Object} row - 数据行
 * @returns {boolean} 表达式是否为真
 */
function evaluateExpression(expression, row) {
  // 调试日志函数
  const log = (msg) => {
    if (typeof Logger !== 'undefined' && Logger.log) {
      Logger.log(msg);
    } else {
      console.log(msg);
    }
  };

  try {
    // 递归解析嵌套函数的核心函数
    function resolveNestedFunctions(expr) {
      // 正则表达式匹配函数调用，支持嵌套
      const functionRegex = /(\w+)\(([^()]*(?:\([^()]*\)[^()]*)?)\)/;
      
      let match;
      while ((match = expr.match(functionRegex))) {
        const fullMatch = match[0];
        const funcName = match[1].toUpperCase();
        const argsStr = match[2];
        
        // 解析参数，处理嵌套
        const args = parseArguments(argsStr);
        
        // 执行函数调用并获取结果
        const result = executeFunctionCall(funcName, args, row);
        
        // 替换原始函数调用
        expr = expr.replace(fullMatch, result);
      }
      
      return expr;
    }

    // 复杂参数解析函数
    function parseArguments(argsStr) {
      const args = [];
      let currentArg = '';
      let inQuotes = false;
      let quoteChar = '';
      let nestingLevel = 0;

      for (let i = 0; i < argsStr.length; i++) {
        const char = argsStr[i];

        // 跟踪括号嵌套层级
        if (char === '(') nestingLevel++;
        if (char === ')') nestingLevel--;

        // 处理引号
        if ((char === "'" || char === '"') && (i === 0 || argsStr[i-1] !== '\\')) {
          if (!inQuotes) {
            inQuotes = true;
            quoteChar = char;
          } else if (char === quoteChar) {
            inQuotes = false;
          }
        }

        // 分割参数
        if (char === ',' && !inQuotes && nestingLevel === 0) {
          args.push(currentArg.trim());
          currentArg = '';
        } else {
          currentArg += char;
        }
      }

      if (currentArg.trim()) {
        args.push(currentArg.trim());
      }

      // 处理每个参数
      return args.map(arg => {
        // 去除引号
        if ((arg.startsWith("'") && arg.endsWith("'")) || 
            (arg.startsWith('"') && arg.endsWith('"'))) {
          return arg.substring(1, arg.length - 1);
        }
        
        // 处理嵌套函数
        const funcMatch = arg.match(/^(\w+)\(.*\)$/);
        if (funcMatch) {
          return resolveNestedFunctions(arg);
        }
        
        // 检查是否是列引用
        if (arg in row) {
          return row[arg];
        }
        
        // 尝试转换为数字
        if (!isNaN(arg)) {
          return Number(arg);
        }
        
        return arg;
      });
    }

    // 执行函数调用
    function executeFunctionCall(funcName, args, row) {
      try {
        switch (funcName) {
          case 'JSON_EXTRACT_FILTERED':
            if (args.length === 4) {
              const data = typeof args[0] === 'string' 
                ? JSON.parse(args[0]) 
                : args[0];
              
              if (!Array.isArray(data)) return '[]';
              
              const filteredData = data.filter(item => 
                item && item[args[1]] === args[2]
              ).map(item => item[args[3]]);
              
              return JSON.stringify(filteredData);
            }
            break;
            
          case 'ARRAY_LENGTH':
            if (args.length === 1) {
              try {
                const arr = args[0];
                
                // 尝试解析 JSON 字符串
                if (typeof arr === 'string') {
                  try {
                    const parsed = JSON.parse(arr);
                    return Array.isArray(parsed) ? parsed.length : 1;
                  } catch {
                    // 如果不是有效 JSON，可能是逗号分隔的字符串
                    return arr.split(',').filter(item => item.trim()).length;
                  }
                }
                
                // 如果已经是数组
                return Array.isArray(arr) ? arr.length : 1;
              } catch (e) {
                log(`ARRAY_LENGTH 错误: ${e}`);
                return 0;
              }
            }
            break;
            
          case 'JSON_EXTRACT':
            if (args.length === 2) {
              try {
                const data = typeof args[0] === 'string' 
                  ? JSON.parse(args[0]) 
                  : args[0];
                
                // 支持路径提取
                const path = args[1].replace(/^['"]|['"]$/g, '');
                return data[path];
              } catch (e) {
                log(`JSON_EXTRACT 错误: ${e}`);
                return null;
              }
            }
            break;
            
          default:
            log(`未知函数: ${funcName}`);
            return '0';
        }
      } catch (e) {
        log(`函数执行错误: ${e}`);
        return '0';
      }
      
      return '0';
    }

    // 处理比较逻辑
    function evaluateComparison(resolvedExpression) {
      const operators = {
        '=': (a, b) => String(a) == String(b),
        '==': (a, b) => String(a) == String(b),
        '!=': (a, b) => String(a) != String(b),
        '>': (a, b) => Number(a) > Number(b),
        '>=': (a, b) => Number(a) >= Number(b),
        '<': (a, b) => Number(a) < Number(b),
        '<=': (a, b) => Number(a) <= Number(b)
      };

      // 处理 AND/OR 逻辑
      if (resolvedExpression.includes(' AND ')) {
        const conditions = resolvedExpression.split(' AND ');
        return conditions.every(cond => evaluateComparison(cond.trim()));
      }

      if (resolvedExpression.includes(' OR ')) {
        const conditions = resolvedExpression.split(' OR ');
        return conditions.some(cond => evaluateComparison(cond.trim()));
      }

      // 处理比较操作
      for (const op of Object.keys(operators)) {
        const regex = new RegExp(`(.+)\\s*${op.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s*(.+)`);
        const match = resolvedExpression.match(regex);
        
        if (match) {
          let left = match[1].trim();
          let right = match[2].trim();

          // 处理列引用和数值转换
          left = left in row ? row[left] : (isNaN(left) ? left : Number(left));
          right = right in row ? row[right] : (isNaN(right) ? right : Number(right));

          return operators[op](left, right);
        }
      }

      // 默认情况
      return false;
    }

    // 主流程
    const resolvedExpression = resolveNestedFunctions(expression);
    return evaluateComparison(resolvedExpression);

  } catch (e) {
    log(`表达式求值总体错误: ${e}`);
    return false;
  }
}

// 详细测试用例
function testEvaluateExpression() {
  const testCases = [
    {
      row: {
        A_ARR_activity_components: [
          { typ: 'floor_without_gacha', id: 1 },
          { typ: 'floor_with_gacha', id: 2 }
        ]
      },
      expr: 'ARRAY_LENGTH(JSON_EXTRACT_FILTERED(A_ARR_activity_components, "typ", "floor_without_gacha", "id")) > 0',
      expected: true,
      description: "两层嵌套函数：ARRAY_LENGTH 和 JSON_EXTRACT_FILTERED"
    },
    {
      row: { 
        A_ARR_activity_components: [
          { typ: 'floor_without_gacha', id: 1 },
          { typ: 'floor_with_gacha', id: 2 }
        ] 
      },
      expr: 'ARRAY_LENGTH(JSON_EXTRACT_FILTERED(A_ARR_activity_components, "typ", "floor_without_gacha", "id")) == 1',
      expected: true,
      description: "使用相等比较运算符"
    },
    {
      row: { value: 5 },
      expr: 'value > 3',
      expected: true,
      description: "简单数值比较"
    },
    {
      row: { 
        data: '[{"type":"premium"},{"type":"basic"}]' 
      },
      expr: 'ARRAY_LENGTH(data) == 2',
      expected: true,
      description: "JSON 字符串数组长度判断"
    }
  ];

  // 使用普通日志方式输出测试结果
  testCases.forEach((testCase, index) => {
    const result = evaluateExpression(testCase.expr, testCase.row);
    console.log(`测试用例 ${index + 1}:`);
    console.log(`描述: ${testCase.description}`);
    console.log(`表达式: ${testCase.expr}`);
    console.log(`实际结果: ${result}`);
    console.log(`预期结果: ${testCase.expected}`);
    console.log(`测试状态: ${result === testCase.expected ? '✅ 通过' : '❌ 失败'}`);
    console.log('---');
  });
}

// 直接运行测试
testEvaluateExpression();