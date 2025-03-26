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