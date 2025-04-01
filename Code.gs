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
    const config = getConfig();
    const rootFolder = DriveApp.getFolderById(config.drive.rootFolderId);
    
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
    
    // 处理SQL注释，保留SQL语句
    if (sql.includes('--')) {
      // 移除单行注释 (--后面的内容直到行尾)
      sql = sql.replace(/--.*?(\r\n|\r|\n|$)/g, ' ');
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
    const config = getConfig();
    const userProperties = PropertiesService.getUserProperties();
    const templatesStr = userProperties.getProperty(config.app.userPropertiesKey) || '[]';
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
    userProperties.setProperty(config.app.userPropertiesKey, JSON.stringify(templates));
    
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
    const config = getConfig();
    const userProperties = PropertiesService.getUserProperties();
    const templatesStr = userProperties.getProperty(config.app.userPropertiesKey) || '[]';
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
    const config = getConfig();
    const userProperties = PropertiesService.getUserProperties();
    const templatesStr = userProperties.getProperty(config.app.userPropertiesKey) || '[]';
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
    userProperties.setProperty(config.app.userPropertiesKey, JSON.stringify(templates));
    
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
    const config = getConfig();
    const userProperties = PropertiesService.getUserProperties();
    userProperties.setProperty(config.app.userPropertiesKey, JSON.stringify(templates));
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
  const config = getConfig();
  const userProperties = PropertiesService.getUserProperties();
  const templatesStr = userProperties.getProperty(config.app.userPropertiesKey) || '[]';
  
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
 * 初始化中央模板存储库（如果不存在）
 * @returns {Object} - 包含存储库信息的对象
 */
function initCentralTemplateRepository() {
  try {
    const config = getConfig();
    // 检查是否存在中央模板存储库文件
    const centralRepoId = config.templateRepository.fileId;
    let centralRepo;
    
    try {
      // 尝试直接用ID打开
      centralRepo = SpreadsheetApp.openById(centralRepoId);
      Logger.log("已找到中央模板存储库");
    } catch (e) {
      // 如果不存在，创建新的存储库
      centralRepo = SpreadsheetApp.create("GSQLCentralTemplateRepository");
      
      // 创建所需的工作表（公开模板、审核日志、用户反馈）
      let publicTemplatesSheet = centralRepo.getActiveSheet();
      publicTemplatesSheet.setName(config.templateRepository.sheets.publicTemplates);
      
      // 设置公开模板表头
      const publicHeaders = [
        "ID", "名称", "描述", "SQL代码", "验证规则", "贡献者", "创建日期", 
        "更新日期", "状态", "类别标签", "使用次数"
      ];
      publicTemplatesSheet.getRange(1, 1, 1, publicHeaders.length).setValues([publicHeaders]);
      
      // 创建审核日志表
      let reviewSheet = centralRepo.insertSheet(config.templateRepository.sheets.reviewLog);
      const reviewHeaders = ["模板ID", "审核人", "审核日期", "审核结果", "备注"];
      reviewSheet.getRange(1, 1, 1, reviewHeaders.length).setValues([reviewHeaders]);
      
      // 创建用户反馈表
      let feedbackSheet = centralRepo.insertSheet(config.templateRepository.sheets.userFeedback);
      const feedbackHeaders = ["模板ID", "用户", "评分", "评论", "日期"];
      feedbackSheet.getRange(1, 1, 1, feedbackHeaders.length).setValues([feedbackHeaders]);
      
      Logger.log("已创建中央模板存储库");
    }
    
    return {
      success: true,
      spreadsheetId: centralRepo.getId(),
      url: centralRepo.getUrl()
    };
  } catch (e) {
    Logger.log("初始化中央模板存储库失败: " + e.toString());
    return {
      success: false,
      error: e.toString()
    };
  }
}

/**
 * 获取公共模板列表
 * @returns {Object[]} - 模板列表
 */
function getPublicTemplates() {
  try {
    // 初始化或获取中央存储库
    const repoInfo = initCentralTemplateRepository();
    if (!repoInfo.success) {
      return { error: "无法访问中央模板存储库" };
    }
    
    const config = getConfig();
    const ss = SpreadsheetApp.openById(repoInfo.spreadsheetId);
    const sheet = ss.getSheetByName(config.templateRepository.sheets.publicTemplates);
    
    // 获取所有数据行（跳过表头）
    const data = sheet.getDataRange().getValues();
    if (data.length <= 1) {
      return []; // 只有表头，没有数据
    }
    
    const headers = data[0];
    const templates = [];
    
    // 从第二行开始，这是实际数据
    for (let i = 1; i < data.length; i++) {
      const row = data[i];
      // 只获取状态为"已批准"的模板
      if (row[8] === "已批准") {
        const template = {
          id: row[0],
          name: row[1],
          description: row[2],
          sql: row[3],
          validationRules: row[4] ? JSON.parse(row[4]) : [],
          contributor: row[5],
          created: row[6],
          updated: row[7],
          status: row[8],
          tags: row[9] ? row[9].split(",") : [],
          usageCount: row[10] || 0
        };
        templates.push(template);
      }
    }
    
    return templates;
  } catch (e) {
    Logger.log("获取公共模板失败: " + e.toString());
    return { error: e.toString() };
  }
}

/**
 * 提交模板到公共存储库
 * @param {string} name - 模板名称
 * @param {string} description - 模板描述
 * @param {string} sql - SQL查询语句
 * @param {string[]} validationRules - 验证规则数组
 * @param {string} category - 模板分类
 * @returns {Object} - 提交结果
 */
function submitPublicTemplate(name, description, sql, validationRules = [], category = "") {
  try {
    // 获取当前用户信息
    const userEmail = Session.getActiveUser().getEmail();
    
    // 初始化或获取中央存储库
    const repoInfo = initCentralTemplateRepository();
    if (!repoInfo.success) {
      return { success: false, error: "无法访问中央模板存储库" };
    }
    
    const ss = SpreadsheetApp.openById(repoInfo.spreadsheetId);
    const sheet = ss.getSheetByName("公开模板");
    
    // 生成唯一ID
    const templateId = "TPL" + new Date().getTime();
    const now = new Date().toISOString();
    
    // 准备模板数据
    const templateData = [
      templateId,
      name,
      description,
      sql,
      JSON.stringify(validationRules),
      userEmail,
      now,
      now,
      "待审核", // 初始状态为待审核
      category,
      0 // 初始使用次数为0
    ];
    
    // 添加到表格中
    sheet.appendRow(templateData);
    
    return {
      success: true,
      message: "模板已提交，等待审核",
      templateId: templateId
    };
  } catch (e) {
    Logger.log("提交公共模板失败: " + e.toString());
    return {
      success: false,
      error: e.toString()
    };
  }
}

/**
 * 增加模板使用次数
 * @param {string} templateId - 模板ID
 */
function incrementTemplateUsage(templateId) {
  try {
    const repoInfo = initCentralTemplateRepository();
    if (!repoInfo.success) return;
    
    const ss = SpreadsheetApp.openById(repoInfo.spreadsheetId);
    const sheet = ss.getSheetByName("公开模板");
    
    // 查找模板行
    const data = sheet.getDataRange().getValues();
    for (let i = 1; i < data.length; i++) {
      if (data[i][0] === templateId) {
        // 更新使用次数列
        const currentCount = data[i][10] || 0;
        sheet.getRange(i + 1, 11).setValue(currentCount + 1);
        break;
      }
    }
  } catch (e) {
    Logger.log("更新模板使用次数失败: " + e.toString());
  }
}

/**
 * 提交模板反馈
 * @param {string} templateId - 模板ID
 * @param {number} rating - 评分 (1-5)
 * @param {string} comment - 评论
 */
function submitTemplateFeedback(templateId, rating, comment) {
  try {
    const userEmail = Session.getActiveUser().getEmail();
    const repoInfo = initCentralTemplateRepository();
    if (!repoInfo.success) {
      return { success: false, error: "无法访问中央模板存储库" };
    }
    
    const ss = SpreadsheetApp.openById(repoInfo.spreadsheetId);
    const sheet = ss.getSheetByName("用户反馈");
    
    // 添加反馈
    sheet.appendRow([
      templateId,
      userEmail,
      rating,
      comment,
      new Date().toISOString()
    ]);
    
    return { success: true, message: "感谢您的反馈" };
  } catch (e) {
    return { success: false, error: e.toString() };
  }
}

/**
 * 审核模板（管理员功能）
 * @param {string} templateId - 模板ID
 * @param {boolean} approved - 是否批准
 * @param {string} comment - 审核意见
 */
function reviewTemplate(templateId, approved, comment) {
  try {
    const userEmail = Session.getActiveUser().getEmail();
    
    // 这里可以添加管理员权限检查
    // if (!isAdmin(userEmail)) return { success: false, error: "权限不足" };
    
    const repoInfo = initCentralTemplateRepository();
    if (!repoInfo.success) {
      return { success: false, error: "无法访问中央模板存储库" };
    }
    
    const ss = SpreadsheetApp.openById(repoInfo.spreadsheetId);
    const publicSheet = ss.getSheetByName("公开模板");
    const reviewSheet = ss.getSheetByName("审核日志");
    
    // 查找模板行
    const data = publicSheet.getDataRange().getValues();
    let templateRow = -1;
    
    for (let i = 1; i < data.length; i++) {
      if (data[i][0] === templateId) {
        templateRow = i + 1; // 加1因为索引从0开始，但sheet行从1开始
        break;
      }
    }
    
    if (templateRow === -1) {
      return { success: false, error: "找不到指定的模板" };
    }
    
    // 更新状态
    publicSheet.getRange(templateRow, 9).setValue(approved ? "已批准" : "已拒绝");
    
    // 记录审核日志
    reviewSheet.appendRow([
      templateId,
      userEmail,
      new Date().toISOString(),
      approved ? "已批准" : "已拒绝",
      comment
    ]);
    
    return { 
      success: true, 
      message: "审核完成: " + (approved ? "已批准" : "已拒绝") 
    };
  } catch (e) {
    return { success: false, error: e.toString() };
  }
}

/**
 * 为客户端暴露的公共模板API
 */
function getPublicTemplatesForClient() {
  return getPublicTemplates();
}

function submitPublicTemplateForClient(name, description, sql, validationRules, category) {
  return submitPublicTemplate(name, description, sql, validationRules, category);
}

function incrementTemplateUsageForClient(templateId) {
  incrementTemplateUsage(templateId);
  return { success: true };
}

function submitTemplateFeedbackForClient(templateId, rating, comment) {
  return submitTemplateFeedback(templateId, rating, comment);
}

function importPublicTemplate(templateId) {
  try {
    // 获取公共模板
    const publicTemplates = getPublicTemplates();
    const template = publicTemplates.find(t => t.id === templateId);
    
    if (!template) {
      return { success: false, error: "找不到指定的公共模板" };
    }
    
    // 保存到用户个人模板
    const result = saveSQLTemplate(
      template.name,
      template.description + " (从公共模板导入)",
      template.sql,
      template.validationRules
    );
    
    // 增加使用次数
    incrementTemplateUsage(templateId);
    
    return {
      success: true,
      message: "已导入到个人模板",
      template: result.template
    };
  } catch (e) {
    return { success: false, error: e.toString() };
  }
}

function importPublicTemplateForClient(templateId) {
  return importPublicTemplate(templateId);
}

function reviewTemplateForClient(templateId, approved, comment) {
  return reviewTemplate(templateId, approved, comment);
}

/**
 * 获取配置检查页面的HTML内容
 * @returns {string} HTML内容
 */
function getConfigCheckPage() {
  return HtmlService.createHtmlOutputFromFile('ConfigCheck')
    .getContent();
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

/**
 * 更新公共模板库中的模板
 * @param {string} templateId - 要更新的模板ID
 * @param {string} name - 更新后的模板名称
 * @param {string} description - 更新后的模板描述
 * @param {string} sql - 更新后的SQL查询语句
 * @param {string[]} validationRules - 更新后的验证规则数组
 * @param {string} category - 更新后的模板分类
 * @returns {Object} - 更新结果
 */
function updatePublicTemplate(templateId, name, description, sql, validationRules = [], category = "") {
  try {
    // 获取当前用户信息
    const userEmail = Session.getActiveUser().getEmail();
    
    // 初始化或获取中央存储库
    const repoInfo = initCentralTemplateRepository();
    if (!repoInfo.success) {
      return { success: false, error: "无法访问中央模板存储库" };
    }
    
    const ss = SpreadsheetApp.openById(repoInfo.spreadsheetId);
    const sheet = ss.getSheetByName("公开模板");
    
    // 查找模板行
    const data = sheet.getDataRange().getValues();
    let templateRow = -1;
    let originalContributor = "";
    
    for (let i = 1; i < data.length; i++) {
      if (data[i][0] === templateId) {
        templateRow = i + 1; // 加1因为索引从0开始，但sheet行从1开始
        originalContributor = data[i][5]; // 保存原始贡献者
        break;
      }
    }
    
    if (templateRow === -1) {
      return { success: false, error: "找不到指定的模板" };
    }
    
    // 验证用户是否有权限更新（原始贡献者或管理员）
    // 这里可以添加管理员检查逻辑
    // if (userEmail !== originalContributor && !isAdmin(userEmail)) {
    //   return { success: false, error: "您没有权限更新此模板" };
    // }
    
    // 更新模板数据
    const now = new Date().toISOString();
    sheet.getRange(templateRow, 2).setValue(name); // 名称
    sheet.getRange(templateRow, 3).setValue(description); // 描述
    sheet.getRange(templateRow, 4).setValue(sql); // SQL
    sheet.getRange(templateRow, 5).setValue(JSON.stringify(validationRules)); // 验证规则
    sheet.getRange(templateRow, 8).setValue(now); // 更新日期
    sheet.getRange(templateRow, 9).setValue("待审核"); // 状态重置为待审核
    sheet.getRange(templateRow, 10).setValue(category); // 类别
    
    return {
      success: true,
      message: "模板已更新，等待审核",
      templateId: templateId
    };
  } catch (e) {
    Logger.log("更新公共模板失败: " + e.toString());
    return {
      success: false,
      error: e.toString()
    };
  }
}

/**
 * 为客户端暴露的更新公共模板API
 */
function updatePublicTemplateForClient(templateId, name, description, sql, validationRules, category) {
  return updatePublicTemplate(templateId, name, description, sql, validationRules, category);
}
