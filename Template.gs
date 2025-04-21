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