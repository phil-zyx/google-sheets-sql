/**
 * 显示配置检查工作流界面
 * @returns {HtmlOutput} - HTML输出
 */
function showConfigCheckPage() {
  const config = getConfig();
  return HtmlService.createHtmlOutputFromFile('ConfigCheck')
    .setTitle('配置检查工作流')
    .setWidth(1200)
    .setHeight(800)
    .setFaviconUrl(config.app.faviconUrl);
}

/**
 * 保存检查工作流
 * @param {Object} workflow - 工作流对象
 * @returns {Object} - 保存结果
 */
function saveCheckWorkflow(workflow) {
  try {
    const userEmail = Session.getActiveUser().getEmail();
    const userProperties = PropertiesService.getUserProperties();
    const workflowsKey = 'checkWorkflows';
    
    // 获取现有工作流
    const workflowsStr = userProperties.getProperty(workflowsKey) || '[]';
    const workflows = JSON.parse(workflowsStr);
    
    // 添加用户信息和日期
    workflow.createdBy = userEmail;
    workflow.created = new Date().toISOString();
    workflow.updated = new Date().toISOString();
    
    // 检查是否存在同名工作流
    const existingIndex = workflows.findIndex(w => w.name === workflow.name);
    
    if (existingIndex >= 0) {
      // 更新现有工作流
      workflow.created = workflows[existingIndex].created;
      workflows[existingIndex] = workflow;
    } else {
      // 添加新工作流
      workflows.push(workflow);
    }
    
    // 保存回 Properties
    userProperties.setProperty(workflowsKey, JSON.stringify(workflows));
    
    return {
      success: true,
      message: '工作流已保存',
      workflow: workflow
    };
  } catch (e) {
    Logger.log('保存检查工作流失败: ' + e);
    return {
      success: false,
      error: e.toString()
    };
  }
}

/**
 * 获取所有检查工作流
 * @returns {Object[]} - 工作流列表
 */
function getCheckWorkflows() {
  try {
    const userProperties = PropertiesService.getUserProperties();
    const workflowsKey = 'checkWorkflows';
    const workflowsStr = userProperties.getProperty(workflowsKey) || '[]';
    return JSON.parse(workflowsStr);
  } catch (e) {
    Logger.log('获取检查工作流失败: ' + e);
    return [];
  }
}

/**
 * 导出检查报告
 * @param {Object[]} results - 检查结果数组
 * @returns {string} - 报告电子表格URL
 */
function exportCheckReport(results) {
  try {
    const userEmail = Session.getActiveUser().getEmail();
    const reportName = '配置检查报告-' + new Date().toISOString().slice(0, 10);
    
    // 创建新的电子表格
    const ss = SpreadsheetApp.create(reportName);
    const sheet = ss.getActiveSheet();
    sheet.setName('检查结果汇总');
    
    // 添加报告标题和元数据
    sheet.getRange('A1').setValue('配置检查报告');
    sheet.getRange('A2').setValue('执行时间: ' + new Date().toISOString());
    sheet.getRange('A3').setValue('执行用户: ' + userEmail);
    
    // 计算总体统计信息
    let totalErrors = 0;
    results.forEach(item => {
      if (item.result.validation && item.result.validation.errorRows) {
        totalErrors += item.result.validation.errorRows;
      }
      if (item.result.error) {
        totalErrors++;
      }
    });
    
    // 添加总体状态
    sheet.getRange('A5').setValue('检查状态:');
    sheet.getRange('B5').setValue(totalErrors > 0 ? '失败' : '通过');
    sheet.getRange('A6').setValue('问题总数:');
    sheet.getRange('B6').setValue(totalErrors);
    
    // 添加详细结果表格
    sheet.getRange('A8').setValue('步骤');
    sheet.getRange('B8').setValue('状态');
    sheet.getRange('C8').setValue('问题数');
    sheet.getRange('D8').setValue('执行时间(ms)');
    sheet.getRange('E8').setValue('备注');
    
    // 填充详细结果
    let row = 9;
    results.forEach((item, index) => {
      const hasError = (item.result.validation && item.result.validation.errorRows > 0) || item.result.error;
      const errorCount = item.result.validation ? item.result.validation.errorRows : (item.result.error ? 1 : 0);
      const executionTime = item.result.stats ? item.result.stats.executionTime : 0;
      
      sheet.getRange(`A${row}`).setValue(item.step);
      sheet.getRange(`B${row}`).setValue(hasError ? '失败' : '通过');
      sheet.getRange(`C${row}`).setValue(errorCount);
      sheet.getRange(`D${row}`).setValue(executionTime);
      sheet.getRange(`E${row}`).setValue(item.result.error || '');
      
      row++;
    });
    
    // 为每个步骤创建详细结果表
    results.forEach((item, index) => {
      // 创建新表
      const detailSheet = ss.insertSheet(item.step.substring(0, 30)); // 限制工作表名长度
      
      // 添加标题和基本信息
      detailSheet.getRange('A1').setValue(`检查步骤: ${item.step}`);
      
      if (item.result.error) {
        // 显示错误信息
        detailSheet.getRange('A3').setValue('错误:');
        detailSheet.getRange('B3').setValue(item.result.error);
      } else if (item.result.validation) {
        // 显示验证结果
        detailSheet.getRange('A3').setValue('总行数:');
        detailSheet.getRange('B3').setValue(item.result.validation.totalRows);
        detailSheet.getRange('A4').setValue('错误行数:');
        detailSheet.getRange('B4').setValue(item.result.validation.errorRows);
        
        // 如果有错误，显示错误详情
        if (item.result.validation.errors && item.result.validation.errors.length > 0) {
          detailSheet.getRange('A6').setValue('错误详情:');
          
          // 添加错误表头
          detailSheet.getRange('A7').setValue('行号');
          detailSheet.getRange('B7').setValue('规则');
          
          // 添加数据字段作为列标题
          if (item.result.validation.errors[0].data) {
            const columns = Object.keys(item.result.validation.errors[0].data);
            let col = 3;
            columns.forEach(column => {
              if (!column.startsWith('_validation')) {  // 排除验证状态列
                detailSheet.getRange(7, col).setValue(column);
                col++;
              }
            });
          }
          
          // 填充错误数据
          let errorRow = 8;
          item.result.validation.errors.forEach(error => {
            detailSheet.getRange(`A${errorRow}`).setValue(error.rowIndex + 1);
            detailSheet.getRange(`B${errorRow}`).setValue(error.rule);
            
            // 添加行数据
            if (error.data) {
              const columns = Object.keys(error.data);
              let col = 3;
              columns.forEach(column => {
                if (!column.startsWith('_validation')) {  // 排除验证状态列
                  detailSheet.getRange(errorRow, col).setValue(
                    typeof error.data[column] === 'object' 
                      ? JSON.stringify(error.data[column]) 
                      : error.data[column]
                  );
                  col++;
                }
              });
            }
            
            errorRow++;
          });
        }
      }
    });
    
    // 返回电子表格URL
    return ss.getUrl();
  } catch (e) {
    Logger.log('导出检查报告失败: ' + e);
    return null;
  }
}

// 为前端暴露函数
function saveCheckWorkflowForClient(workflow) {
  return saveCheckWorkflow(workflow);
}

function getCheckWorkflowsForClient() {
  return getCheckWorkflows();
}

function exportCheckReportForClient(results) {
  return exportCheckReport(results);
}