/**
 * Google Sheets SQL 配置文件
 * 集中存储所有配置参数，便于统一管理
 */

// 返回应用程序配置对象
function getConfig() {
  return {
    // Google Drive 文件夹配置
    drive: {
      // 用于搜索 Sheets 文件的根文件夹 ID
      rootFolderId: "YOUR_SPREADSHEET_ID_HERE", // 替换为您的文件夹ID
    },
    
    // 中央模板存储库配置
    templateRepository: {
      // 中央模板存储库文件ID
      fileId: "YOUR_SPREADSHEET_ID_HERE", // 替换为您的电子表格ID
      
      // 工作表名称配置
      sheets: {
        publicTemplates: "公开模板",
        reviewLog: "审核日志",
        userFeedback: "用户反馈"
      }
    },
    
    // 应用程序其他配置
    app: {
      // 应用程序名称
      name: "Google Sheets SQL",
      // favicon URL
      faviconUrl: "https://www.google.com/images/favicon.ico",
      // 用户属性存储键
      userPropertiesKey: "sql_templates",
    },

    // 数据相关配置
    data: {
      // 不加载的列名列表
      excludedColumns: []
    }
  };
}
