/**
 * 为 AlaSQL 注册 JSON 相关的函数
 * @param {Object} alasql - AlaSQL 实例
 */
function registerJSONFunctions(alasql) {
  // JSON_EXTRACT - 提取 JSON 字符串中的值
  alasql.fn.JSON_EXTRACT = function(jsonStr, path) {
    if (!jsonStr) return null;
    
    try {
      // 确保 jsonStr 是字符串
      if (typeof jsonStr !== 'string') {
        jsonStr = JSON.stringify(jsonStr);
      }
      
      const obj = JSON.parse(jsonStr);
      
      // 处理路径，支持 $.property.array[0] 格式
      if (!path || path === '$') return obj;
      
      const pathParts = path.replace(/^\$\.?/, '').split('.');
      let result = obj;
      
      for (let i = 0; i < pathParts.length; i++) {
        let part = pathParts[i];
        
        // 处理数组索引 [n]
        const arrayMatch = part.match(/^([^\[]+)\[(\d+)\]$/);
        if (arrayMatch) {
          const prop = arrayMatch[1];
          const index = parseInt(arrayMatch[2]);
          
          result = result[prop][index];
        } else {
          result = result[part];
        }
        
        if (result === undefined) return null;
      }
      
      return result;
    } catch (e) {
      return null;
    }
  };
  
  // JSON_OBJECT - 创建 JSON 对象
  alasql.fn.JSON_OBJECT = function() {
    const obj = {};
    
    for (let i = 0; i < arguments.length; i += 2) {
      if (i + 1 < arguments.length) {
        const key = arguments[i];
        const value = arguments[i + 1];
        obj[key] = value;
      }
    }
    
    return JSON.stringify(obj);
  };
  
  // JSON_ARRAY - 创建 JSON 数组
  alasql.fn.JSON_ARRAY = function() {
    return JSON.stringify(Array.from(arguments));
  };
  
  // JSON_CONTAINS - 检查 JSON 是否包含值
  alasql.fn.JSON_CONTAINS = function(jsonStr, searchStr) {
    try {
      const obj = typeof jsonStr === 'string' ? JSON.parse(jsonStr) : jsonStr;
      const search = typeof searchStr === 'string' ? JSON.parse(searchStr) : searchStr;
      
      return containsObject(obj, search);
    } catch (e) {
      return false;
    }
  };
  
  // 辅助函数：检查对象包含
  function containsObject(obj, search) {
    // 基本类型直接比较
    if (typeof search !== 'object' || search === null) {
      return obj === search;
    }
    
    // 处理数组
    if (Array.isArray(search)) {
      if (!Array.isArray(obj)) return false;
      
      return search.every(item => {
        return obj.some(objItem => containsObject(objItem, item));
      });
    }
    
    // 处理对象
    for (const key in search) {
      if (!obj || typeof obj !== 'object' || !(key in obj)) {
        return false;
      }
      
      if (!containsObject(obj[key], search[key])) {
        return false;
      }
    }
    
    return true;
  }
  
  // JSON_SCHEMA_VALID - 验证 JSON 结构
  alasql.fn.JSON_SCHEMA_VALID = function(schema, jsonStr) {
    try {
      const schemaObj = typeof schema === 'string' ? JSON.parse(schema) : schema;
      const jsonObj = typeof jsonStr === 'string' ? JSON.parse(jsonStr) : jsonStr;
      
      // 简单的结构验证逻辑
      return validateSchema(schemaObj, jsonObj);
    } catch (e) {
      return false;
    }
  };
  
  // 辅助函数：验证 JSON 结构
  function validateSchema(schema, json) {
    if (!schema || typeof schema !== 'object') return true;
    
    // 类型检查
    if (schema.type) {
      const type = schema.type;
      
      if (type === 'string' && typeof json !== 'string') return false;
      if (type === 'number' && typeof json !== 'number') return false;
      if (type === 'boolean' && typeof json !== 'boolean') return false;
      if (type === 'object' && (typeof json !== 'object' || json === null || Array.isArray(json))) return false;
      if (type === 'array' && !Array.isArray(json)) return false;
      if (type === 'null' && json !== null) return false;
    }
    
    // 属性验证
    if (schema.properties && typeof json === 'object' && !Array.isArray(json)) {
      for (const prop in schema.properties) {
        if (schema.required && schema.required.includes(prop) && !(prop in json)) {
          return false;
        }
        
        if (prop in json && !validateSchema(schema.properties[prop], json[prop])) {
          return false;
        }
      }
    }
    
    // 数组项验证
    if (schema.items && Array.isArray(json)) {
      for (const item of json) {
        if (!validateSchema(schema.items, item)) {
          return false;
        }
      }
    }
    
    return true;
  }
  
  // 为 JSON_EXTRACT 添加数组过滤功能
  alasql.fn.JSON_EXTRACT_FILTERED = function(jsonStr, keyName, keyValue, targetProp) {
    if (!jsonStr) return [];
    
    try {
      const obj = typeof jsonStr === 'string' ? JSON.parse(jsonStr) : jsonStr;
      
      if (!Array.isArray(obj)) return [];
      
      // 始终返回一个数组
      const matches = [];
      for (let i = 0; i < obj.length; i++) {
        if (obj[i][keyName] === keyValue) {
          matches.push(obj[i][targetProp]);
        }
      }
      
      return matches;
    } catch (e) {
      return [];
    }
  };
}

/**
 * 注册自定义 SQL 函数
 * @param {Object} alasql - AlaSQL 实例
 */
function registerCustomSQLFunctions(alasql) {
  // ARRAY_CONTAINS - 检查数组是否包含某个值
  alasql.fn.ARRAY_CONTAINS = function(arr, value) {
    // 如果输入不是数组，返回 false
    if (!Array.isArray(arr)) return false;
    
    // 检查数组中是否包含该值
    return arr.includes(value);
  };

  // 注册数组展开函数
  alasql.fn.UNNEST = function(arr) {
    if (!arr) return [];
    
    // Handle different input types
    let array;
    if (typeof arr === 'string') {
      try {
        array = JSON.parse(arr);
      } catch (e) {
        // If not valid JSON, split by comma
        array = arr.split(',').map(item => item.trim());
      }
    } else {
      array = arr;
    }
    
    // Ensure it's an array
    if (!Array.isArray(array)) {
      return [{ value: array }];
    }
    
    // Map primitive values to objects with 'value' property
    // and keep objects as they are
    return array.map(item => {
      if (typeof item === 'object' && item !== null) {
        return item;
      } else {
        return { value: item };
      }
    });
  };
  
  // 如果没有自定义函数，提供一个空的实现
  // 可以根据需要添加自定义函数
  // 例如:
  // alasql.fn.myCustomFunction = function(x) { return x * 2; };
}

/**
 * 尝试解析 JSON 字符串
 * @param {string} str - JSON 字符串
 * @returns {Object|null} - 解析后的对象，如果解析失败则返回 null
 */
function tryParseJSON(str) {
  try {
    return JSON.parse(str);
  } catch (e) {
    return null;
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
