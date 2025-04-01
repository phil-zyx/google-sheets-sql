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
// testEvaluateExpression();