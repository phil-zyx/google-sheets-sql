# Google Sheets SQL

基于 Google Apps Scripts 和 AlaSQL 的强大工具，支持查询 Google Drive 下的所有 Sheets 表的数据，支持联表查询、JSON 解析等高级功能。

## 功能特点

- 🔍 **强大的查询能力**：使用 SQL 查询语法，包括 SELECT, JOIN, WHERE, GROUP BY 等
- 📊 **查询 Google Drive 中的任何 Sheet**：无需导入数据，直接查询现有工作表
- 🔄 **支持联表查询**：跨不同工作表或文件进行联表查询
- 📝 **JSON 解析**：自动检测和解析 JSON 字段，支持 JSON 路径查询
- 📋 **元数据浏览**：查看所有可用的表和字段信息
- 📜 **查询历史**：保存和重用之前执行的查询
- ⏱️ **性能统计**：查看查询执行时间和结果行数

## 使用方法

1. 打开 Google Apps Script 编辑器
2. 创建一个新项目
3. 复制本项目中的所有文件到您的项目中
4. 部署为网络应用程序
5. 访问生成的 URL 使用此工具

## SQL 查询示例

### 基本查询

```sql
-- 查询单个工作表
SELECT * FROM 工资表.Sheet1 LIMIT 10
```

### 联表查询

```sql
-- 跨表联查
SELECT 
  emp.姓名, 
  emp.部门, 
  dept.部门主管,
  emp.薪资
FROM 员工数据.员工信息 emp
JOIN 部门信息.部门列表 dept ON emp.部门 = dept.部门名称
WHERE emp.薪资 > 8000
ORDER BY emp.薪资 DESC
```

### JSON 字段解析

```sql
-- 解析包含 JSON 的字段
SELECT
  id,
  JSON_VALUE(用户信息, "$.name") AS 姓名,
  JSON_VALUE(用户信息, "$.address.city") AS 城市
FROM 用户数据.用户资料
WHERE JSON_VALUE(用户信息, "$.age") > 30
```

### 聚合查询

```sql
-- 分组聚合
SELECT 
  部门,
  COUNT(*) AS 员工数,
  SUM(薪资) AS 总薪资,
  AVG(薪资) AS 平均薪资
FROM 员工数据.薪资信息
GROUP BY 部门
HAVING COUNT(*) > 5
ORDER BY 总薪资 DESC
```

## 技术实现

项目使用了以下技术：

- Google Apps Scripts：提供对 Google Drive 和 Sheets 的访问
- AlaSQL：轻量级的客户端 SQL 数据库引擎
- Bootstrap 5：UI 框架
- CodeMirror：SQL 编辑器

## 注意事项

- 工具使用者需要对查询的 Google Sheets 有访问权限
- 对于大型表格，查询可能需要较长时间
- 首次查询表格时会有一定延迟，因为需要加载数据
- 对于包含 JSON 的列，会自动检测并解析为 JavaScript 对象