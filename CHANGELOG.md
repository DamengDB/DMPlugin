# Changelog

All notable changes to the DMPlugin (QGIS ↔ Dameng Database) project are documented here.

## [0.2.0] - 2026-11-18

本次更新集中在两件事：**让 QGIS 的访问行为在达梦审计里可识别**，以及**把空间表导入链路的一系列失败修到可稳定完成**。

### 新增

- **SQL 审计标识（方案 ②）**：新增 `sql_tag.py`，提供常量 `SQL_TAG = "/* QGIS-DMPlugin */"` 与 `tag(sql)`。标识中**不含版本号**，避免每次发版都要更新；审计侧统一用 `SQL_TEXT LIKE '%QGIS-DMPlugin%'` 匹配。
  - `import_dialog.py` 全部 16 处 `cursor.execute` 已统一走 `tag()`；
  - `damengplugin_dialog.py` 的 `SELECT SF_GET_UNICODE_FLAG()` 同样打标；
  - 已含 `/* ... */` 注释的 SQL 不会重复叠加，空串原样返回。
  - 背景：达梦 `V$AUDITRECORDS` 字段为 `USERID / USERNAME / ROLENAME / IP / SCHID / SCHNAME / OBJID / OBJNAME / OPERATION / SQL_TEXT / MAC / SEQNO`，**不记录客户端程序名**；`dmPython.app_name` 与 `DBMS_APPLICATION_INFO.SET_MODULE` 只影响会话监控，不落审计文件。因此唯一能让 "QGIS" 出现在审计中的位置就是 `SQL_TEXT`。
- **图层类型按数据自适应**：导入前先取一批数据，用第一条有效几何的真实 `wkbType` 建立图层（`QgsWkbTypes.displayString`），不再依赖类型下拉框文本；数据无有效几何时才回退到 `typeBox`。
- **几何类型对齐**：新增 `_fit_geometry()`，单类型 → 多类型时自动 `convertToMultiType()`，无法对齐的要素单条跳过并计数，不拖垮整批。
- **失败要素定位**：整批 `addFeatures` 被拒时逐条 `addFeature` 重试，保留可写入部分，并输出第一条失败要素的
  `uri / layerWkb / featureWkb / layerFields / featureAttrs / attrTypes / wkt / providerError`。
- **provider 错误原文透出**：`addFeatures` 失败后读取 `provider.error()` 与 `layer.lastError()`，直接打印 QGIS 给出的真实原因（例如 `Could not store attribute "ZHAA11A320": Overflow`）。
- **逐值类型校验**：写要素前按图层字段类型核对属性值的实际 Python 类型（`Int / Double / String / Date / Time / DateTime`），不符则只跳过该条要素并点名；同一坏值重复上千次时只报前 3 条，导入结束再给汇总。
- **空结果提示**：查询返回 0 行时明确提示 `srid` / 几何类型过滤条件可能与表定义不符，而不是静默生成空图层。
- **字段名去重**：`_build_fields()` 对重名字段自动加 `_2` 后缀、空名兜底为 `field`，避免 `addAttributes()` 失败导致后续整批写入异常。
- **精度还原**：新增 `_compose_dm_type()`，从 `ALL_TAB_COLUMNS.DATA_PRECISION / DATA_SCALE` 还原出 `NUMBER(18,2)`、`VARCHAR2(100)` 这类完整类型串。

### 修复

- **`'NoneType' object has no attribute 'year'`**：`QVariant.Time / Date / DateTime` 分支对 NULL 值无条件访问 `.hour / .year`，一条空值即中断整批导入。已改为类型安全转换 `convert_value(qvariant_type, value)`，NULL 优先返回 `None`，并支持 `datetime / date / time / str` 多格式解析。
- **`'DatabaseError' object has no attribute 'message'`**：达梦 dmPython 的错误对象没有 `.message`，`get_table_fields` 与 `get_total_size` 已统一改用 `str(e)`（否则异常处理里会再抛 `AttributeError`，把真实错误掩盖掉）。
- **表字段列表错位**：同一 `cursor` 连续调用两次 `fetchall()`，第二次为空导致 `geometryTypes[i]` 越界。已改为单次 `fetchall()` 后拆分。
- **占位符被引号包住**：`OWNER = '?'` 恒不匹配，导致查询结果为空；已改为 `OWNER = ?`。
- **几何列混入属性列**：`COLUMN_NAME != ?` 受大小写/空格影响时不生效；现除 SQL 过滤外，在 Python 侧再按小写比对剔除一次。几何对象一旦进入属性列表，整批要素都会被拒。
- **字段重名导致 schema 建不起来**：已见"新增"中的去重逻辑。
- **`NUMBER(p,s)` 被判成整数**（本次最关键）：
  - 小数被截断：`NUMBER(10,2)` 的 `102.57` 会变成 `102`；
  - 大数溢出：`NUMBER(18,2)` 的值 `4800000200000000` 超出 32 位整数，provider 的 `convertCompatible` 失败，返回 `Could not store attribute "ZHAA11A320": Overflow`，导致整批要素全被拒。
  - 现按标度解析：`scale > 0 → Double`，`scale = 0 → Int`；`Int` 字段额外做 ±2³¹ 范围检查。
- **`ST_POINT` 因含子串 `INT` 被误判为整数类型**：旧规则 `"INT" in t` 命中 `ST_POINT`，几何列一旦漏进属性列表就会以 `Int` 字段接收几何对象。现改为按 base type 精确匹配。
- **`add features fail, failed number:1000`**：综合原因为 ① 图层类型取自 `typeBox` 文本而非真实数据；② `QgsFeature()` 未携带字段 schema；③ 属性值含 `Decimal` 等未转换类型。三项均已修复（数据定类型、`QgsFeature(layer.fields())`、`convert_value`）。
- **`QgsGeometry.fromWkt()` 返回空对象**：补充 `geom is None or geom.isEmpty() or not geom.isGeosValid()` 三段判断，并跳过无效 WKT。
- **进度计算除零**：`total_size` 为 0 时不再做百分比运算，进度封顶 100。

### 变更

- `dm_type_to_qgis()` 重写为按 base type 精确匹配，补充 `CLOB / BLOB / NVARCHAR2 / NCHAR` → `String`、`BIGINT` → `LongLong`、`TIMESTAMP(n)` / `DATETIME` → `DateTime`、`DATE` → `Date`。
- `cursor.execute()` 的绑定参数统一为与 `?` 等长的位置参数；`dmPython` 接受多参形式，但显式元组更不易出错。
- 导入线程内的错误处理统一为"跳过当前要素 + 记录字段名与值"，不再中断整批。

### 已知限制

- 达梦审计记录中没有客户端程序名列，QGIS 来源只能通过 `SQL_TEXT` 注释识别：
  ```sql
  SELECT * FROM V$AUDITRECORDS WHERE SQL_TEXT LIKE '%QGIS-DMPlugin%';
  ```
  使用前需 `SP_SET_ENABLE_AUDIT(1)` 并配置 `SP_AUDIT_STMT / SP_AUDIT_OBJECT` 规则，建议 `AUDIT_FLUSH_LEVEL = 0`（语句级刷盘）。
- `V$AUDITRECORDS` 属 `SYSAUDITOR` 名下，普通账号查询会报 `-5504`；验证阶段可临时 `GRANT SELECT ON SYSAUDITOR.V$AUDITRECORDS TO "用户"`，验证后应回收。
- 大表（如 38 万行 × 161 列）使用 memory 图层承载时内存占用较高，后续可考虑 `QgsVectorFileWriter` 落盘 GPKG。

### 验证

桩环境（脱离 QGIS 的模块打桩）回归结果：

| 校验项 | 结果 |
|---|---|
| `import_dialog.py` / `damengplugin_dialog.py` / `sql_tag.py` / `importWorker.py` 语法 | 全部通过 |
| `cursor.execute` 打标覆盖率 | 16 / 16，未打标 0 |
| `convert_value` 类型转换用例 | 16 项，0 失败 |
| `_fit_geometry`（单→多、多→单） | 符合预期 |
| `_build_fields`（重名、空名） | `['ZHAA11A955', 'zhaa11a955_2', 'field', 'ZHAA11A016']` |
| `_compose_dm_type` → `dm_type_to_qgis` | `NUMBER(18,2)→Double`、`NUMBER(8,0)→Int`、`VARCHAR2(20)→String`、`TIMESTAMP→DateTime`，0 失败 |
| 诊断串拼接（含 `providerError`） | 通过 |

## [0.1.0] - 初始版本

- 通过 dmPython 连接达梦数据库，列出模式下空间表并按 SRID / 几何类型 / ROWNUM 过滤导入为 QGIS 临时图层。
