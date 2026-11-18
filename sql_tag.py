# -*- coding: utf-8 -*-
"""SQL 审计打标工具。

所有发往达梦数据库的语句统一前置一段注释，便于在审计记录
V$AUDITRECORDS.SQL_TEXT（以及 dmaudtool 导出的审计文件）中直接识别
"这条 SQL 来自 QGIS 插件"。

背景：达梦审计记录（USERNAME / IP / SCHNAME / OBJNAME / OPERATION /
SQL_TEXT / MAC）本身没有"客户端工具 / 程序名"字段，SQL_TEXT 是唯一能
承载来源标识的位置，因此统一加注释是最可靠的落地方式。
"""

SQL_TAG = "/***QGIS-DMPlugin***/"


def tag(sql):
    """给 SQL 前置审计标识注释；已有注释的语句原样返回。

    :param sql: 原始 SQL 字符串
    :return: 带标识注释的 SQL 字符串
    """
    if not sql:
        return sql
    text = sql.lstrip()
    if text.startswith("/*"):
        return text
    return SQL_TAG + " " + text
