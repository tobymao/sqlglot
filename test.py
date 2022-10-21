import importlib
import sqlglot
from sqlglot import *

tsql = """
SELECT
 CONVERT(NVARCHAR(200), x) as test1
,CONVERT(NVARCHAR, x) as test2
,CONVERT(NVARCHAR(MAX), x) as test2
,CONVERT(VARCHAR(200), x) as test1
,CONVERT(VARCHAR, x) as test2
,CONVERT(VARCHAR(MAX), x) as test2

,CONVERT(CHAR(40), x) as test4
,CONVERT(CHAR, x) as test4
,CONVERT(NCHAR(40), x) as test4
,CONVERT(NCHAR, x) as test4

,CONVERT(VARCHAR, x, 121) as test4
,CONVERT(VARCHAR(40), x, 121) as test4
,CONVERT(VARCHAR(MAX), x, 121) as test4

,CONVERT(NVARCHAR, x, 121) as test4
,CONVERT(NVARCHAR(40), x, 121) as test4
,CONVERT(NVARCHAR(MAX), x, 121) as test4

,CONVERT(DATE, x, 121) as test4
,CONVERT(DATETIME, x, 121) as test4
,CONVERT(DATETIME2, x, 121) as test4

,CONVERT(INT, x, 121) as test4

FROM dbo.test
"""
#print(sqlglot.parse(sql=tsql, read="tsql"))


print(sqlglot.transpile(tsql, read='tsql', write='spark', pretty=True, max_unsupported=0)[0])