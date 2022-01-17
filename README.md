# rqlite-java-jdbc
rqlite java 客户端，此项目是基于 github rqlite/rqlite-java项目二次开发的

在原有基础上面增加了数据库JDBC的相关操作

# 如何使用
继承BaseMapper，类似hibernate全自动ORM框架

示例代码包含增删改查常用操作，返回类型包含常用对象、集合、String、int等。

插入操作可以批量插入或者逐条插入