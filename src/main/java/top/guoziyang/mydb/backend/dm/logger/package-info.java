package top.guoziyang.mydb.backend.dm.logger;
/*
  4. 日志文件与恢复策略
  https://blog.csdn.net/qq_40856284/article/details/121873723
  日志模块

  负责将数据写入磁盘，并记录日志

  1. 将数据写入磁盘
  2. 记录日志
  3. 提供日志恢复功能
  <p>
  日志模块是整个数据库系统的核心，它负责将数据写入磁盘，并记录日志。日志模块提供了以下功能：
 */