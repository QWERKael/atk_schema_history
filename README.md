通过监听binlog更新，实时获取表结构修改，并存到指定到表中。

程序会在配置文件指定的库中创建3张表。

- TABLES_LOG :: 每次表更新后备份，information_schema.tables的数据

- COLUMNS_LOG :: 每次表更新后备份，information_schema.columns的数据

- SCHEMA_CHANGE_LOG :: 每次表更新后备份，记录DDL语句和新的CREATE TABLE语句

目前只记录`CREATE` `ALTER` `RENAME`这3中DDL语句，`DROP`以为不会造成历史表结构数据丢失，所以暂时不支持

每张表中的`insert_time`是binlog中DDL Event的时间戳（初始化时除外）

每次启动都会进行一次数据初始化，会获取除系统表外所有表的表结构，此时的`insert_time`是当前的时间