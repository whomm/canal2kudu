# 简介:
实时同步Mysql数据到kudu

# 使用说明
1. 实时数据同步。
    1. 启动进程后订阅所有的数据到kudu的对应表格。
    2. 如果设置了DbTableName 会筛选对应表格。 如果同时设置 DbTableName 会将对应的数据库表格导入到kudu的表格。
    
    ```
    java  -Djava.ext.dirs=./ -DDestination=datares -DKuduMaster=10.0.2.61:7051 -DCanalSevZk=10.0.2.61:2181 -DBatchNum=10 -DKdTableName=testuser  -DDbTableName=xgooduser  whomm.canal2kudu.CanalClient
    KuduMaster   kudu的master地址
    CanalSevZk   canal的zk地址
    BatchNum     单次binlog数目 默认1024
    KdTableName  kudu表格名称 可选。
    DbTableName  mysql表格名称 可选。
    目前不支持kudu rpc的ssl
    需设置：kudu master和table的配置
    --rpc_authentication=disabled
    --rpc_encryption=disabled
    ```

2. 历史数据的一致性。
    1. 实时同步只能在canel server启动之后的修改才能同步过来。但是历史的数据如何能保证和database一致。目前该工具提供一个近似的解决方案。使用import工具导入ID小于某个时间点产生的一个ID实现非热数据的导入，但该方法无法保证数据绝对一致。
    2. 调用方法

    ```
        java -Djava.ext.dirs=./  -DKuduMaster=127.0.0.1:7051 -DMysqlStr=jdbc:mysql://127.0.0.1:8306/mytest -DDbUser=www -DDbPwd=123456 -DDbTableName=user -DKdTableName=user -DStartId=0 -DMaxId=20 -DBatchNum=1 -DThreadScount=5  whomm.canal2kudu.ImportMysql 
        
        
        KuduMaster   kudu的master地址
        MysqlStr     数据库链接字符串
        DbUser       数据库用户名
        DbPwd        数据库密码
        DbTableName  数据库表格名称
        KdTableName  kudu的表格名称
        StartId      本次导入数据id起始值
        MaxId        本次导入数据id最大值
        BatchNum     每次导入数据条数
        ThreadScount 并发线程数目
        
        线程内每个BatchNum是一次链接+查询,然后 sleep 10 ms
    ```

3. 关于性能。
	1. 目前仅仅完成了功能的开发性能还没来得及优化。