1. 为什么不支持 ResultRowMeta.getFieldType() 要返回 UNKNOWN ?
    1. 因为 postgre client 协议设计有不好,在 row 元素数据消息中没有返回 columnName,tableName等.
    2. 那为什么不在 select 语句执行之前先查询所需的元数据,因为如果调用的是存储过程将会造成逻辑漏洞.
    
