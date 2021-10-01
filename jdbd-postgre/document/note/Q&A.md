1. 为什么不支持 ResultRowMeta.getFieldType() 要返回 UNKNOWN ?
   1. 因为 postgre client 协议设计得不好,在 row 元素数据消息中没有返回 columnName,tableName等.
   2. 那为什么不在 select 语句执行之前先查询所需的元数据,因为如果调用的是存储过程将会造成逻辑漏洞.

2. ExtendedQueryTask 为什么有参的情况下全采用先 describe 得到所有参数元数数据 再绑定执行的模式? 1.PreparedStatement api 的特性本来就要求这样. 2.如果 BindStatement
   语句在调用 ExtendedQueryTask,那么这样可以保证对参数的兼容性, 如:column 是 bigint ,可以把持 shor,int,long,String,BigDecimal,BigInteger的类型的参数,
   只要其范围不超过 long 的范围就可以.

3. 为什么 SimpleQueryTask 不选择创建完成 query message 后再提交任务? 1.SimpleQueryTask 决定要支持 Publisher 2.在 Publisher 的情况下 client charset
   可能被改变 3.创建 Query message 需要 client charset,尤其 bytea array 类型需要使用 client charset.
   
