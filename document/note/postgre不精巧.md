1. ExtendedQuery bind param type , 只能在 Parse message 指定,Bind Message 不能指定类型.
2. postgre 类型系统复杂且支持用户自定义类型,然而绑定 参数只支持 oid ，所以可能在运行时 bind 未知类型.
3. postgre 类型系统复杂且支持用户自定义类型 ,然而返回的 row meat 没有类型名.
4. binary 格式 没有文档
5. kill 查询必须 new 一个连接发消息 kill
6. 很多消息类型根本没有必要设计,比如 解析完成. 这个消息完全可像 MySQL 一样返回一个 同样的 ok 消息,并返回事务信息.
