增加一个新的 jdbd 实现需要确定的核心问题?

* 底层协议在执行 update(insert,update,delete) 时是否能返回 完整的 ResultStatus
* 底层协议在执行 query(select) 时是否能返回 完整的 ResultStatus
* 对于非 prepare 协议它的 字符串逃逸规则
* 有无 数据库时区的概念
* 对于字符集的支持情况
* 是否能支持多语句
* 怎样调用存储过程
* 哪一种协议能支持 存储过程的 out parameter
* 能不能支持 kill query 以便能支持执行超时
* 如果 server 器主动 kill query 处理情况是否有与 MySQL 与差别
* 是否会有数据库主动发送的消息(比如通知)
* 当 connection 断开时,未完成事务的处理方案
* 如果关闭 session 时还有事务,处事方案是什么