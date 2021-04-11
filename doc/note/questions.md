1. 发送 sql 包时 若 长参数 出异常，怎么处理,如:MySQL ClientPrepared 发送 命令包.
2. 对于 detectCustomCollations 属性,如果定制的字符集是未知的那么将无法转换成 java 字符庥.
3. 一个 SQLType 可以被映射成多种JAVA类型,如:MySQL 的 TIME 可以 映射成 LocalTime 和 Duration.

How to clear channel?

1. 把 ByteBuf 给 Task 之前 记录 packet header index.
2. 出错时 reset 到 之前记录的 index
3. 交给之类 清空 channel.

任务执行器 在任务正常结束 时要检查 ByteBuf 是否已 读取完毕.

 