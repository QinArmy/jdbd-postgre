1. 发送 sql 包时 若 长参数 出异常，怎么处理,如:MySQL ClientPrepared 发送 命令包.
2. 对于 detectCustomCollations 属性,如果定制的字符集是未知的那么将无法转换成 java 字符庥.
3. 一个 SQLType 可以被映射成多种JAVA类型,如:MySQL 的 TIME 可以 映射成 LocalTime 和 Duration.

How to clear channel?

1. 把 ByteBuf 给 Task 之前 记录 packet header index.
2. 出错时 reset 到 之前记录的 index
3. 交给之类 清空 channel.

任务执行器 在任务正常结束 时要检查 ByteBuf 是否已 读取完毕,若没有读取完毕,则下一个任务会读取到 脏数据.

4. 为什么一定要坚持在创建 packet 的过程中使用 List<ByteBuf> 表示 multi packet?
    1. 因为 byte[] 数组的长度最大为 (1<< 30),但 MySQL最大的包是 (1<< 30)
       ,这就使用是无法发送最大的包,因为每个单包必须有 header.
    2. 因为 可以避免不必要的复制.   