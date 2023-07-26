driver spi 设计 感悟:

1. 除标准还要足方言，就是要方言发挥到极致.
2. 如 : io.jdbd.statement.ParametrizedStatement.bind() 设计 DataType 就可以把言类型全部发挥,
   再如: column meta ,MySQL 与 postgre 返回 数据量不同,就是可以提供 一个像 DataType 一样的 interface 来发挥出方言的全部特性.
