1. 为什么 Statement 接口的 bind 方法没有设计返回值?
   * database driver 本质上是为框架开发者开发的,不是给应用开发者开发的,因为在这个时代 应用开发者如果选择直接使用 driver api 开发应用那是不明智的.
   * 基于以上原因 bind 方法只能通过循环调用,这时 bind 方法的返回值毫无意义.

2. 为什么 io.jdbd.stmt.PreparedStatement 接口只有一个 bind 方法且不能指定 sql type?
   * prepare 语句已经从数据库 server 获取了参数类型和参数个数,这时 sql 类型是无意义的,所以只需要一个 bind 方法.

3. 为什么 statement 的 bind 方法不支持命名参数?
   * 数据库底层只支持 ? 作为参数占位符.
   * 用 int 绑定参数无论是对 driver 的实现者还是 持久化框架的开发者来说都简单高效且不易出bug.
   * ? 作为参数占位符有多年的用户基础.
   * 命名参数不是必需的, 秦军 开源组织的理念是 "若可有可无,则尽可能选择 无".

4. 为什么要为 io.jdbd.stmt.PreparedStatement 和 io.jdbd.stmt.BindableStatement 设计 base interface
   io.jdbd.stmt.BindableSingleStatement ?
   * 它们有共同的方法,设计 base interface 是常规
   * 更重要的是 在一些场景下可以使用 io.jdbd.stmt.BindableSingleStatement 的方法引用.

5. 为什么要为 io.jdbd.stmt.PreparedStatement 和 io.jdbd.stmt.MultiStatement 设计 base interface
   io.jdbd.stmt.BindableMultiResultStatement ?
   * 它们有共同的方法,设计 base interface 是常规
   * 更重要的是 在一些场景下可以使用 io.jdbd.stmt.BindableMultiResultStatement 的方法引用.

6. 为什么 io.jdbd.vendor.result.MultiResultSink 没有 isCancelled 方法?
   * 要让下游能能到信号,避免 bug.

7. 为什么 io.jdbd.vendor.result.QuerySink 没能向上游抛出异常?
   * 可能导致 网络 channel 有脏数据,异常只能向下游抛出.

8. 为什么 executeAsFlux 能执行所有类型的 sql,还要设计其它方法呢 ?
   * executeAsFlux 虽然可执行所有类型的 sql,但在 reactor api 环境中使用不便.那为什么不设计得简单点呢,因为这是最简单的了.
   * 若不执行过程调用的sql不必这样的方法,在这个时代开发应用使用过程调用的频率并不高.
   * executeUpdate,executeQuery,executeBatch 这些方法 在 reactor api 环境中使用起来更简单,频率最高.
   * executeAsFlux 和 executeAsMulti 只是为过程调用能返回多个 result 而设计的.
   


