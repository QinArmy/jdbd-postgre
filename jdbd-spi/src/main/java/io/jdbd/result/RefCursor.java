package io.jdbd.result;

import io.jdbd.JdbdException;
import io.jdbd.session.Closeable;
import io.jdbd.session.DatabaseSession;
import org.reactivestreams.Publisher;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>
 * This interface representing reference of server sql cursor.
 * </p>
 * <p>
 * The cursor will be close in following scenarios :
 *     <ul>
 *         <li>the any method of {@link RefCursor} emit {@link JdbdException}</li>
 *         <li>You invoke {@link #close()}</li>
 *     </ul>
 * If the methods of {@link RefCursor} don't emit any {@link Throwable},then you must invoke {@link #close()}  for closing cursor,
 * or the {@link DatabaseSession} that create this {@link RefCursor}
 * can't possibly execute any new {@link io.jdbd.statement.Statement}.
 * </p>
 *
 * @see io.jdbd.meta.JdbdType#REF_CURSOR
 * @since 1.0
 */
public interface RefCursor extends Closeable {

    /**
     * @return cursor name
     */
    String name();

    /*-------------------below first method-------------------*/

    /**
     * <p>
     * This method is equivalent to following :
     * <pre>
     *         <code><br/>
     *             // cursor is instance of RefCursor
     *             cursor.first(function,states->{}) ; // ignore ResultStates instance.
     *         </code>
     *     </pre>
     * </p>
     *
     * @see #first(Function, Consumer)
     */
    <T> Publisher<T> first(Function<CurrentRow, T> function);

    /**
     * <p>
     * Fetch the first row of the query.
     * </p>
     *
     * @throws NullPointerException emit(not throw) when function is null or consumer is null.
     * @throws JdbdException        emit(not throw) when
     *                              <ul>
     *                                  <li>driver don't support this method</li>
     *                                  <li>session close</li>
     *                                  <li>cursor have closed</li>
     *                                  <li>server response error</li>
     *                              </ul>
     */
    <T> Publisher<T> first(Function<CurrentRow, T> function, Consumer<ResultStates> consumer);

    /**
     * <p>
     * Fetch the first row of the query.
     * </p>
     *
     * @throws JdbdException emit(not throw) when
     *                       <ul>
     *                           <li>driver don't support this method</li>
     *                           <li>session close</li>
     *                           <li>cursor have closed</li>
     *                           <li>server response error</li>
     *                       </ul>
     */
    OrderedFlux first();

    /*-------------------below last method-------------------*/

    /**
     * <p>
     * This method is equivalent to following :
     * <pre>
     *         <code><br/>
     *             // cursor is instance of RefCursor
     *             cursor.last(function,states->{}) ; // ignore ResultStates instance.
     *         </code>
     *     </pre>
     * </p>
     *
     * @see #last(Function, Consumer)
     */
    <T> Publisher<T> last(Function<CurrentRow, T> function);

    /**
     * <p>
     * Fetch the last row of the query.
     * </p>
     *
     * @throws NullPointerException emit(not throw) when function is null or consumer is null.
     * @throws JdbdException        emit(not throw) when
     *                              <ul>
     *                                  <li>driver don't support this method</li>
     *                                  <li>session close</li>
     *                                  <li>cursor have closed</li>
     *                                  <li>server response error</li>
     *                              </ul>
     */
    <T> Publisher<T> last(Function<CurrentRow, T> function, Consumer<ResultStates> consumer);

    /**
     * <p>
     * Fetch the last row of the query.
     * </p>
     *
     * @throws JdbdException emit(not throw) when
     *                       <ul>
     *                           <li>driver don't support this method</li>
     *                           <li>session close</li>
     *                           <li>cursor have closed</li>
     *                           <li>server response error</li>
     *                       </ul>
     */
    OrderedFlux last();

    /*-------------------below prior method-------------------*/

    /**
     * <p>
     * This method is equivalent to following :
     * <pre>
     *         <code><br/>
     *             // cursor is instance of RefCursor
     *             cursor.prior(function,states->{}) ; // ignore ResultStates instance.
     *         </code>
     *     </pre>
     * </p>
     *
     * @see #prior(Function, Consumer)
     */
    <T> Publisher<T> prior(Function<CurrentRow, T> function);

    /**
     * <p>
     * Fetch the prior row.
     * </p>
     *
     * @throws NullPointerException emit(not throw) when function is null or consumer is null.
     * @throws JdbdException        emit(not throw) when
     *                              <ul>
     *                                  <li>driver don't support this method</li>
     *                                  <li>session close</li>
     *                                  <li>cursor have closed</li>
     *                                  <li>server response error</li>
     *                              </ul>
     */
    <T> Publisher<T> prior(Function<CurrentRow, T> function, Consumer<ResultStates> consumer);

    /**
     * <p>
     * Fetch the prior row.
     * </p>
     *
     * @throws JdbdException emit(not throw) when
     *                       <ul>
     *                           <li>driver don't support this method</li>
     *                           <li>session close</li>
     *                           <li>cursor have closed</li>
     *                           <li>server response error</li>
     *                       </ul>
     */
    OrderedFlux prior();

    /*-------------------below next method-------------------*/

    /**
     * <p>
     * This method is equivalent to following :
     * <pre>
     *         <code><br/>
     *             // cursor is instance of RefCursor
     *             cursor.next(function,states->{}) ; // ignore ResultStates instance.
     *         </code>
     *     </pre>
     * </p>
     *
     * @see #next(Function, Consumer)
     */
    <T> Publisher<T> next(Function<CurrentRow, T> function);

    /**
     * <p>
     * Fetch the next row.
     * </p>
     *
     * @throws NullPointerException emit(not throw) when function is null or consumer is null.
     * @throws JdbdException        emit(not throw) when
     *                              <ul>
     *                                  <li>driver don't support this method</li>
     *                                  <li>session close</li>
     *                                  <li>cursor have closed</li>
     *                                  <li>server response error</li>
     *                              </ul>
     */
    <T> Publisher<T> next(Function<CurrentRow, T> function, Consumer<ResultStates> consumer);

    /**
     * <p>
     * Fetch the next row.
     * </p>
     *
     * @throws JdbdException emit(not throw) when
     *                       <ul>
     *                           <li>driver don't support this method</li>
     *                           <li>session close</li>
     *                           <li>cursor have closed</li>
     *                           <li>server response error</li>
     *                       </ul>
     */
    OrderedFlux next();

    /*-------------------below absolute method-------------------*/

    /**
     * <p>
     * This method is equivalent to following :
     * <pre>
     *         <code><br/>
     *             // cursor is instance of RefCursor
     *             cursor.absolute(count,function,states->{}) ; // ignore ResultStates instance.
     *         </code>
     *     </pre>
     * </p>
     *
     * @see #absolute(long, Function, Consumer)
     */
    <T> Publisher<T> absolute(long count, Function<CurrentRow, T> function);

    /**
     * <p>
     * Fetch the count'th row of the query, or the abs(count)'th row from the end if count is negative.<br/>
     * Position before first row or after last row if count is out of range; in particular,<br/>
     * ABSOLUTE 0 positions before the first row.
     * </p>
     *
     * @param count positive
     * @throws IllegalArgumentException emit(not throw) when count error.
     * @throws NullPointerException     emit(not throw) when function is null or consumer is null.
     * @throws JdbdException            emit(not throw) when
     *                                  <ul>
     *                                      <li>driver don't support this method</li>
     *                                      <li>session close</li>
     *                                      <li>cursor have closed</li>
     *                                      <li>server response error</li>
     *                                  </ul>
     */
    <T> Publisher<T> absolute(long count, Function<CurrentRow, T> function, Consumer<ResultStates> consumer);

    /**
     * <p>
     * Fetch the count'th row of the query, or the abs(count)'th row from the end if count is negative.<br/>
     * Position before first row or after last row if count is out of range; in particular,<br/>
     * ABSOLUTE 0 positions before the first row.
     * </p>
     *
     * @param count positive
     * @throws IllegalArgumentException emit(not throw) then count error.
     * @throws JdbdException            emit(not throw) when
     *                                  <ul>
     *                                      <li>driver don't support this method</li>
     *                                      <li>session close</li>
     *                                      <li>cursor have closed</li>
     *                                      <li>server response error</li>
     *                                  </ul>
     */
    OrderedFlux absolute(long count);

    /*-------------------below relative method-------------------*/

    /**
     * <p>
     * This method is equivalent to following :
     * <pre>
     *         <code><br/>
     *             // cursor is instance of RefCursor
     *             cursor.relative(count,function,states->{}) ; // ignore ResultStates instance.
     *         </code>
     *     </pre>
     * </p>
     *
     * @see #relative(long, Function, Consumer)
     */
    <T> Publisher<T> relative(long count, Function<CurrentRow, T> function);

    /**
     * <p>
     * Fetch the count'th succeeding row, or the abs(count)'th prior row if count is negative. <br/>
     * RELATIVE 0 re-fetches the current row, if any.
     * </p>
     *
     * @param count positive
     * @throws IllegalArgumentException emit(not throw) when count error.
     * @throws NullPointerException     emit(not throw) when function is null or consumer is null.
     * @throws JdbdException            emit(not throw) when
     *                                  <ul>
     *                                      <li>driver don't support this method</li>
     *                                      <li>session close</li>
     *                                      <li>cursor have closed</li>
     *                                      <li>server response error</li>
     *                                  </ul>
     */
    <T> Publisher<T> relative(long count, Function<CurrentRow, T> function, Consumer<ResultStates> consumer);

    /**
     * <p>
     * Fetch the count'th succeeding row, or the abs(count)'th prior row if count is negative. <br/>
     * RELATIVE 0 re-fetches the current row, if any.
     * </p>
     *
     * @param count positive
     * @throws IllegalArgumentException emit(not throw) then count error.
     * @throws JdbdException            emit(not throw) when
     *                                  <ul>
     *                                      <li>driver don't support this method</li>
     *                                      <li>session close</li>
     *                                      <li>cursor have closed</li>
     *                                      <li>server response error</li>
     *                                  </ul>
     */
    OrderedFlux relative(long count);

    /*-------------------below forward method-------------------*/

    /**
     * <p>
     * This method is equivalent to following :
     * <pre>
     *         <code><br/>
     *             // cursor is instance of RefCursor
     *             cursor.forward(count,function,states->{}) ; // ignore ResultStates instance.
     *         </code>
     *     </pre>
     * </p>
     *
     * @see #forward(long, Function, Consumer)
     */
    <T> Publisher<T> forward(long count, Function<CurrentRow, T> function);

    /**
     * <p>
     * Fetch the next count rows. FORWARD 0 re-fetches the current row.
     * </p>
     *
     * @param count positive
     * @throws IllegalArgumentException emit(not throw) when count error.
     * @throws NullPointerException     emit(not throw) when function is null or consumer is null.
     * @throws JdbdException            emit(not throw) when
     *                                  <ul>
     *                                      <li>driver don't support this method</li>
     *                                      <li>session close</li>
     *                                      <li>cursor have closed</li>
     *                                      <li>server response error</li>
     *                                  </ul>
     */
    <T> Publisher<T> forward(long count, Function<CurrentRow, T> function, Consumer<ResultStates> consumer);

    /**
     * <p>
     * Fetch the next count rows. FORWARD 0 re-fetches the current row.
     * </p>
     *
     * @param count positive
     * @throws IllegalArgumentException emit(not throw) then count error.
     * @throws JdbdException            emit(not throw) when
     *                                  <ul>
     *                                      <li>driver don't support this method</li>
     *                                      <li>session close</li>
     *                                      <li>cursor have closed</li>
     *                                      <li>server response error</li>
     *                                  </ul>
     */
    OrderedFlux forward(long count);

    /*-------------------below forwardAll method-------------------*/

    /**
     * <p>
     * This method is equivalent to following :
     * <pre>
     *         <code><br/>
     *             // cursor is instance of RefCursor
     *             cursor.forwardAll(function,states->{}) ; // ignore ResultStates instance.
     *         </code>
     *     </pre>
     * </p>
     *
     * @see #forwardAll(Function, Consumer)
     */
    <T> Publisher<T> forwardAll(Function<CurrentRow, T> function);

    /**
     * <p>
     * Fetch all remaining rows.
     * </p>
     *
     * @throws NullPointerException emit(not throw) when function is null or consumer is null.
     * @throws JdbdException        emit(not throw) when
     *                              <ul>
     *                                  <li>driver don't support this method</li>
     *                                  <li>session close</li>
     *                                  <li>cursor have closed</li>
     *                                  <li>server response error</li>
     *                              </ul>
     */
    <T> Publisher<T> forwardAll(Function<CurrentRow, T> function, Consumer<ResultStates> consumer);

    /**
     * <p>
     * Fetch all remaining rows.
     * </p>
     *
     * @throws JdbdException emit(not throw) when
     *                       <ul>
     *                           <li>driver don't support this method</li>
     *                           <li>session close</li>
     *                           <li>cursor have closed</li>
     *                           <li>server response error</li>
     *                       </ul>
     */
    OrderedFlux forwardAll();

    /*-------------------below backward method-------------------*/


    /**
     * <p>
     * This method is equivalent to following :
     * <pre>
     *         <code><br/>
     *             // cursor is instance of RefCursor
     *             cursor.backward(count,function,states->{}) ; // ignore ResultStates instance.
     *         </code>
     *     </pre>
     * </p>
     *
     * @see #backward(long, Function, Consumer)
     */
    <T> Publisher<T> backward(long count, Function<CurrentRow, T> function);

    /**
     * <p>
     * Fetch the prior count rows (scanning backwards). BACKWARD 0 re-fetches the current row.
     * </p>
     *
     * @param count positive
     * @throws IllegalArgumentException emit(not throw) when count error.
     * @throws NullPointerException     emit(not throw) when function is null or consumer is null.
     * @throws JdbdException            emit(not throw) when
     *                                  <ul>
     *                                      <li>driver don't support this method</li>
     *                                      <li>session close</li>
     *                                      <li>cursor have closed</li>
     *                                      <li>server response error</li>
     *                                  </ul>
     */
    <T> Publisher<T> backward(long count, Function<CurrentRow, T> function, Consumer<ResultStates> consumer);

    /**
     * <p>
     * Fetch the prior count rows (scanning backwards). BACKWARD 0 re-fetches the current row.
     * </p>
     *
     * @param count positive
     * @throws IllegalArgumentException emit(not throw) then count error.
     * @throws JdbdException            emit(not throw) when
     *                                  <ul>
     *                                      <li>driver don't support this method</li>
     *                                      <li>session close</li>
     *                                      <li>cursor have closed</li>
     *                                      <li>server response error</li>
     *                                  </ul>
     */
    OrderedFlux backward(long count);

    /*-------------------below backwardAll method-------------------*/

    /**
     * <p>
     * This method is equivalent to following :
     * <pre>
     *         <code><br/>
     *             // cursor is instance of RefCursor
     *             cursor.backwardAll(function,states->{}) ; // ignore ResultStates instance.
     *         </code>
     *     </pre>
     * </p>
     *
     * @see #backwardAll(Function, Consumer)
     */
    <T> Publisher<T> backwardAll(Function<CurrentRow, T> function);

    /**
     * <p>
     * Fetch all prior rows (scanning backwards).
     * </p>
     *
     * @throws NullPointerException emit(not throw) when function is null or consumer is null.
     * @throws JdbdException        emit(not throw) when
     *                              <ul>
     *                                  <li>driver don't support this method</li>
     *                                  <li>session close</li>
     *                                  <li>cursor have closed</li>
     *                                  <li>server response error</li>
     *                              </ul>
     */
    <T> Publisher<T> backwardAll(Function<CurrentRow, T> function, Consumer<ResultStates> consumer);

    /**
     * <p>
     * Fetch all prior rows (scanning backwards).
     * </p>
     *
     * @throws JdbdException emit(not throw) when
     *                       <ul>
     *                           <li>driver don't support this method</li>
     *                           <li>session close</li>
     *                           <li>cursor have closed</li>
     *                           <li>server response error</li>
     *                       </ul>
     */
    OrderedFlux backwardAll();

    /**
     * <p>
     * close cursor. <strong>NOTE</strong> :
     * <ul>
     *     <li>If cursor have closed,emit nothing</li>
     *     <li>If cursor don't need to close (eg : postgre - current transaction is aborted, commands ignored until end of transaction ),emit nothing</li>
     * </ul>
     * </p>
     *
     * @return the {@link Publisher} that emit nothing or emit {@link JdbdException}
     * @throws JdbdException emit(not throw) when
     *                       <ul>
     *                           <li>session close</li>
     *                           <li>cursor have closed</li>
     *                           <li>server response error</li>
     *                       </ul>
     */
    @Override
    <T> Publisher<T> close();


    /**
     * override {@link Object#toString()}
     *
     * @return RefCursor info, contain : <ol>
     * <li>class name</li>
     * <li>{@link #name()}</li>
     * <li>column index if exists</li>
     * <li>column label if exists</li>
     * <li>{@link System#identityHashCode(Object)}</li>
     * </ol>
     */
    @Override
    String toString();


}
