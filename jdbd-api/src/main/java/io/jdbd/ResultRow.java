package io.jdbd;

import reactor.util.annotation.Nullable;

import java.time.ZoneId;
import java.time.temporal.Temporal;


public interface ResultRow {

    @Nullable
    Object getObject(int indexBaseZero) throws ReactiveSQLException;

    @Nullable
    <T> T getObject(int indexBaseZero, Class<T> columnClass) throws ReactiveSQLException;

    @Nullable
    Object getObject(String alias) throws ReactiveSQLException;

    @Nullable
    <T> T getObject(String alias, Class<T> columnClass) throws ReactiveSQLException;

    /**
     * <p>
     * return date time object with zone ,but precondition:{@link #getObject(int)} return below one of
     * <ul>
     * <li>{@link java.time.LocalDateTime}</li>
     * <li>{@link java.time.LocalTime}</li>
     * <li> {@link java.time.ZonedDateTime} </li>
     * <li> {@link java.time.OffsetDateTime} </li>
     * <li> {@link java.time.OffsetTime} </li>
     * </ul>
     * </p>
     *
     * @param indexBaseZero base 1,the first column is 1, the second is 2, ...
     * @param targetClass   result java class
     * @param targetZoneId  target zoneId
     * @param <T>           result java type
     * @return blow one of
     * <ul>
     *     <li>{@link java.time.ZonedDateTime}, {@link #getObject(int)} with date</li>
     *     <li>{@link java.time.OffsetDateTime}, {@link #getObject(int)} with date</li>
     *     <li>{@link java.time.LocalDateTime}, {@link #getObject(int)} with date</li>
     *     <li>{@link java.time.LocalDate}, {@link #getObject(int)} with date</li>
     *     <li>{@link java.time.OffsetTime}</li>
     *     <li>{@link java.time.LocalTime}</li>
     *     <li>{@link java.time.Year}, {@link #getObject(int)} with date</li>
     *     <li>{@link java.time.YearMonth, {@link #getObject(int)} with date}</li>
     *     <li>{@link java.time.Instant}</li>
     * </ul>
     * @throws ReactiveSQLException if a database access error occurs
     */
    @Nullable
    <T extends Temporal> T getObject(int indexBaseZero, Class<T> targetClass, ZoneId targetZoneId) throws ReactiveSQLException;

    /**
     * <p>
     * return date time object with zone ,but precondition:{@link #getObject(String)} return below one of
     * <ul>
     * <li>{@link java.time.LocalDateTime}</li>
     * <li>{@link java.time.LocalTime}</li>
     * <li> {@link java.time.ZonedDateTime} </li>
     * <li> {@link java.time.OffsetDateTime} </li>
     * <li> {@link java.time.OffsetTime} </li>
     * </ul>
     * </p>
     *
     * @param alias        base 1,the first column is 1, the second is 2, ...
     * @param targetClass  result java class
     * @param targetZoneId target zone id
     * @param <T>          result java type
     * @return blow one of
     * <ul>
     *     <li>{@link java.time.ZonedDateTime}, {@link #getObject(int)} with date</li>
     *     <li>{@link java.time.OffsetDateTime}, {@link #getObject(int)} with date</li>
     *     <li>{@link java.time.LocalDateTime}, {@link #getObject(int)} with date</li>
     *     <li>{@link java.time.LocalDate}, {@link #getObject(int)} with date</li>
     *     <li>{@link java.time.OffsetTime}</li>
     *     <li>{@link java.time.LocalTime}</li>
     *     <li>{@link java.time.Year}, {@link #getObject(int)} with date</li>
     *     <li>{@link java.time.YearMonth, {@link #getObject(int)} with date}</li>
     *     <li>{@link java.time.Instant}</li>
     * </ul>
     * @throws ReactiveSQLException if a database access error occurs
     */
    @Nullable
    <T extends Temporal> T getObject(String alias, Class<T> targetClass, ZoneId targetZoneId) throws ReactiveSQLException;

}
