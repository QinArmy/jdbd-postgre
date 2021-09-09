package io.jdbd.postgre.session;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.meta.SQLType;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BindMultiStmt;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.stmt.PgStmts;
import io.jdbd.postgre.util.PgBinds;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultStates;
import io.jdbd.result.SafePublisher;
import io.jdbd.stmt.MultiStatement;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * <p>
 * This class is a implementation of {@link MultiStatement} with postgre client protocol.
 * </p>
 *
 * @see PgDatabaseSession#multi()
 */
final class PgMultiStatement extends PgStatement implements MultiStatement {

    /**
     * @see PgDatabaseSession#multi()
     */
    static PgMultiStatement create(PgDatabaseSession session) {
        return new PgMultiStatement(session);
    }

    private final List<BindStmt> stmtGroup = new ArrayList<>();

    private String currentSql;

    private List<BindValue> paramGroup;

    private PgMultiStatement(PgDatabaseSession session) {
        super(session);
    }

    @Override
    public final void addStatement(final String sql) {
        if (!PgStrings.hasText(sql)) {
            throw new IllegalArgumentException("Sql must have text.");
        }
        final String currentSql = this.currentSql;
        if (currentSql != null) {
            final List<BindValue> paramGroup = Objects.requireNonNull(this.paramGroup, "this.paramGroup");
            this.stmtGroup.add(PgStmts.bind(currentSql, paramGroup, this));
        } else if (!this.stmtGroup.isEmpty()) {
            throw PgExceptions.cannotReuseStatement(MultiStatement.class);
        }
        this.currentSql = sql;
        this.paramGroup = new ArrayList<>();
    }

    @Override
    public final void bind(final int indexBasedZero, final JDBCType jdbcType, final @Nullable Object nullable)
            throws JdbdException {
        final List<BindValue> paramGroup = this.paramGroup;
        if (paramGroup == null) {
            throw PgExceptions.multiStmtNoSql();
        }
        final PgType pgType = PgBinds.mapJdbcTypeToPgType(jdbcType, nullable);
        paramGroup.add(BindValue.wrap(checkIndex(indexBasedZero), pgType, nullable));
    }

    @Override
    public final void bind(final int indexBasedZero, final SQLType sqlType, final @Nullable Object nullable)
            throws JdbdException {
        final List<BindValue> paramGroup = this.paramGroup;
        if (paramGroup == null) {
            throw PgExceptions.multiStmtNoSql();
        }
        paramGroup.add(BindValue.wrap(checkIndex(indexBasedZero), checkSqlType(sqlType), nullable));
    }

    @Override
    public final void bind(final int indexBasedZero, final @Nullable Object nullable)
            throws JdbdException {
        final List<BindValue> paramGroup = this.paramGroup;
        if (paramGroup == null) {
            throw PgExceptions.multiStmtNoSql();
        }
        paramGroup.add(BindValue.wrap(checkIndex(indexBasedZero), PgBinds.inferPgType(nullable), nullable));
    }

    @Override
    public final Flux<ResultStates> executeBatch() {
        return this.session.protocol.multiStmtBatch(createMultiBindStmt());
    }

    @Override
    public final MultiResult executeBatchAsMulti() {
        return this.session.protocol.multiStmtAsMulti(createMultiBindStmt());
    }

    @Override
    public final SafePublisher executeBatchAsFlux() {
        return this.session.protocol.multiStmtAsFlux(createMultiBindStmt());
    }

    @Override
    public final boolean setFetchSize(int fetchSize) {
        return false;
    }

    @Override
    public final boolean supportPublisher() {
        return false;
    }

    @Override
    public final int getFetchSize() {
        return 0;
    }

    /*################################## blow private method ##################################*/


    /**
     * @throws JdbdSQLException when indexBasedZero error
     */
    private int checkIndex(final int indexBasedZero) throws JdbdSQLException {
        if (indexBasedZero < 0) {
            throw PgExceptions.invalidParameterValue(this.stmtGroup.size(), indexBasedZero);
        }
        return indexBasedZero;
    }

    /**
     * @see #executeBatch()
     * @see #executeBatchAsMulti()
     * @see #executeBatchAsFlux()
     */
    private BindMultiStmt createMultiBindStmt() throws JdbdException {
        final String currentSql = this.currentSql;
        final List<BindStmt> stmtGroup = this.stmtGroup;

        if (currentSql == null) {
            if (stmtGroup.isEmpty()) {
                throw PgExceptions.multiStmtNoSql();
            } else {
                throw PgExceptions.cannotReuseStatement(MultiStatement.class);
            }
        }

        final List<BindValue> paramGroup = Objects.requireNonNull(this.paramGroup, "this.paramGroup");
        stmtGroup.add(PgStmts.bind(currentSql, paramGroup, this));

        this.currentSql = null;
        this.paramGroup = null;

        return PgStmts.multi(stmtGroup, this);

    }


}
