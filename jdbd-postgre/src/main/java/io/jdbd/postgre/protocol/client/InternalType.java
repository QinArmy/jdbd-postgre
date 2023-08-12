package io.jdbd.postgre.protocol.client;


import io.jdbd.meta.JdbdType;
import io.jdbd.meta.SQLType;
import io.jdbd.postgre.PgDriver;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.CurrentRow;
import reactor.core.publisher.Flux;

import java.util.Locale;

/**
 * <p>
 * This class representing postgre internal data type. For example regclass,xid,cid,xid8 .
 * </p>
 *
 * @see <a href="https://www.postgresql.org/docs/current/datatype-oid.html">Object Identifier Types</a>
 * @see <a href="https://www.postgresql.org/docs/current/catalog-pg-type.html">pg_type</a>
 * @since 1.0
 */
final class InternalType implements SQLType {


    private static final String INTERNAL_TYPES_SQL = "SELECT t.oid, t.typname, t.typcategory, t.typtype, t.typarray, pn.nspname\n" +
            "FROM pg_namespace pn\n" +
            "         JOIN pg_type AS t ON t.typnamespace = pn.oid\n" +
            "WHERE pn.nspname IN ('pg_catalog', 'pg_toast', 'information_schema')\n" +
            "  AND t.typcategory = 'U'\n" +
            "  AND t.typname NOT IN\n" +
            "      ('bytea', 'json', 'xml', 'xid8', 'macaddr', 'macaddr8', 'refcursor', 'uuid', 'tsvector', 'tsquery', 'jsonb',\n" +
            "       'jsonpath')";


    /**
     * @param builder just append sql,don't append semicolons {@code ; } .
     */
    static void appendInternalQuery(StringBuilder builder) {
        builder.append(INTERNAL_TYPES_SQL);
    }


    /**
     * @see #INTERNAL_TYPES_SQL
     */
    static Flux<InternalType> from(final CurrentRow row) {
        if (!"U".equals(row.get(2))) {
            // avoid bug
            return Flux.empty();
        }
        final InternalType[] types = new InternalType[2];
        types[0] = new InternalType(row); // create base type
        types[1] = new InternalType(row.getNonNull(3, Integer.class), types[0]); // create array type of base type.
        return Flux.fromArray(types);
    }


    final int oid;

    final String typeName;

    final TypeCategory category;

    final InternalType elementType;

    /**
     * @see #INTERNAL_TYPES_SQL
     */
    private InternalType(final CurrentRow row) {
        this.oid = row.getNonNull(0, Integer.class);
        this.typeName = row.getNonNull(1, String.class);
        this.category = TypeCategory.from(row.getNonNull(2, String.class));
        this.elementType = null;
    }

    private InternalType(int arrayOid, InternalType elementTypeOfArray) {
        this.oid = arrayOid;
        this.typeName = elementTypeOfArray.typeName.toUpperCase(Locale.ROOT) + "[]";
        this.category = TypeCategory.ARRAY;
        this.elementType = elementTypeOfArray;
    }


    @Override
    public String name() {
        return this.typeName();
    }

    @Override
    public String typeName() {
        return this.typeName;
    }

    @Override
    public boolean isArray() {
        return this.category == TypeCategory.ARRAY;
    }

    @Override
    public boolean isUnknown() {
        return this.category == TypeCategory.UNKNOWN;
    }

    @Override
    public boolean isUserDefined() {
        // false , this class is internal-use
        return false;
    }

    @Override
    public JdbdType jdbdType() {
        final JdbdType jdbdType;
        switch (this.category) {
            case ENUM:
                jdbdType = JdbdType.ENUM;
                break;
            case COMPOSITE:
                jdbdType = JdbdType.COMPOSITE;
                break;
            case BOOLEAN:
                jdbdType = JdbdType.BOOLEAN;
                break;
            case ARRAY:
                jdbdType = JdbdType.ARRAY;
                break;
            case UNKNOWN:
                jdbdType = JdbdType.UNKNOWN;
                break;
            case GEOMETRIC:
            case DATE_OR_TIME:
            case USER_DEFINED:
            case NUMERIC:// number type
            case RANGE:
            case PSEUDO:
            case TIMESPAN:
            case STRING:
            case BIT_STRING:
            case INTERNAL_USE:
            case NETWORK_ADDRESS:
            default:
                jdbdType = JdbdType.DIALECT_TYPE;
        }
        return jdbdType;
    }

    @Override
    public Class<?> firstJavaType() {
        return String.class;
    }

    @Override
    public Class<?> secondJavaType() {
        return null;
    }

    @Override
    public SQLType elementType() {
        return this.elementType;
    }

    @Override
    public String vendor() {
        return PgDriver.PG_DRIVER_VENDOR;
    }

    @Override
    public String toString() {
        return PgStrings.builder()
                .append(InternalType.class.getSimpleName())
                .append("[ name : ")
                .append(this.name())
                .append(" , typeName : ")
                .append(this.typeName)
                .append(" , oid : ")
                .append(this.oid)
                .append(" , category : ")
                .append(this.category.name())
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }


}
