package io.jdbd.postgre.protocol.client;


import io.jdbd.meta.JdbdType;
import io.jdbd.meta.SQLType;
import io.jdbd.postgre.PgDriver;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.CurrentRow;

import java.util.Locale;
import java.util.Map;

/**
 * <p>
 * This class representing postgre internal data type. For example regclass,xid,cid,xid8 .
 * </p>
 * <p>
 * Query sql like following :
 * <pre>
 *     <code><br/>
 *       select t.oid, t.typname,t.typcategory,t.typarray, pn.nspname
 *       from pg_namespace pn
 *                join pg_type as t on t.typnamespace = pn.oid
 *       where pn.nspname  in ('pg_catalog', 'pg_toast', 'information_schema')
 *       and t.typname in ('bool','_bool')
 *     </code>
 * </pre>
 * </p>
 *
 * @see <a href="https://www.postgresql.org/docs/current/datatype-oid.html">Object Identifier Types</a>
 * @see <a href="https://www.postgresql.org/docs/current/catalog-pg-type.html">pg_type</a>
 * @since 1.0
 */
final class InternalType implements SQLType {

    static final InternalType XID = new InternalType(28, "XID", TypeCategory.USER_DEFINED);

    static final InternalType XID_ARRAY = new InternalType(1011, XID);


    static final InternalType TID = new InternalType(27, "tid", TypeCategory.USER_DEFINED);

    static final InternalType TID_ARRAY = new InternalType(1010, TID);

    static final InternalType CID = new InternalType(27, "cid", TypeCategory.USER_DEFINED);

    static final InternalType CID_ARRAY = new InternalType(1010, TID);


    private static Map<Integer, InternalType> createKnownInternalTypeMap() {
        final Map<Integer, InternalType> map = PgCollections.hashMap();
        InternalType type;

        type = new InternalType(27, "tid", TypeCategory.USER_DEFINED);
        map.put(type.oid, type);
        map.put(1011, new InternalType(1011, type));

        type = new InternalType(28, "xid", TypeCategory.USER_DEFINED);
        map.put(type.oid, type);
        map.put(1011, new InternalType(1011, type));

        type = new InternalType(29, "cid", TypeCategory.USER_DEFINED);
        map.put(type.oid, type);
        map.put(1012, new InternalType(1012, type));

        type = new InternalType(5069, "xid8", TypeCategory.USER_DEFINED);
        map.put(type.oid, type);
        map.put(271, new InternalType(271, type));

        return PgCollections.unmodifiableMap(map);
    }


    final int oid;

    final String typeName;

    final TypeCategory category;

    final InternalType elementType;

    private InternalType(final CurrentRow row) {
        this.oid = row.getNonNull(0, Integer.class);
        this.typeName = row.getNonNull(1, String.class);
        this.category = TypeCategory.from(row.getNonNull(2, String.class));
        this.elementType = null;
    }


    private InternalType(int oid, String typeName, TypeCategory category) {
        this.oid = oid;
        this.typeName = typeName;
        this.category = category;
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
