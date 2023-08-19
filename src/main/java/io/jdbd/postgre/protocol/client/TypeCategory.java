package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.util.PgStrings;


/**
 * <p>
 * This enum representing postgre typcategory
 * </p>
 *
 * @see <a href="https://www.postgresql.org/docs/current/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE">typcategory Codes</a>
 */
enum TypeCategory {

    ARRAY,

    BOOLEAN,

    COMPOSITE,

    DATE_OR_TIME,

    ENUM,

    GEOMETRIC,

    NETWORK_ADDRESS,

    NUMERIC,

    PSEUDO,

    RANGE,

    STRING,

    TIMESPAN,

    USER_DEFINED,

    BIT_STRING,

    INTERNAL_USE,

    UNKNOWN;


    @Override
    public final String toString() {
        return PgStrings.builder()
                .append(TypeCategory.class.getSimpleName())
                .append('.')
                .append(this.name())
                .toString();
    }

    static TypeCategory from(final String code) {
        final TypeCategory category;
        switch (code) {
            case "A":
                category = ARRAY;
                break;
            case "B":
                category = BOOLEAN;
                break;
            case "C":
                category = COMPOSITE;
                break;
            case "D":
                category = DATE_OR_TIME;
                break;

            case "E":
                category = ENUM;
                break;
            case "G":
                category = GEOMETRIC;
                break;
            case "I":
                category = NETWORK_ADDRESS;
                break;
            case "N":
                category = NUMERIC;
                break;

            case "P":
                category = PSEUDO;
                break;
            case "R":
                category = RANGE;
                break;
            case "S":
                category = STRING;
                break;
            case "T":
                category = TIMESPAN;
                break;

            case "U":
                category = USER_DEFINED;
                break;
            case "V":
                category = BIT_STRING;
                break;
            case "X":
                category = UNKNOWN;
                break;
            case "Z":
            default:
                category = INTERNAL_USE;

        }
        return category;
    }


}
