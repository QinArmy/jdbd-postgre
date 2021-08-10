package io.jdbd.postgre;

public interface PgConstant {

    int TYPE_UNSPECIFIED = 0;
    int TYPE_INT2 = 21;
    int TYPE_INT2_ARRAY = 1005;
    int TYPE_INT4 = 23;

    int TYPE_INT4_ARRAY = 1007;
    int TYPE_INT8 = 20;
    int TYPE_INT8_ARRAY = 1016;
    int TYPE_TEXT = 25;

    int TYPE_TEXT_ARRAY = 1009;
    int TYPE_NUMERIC = 1700;
    int TYPE_NUMERIC_ARRAY = 1231;
    int TYPE_FLOAT4 = 700;

    int TYPE_FLOAT4_ARRAY = 1021;
    int TYPE_FLOAT8 = 701;
    int TYPE_FLOAT8_ARRAY = 1022;
    int TYPE_BOOLEAN = 16;

    int TYPE_BOOL_ARRAY = 1000;
    int TYPE_DATE = 1082;
    int TYPE_DATE_ARRAY = 1182;
    int TYPE_TIME = 1083;

    int TYPE_TIME_ARRAY = 1183;
    int TYPE_TIMETZ = 1266;
    int TYPE_TIMETZ_ARRAY = 1270;
    int TYPE_TIMESTAMP = 1114;

    int TYPE_TIMESTAMP_ARRAY = 1115;
    int TYPE_TIMESTAMPTZ = 1184;
    int TYPE_TIMESTAMPTZ_ARRAY = 1185;
    int TYPE_BYTEA = 17;

    int TYPE_BYTEA_ARRAY = 1001;
    int TYPE_VARCHAR = 1043;
    int TYPE_VARCHAR_ARRAY = 1015;
    int TYPE_OID = 26;

    int TYPE_OID_ARRAY = 1028;
    int TYPE_BPCHAR = 1042;
    int TYPE_BPCHAR_ARRAY = 1014;
    int TYPE_MONEY = 790;

    int TYPE_MONEY_ARRAY = 791;
    int TYPE_NAME = 19;
    int TYPE_NAME_ARRAY = 1003;
    int TYPE_BIT = 1560;

    int TYPE_BIT_ARRAY = 1561;
    int TYPE_VOID = 2278;
    int TYPE_INTERVAL = 1186;
    int TYPE_INTERVAL_ARRAY = 1187;

    int TYPE_CHAR = 18; // This is not char(N), this is "char" a single byte type.
    int TYPE_CHAR_ARRAY = 1002;
    int TYPE_VARBIT = 1562;
    int TYPE_VARBIT_ARRAY = 1563;

    int TYPE_UUID = 2950;
    int TYPE_UUID_ARRAY = 2951;
    int TYPE_XML = 142;
    int TYPE_XML_ARRAY = 143;

    int TYPE_POINT = 600;
    int TYPE_POINT_ARRAY = 1017;
    int TYPE_BOX = 603;
    int TYPE_JSONB = 3802;

    int TYPE_JSONB_ARRAY = 3807;
    int TYPE_JSON = 114;
    int TYPE_JSON_ARRAY = 199;
    int TYPE_REF_CURSOR = 1790;

    int TYPE_REF_CURSOR_ARRAY = 2201;


}
