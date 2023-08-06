package io.jdbd.postgre;

public interface PgConstant {

    String NULL = "NULL";

    String TRUE = "TRUE";

    String FALSE = "FALSE";

    String INFINITY = "infinity";

    String NEG_INFINITY = "-infinity";

    String NaN = "NaN";

    String SPACE_SEMICOLON_SPACE = " ; ";

    byte BACK_SLASH_BYTE = '\\';

    char NUL = '\0';

    char SPACE = ' ';

    char QUOTE = '\'';

    char COMMA = ',';

    char SEMICOLON = ';';

    char LEFT_BRACE = '{';

    char RIGHT_BRACE = '}';

    char DOUBLE_QUOTE = '"';

    char BACK_SLASH = '\\';

    byte TYPE_UNSPECIFIED = 0;
    byte TYPE_INT2 = 21;
    short TYPE_INT2_ARRAY = 1005;
    byte TYPE_INT4 = 23;

    short TYPE_INT4_ARRAY = 1007;
    byte TYPE_INT8 = 20;
    short TYPE_INT8_ARRAY = 1016;
    byte TYPE_TEXT = 25;

    short TYPE_TEXT_ARRAY = 1009;
    short TYPE_NUMERIC = 1700;
    short TYPE_NUMERIC_ARRAY = 1231;
    short TYPE_FLOAT4 = 700;

    short TYPE_FLOAT4_ARRAY = 1021;
    short TYPE_FLOAT8 = 701;
    short TYPE_FLOAT8_ARRAY = 1022;
    byte TYPE_BOOLEAN = 16;

    short TYPE_BOOLEAN_ARRAY = 1000;
    short TYPE_DATE = 1082;
    short TYPE_DATE_ARRAY = 1182;
    short TYPE_TIME = 1083;

    short TYPE_TIME_ARRAY = 1183;
    short TYPE_TIMETZ = 1266;
    short TYPE_TIMETZ_ARRAY = 1270;
    short TYPE_TIMESTAMP = 1114;

    short TYPE_TIMESTAMP_ARRAY = 1115;
    short TYPE_TIMESTAMPTZ = 1184;
    short TYPE_TIMESTAMPTZ_ARRAY = 1185;
    byte TYPE_BYTEA = 17;

    short TYPE_BYTEA_ARRAY = 1001;
    short TYPE_VARCHAR = 1043;
    short TYPE_VARCHAR_ARRAY = 1015;
    byte TYPE_OID = 26;

    short TYPE_INT4RANGE = 3904;

    short TYPE_INT8RANGE = 3926;

    short TYPE_NUMRANGE = 3906;

    short TYPE_DATERANGE = 3912;
    short TYPE_TSRANGE = 3908;
    short TYPE_TSTZRANGE = 3910;

    short TYPE_INT4MULTIRANGE = 4451;
    short TYPE_INT8MULTIRANGE = 4536;

    short TYPE_NUMMULTIRANGE = 4532;

    short TYPE_DATEMULTIRANGE = 4535;

    short TYPE_TSMULTIRANGE = 4533;

    short TYPE_TSTZMULTIRANGE = 4534;


    short TYPE_OID_ARRAY = 1028;
    short TYPE_BPCHAR = 1042;// “blank-padded char”, the internal name of the character data type
    short TYPE_BPCHAR_ARRAY = 1014;
    short TYPE_MONEY = 790;

    short TYPE_MONEY_ARRAY = 791;
    byte TYPE_NAME = 19;
    short TYPE_NAME_ARRAY = 1003;
    short TYPE_BIT = 1560;

    short TYPE_BIT_ARRAY = 1561;
    short TYPE_VOID = 2278; // maybe function out parameter
    short TYPE_INTERVAL = 1186;
    short TYPE_INTERVAL_ARRAY = 1187;

    byte TYPE_CHAR = 18; // This is not char(N), this is "char" a single byte type.
    short TYPE_CHAR_ARRAY = 1002;
    short TYPE_VARBIT = 1562;
    short TYPE_VARBIT_ARRAY = 1563;

    short TYPE_UUID = 2950;
    short TYPE_UUID_ARRAY = 2951;
    short TYPE_XML = 142;
    short TYPE_XML_ARRAY = 143;

    short TYPE_POINT = 600;
    short TYPE_POINT_ARRAY = 1017;
    short TYPE_LINE = 628;//Values of type line are output in the following form : { A, B, C } ,so can't convert to WKB,not support now.
    short TYPE_LSEG = 601;

    short TYPE_PATH = 602;
    short TYPE_POLYGON = 604;
    short TYPE_CIRCLE = 718;
    short TYPE_BOX = 603;

    short TYPE_JSONB = 3802;

    short TYPE_JSONPATH = 4072;

    short TYPE_JSONPATH_ARRAY = 4073;
    short TYPE_JSONB_ARRAY = 3807;
    short TYPE_JSON = 114;
    short TYPE_JSON_ARRAY = 199;

    short TYPE_REF_CURSOR = 1790;
    short TYPE_REF_CURSOR_ARRAY = 2201;
    short TYPE_MAC_ADDR = 829;
    short TYPE_MAC_ADDR8 = 774;

    short TYPE_INET = 869;
    short TYPE_CIDR = 650;
    short TYPE_TSVECTOR = 3614;
    short TYPE_TSVECTOR_ARRAY = 3643;

    short TYPE_TSQUERY = 3615;
    short TYPE_TSQUERY_ARRAY = 3645;
    short TYPE_LINE_ARRAY = 629;
    short TYPE_LINE_LSEG_ARRAY = 1018;

    short TYPE_BOX_ARRAY = 1020;
    short TYPE_PATH_ARRAY = 1019;
    short TYPE_POLYGON_ARRAY = 1027;
    short TYPE_CIRCLES_ARRAY = 719;

    short TYPE_CIDR_ARRAY = 651;
    short TYPE_INET_ARRAY = 1041;
    short TYPE_MACADDR_ARRAY = 1040;
    short TYPE_MACADDR8_ARRAY = 775;


    short TYPE_INT4MULTIRANGE_ARRAY = 6150;

    short TYPE_INT8MULTIRANGE_ARRAY = 6157;

    short TYPE_NUMMULTIRANGE_ARRAY = 6151;

    short TYPE_DATEMULTIRANGE_ARRAY = 6155;

    short TYPE_TSMULTIRANGE_ARRAY = 6152;

    short TYPE_TSTZMULTIRANGE_ARRAY = 6153;


    short TYPE_INT4RANGE_ARRAY = 3905;
    short TYPE_TSRANGE_ARRAY = 3909;
    short TYPE_TSTZRANGE_ARRAY = 3911;

    short TYPE_DATERANGE_ARRAY = 3913;
    short TYPE_INT8RANGE_ARRAY = 3927;

    short TYPE_NUMRANGE_ARRAY = 3907;


}
