package io.jdbd.vendor.util;

import io.jdbd.type.CodeEnum;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

/**
 * This class design for geometry sql type test.
 *
 * @see <a href="https://www.ogc.org/standards/sfa">Simple Feature Access - Part 1: Common Architecture PDF</a>
 */
public enum WkbType implements CodeEnum {

    GEOMETRY(Constant.GEOMETRY, "GEOMETRY") {
        @Override
        public WkbType elementType() {
            return null;
        }
    },
    POINT(Constant.POINT, "POINT") {
        @Override
        public WkbType elementType() {
            return null;
        }
    },
    LINE_STRING(Constant.LINE_STRING, "LINESTRING") {
        @Override
        public WkbType elementType() {
            return POINT;
        }
    },
    POLYGON(Constant.POLYGON, "POLYGON") {
        @Override
        public WkbType elementType() {
            return LINE_STRING;
        }
    },

    MULTI_POINT(Constant.MULTI_POINT, "MULTIPOINT") {
        @Override
        public WkbType elementType() {
            return POINT;
        }
    },
    MULTI_LINE_STRING(Constant.MULTI_LINE_STRING, "MULTILINESTRING") {
        @Override
        public WkbType elementType() {
            return LINE_STRING;
        }
    },
    MULTI_POLYGON(Constant.MULTI_POLYGON, "MULTIPOLYGON") {
        @Override
        public WkbType elementType() {
            return POLYGON;
        }
    },
    GEOMETRY_COLLECTION(Constant.GEOMETRY_COLLECTION, "GEOMETRYCOLLECTION") {
        @Override
        public WkbType elementType() {
            return GEOMETRY;
        }
    },

    CIRCULAR_STRING(Constant.CIRCULAR_STRING, "CIRCULARSTRING") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    }, // future use
    COMPOUND_CURVE(Constant.COMPOUND_CURVE, "COMPOUNDCURVE") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    }, // future use
    CURVE_POLYGON(Constant.CURVE_POLYGON, "CURVEPOLYGON") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    }, // future use
    MULTI_CURVE(Constant.MULTI_CURVE, "MULTICURVE") {
        @Override
        public WkbType elementType() {
            return CURVE;
        }
    },

    MULTI_SURFACE(Constant.MULTI_SURFACE, "MULTISURFACE") {
        @Override
        public WkbType elementType() {
            return SURFACE;
        }
    },
    CURVE(Constant.CURVE, "CURVE") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    },
    SURFACE(Constant.SURFACE, "SURFACE") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    },
    POLYHEDRAL_SURFACE(Constant.POLYHEDRAL_SURFACE, "POLYHEDRALSURFACE") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    },

    TIN(Constant.TIN, "TIN") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    },
    TRIANGLE(Constant.TRIANGLE, "TRIANGLE") {
        @Override
        public WkbType elementType() {
            return LINE_STRING;
        }
    },


    GEOMETRY_Z(Constant.GEOMETRY_Z, "GEOMETRY Z") {
        @Override
        public WkbType elementType() {
            return null;
        }
    },
    POINT_Z(Constant.POINT_Z, "POINT Z") {
        @Override
        public WkbType elementType() {
            return null;
        }
    },
    LINE_STRING_Z(Constant.LINE_STRING_Z, "LINESTRING Z") {
        @Override
        public WkbType elementType() {
            return POINT_Z;
        }
    },
    POLYGON_Z(Constant.POLYGON_Z, "POLYGON Z") {
        @Override
        public WkbType elementType() {
            return LINE_STRING_Z;
        }
    },

    MULTI_POINT_Z(Constant.MULTI_POINT_Z, "MULTIPOINT Z") {
        @Override
        public WkbType elementType() {
            return POINT_Z;
        }
    },
    MULTI_LINE_STRING_Z(Constant.MULTI_LINE_STRING_Z, "MULTILINESTRING Z") {
        @Override
        public WkbType elementType() {
            return LINE_STRING_Z;
        }
    },
    MULTI_POLYGON_Z(Constant.MULTI_POLYGON_Z, "MULTIPOLYGON Z") {
        @Override
        public WkbType elementType() {
            return POLYGON_Z;
        }
    },
    GEOMETRY_COLLECTION_Z(Constant.GEOMETRY_COLLECTION_Z, "GEOMETRYCOLLECTION Z") {
        @Override
        public WkbType elementType() {
            return GEOMETRY_Z;
        }
    },

    CIRCULAR_STRING_Z(Constant.CIRCULAR_STRING_Z, "CIRCULARSTRING Z") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    }, // future use
    COMPOUND_CURVE_Z(Constant.COMPOUND_CURVE_Z, "COMPOUNDCURVE Z") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    }, // future use
    CURVE_POLYGON_Z(Constant.CURVE_POLYGON_Z, "CURVEPOLYGON Z") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    }, // future use
    MULTI_CURVE_Z(Constant.MULTI_CURVE_Z, "MULTICURVE Z") {
        @Override
        public WkbType elementType() {
            return CURVE_Z;
        }
    },
    MULTI_SURFACE_Z(Constant.MULTI_SURFACE_Z, "MULTISURFACE Z") {
        @Override
        public WkbType elementType() {
            return SURFACE_Z;
        }
    },
    CURVE_Z(Constant.CURVE_Z, "CURVE Z") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    },
    SURFACE_Z(Constant.SURFACE_Z, "SURFACE Z") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    },
    POLYHEDRAL_SURFACE_Z(Constant.POLYHEDRAL_SURFACE_Z, "POLYHEDRALSURFACE Z") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    },

    TIN_Z(Constant.TIN_Z, "TIN Z") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    },
    TRIANGLE_Z(Constant.TRIANGLE_Z, "TRIANGLE Z") {
        @Override
        public WkbType elementType() {
            return LINE_STRING_Z;
        }
    },

    /*################################## blow M Coordinate  ##################################*/

    GEOMETRY_M(Constant.GEOMETRY_M, "GEOMETRY M") {
        @Override
        public WkbType elementType() {
            return null;
        }
    },
    POINT_M(Constant.POINT_M, "POINT M") {
        @Override
        public WkbType elementType() {
            return null;
        }
    },
    LINE_STRING_M(Constant.LINE_STRING_M, "LINESTRING M") {
        @Override
        public WkbType elementType() {
            return POINT_M;
        }
    },
    POLYGON_M(Constant.POLYGON_M, "POLYGON M") {
        @Override
        public WkbType elementType() {
            return LINE_STRING_M;
        }
    },

    MULTI_POINT_M(Constant.MULTI_POINT_M, "MULTIPOINT M") {
        @Override
        public WkbType elementType() {
            return POINT_M;
        }
    },
    MULTI_LINE_STRING_M(Constant.MULTI_LINE_STRING_M, "MULTILINESTRING M") {
        @Override
        public WkbType elementType() {
            return LINE_STRING_M;
        }
    },
    MULTI_POLYGON_M(Constant.MULTI_POLYGON_M, "MULTIPOLYGON M") {
        @Override
        public WkbType elementType() {
            return POLYGON_M;
        }
    },
    GEOMETRY_COLLECTION_M(Constant.GEOMETRY_COLLECTION_M, "GEOMETRYCOLLECTION M") {
        @Override
        public WkbType elementType() {
            return GEOMETRY_M;
        }
    },

    CIRCULAR_STRING_M(Constant.CIRCULAR_STRING_M, "CIRCULARSTRING M") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    }, // future use
    COMPOUND_CURVE_M(Constant.COMPOUND_CURVE_M, "COMPOUNDCURVE M") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    }, // future use
    CURVE_POLYGON_M(Constant.CURVE_POLYGON_M, "CURVEPOLYGON M") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    }, // future use
    MULTI_CURVE_M(Constant.MULTI_CURVE_M, "MULTICURVE M") {
        @Override
        public WkbType elementType() {
            return CURVE_M;
        }
    },
    MULTI_SURFACE_M(Constant.MULTI_SURFACE_M, "MULTISURFACE M") {
        @Override
        public WkbType elementType() {
            return SURFACE_M;
        }
    },
    CURVE_M(Constant.CURVE_M, "CURVE M") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    },
    SURFACE_M(Constant.SURFACE_M, "SURFACE M") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    },
    POLYHEDRAL_SURFACE_M(Constant.POLYHEDRAL_SURFACE_M, "POLYHEDRALSURFACE M") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    },

    TIN_M(Constant.TIN_M, "TIN M") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    },
    TRIANGLE_M(Constant.TRIANGLE_M, "TRIANGLE M") {
        @Override
        public WkbType elementType() {
            return LINE_STRING_M;
        }
    },

    /*################################## blow ZM Coordinate  ##################################*/


    GEOMETRY_ZM(Constant.GEOMETRY_ZM, "GEOMETRY ZM") {
        @Override
        public WkbType elementType() {
            return null;
        }
    },
    POINT_ZM(Constant.POINT_ZM, "POINT ZM") {
        @Override
        public WkbType elementType() {
            return null;
        }
    },
    LINE_STRING_ZM(Constant.LINE_STRING_ZM, "LINESTRING ZM") {
        @Override
        public WkbType elementType() {
            return POINT_ZM;
        }
    },
    POLYGON_ZM(Constant.POLYGON_ZM, "POLYGON ZM") {
        @Override
        public WkbType elementType() {
            return LINE_STRING_ZM;
        }
    },
    MULTI_POINT_ZM(Constant.MULTI_POINT_ZM, "MULTIPOINT ZM") {
        @Override
        public WkbType elementType() {
            return POINT_ZM;
        }
    },
    MULTI_LINE_STRING_ZM(Constant.MULTI_LINE_STRING_ZM, "MULTILINESTRING ZM") {
        @Override
        public WkbType elementType() {
            return LINE_STRING_ZM;
        }
    },
    MULTI_POLYGON_ZM(Constant.MULTI_POLYGON_ZM, "MULTIPOLYGON ZM") {
        @Override
        public WkbType elementType() {
            return POLYGON_ZM;
        }
    },
    GEOMETRY_COLLECTION_ZM(Constant.GEOMETRY_COLLECTION_ZM, "GEOMETRYCOLLECTION ZM") {
        @Override
        public WkbType elementType() {
            return GEOMETRY_ZM;
        }
    },
    CIRCULAR_STRING_ZM(Constant.CIRCULAR_STRING_ZM, "CIRCULARSTRING ZM") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    }, // future use
    COMPOUND_CURVE_ZM(Constant.COMPOUND_CURVE_ZM, "COMPOUNDCURVE ZM") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    }, // future use
    CURVE_POLYGON_ZM(Constant.CURVE_POLYGON_ZM, "CURVEPOLYGON ZM") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    }, // future use
    MULTI_CURVE_ZM(Constant.MULTI_CURVE_ZM, "MULTICURVE ZM") {
        @Override
        public WkbType elementType() {
            return CURVE_ZM;
        }
    },
    MULTI_SURFACE_ZM(Constant.MULTI_SURFACE_ZM, "MULTISURFACE ZM") {
        @Override
        public WkbType elementType() {
            return SURFACE_ZM;
        }
    },
    CURVE_ZM(Constant.CURVE_ZM, "CURVE ZM") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    },
    SURFACE_ZM(Constant.SURFACE_ZM, "SURFACE ZM") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    },
    POLYHEDRAL_SURFACE_ZM(Constant.POLYHEDRAL_SURFACE_ZM, "POLYHEDRALSURFACE ZM") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    },

    TIN_ZM(Constant.TIN_ZM, "TIN ZM") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }
    },
    TRIANGLE_ZM(Constant.TRIANGLE_ZM, "TRIANGLE ZM") {
        @Override
        public WkbType elementType() {
            return LINE_STRING_ZM;
        }
    };


    private static final Map<Integer, WkbType> CODE_MAP = CodeEnum.getCodeMap(WkbType.class);


    public final int code;

    public final String wktType;

    WkbType(int code, String wktType) {
        this.code = code;
        this.wktType = wktType;
    }

    @Override
    public int code() {
        return this.code;
    }

    @Override
    public String display() {
        return this.wktType;
    }

    @Nullable
    public abstract WkbType elementType();

    public int coordinates() {
        final int coordinateCount;
        if (this.code < 1000) {
            coordinateCount = 2;
        } else if (this.code < 3000) {
            coordinateCount = 3;
        } else {
            coordinateCount = 4;
        }
        return coordinateCount;
    }

    public boolean supportPointText() {
        final boolean pointText;
        switch (this) {
            case POLYGON:
            case POLYGON_Z:
            case POLYGON_M:
            case POLYGON_ZM:
            case MULTI_POINT:
            case MULTI_POINT_Z:
            case MULTI_POINT_M:
            case MULTI_POINT_ZM:
            case MULTI_POLYGON:
            case MULTI_POLYGON_Z:
            case MULTI_POLYGON_M:
            case MULTI_POLYGON_ZM:
                pointText = true;
                break;
            default:
                pointText = false;
        }
        return pointText;
    }


    public boolean supportLinearRing() {
        final boolean linearRing;
        switch (this) {
            case POLYGON:
            case POLYGON_Z:
            case POLYGON_M:
            case POLYGON_ZM:
            case MULTI_POLYGON:
            case MULTI_POLYGON_Z:
            case MULTI_POLYGON_M:
            case MULTI_POLYGON_ZM:
                linearRing = true;
                break;
            default:
                linearRing = false;
        }
        return linearRing;
    }


    public interface Constant {

        byte GEOMETRY = 0;
        byte POINT = 1;
        byte LINE_STRING = 2;
        byte POLYGON = 3;

        byte MULTI_POINT = 4;
        byte MULTI_LINE_STRING = 5;
        byte MULTI_POLYGON = 6;
        byte GEOMETRY_COLLECTION = 7;

        byte CIRCULAR_STRING = 8;
        byte COMPOUND_CURVE = 9;
        byte CURVE_POLYGON = 10;
        byte MULTI_CURVE = 11;

        byte MULTI_SURFACE = 12;
        byte CURVE = 13;
        byte SURFACE = 14;
        byte POLYHEDRAL_SURFACE = 15;

        byte TIN = 16;
        byte TRIANGLE = 17;

        /*################################## blow Z Coordinate  ##################################*/

        int GEOMETRY_Z = 1000;
        int POINT_Z = 1001;
        int LINE_STRING_Z = 1002;
        int POLYGON_Z = 1003;

        int MULTI_POINT_Z = 1004;
        int MULTI_LINE_STRING_Z = 1005;
        int MULTI_POLYGON_Z = 1006;
        int GEOMETRY_COLLECTION_Z = 1007;

        int CIRCULAR_STRING_Z = 1008;
        int COMPOUND_CURVE_Z = 1009;
        int CURVE_POLYGON_Z = 1010;
        int MULTI_CURVE_Z = 1011;

        int MULTI_SURFACE_Z = 1012;
        int CURVE_Z = 1013;
        int SURFACE_Z = 1014;
        int POLYHEDRAL_SURFACE_Z = 1015;

        int TIN_Z = 1016;
        int TRIANGLE_Z = 1017;

        /*################################## blow M Coordinate  ##################################*/

        int GEOMETRY_M = 2000;
        int POINT_M = 2001;
        int LINE_STRING_M = 2002;
        int POLYGON_M = 2003;

        int MULTI_POINT_M = 2004;
        int MULTI_LINE_STRING_M = 2005;
        int MULTI_POLYGON_M = 2006;
        int GEOMETRY_COLLECTION_M = 2007;

        int CIRCULAR_STRING_M = 2008;
        int COMPOUND_CURVE_M = 2009;
        int CURVE_POLYGON_M = 2010;
        int MULTI_CURVE_M = 2011;

        int MULTI_SURFACE_M = 2012;
        int CURVE_M = 2013;
        int SURFACE_M = 2014;
        int POLYHEDRAL_SURFACE_M = 2015;

        int TIN_M = 2016;
        int TRIANGLE_M = 2017;

        /*################################## blow ZM Coordinate  ##################################*/

        int GEOMETRY_ZM = 3000;
        int POINT_ZM = 3001;
        int LINE_STRING_ZM = 3002;
        int POLYGON_ZM = 3003;

        int MULTI_POINT_ZM = 3004;
        int MULTI_LINE_STRING_ZM = 3005;
        int MULTI_POLYGON_ZM = 3006;
        int GEOMETRY_COLLECTION_ZM = 3007;

        int CIRCULAR_STRING_ZM = 3008;
        int COMPOUND_CURVE_ZM = 3009;
        int CURVE_POLYGON_ZM = 3010;
        int MULTI_CURVE_ZM = 3011;

        int MULTI_SURFACE_ZM = 3012;
        int CURVE_ZM = 3013;
        int SURFACE_ZM = 3014;
        int POLYHEDRAL_SURFACE_ZM = 3015;

        int TIN_ZM = 3016;
        int TRIANGLE_ZM = 3017;

    }


    @Nullable
    public static WkbType resolve(int code) {
        return CODE_MAP.get(code);
    }

    public static WkbType resolveWkbType(final byte[] wkbArray, int offset) throws IllegalArgumentException {
        final byte byteOrder = wkbArray[offset++];
        if (byteOrder != 0 && byteOrder != 1) {
            throw new IllegalArgumentException(String.format("Illegal byteOrder[%s].", byteOrder));
        }
        int typeCode = readInt(byteOrder == 0, wkbArray, offset);
        WkbType wkbType = resolve(typeCode);
        if (wkbType == null) {
            throw new IllegalArgumentException(String.format("Unknown WKB type[%s].", typeCode));
        }
        return wkbType;
    }


    public static WkbType resolveWkbType(final Path path) throws IllegalArgumentException, IOException {
        try (FileChannel in = FileChannel.open(path, StandardOpenOption.READ)) {
            byte[] bufferArray = new byte[5];
            ByteBuffer buffer = ByteBuffer.wrap(bufferArray);
            if (in.read(buffer) < 5) {
                throw new IllegalArgumentException("Empty path");
            }
            buffer.flip();
            return resolveWkbType(bufferArray, 0);
        } catch (IOException | IllegalArgumentException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException(e.getMessage(), e);
        }
    }


    private static int readInt(final boolean bigEndian, final byte[] wkbArray, int offset) {
        if (wkbArray.length < 5) {
            throw new IllegalArgumentException("WKB length < 5 .");
        }
        if (wkbArray.length - offset < 4) {
            throw new IllegalArgumentException(String.format("WKB length < %s .", offset + 4));
        }
        final int num;
        if (bigEndian) {
            num = ((wkbArray[offset++] & 0xFF) << 24)
                    | ((wkbArray[offset++] & 0xFF) << 16)
                    | ((wkbArray[offset++] & 0xFF) << 8)
                    | (wkbArray[offset] & 0xFF);
        } else {
            num = (wkbArray[offset++] & 0xFF)
                    | ((wkbArray[offset++] & 0xFF) << 8)
                    | ((wkbArray[offset++] & 0xFF) << 16)
                    | ((wkbArray[offset] & 0xFF) << 24);
        }
        return num;
    }

    private static String wktTypeName(String name) {
        final int len = name.length();
        int underlineCount = 0;
        for (int i = 0; i < len; i++) {
            if (name.charAt(i) == '_') {
                underlineCount++;
            }
        }
        final String wktType;
        if (underlineCount > 0) {
            char[] charArray = new char[len - underlineCount];
            char ch;
            for (int i = 0, chIndex = 0; i < len; i++) {
                ch = name.charAt(i);
                if (ch != '_') {
                    charArray[chIndex] = ch;
                    chIndex++;
                }
            }
            wktType = new String(charArray);
        } else {
            wktType = name;
        }
        return wktType;

    }


}
