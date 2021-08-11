package io.jdbd.type.geometry;

import io.jdbd.type.CodeEnum;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
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

        @Override
        public WkbType family() {
            return this;
        }
    },
    POINT(Constant.POINT, "POINT") {
        @Override
        public WkbType elementType() {
            return null;
        }

        @Override
        public WkbType family() {
            return this;
        }
    },
    LINE_STRING(Constant.LINE_STRING, "LINESTRING") {
        @Override
        public WkbType elementType() {
            return POINT;
        }

        @Override
        public WkbType family() {
            return this;
        }
    },
    POLYGON(Constant.POLYGON, "POLYGON") {
        @Override
        public WkbType elementType() {
            return LINE_STRING;
        }

        @Override
        public WkbType family() {
            return this;
        }
    },

    MULTI_POINT(Constant.MULTI_POINT, "MULTIPOINT") {
        @Override
        public WkbType elementType() {
            return POINT;
        }

        @Override
        public WkbType family() {
            return this;
        }
    },
    MULTI_LINE_STRING(Constant.MULTI_LINE_STRING, "MULTILINESTRING") {
        @Override
        public WkbType elementType() {
            return LINE_STRING;
        }

        @Override
        public WkbType family() {
            return this;
        }
    },
    MULTI_POLYGON(Constant.MULTI_POLYGON, "MULTIPOLYGON") {
        @Override
        public WkbType elementType() {
            return POLYGON;
        }

        @Override
        public WkbType family() {
            return this;
        }
    },
    GEOMETRY_COLLECTION(Constant.GEOMETRY_COLLECTION, "GEOMETRYCOLLECTION") {
        @Override
        public WkbType elementType() {
            return GEOMETRY;
        }

        @Override
        public WkbType family() {
            return this;
        }
    },

    CIRCULAR_STRING(Constant.CIRCULAR_STRING, "CIRCULARSTRING") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return this;
        }
    }, // future use
    COMPOUND_CURVE(Constant.COMPOUND_CURVE, "COMPOUNDCURVE") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return this;
        }
    }, // future use
    CURVE_POLYGON(Constant.CURVE_POLYGON, "CURVEPOLYGON") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return this;
        }
    }, // future use
    MULTI_CURVE(Constant.MULTI_CURVE, "MULTICURVE") {
        @Override
        public WkbType elementType() {
            return CURVE;
        }

        @Override
        public WkbType family() {
            return this;
        }
    },

    MULTI_SURFACE(Constant.MULTI_SURFACE, "MULTISURFACE") {
        @Override
        public WkbType elementType() {
            return SURFACE;
        }

        @Override
        public WkbType family() {
            return this;
        }
    },
    CURVE(Constant.CURVE, "CURVE") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return this;
        }
    },
    SURFACE(Constant.SURFACE, "SURFACE") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return this;
        }
    },
    POLYHEDRAL_SURFACE(Constant.POLYHEDRAL_SURFACE, "POLYHEDRALSURFACE") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return this;
        }
    },

    TIN(Constant.TIN, "TIN") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return this;
        }
    },
    TRIANGLE(Constant.TRIANGLE, "TRIANGLE") {
        @Override
        public WkbType elementType() {
            return LINE_STRING;
        }

        @Override
        public WkbType family() {
            return this;
        }
    },


    GEOMETRY_Z(Constant.GEOMETRY_Z, "GEOMETRY Z") {
        @Override
        public WkbType elementType() {
            return null;
        }

        @Override
        public WkbType family() {
            return GEOMETRY;
        }
    },
    POINT_Z(Constant.POINT_Z, "POINT Z") {
        @Override
        public WkbType elementType() {
            return null;
        }

        @Override
        public WkbType family() {
            return POINT;
        }
    },
    LINE_STRING_Z(Constant.LINE_STRING_Z, "LINESTRING Z") {
        @Override
        public WkbType elementType() {
            return POINT_Z;
        }

        @Override
        public WkbType family() {
            return LINE_STRING;
        }
    },
    POLYGON_Z(Constant.POLYGON_Z, "POLYGON Z") {
        @Override
        public WkbType elementType() {
            return LINE_STRING_Z;
        }

        @Override
        public WkbType family() {
            return POLYGON;
        }
    },

    MULTI_POINT_Z(Constant.MULTI_POINT_Z, "MULTIPOINT Z") {
        @Override
        public WkbType elementType() {
            return POINT_Z;
        }

        @Override
        public WkbType family() {
            return MULTI_POINT;
        }
    },
    MULTI_LINE_STRING_Z(Constant.MULTI_LINE_STRING_Z, "MULTILINESTRING Z") {
        @Override
        public WkbType elementType() {
            return LINE_STRING_Z;
        }

        @Override
        public WkbType family() {
            return MULTI_LINE_STRING;
        }
    },
    MULTI_POLYGON_Z(Constant.MULTI_POLYGON_Z, "MULTIPOLYGON Z") {
        @Override
        public WkbType elementType() {
            return POLYGON_Z;
        }

        @Override
        public WkbType family() {
            return MULTI_POLYGON;
        }
    },
    GEOMETRY_COLLECTION_Z(Constant.GEOMETRY_COLLECTION_Z, "GEOMETRYCOLLECTION Z") {
        @Override
        public WkbType elementType() {
            return GEOMETRY_Z;
        }

        @Override
        public WkbType family() {
            return GEOMETRY_COLLECTION;
        }
    },

    CIRCULAR_STRING_Z(Constant.CIRCULAR_STRING_Z, "CIRCULARSTRING Z") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return CIRCULAR_STRING;
        }
    }, // future use
    COMPOUND_CURVE_Z(Constant.COMPOUND_CURVE_Z, "COMPOUNDCURVE Z") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return COMPOUND_CURVE;
        }
    }, // future use
    CURVE_POLYGON_Z(Constant.CURVE_POLYGON_Z, "CURVEPOLYGON Z") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return CURVE_POLYGON;
        }
    }, // future use
    MULTI_CURVE_Z(Constant.MULTI_CURVE_Z, "MULTICURVE Z") {
        @Override
        public WkbType elementType() {
            return CURVE_Z;
        }

        @Override
        public WkbType family() {
            return MULTI_CURVE;
        }
    },
    MULTI_SURFACE_Z(Constant.MULTI_SURFACE_Z, "MULTISURFACE Z") {
        @Override
        public WkbType elementType() {
            return SURFACE_Z;
        }

        @Override
        public WkbType family() {
            return MULTI_SURFACE;
        }
    },
    CURVE_Z(Constant.CURVE_Z, "CURVE Z") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return CURVE;
        }
    },
    SURFACE_Z(Constant.SURFACE_Z, "SURFACE Z") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return SURFACE;
        }
    },
    POLYHEDRAL_SURFACE_Z(Constant.POLYHEDRAL_SURFACE_Z, "POLYHEDRALSURFACE Z") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return POLYHEDRAL_SURFACE;
        }
    },

    TIN_Z(Constant.TIN_Z, "TIN Z") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return TIN;
        }
    },
    TRIANGLE_Z(Constant.TRIANGLE_Z, "TRIANGLE Z") {
        @Override
        public WkbType elementType() {
            return LINE_STRING_Z;
        }

        @Override
        public WkbType family() {
            return TRIANGLE;
        }
    },

    /*################################## blow M Coordinate  ##################################*/

    GEOMETRY_M(Constant.GEOMETRY_M, "GEOMETRY M") {
        @Override
        public WkbType elementType() {
            return null;
        }

        @Override
        public WkbType family() {
            return GEOMETRY;
        }
    },
    POINT_M(Constant.POINT_M, "POINT M") {
        @Override
        public WkbType elementType() {
            return null;
        }

        @Override
        public WkbType family() {
            return POINT;
        }
    },
    LINE_STRING_M(Constant.LINE_STRING_M, "LINESTRING M") {
        @Override
        public WkbType elementType() {
            return POINT_M;
        }

        @Override
        public WkbType family() {
            return LINE_STRING;
        }
    },
    POLYGON_M(Constant.POLYGON_M, "POLYGON M") {
        @Override
        public WkbType elementType() {
            return LINE_STRING_M;
        }

        @Override
        public WkbType family() {
            return POLYGON;
        }
    },

    MULTI_POINT_M(Constant.MULTI_POINT_M, "MULTIPOINT M") {
        @Override
        public WkbType elementType() {
            return POINT_M;
        }

        @Override
        public WkbType family() {
            return MULTI_POINT;
        }
    },
    MULTI_LINE_STRING_M(Constant.MULTI_LINE_STRING_M, "MULTILINESTRING M") {
        @Override
        public WkbType elementType() {
            return LINE_STRING_M;
        }

        @Override
        public WkbType family() {
            return MULTI_LINE_STRING;
        }
    },
    MULTI_POLYGON_M(Constant.MULTI_POLYGON_M, "MULTIPOLYGON M") {
        @Override
        public WkbType elementType() {
            return POLYGON_M;
        }

        @Override
        public WkbType family() {
            return MULTI_POLYGON;
        }
    },
    GEOMETRY_COLLECTION_M(Constant.GEOMETRY_COLLECTION_M, "GEOMETRYCOLLECTION M") {
        @Override
        public WkbType elementType() {
            return GEOMETRY_M;
        }

        @Override
        public WkbType family() {
            return GEOMETRY_COLLECTION;
        }
    },

    CIRCULAR_STRING_M(Constant.CIRCULAR_STRING_M, "CIRCULARSTRING M") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return CIRCULAR_STRING;
        }
    }, // future use
    COMPOUND_CURVE_M(Constant.COMPOUND_CURVE_M, "COMPOUNDCURVE M") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return COMPOUND_CURVE;
        }
    }, // future use
    CURVE_POLYGON_M(Constant.CURVE_POLYGON_M, "CURVEPOLYGON M") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return CURVE_POLYGON;
        }
    }, // future use
    MULTI_CURVE_M(Constant.MULTI_CURVE_M, "MULTICURVE M") {
        @Override
        public WkbType elementType() {
            return CURVE_M;
        }

        @Override
        public WkbType family() {
            return MULTI_CURVE;
        }
    },
    MULTI_SURFACE_M(Constant.MULTI_SURFACE_M, "MULTISURFACE M") {
        @Override
        public WkbType elementType() {
            return SURFACE_M;
        }

        @Override
        public WkbType family() {
            return MULTI_SURFACE;
        }
    },
    CURVE_M(Constant.CURVE_M, "CURVE M") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return CURVE;
        }
    },
    SURFACE_M(Constant.SURFACE_M, "SURFACE M") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return SURFACE;
        }
    },
    POLYHEDRAL_SURFACE_M(Constant.POLYHEDRAL_SURFACE_M, "POLYHEDRALSURFACE M") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return POLYHEDRAL_SURFACE;
        }
    },

    TIN_M(Constant.TIN_M, "TIN M") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return TIN;
        }
    },
    TRIANGLE_M(Constant.TRIANGLE_M, "TRIANGLE M") {
        @Override
        public WkbType elementType() {
            return LINE_STRING_M;
        }

        @Override
        public WkbType family() {
            return TRIANGLE;
        }
    },

    /*################################## blow ZM Coordinate  ##################################*/


    GEOMETRY_ZM(Constant.GEOMETRY_ZM, "GEOMETRY ZM") {
        @Override
        public WkbType elementType() {
            return null;
        }

        @Override
        public WkbType family() {
            return GEOMETRY;
        }
    },
    POINT_ZM(Constant.POINT_ZM, "POINT ZM") {
        @Override
        public WkbType elementType() {
            return null;
        }

        @Override
        public WkbType family() {
            return POINT;
        }
    },
    LINE_STRING_ZM(Constant.LINE_STRING_ZM, "LINESTRING ZM") {
        @Override
        public WkbType elementType() {
            return POINT_ZM;
        }

        @Override
        public WkbType family() {
            return LINE_STRING;
        }
    },
    POLYGON_ZM(Constant.POLYGON_ZM, "POLYGON ZM") {
        @Override
        public WkbType elementType() {
            return LINE_STRING_ZM;
        }

        @Override
        public WkbType family() {
            return POLYGON;
        }
    },
    MULTI_POINT_ZM(Constant.MULTI_POINT_ZM, "MULTIPOINT ZM") {
        @Override
        public WkbType elementType() {
            return POINT_ZM;
        }

        @Override
        public WkbType family() {
            return MULTI_POINT;
        }
    },
    MULTI_LINE_STRING_ZM(Constant.MULTI_LINE_STRING_ZM, "MULTILINESTRING ZM") {
        @Override
        public WkbType elementType() {
            return LINE_STRING_ZM;
        }

        @Override
        public WkbType family() {
            return MULTI_LINE_STRING;
        }
    },
    MULTI_POLYGON_ZM(Constant.MULTI_POLYGON_ZM, "MULTIPOLYGON ZM") {
        @Override
        public WkbType elementType() {
            return POLYGON_ZM;
        }

        @Override
        public WkbType family() {
            return MULTI_POLYGON;
        }
    },
    GEOMETRY_COLLECTION_ZM(Constant.GEOMETRY_COLLECTION_ZM, "GEOMETRYCOLLECTION ZM") {
        @Override
        public WkbType elementType() {
            return GEOMETRY_ZM;
        }

        @Override
        public WkbType family() {
            return GEOMETRY_COLLECTION;
        }
    },
    CIRCULAR_STRING_ZM(Constant.CIRCULAR_STRING_ZM, "CIRCULARSTRING ZM") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return CIRCULAR_STRING;
        }
    }, // future use
    COMPOUND_CURVE_ZM(Constant.COMPOUND_CURVE_ZM, "COMPOUNDCURVE ZM") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return COMPOUND_CURVE;
        }
    }, // future use
    CURVE_POLYGON_ZM(Constant.CURVE_POLYGON_ZM, "CURVEPOLYGON ZM") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return CURVE_POLYGON;
        }
    }, // future use
    MULTI_CURVE_ZM(Constant.MULTI_CURVE_ZM, "MULTICURVE ZM") {
        @Override
        public WkbType elementType() {
            return CURVE_ZM;
        }

        @Override
        public WkbType family() {
            return MULTI_CURVE;
        }
    },
    MULTI_SURFACE_ZM(Constant.MULTI_SURFACE_ZM, "MULTISURFACE ZM") {
        @Override
        public WkbType elementType() {
            return SURFACE_ZM;
        }

        @Override
        public WkbType family() {
            return MULTI_SURFACE;
        }
    },
    CURVE_ZM(Constant.CURVE_ZM, "CURVE ZM") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return CURVE;
        }
    },
    SURFACE_ZM(Constant.SURFACE_ZM, "SURFACE ZM") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return SURFACE;
        }
    },
    POLYHEDRAL_SURFACE_ZM(Constant.POLYHEDRAL_SURFACE_ZM, "POLYHEDRALSURFACE ZM") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return POLYHEDRAL_SURFACE;
        }
    },

    TIN_ZM(Constant.TIN_ZM, "TIN ZM") {
        @Override
        public WkbType elementType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WkbType family() {
            return TIN;
        }
    },
    TRIANGLE_ZM(Constant.TRIANGLE_ZM, "TRIANGLE ZM") {
        @Override
        public WkbType elementType() {
            return LINE_STRING_ZM;
        }

        @Override
        public WkbType family() {
            return TRIANGLE;
        }
    };


    private static final Map<Integer, WkbType> CODE_MAP = CodeEnum.getCodeMap(WkbType.class);

    private static final Map<String, WkbType> WKT_MAP = createWktMap();


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

    @Override
    public abstract WkbType family();

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

    public boolean sameDimension(WkbType wkbType) {
        final boolean match;
        if (this.code < 1000) {
            match = wkbType.code < 1000;
        } else if (this.code < 2000) {
            match = wkbType.code < 2000;
        } else if (this.code < 3000) {
            match = wkbType.code < 3000;
        } else {
            match = true;
        }
        return match;
    }

    public boolean supportPointText() {
        final boolean pointText;
        switch (this) {
            case MULTI_POINT:
            case MULTI_POINT_Z:
            case MULTI_POINT_M:
            case MULTI_POINT_ZM:
                pointText = true;
                break;
            default:
                pointText = false;
        }
        return pointText;
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
    public static WkbType fromCode(int code) {
        return CODE_MAP.get(code);
    }

    public static WkbType fromWkbArray(final byte[] wkbArray, int offset) throws IllegalArgumentException {
        if (offset < 0 || offset >= wkbArray.length) {
            throw new IllegalArgumentException(String.format("offset[%s] not in [0,%s)", offset, wkbArray.length));
        }
        if (wkbArray.length < offset + 5) {
            throw new IllegalArgumentException(
                    String.format("wkbArray length[%s] error ,expect length[%s]", wkbArray.length, offset + 5));
        }
        final byte byteOrder = wkbArray[offset++];
        if (byteOrder != 0 && byteOrder != 1) {
            throw new IllegalArgumentException(String.format("Illegal byteOrder[%s].", byteOrder));
        }
        int typeCode = readInt(byteOrder == 0, wkbArray, offset);
        WkbType wkbType = fromCode(typeCode);
        if (wkbType == null) {
            throw new IllegalArgumentException(String.format("Unknown WKB type[%s].", typeCode));
        }
        return wkbType;
    }

    @Nullable
    public static WkbType fromWkt(String wktType) {
        return WKT_MAP.get(wktType.toUpperCase());
    }


    public static WkbType fromPath(final Path path) throws IllegalArgumentException, IOException {
        try (FileChannel in = FileChannel.open(path, StandardOpenOption.READ)) {
            return fromChannel(in);
        } catch (IOException | IllegalArgumentException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    public static WkbType fromChannel(FileChannel in) throws IllegalArgumentException, IOException {
        byte[] bufferArray = new byte[5];
        ByteBuffer buffer = ByteBuffer.wrap(bufferArray);
        if (in.read(buffer) < 5) {
            throw new IllegalArgumentException("Empty path");
        }
        buffer.flip();
        return fromWkbArray(bufferArray, 0);
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

    private static Map<String, WkbType> createWktMap() {
        WkbType[] wkbTypes = WkbType.values();
        Map<String, WkbType> wktMap = new HashMap<>((int) (wkbTypes.length / 0.75F));

        for (WkbType wkbType : wkbTypes) {
            if (wktMap.putIfAbsent(wkbType.wktType, wkbType) != null) {
                throw new IllegalStateException(String.format("WKB[%s] wkt duplication.", wkbType));
            }
        }
        return Collections.unmodifiableMap(wktMap);
    }


}
