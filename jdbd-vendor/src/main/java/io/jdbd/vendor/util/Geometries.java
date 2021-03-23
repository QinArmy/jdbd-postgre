package io.jdbd.vendor.util;

import java.util.Arrays;

public abstract class Geometries {

    protected Geometries() {
        throw new UnsupportedOperationException();
    }

    public static final byte WKB_POINT = 1;

    public static final byte WKB_LINE_STRING = 2;

    public static final byte WKB_POINT_BYTES = 21;

    public static final String POINT = "POINT";

    public static final String LINE_STRING = "LINESTRING";


    public boolean equals(final byte[] geometryOne, final byte[] geometryTwo) {
        final boolean match;
        if (geometryOne.length != geometryTwo.length) {
            match = false;
        } else {
            final byte oneByteOrder = GeometryConvertUtils.checkByteOrder(geometryOne[0]);
            final byte towByteOrder = GeometryConvertUtils.checkByteOrder(geometryTwo[0]);

            if (oneByteOrder == towByteOrder) {
                match = Arrays.equals(geometryOne, geometryTwo);
            } else {
                byte[] copyTwo = Arrays.copyOf(geometryTwo, geometryTwo.length);
                GeometryConvertUtils.pointWkbReverse(copyTwo);
                match = Arrays.equals(geometryOne, copyTwo);
            }
        }

        return match;
    }


}
