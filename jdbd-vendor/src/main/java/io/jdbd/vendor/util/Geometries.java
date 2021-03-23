package io.jdbd.vendor.util;

import java.util.Arrays;

public abstract class Geometries {

    protected Geometries() {
        throw new UnsupportedOperationException();
    }

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
