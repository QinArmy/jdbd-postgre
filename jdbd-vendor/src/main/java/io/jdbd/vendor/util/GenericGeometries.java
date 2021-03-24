package io.jdbd.vendor.util;

import io.jdbd.type.WkbType;

import java.util.Arrays;

abstract class GenericGeometries {

    GenericGeometries() {
        throw new UnsupportedOperationException();
    }


    public static boolean equals(final byte[] geometryOne, final byte[] geometryTwo) {
        final boolean match;
        if (geometryOne.length != geometryTwo.length) {
            match = false;
        } else {
            final byte oneByteOrder = checkByteOrder(geometryOne[0]);
            final byte towByteOrder = checkByteOrder(geometryTwo[0]);

            if (oneByteOrder == towByteOrder) {
                match = Arrays.equals(geometryOne, geometryTwo);
            } else {
                byte[] copyTwo = Arrays.copyOf(geometryTwo, geometryTwo.length);
                reverseWkb(copyTwo);
                match = Arrays.equals(geometryOne, copyTwo);
            }
        }

        return match;
    }

    public static void reverseWkb(final byte[] wkbArray) {
        final WkbType wkbType = WkbType.resolveWkbType(wkbArray);
        if (wkbType.code < 1000) {
            Geometries.geometryWkbReverse(wkbType, wkbArray);
        } else {
            throw new IllegalArgumentException(String.format("Unknown WKB type[%s]", wkbType.code));
        }

    }


    static byte checkByteOrder(byte byteOrder) {
        if (byteOrder != 0 && byteOrder != 1) {
            throw createIllegalByteOrderError(byteOrder);
        }
        return byteOrder;
    }


    static IllegalArgumentException createIllegalByteOrderError(byte byteOrder) {
        return new IllegalArgumentException(String.format("Illegal byteOrder[%s].", byteOrder));
    }


}
