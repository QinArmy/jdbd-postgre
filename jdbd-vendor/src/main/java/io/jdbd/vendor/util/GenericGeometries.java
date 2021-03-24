package io.jdbd.vendor.util;

import io.jdbd.type.WkbType;

import java.util.Arrays;
import java.util.function.Function;

abstract class GenericGeometries {

    GenericGeometries() {
        throw new UnsupportedOperationException();
    }

    static final byte HEADER_LENGTH = 9;


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

    static int checkAndReverseHeader(final byte[] wkbArray, WkbType wkbType, Function<Integer, Integer> function) {
        if (wkbArray.length < 5) {
            throw createIllegalWkbLengthError(wkbArray.length, 5);
        }
        final byte byteOrder = checkByteOrder(wkbArray[0]);
        final int wkbTypeCode, elementCount;
        if (byteOrder == 1) {
            wkbTypeCode = JdbdNumberUtils.readIntFromBigEndian(wkbArray, 1, 4);
            elementCount = JdbdNumberUtils.readIntFromBigEndian(wkbArray, 5, 4);
        } else {
            wkbTypeCode = JdbdNumberUtils.readIntFromLittleEndian(wkbArray, 1, 4);
            elementCount = JdbdNumberUtils.readIntFromLittleEndian(wkbArray, 5, 4);
        }
        if (wkbTypeCode != wkbType.code) {
            throw createWkbTypeNotMatchError(wkbType.name(), wkbTypeCode);
        }

        final int needBytes = HEADER_LENGTH + function.apply(elementCount);

        if (wkbArray.length < needBytes) {
            throw createWkbLengthError(wkbType.name(), wkbArray.length, needBytes);
        }

        wkbArray[0] ^= 1;
        JdbdArrayUtils.reverse(wkbArray, 1, 4, 2);
        return elementCount;
    }

    protected static IllegalArgumentException createIllegalByteOrderError(byte byteOrder) {
        return new IllegalArgumentException(String.format("Illegal byteOrder[%s].", byteOrder));
    }

    protected static IllegalArgumentException createIllegalWkbLengthError(long length, long expectLength) {
        return new IllegalArgumentException(String.format("WKB length[%s] but expect min length[%s]."
                , length, expectLength));
    }

    protected static IllegalArgumentException createWkbTypeNotMatchError(String type, int wkbType) {
        return new IllegalArgumentException(String.format("WKB type[%s] and %s not match.", wkbType, type));
    }

    protected static IllegalArgumentException createWkbLengthError(String type, long length, long exceptLength) {
        return new IllegalArgumentException(String.format("WKB length[%s] and %s except length[%s] not match."
                , length, type, exceptLength));
    }

    protected static IllegalArgumentException createUnknownWkbTypeError(int wkbType) {
        return new IllegalArgumentException(String.format("Unknown WKB-Type[%s].", wkbType));
    }


}
