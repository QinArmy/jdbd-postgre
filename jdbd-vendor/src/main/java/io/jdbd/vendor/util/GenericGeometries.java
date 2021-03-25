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
                final WkbType wkbTypeOne = WkbType.resolveWkbType(geometryOne, 0);
                final WkbType wkbTypeTwo = WkbType.resolveWkbType(geometryTwo, 0);
                if (wkbTypeOne != WkbType.POINT && geometryOne.length < HEADER_LENGTH) {
                    throw createWkbLengthError(wkbTypeOne.name(), geometryOne.length, HEADER_LENGTH);
                }
                if (wkbTypeOne == wkbTypeTwo) {
                    switch (wkbTypeOne) {
                        case POINT:
                            match = Geometries.pointReverseEquals(geometryOne, geometryTwo);
                            break;
                        case LINE_STRING:
                            match = Geometries.lineStringReverseEquals(geometryOne, geometryTwo);
                            break;
                        case POLYGON:
                            match = Geometries.polygonReverseEquals(geometryOne, geometryTwo);
                            break;
                        case MULTI_POINT:

                        case MULTI_LINE_STRING:
                        case MULTI_POLYGON:
                        case GEOMETRY_COLLECTION:
                        default:
                            throw new IllegalArgumentException(String.format("not support %s now.", wkbTypeOne));
                    }
                } else {
                    match = false;
                }
            }
        }
        return match;
    }

    public static void reverseWkb(final byte[] wkbArray, final int offset) {
        final WkbType wkbType = WkbType.resolveWkbType(wkbArray, offset);
        if (wkbType.code < 1000) {
            Geometries.geometryWkbReverse(wkbType, wkbArray, offset);
        } else {
            throw new IllegalArgumentException(String.format("Unknown WKB type[%s]", wkbType.code));
        }

    }


    protected static byte checkByteOrder(byte byteOrder) {
        if (byteOrder != 0 && byteOrder != 1) {
            throw createIllegalByteOrderError(byteOrder);
        }
        return byteOrder;
    }

    /**
     * @return element count.
     */
    protected static int checkAndReverseHeader(final byte[] wkbArray, int offset, WkbType wkbType, Function<Integer, Integer> function) {
        if (wkbArray.length < 5) {
            throw createIllegalWkbLengthError(wkbArray.length, 5);
        }
        final byte byteOrder = checkByteOrder(wkbArray[offset++]);
        final int wkbTypeCode, elementCount;
        if (byteOrder == 1) {
            wkbTypeCode = JdbdNumberUtils.readIntFromBigEndian(wkbArray, offset, 4);
            offset += 4;
            elementCount = JdbdNumberUtils.readIntFromBigEndian(wkbArray, offset, 4);
        } else {
            wkbTypeCode = JdbdNumberUtils.readIntFromLittleEndian(wkbArray, offset, 4);
            offset += 4;
            elementCount = JdbdNumberUtils.readIntFromLittleEndian(wkbArray, offset, 4);
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

    protected static IllegalArgumentException createOffsetError(int offset, int arrayLength) {
        return new IllegalArgumentException(String.format("offset[%s] not in [0,%s).", offset, arrayLength));
    }

    protected static IllegalArgumentException createIllegalElementCount(int elementCount) {
        return new IllegalArgumentException(String.format("elementCount[%s] error", elementCount));
    }

    protected static IllegalArgumentException createIllegalLinearRingCountError(int linearRingCount) {
        return new IllegalArgumentException(String.format("linearRingCount[%s] error", linearRingCount));
    }

    protected static IllegalArgumentException createIllegalLinearPointCountError(int pointCount) {
        return new IllegalArgumentException(String.format("pointCount[%s] error", pointCount));
    }

    protected static IllegalArgumentException createNonLinearRingError(int linearRingIndex) {
        return new IllegalArgumentException(String.format("Polygon LinearRing[%s] isn't LinearRing", linearRingIndex));
    }


}
