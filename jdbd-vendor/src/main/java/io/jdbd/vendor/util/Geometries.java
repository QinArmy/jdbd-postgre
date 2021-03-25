package io.jdbd.vendor.util;

import io.jdbd.type.WkbType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.qinarmy.util.BufferWrapper;
import org.qinarmy.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Stack;

public abstract class Geometries extends GenericGeometries {


    private final static Logger LOG = LoggerFactory.getLogger(Geometries.class);

    public static final byte WKB_POINT_BYTES = 21;


    public static byte[] pointToWkb(final String pointWkt, final boolean bigEndian) {

        final int startIndex, endIndex;
        startIndex = JdbdStringUtils.endIndexOfPrefixes(true, pointWkt, "POINT", "(");
        endIndex = JdbdStringUtils.startIndexOfSuffix(false, pointWkt, ")");
        if (startIndex < 0 || endIndex < 0) {
            throw createWktFormatError(WkbType.POINT.name());
        }
        int numStartIndex = startIndex + 1;
        final int wktLength = pointWkt.length();
        for (; numStartIndex < wktLength; numStartIndex++) {
            if (!Character.isWhitespace(pointWkt.charAt(numStartIndex))) {
                break;
            }
        }
        if (numStartIndex == wktLength) {
            throw createWktFormatError(WkbType.POINT.name());
        }
        int numEndIndex = -1;
        for (int i = numStartIndex + 1; i < wktLength; i++) {
            if (Character.isWhitespace(pointWkt.charAt(i))) {
                numEndIndex = i;
                break;
            }
        }
        if (numEndIndex < 0) {
            throw createWktFormatError(WkbType.POINT.name());
        }

        final double x, y;
        x = Double.parseDouble(pointWkt.substring(numStartIndex, numEndIndex));
        numStartIndex = -1;
        for (int i = numEndIndex; i < wktLength; i++) {
            if (!Character.isWhitespace(pointWkt.charAt(i))) {
                numStartIndex = i;
                break;
            }
        }
        if (numStartIndex < 0) {
            throw createWktFormatError(WkbType.POINT.name());
        }
        y = Double.parseDouble(pointWkt.substring(numStartIndex, endIndex));

        byte[] wkbArray = new byte[WKB_POINT_BYTES];
        int offset = 0;
        wkbArray[offset++] = bigEndian ? (byte) 0 : (byte) 1;
        JdbdNumberUtils.intToEndian(bigEndian, WkbType.POINT.code, wkbArray, offset, 4);
        offset += 4;
        JdbdNumberUtils.doubleToEndian(bigEndian, x, wkbArray, offset);
        offset += 8;
        JdbdNumberUtils.doubleToEndian(bigEndian, y, wkbArray, offset);

        return wkbArray;
    }

    public static Pair<Double, Double> readPointAsPair(final byte[] wkbArray, int offset) {
        if (offset < 0 || offset >= wkbArray.length) {
            throw createOffsetError(offset, wkbArray.length);
        }
        if (wkbArray.length - offset < WKB_POINT_BYTES) {
            throw createIllegalWkbLengthError(wkbArray.length, offset + WKB_POINT_BYTES);
        }

        final boolean bigEndian = checkByteOrder(wkbArray[offset++]) == 0;
        final int wkbType = JdbdNumberUtils.readIntFromEndian(bigEndian, wkbArray, offset, 4);
        if (wkbType != WkbType.POINT.code) {
            throw createWktFormatError(WkbType.POINT.name());
        }
        offset += 4;
        final double x, y;
        x = JdbdNumberUtils.readDoubleFromEndian(bigEndian, wkbArray, offset, 8);
        offset += 8;
        y = JdbdNumberUtils.readDoubleFromEndian(bigEndian, wkbArray, offset, 8);
        return new Pair<>(x, y);
    }


    public static int pointWkbReverse(final byte[] wkbArray, int offset) {
        if (wkbArray.length < WKB_POINT_BYTES) {
            throw createWkbLengthError(WkbType.POINT.wktType, wkbArray.length, WKB_POINT_BYTES);
        }

        checkByteOrder(wkbArray[offset]);

        wkbArray[offset++] ^= 1;
        JdbdArrayUtils.reverse(wkbArray, offset, 4, 1);
        offset += 4;
        JdbdArrayUtils.reverse(wkbArray, offset, 8, 2);
        offset += 16;
        return offset;
    }

    /**
     * @return new offset.
     * @see #geometryWkbReverse(WkbType, byte[], int)
     */
    public static int lineStringWkbReverse(final byte[] wkbArray, int offset) {
        final int elementCount;
        elementCount = checkAndReverseHeader(wkbArray, offset, WkbType.LINE_STRING, count -> count << 4);
        offset += 9;
        JdbdArrayUtils.reverse(wkbArray, offset, 8, elementCount << 1);
        offset += (elementCount << 4);
        return offset;
    }

    /**
     * @see #geometryWkbReverse(WkbType, byte[], int)
     */
    public static int polygonWkbReverse(final byte[] wkbArray, int offset) {
        final int elementCount;
        elementCount = checkAndReverseHeader(wkbArray, offset, WkbType.POLYGON, count -> (count << 2) + (count << 6));
        offset += 9;
        return lineStringElementReverse(WkbType.POLYGON, elementCount, wkbArray, offset);
    }


    /**
     * @see #geometryWkbReverse(WkbType, byte[], int)
     */
    public static int multiPointWkbReverse(final byte[] wkbArray, int offset) {
        final int elementCount;
        elementCount = checkAndReverseHeader(wkbArray, offset, WkbType.MULTI_POINT, count -> count << 4);
        offset += 9;
        JdbdArrayUtils.reverse(wkbArray, offset, 8, elementCount << 1);
        offset += (elementCount << 4);
        return offset;
    }


    /**
     * @see #geometryWkbReverse(WkbType, byte[], int)
     */
    public static int multiLineStringWkbReverse(final byte[] wkbArray, int offset) {
        final int elementCount;
        elementCount = checkAndReverseHeader(wkbArray, offset, WkbType.MULTI_LINE_STRING
                , count -> (count << 2) + (count << 5));
        offset += 9;
        return lineStringElementReverse(WkbType.MULTI_LINE_STRING, elementCount, wkbArray, offset);
    }

    public static int multiPolygonWkbReverse(final byte[] wkbArray, int offset) {
        final int elementCount;
        elementCount = checkAndReverseHeader(wkbArray, offset, WkbType.MULTI_POLYGON, count -> 80 * count);
        offset += 9;
        return polygonElementWkbReverse(WkbType.MULTI_POLYGON, wkbArray, offset, elementCount);
    }

    /**
     * @see #geometryCollectionWkbReverse(byte[], int)
     * @see #geometryWkbReverse(WkbType, byte[], int)
     */
    public static int geometryCollectionWkbReverse(final byte[] wkbArray, int offset) {
        int elementCount;
        elementCount = checkAndReverseHeader(wkbArray, offset, WkbType.GEOMETRY_COLLECTION, count -> count * 20);
        offset += 9;

        final Stack<Pair<Integer, Integer>> pairStack = new Stack<>();
        pairStack.push(new Pair<>(elementCount, 0));
        Pair<Integer, Integer> pair;
        WkbType wkbType;
        while (!pairStack.isEmpty()) {
            pair = pairStack.pop();
            elementCount = pair.getFirst();

            for (int i = pair.getSecond(), wkbCode, itemCount; i < elementCount; i++) {
                if (checkByteOrder(wkbArray[offset]) == 0) {
                    wkbCode = JdbdNumberUtils.readIntFromBigEndian(wkbArray, offset + 1, 4);
                    itemCount = JdbdNumberUtils.readIntFromBigEndian(wkbArray, offset + 5, 4);
                } else {
                    wkbCode = JdbdNumberUtils.readIntFromLittleEndian(wkbArray, offset + 1, 4);
                    itemCount = JdbdNumberUtils.readIntFromLittleEndian(wkbArray, offset + 5, 4);
                }
                wkbType = WkbType.resolve(wkbCode);
                if (wkbType == null) {
                    throw createUnknownWkbTypeError(wkbCode);
                }
                if (wkbType == WkbType.GEOMETRY_COLLECTION) {
                    pairStack.push(new Pair<>(elementCount, i + 1));
                    pairStack.push(new Pair<>(itemCount, 0));
                    offset += 9;
                    break;
                } else {
                    offset = geometryWkbReverse(wkbType, wkbArray, offset);
                }
            }

        }
        return offset;
    }

    /**
     * @see #geometryCollectionWkbReverse(byte[], int)
     * @see #geometryWkbReverse(WkbType, byte[], int)
     */
    public static int triangleWkbReverse(final byte[] wkbArray, int offset) {
        throw new UnsupportedOperationException();
    }

    /**
     * @see #geometryCollectionWkbReverse(byte[], int)
     * @see #geometryWkbReverse(WkbType, byte[], int)
     */
    public static int polyhedralSurfaceWkbReverse(final byte[] wkbArray, int offset) {
        throw new UnsupportedOperationException();
    }

    /**
     * @see #geometryCollectionWkbReverse(byte[], int)
     * @see #geometryWkbReverse(WkbType, byte[], int)
     */
    public static int tinWkbReverse(final byte[] wkbArray, int offset) {
        throw new UnsupportedOperationException();
    }


    public static String pointToWkt(final byte[] wkbArray, int offset) {
        if (wkbArray.length != WKB_POINT_BYTES) {
            throw createWkbLengthError(WkbType.POINT.wktType, wkbArray.length, WKB_POINT_BYTES);
        }
        if (offset < 0 || offset >= wkbArray.length) {
            throw createOffsetError(offset, wkbArray.length);
        }
        final byte byteOrder = checkByteOrder(wkbArray[offset++]);
        final int wkbType;
        final double x, y;
        if (byteOrder == 0) {
            wkbType = JdbdNumberUtils.readIntFromBigEndian(wkbArray, offset, 4);
            if (wkbType != WkbType.POINT.code) {
                throw createWkbTypeNotMatchError(WkbType.POINT.wktType, wkbType);
            }
            offset += 4;
            x = Double.longBitsToDouble(JdbdNumberUtils.readLongFromBigEndian(wkbArray, offset, 8));
            offset += 8;
            y = Double.longBitsToDouble(JdbdNumberUtils.readLongFromBigEndian(wkbArray, offset, 8));
        } else {
            wkbType = JdbdNumberUtils.readIntFromLittleEndian(wkbArray, offset, 4);
            if (wkbType != WkbType.POINT.code) {
                throw createWkbTypeNotMatchError(WkbType.POINT.wktType, wkbType);
            }
            offset += 4;
            x = Double.longBitsToDouble(JdbdNumberUtils.readLongFromLittleEndian(wkbArray, offset, 8));
            offset += 8;
            y = Double.longBitsToDouble(JdbdNumberUtils.readLongFromLittleEndian(wkbArray, offset, 8));
        }
        return String.format("POINT(%s %s)", x, y);
    }

    public static String lineStringToWkt(final byte[] wkbArray, int offset) {
        if (wkbArray.length < HEADER_LENGTH) {
            throw createWkbLengthError(WkbType.LINE_STRING.wktType, wkbArray.length, HEADER_LENGTH);
        }
        if (offset < 0 || offset >= wkbArray.length) {
            throw createOffsetError(offset, wkbArray.length);
        }
        final boolean bigEndian = checkByteOrder(wkbArray[offset++]) == 0;
        final int wkbTypeCode, elementCount;
        wkbTypeCode = JdbdNumberUtils.readIntFromEndian(bigEndian, wkbArray, offset, 4);
        if (wkbTypeCode != WkbType.LINE_STRING.code) {
            throw createWkbTypeNotMatchError(WkbType.LINE_STRING.wktType, wkbTypeCode);
        }
        offset += 4;
        elementCount = JdbdNumberUtils.readIntFromEndian(bigEndian, wkbArray, offset, 4);
        if (elementCount < 2) {
            throw createIllegalElementCount(elementCount);
        }
        offset += 4;
        if (wkbArray.length < (HEADER_LENGTH + (elementCount << 4))) {
            throw createWkbLengthError(WkbType.LINE_STRING.wktType, wkbArray.length
                    , (HEADER_LENGTH + ((long) elementCount << 4)));
        }
        StringBuilder builder = new StringBuilder(elementCount << 3)
                .append(WkbType.LINE_STRING.wktType)
                .append("(");

        for (int i = 0; i < elementCount; i++) {
            if (i > 0) {
                builder.append(",");
            }
            builder.append(JdbdNumberUtils.readDoubleFromEndian(bigEndian, wkbArray, offset, 8))
                    .append(" ");
            offset += 8;
            builder.append(JdbdNumberUtils.readDoubleFromEndian(bigEndian, wkbArray, offset, 8));
            offset += 8;
        }
        builder.append(")");
        return builder.toString();
    }

    /**
     * @see #polygonToWkb(String, boolean)
     */
    public static String polygonToWkt(final byte[] wkbArray, int offset) {
        if (wkbArray.length < HEADER_LENGTH) {
            throw createWkbLengthError(WkbType.POLYGON.wktType, wkbArray.length, HEADER_LENGTH);
        }
        if (offset < 0 || offset >= wkbArray.length) {
            throw createOffsetError(offset, wkbArray.length);
        }
        final boolean bigEndian = checkByteOrder(wkbArray[offset++]) == 0;
        final int wkbCode = JdbdNumberUtils.readIntFromEndian(bigEndian, wkbArray, offset, 4);
        if (wkbCode != WkbType.POLYGON.code) {
            throw createWkbTypeNotMatchError(WkbType.POLYGON.wktType, wkbCode);
        }
        offset += 4;
        final int linearRingCount;
        linearRingCount = JdbdNumberUtils.readIntFromEndian(bigEndian, wkbArray, offset, 4);
        if (linearRingCount < 1) {
            throw createIllegalLinearRingCountError(linearRingCount);
        }
        offset += 4;
        StringBuilder builder = new StringBuilder(linearRingCount << 4)
                .append(WkbType.POLYGON.wktType)
                .append("(");
        final byte[] startPointArray = new byte[16], endPointArray = new byte[startPointArray.length];
        double coordinate;
        for (int i = 0, pointCount; i < linearRingCount; i++) {
            if (i > 0) {
                builder.append(",");
            }
            builder.append("(");
            pointCount = JdbdNumberUtils.readIntFromEndian(bigEndian, wkbArray, offset, 4);
            if (pointCount < 4) {
                throw createIllegalLinearPointCountError(pointCount);
            }
            offset += 4;
            for (int j = 0; j < pointCount; j++) {
                if (j == 0) {
                    System.arraycopy(wkbArray, offset, startPointArray, 0, startPointArray.length);
                }
                if (j == pointCount - 1) {
                    System.arraycopy(wkbArray, offset, endPointArray, 0, endPointArray.length);
                    if (!Arrays.equals(startPointArray, endPointArray)) {
                        throw createNonLinearRingError(i);
                    }
                }
                if (j > 0) {
                    builder.append(",");
                }
                coordinate = JdbdNumberUtils.readDoubleFromEndian(bigEndian, wkbArray, offset, 8);
                offset += 8;
                builder.append(coordinate)
                        .append(" ");
                coordinate = JdbdNumberUtils.readDoubleFromEndian(bigEndian, wkbArray, offset, 8);
                offset += 8;
                builder.append(coordinate);
            }
            builder.append(")");
        }
        return builder.append(")")
                .toString();
    }

    public static byte[] lineStringToWkb(final String wktText, final boolean bigEndian) {

        final BufferWrapper inWrapper = new BufferWrapper(wktText.getBytes(StandardCharsets.US_ASCII));
        //1.read wkt type.
        if (!readWktType(inWrapper, WkbType.LINE_STRING.display())) {
            throw createWktFormatError(WkbType.LINE_STRING.display());
        }
        final ByteBuffer inBuffer = inWrapper.buffer;
        if (!inBuffer.hasRemaining()) {
            throw createWktFormatError(WkbType.LINE_STRING.display());
        }

        try (ByteArrayOutputStream out = new ByteArrayOutputStream(inWrapper.bufferArray.length)) {
            final BufferWrapper outWrapper = new BufferWrapper(inWrapper.bufferArray.length);
            writeWkbPrefix(bigEndian, WkbType.LINE_STRING.code, 0, outWrapper.bufferArray, 0);
            out.write(outWrapper.bufferArray, 0, 9);

            int elementCount = 0;
            while (inBuffer.hasRemaining()) {
                elementCount += readAndWritePoints(bigEndian, 2, false, inWrapper, outWrapper);
                outWrapper.buffer.flip();
                out.write(outWrapper.bufferArray, 0, outWrapper.buffer.limit());
                outWrapper.buffer.clear();
                if (inBuffer.get(inBuffer.position() - 1) == ')') {
                    break;
                }
            }
            if (elementCount < 2) {
                throw new IllegalArgumentException("LineString elementCount must great or equals than 2 .");
            }
            // write elementCount
            byte[] wkbArray = out.toByteArray();
            JdbdNumberUtils.intToEndian(bigEndian, elementCount, wkbArray, 5, 4);
            return wkbArray;
        } catch (IOException e) {
            // no bug ,never here.
            throw new IllegalStateException(e.getMessage(), e);
        }

    }

    public static byte[] polygonToWkb(final String wktText, final boolean bigEndian) {

        final BufferWrapper inWrapper = new BufferWrapper(wktText.getBytes(StandardCharsets.US_ASCII));
        //1.read wkt type.
        if (!readWktType(inWrapper, WkbType.POLYGON.wktType)) {
            throw createWktFormatError(WkbType.POLYGON.wktType);
        }
        final ByteBuffer inBuffer = inWrapper.buffer;
        if (!inBuffer.hasRemaining()) {
            throw createWktFormatError(WkbType.POLYGON.wktType);
        }
        final BufferWrapper outWrapper = new BufferWrapper(inWrapper.bufferArray.length + 8);

        final byte[] inArray = inWrapper.bufferArray, outArray = outWrapper.bufferArray;
        final byte[] startPointArray = new byte[16], endPointArray = new byte[startPointArray.length];
        final int inLimit = inBuffer.limit();
        final ByteBuffer outBuffer = outWrapper.buffer;

        final ByteBuf outChannel = ByteBufAllocator.DEFAULT.buffer(inArray.length, (1 << 30));
        if (bigEndian) {
            outChannel.writeByte(0);
            outChannel.writeInt(WkbType.POLYGON.code);
        } else {
            outChannel.writeByte(1);
            outChannel.writeIntLE(WkbType.POLYGON.code);
        }
        outChannel.writeZero(4);// placeholder of linearRingCount

        int linearRingCount = 0;
        boolean ringEnd;
        for (int inPosition = inBuffer.position(), linearCountIndex; inPosition < inLimit; ) {
            if (Character.isWhitespace(inArray[inPosition])) {
                inPosition++;
                continue;
            }
            if (linearRingCount > 0) {
                if (inArray[inPosition] == ')') {
                    break;
                } else if (inArray[inPosition] != ',') {
                    throw createWktFormatError(WkbType.POLYGON.wktType);
                }
                inPosition++;
                for (; inPosition < inLimit; inPosition++) {
                    if (!Character.isWhitespace(inArray[inPosition])) {
                        break;
                    }
                }
            }
            if (inPosition == inLimit) {
                throw createWktFormatError(WkbType.POLYGON.wktType);
            }
            if (inArray[inPosition] != '(') {
                throw createWktFormatError(WkbType.POLYGON.wktType);
            }
            inPosition++;
            inBuffer.position(inPosition);
            linearCountIndex = outChannel.writerIndex();
            outChannel.writeZero(4);// placeholder of pointCount
            ringEnd = false;
            for (int i = 0, p = inBuffer.position(), startPosition, pointCount = 0; ; i++) {
                startPosition = outBuffer.position();
                pointCount += readAndWritePoints(bigEndian, 2, false, inWrapper, outWrapper);
                if (i == 0) {
                    // copy start point.
                    System.arraycopy(outArray, startPosition, startPointArray, 0, startPointArray.length);
                }
                if (inBuffer.get(inBuffer.position() - 1) == ')') {
                    ringEnd = true;
                    linearRingCount++;
                    System.arraycopy(outArray, outBuffer.position() - 16, endPointArray, 0, endPointArray.length);
                    if (pointCount < 4 || !Arrays.equals(startPointArray, endPointArray)) {
                        throw createWktFormatError(WkbType.POLYGON.wktType);
                    }
                    // output LinearRing count
                    outChannel.markWriterIndex();
                    outChannel.writerIndex(linearCountIndex);
                    if (bigEndian) {
                        outChannel.writeInt(pointCount);
                    } else {
                        outChannel.writeIntLE(pointCount);
                    }
                    outChannel.resetWriterIndex();
                }

                outBuffer.flip();
                outChannel.writeBytes(outArray, 0, outBuffer.limit());
                outBuffer.clear();
                if (ringEnd) {
                    break;
                }
                if (inBuffer.position() == p) {
                    throw createWktFormatError(WkbType.LINE_STRING.wktType);
                }
                p = inBuffer.position();
            }
            inPosition = inBuffer.position();

        }
        if (linearRingCount < 1) {
            throw new IllegalArgumentException("polygon elementCount must great or equals than 1 .");
        }
        // write elementCount

        outChannel.markWriterIndex();
        outChannel.writerIndex(5);
        if (bigEndian) {
            outChannel.writeInt(linearRingCount);
        } else {
            outChannel.writeIntLE(linearRingCount);
        }
        outChannel.resetWriterIndex();
        byte[] wkbArray = new byte[outChannel.readableBytes()];
        outChannel.readBytes(wkbArray);
        return wkbArray;
    }


    /**
     * @return wkb md5
     */
    public static byte[] lineStringToWktPath(final boolean bigEndian, final Path wkbPath, final Path wktPath)
            throws IOException {
        final boolean wktPathExists;
        wktPathExists = Files.exists(wktPath, LinkOption.NOFOLLOW_LINKS);

        try (FileChannel in = FileChannel.open(wkbPath, StandardOpenOption.READ)) {
            try (FileChannel out = FileChannel.open(wktPath, StandardOpenOption.READ)) {
                return lineStringToWktChannel(bigEndian, in, out);
            }
        } catch (Throwable e) {
            if (wktPathExists) {
                JdbdStreamUtils.truncateIfExists(wkbPath, 0L);
            } else {
                Files.deleteIfExists(wkbPath);
            }
            if (e instanceof Error) {
                throw (Error) e;
            } else if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException(e.getMessage(), e);
            }

        }
    }

    /**
     * @return wkb md5
     */
    public static byte[] lineStringToWktChannel(final boolean bigEndian, final FileChannel in, final FileChannel out)
            throws IOException {

        final BufferWrapper inWrapper = new BufferWrapper(1024), outWrapper = new BufferWrapper(1024);
        final ByteBuffer inBuffer = inWrapper.buffer, outBuffer = outWrapper.buffer;
        final byte[] inArray = inWrapper.bufferArray, outArray = outWrapper.bufferArray;

        if (in.read(inBuffer) < 9) {
            throw createWkbTypeNotMatchError(WkbType.LINE_STRING.wktType, WkbType.LINE_STRING.code);
        }
        inBuffer.flip();

        long readBytes = inBuffer.remaining();

        final Pair<Boolean, Integer> pair;
        pair = readWkbHead(inArray, 0, WkbType.LINE_STRING.code);
        inBuffer.position(9);

        final long needBytes = 9 + (Integer.toUnsignedLong(pair.getSecond()) << 4);

        if (in.size() < needBytes) {
            throw createWkbTypeNotMatchError(WkbType.LINE_STRING.wktType, WkbType.LINE_STRING.code);
        }
        final MessageDigest digest = JdbdDigestUtils.createMd5Digest();
        // write LINESTRING(
        outBuffer.put(WkbType.LINE_STRING.wktType.getBytes(StandardCharsets.US_ASCII));
        outBuffer.put((byte) '(');
        writePointsAsWkt(bigEndian, false, inWrapper, outWrapper);


        outBuffer.flip();
        out.write(outBuffer);
        outBuffer.rewind();
        digest.update(outArray, 0, outBuffer.limit());

        outBuffer.clear();

        // cumulate for next read.
        JdbdBufferUtils.cumulate(inBuffer, false);
        boolean outputEndMarker = false;
        for (int readLength; readBytes < needBytes; ) {
            if ((readLength = in.read(inBuffer)) < 0) {
                throw createWkbTypeNotMatchError(WkbType.LINE_STRING.wktType, WkbType.LINE_STRING.code);
            }
            readBytes += readLength;
            inBuffer.flip();

            while (inBuffer.remaining() > 16) {
                writePointsAsWkt(bigEndian, false, inWrapper, outWrapper);

                if (readBytes == needBytes && !inBuffer.hasRemaining()) {
                    outBuffer.put((byte) ')');
                    outputEndMarker = true;
                }

                outBuffer.flip();
                out.write(outBuffer);
                outBuffer.rewind();
                digest.update(outArray, 0, outBuffer.limit());

                outBuffer.clear();
            }
            // cumulate for next read.
            JdbdBufferUtils.cumulate(inBuffer, false);

        }
        if (!outputEndMarker) {
            outBuffer.put((byte) ')');

            outBuffer.flip();
            out.write(outBuffer);
            outBuffer.rewind();
            digest.update(outArray, 0, outBuffer.limit());

        }
        return digest.digest();
    }



    /*################################## blow protected method ##################################*/

    /**
     * @see #equals(byte[], byte[])
     */
    protected static boolean pointReverseEquals(final byte[] pointOne, final byte[] pointTwo) {
        if (pointOne.length != WKB_POINT_BYTES) {
            throw createWkbLengthError(WkbType.POINT.name(), pointOne.length, WKB_POINT_BYTES);
        }
        return JdbdArrayUtils.reverseEquals(pointOne, pointTwo, 5, 8, 2);
    }

    /**
     * @see #equals(byte[], byte[])
     */
    protected static boolean lineStringReverseEquals(final byte[] lineStringOne, final byte[] lineStringTwo) {

        int offset = 5;
        final int elementCount;
        elementCount = JdbdNumberUtils.readIntFromEndian(lineStringOne[0] == 0, lineStringOne, offset, 4);

        boolean match;
        match = JdbdArrayUtils.reverseEquals(lineStringOne, lineStringTwo, offset, 4, 1);
        offset += 4;
        if (match) {
            match = JdbdArrayUtils.reverseEquals(lineStringOne, lineStringTwo, offset, 8, elementCount << 1);
        }
        return match;
    }

    /**
     * @see #equals(byte[], byte[])
     */
    protected static boolean polygonReverseEquals(final byte[] polygonOne, final byte[] polygonTwo) {
        final boolean bigEndian = polygonOne[0] == 0;
        int offset = 5;
        final int elementCount;
        elementCount = JdbdNumberUtils.readIntFromEndian(bigEndian, polygonOne, offset, 4);

        boolean match;
        match = JdbdArrayUtils.reverseEquals(polygonOne, polygonTwo, offset, 4, 1);
        offset += 4;
        if (!match) {
            return false;
        }
        for (int i = 0, pointCount; i < elementCount; i++) {
            if (!JdbdArrayUtils.reverseEquals(polygonOne, polygonTwo, offset, 4, 1)) {
                match = false;
                break;
            }
            pointCount = JdbdNumberUtils.readIntFromEndian(bigEndian, polygonOne, offset, 4);
            if (pointCount < 4) {
                throw createIllegalLinearPointCountError(pointCount);
            }
            offset += 4;
            if (!JdbdArrayUtils.reverseEquals(polygonOne, polygonTwo, offset, 8, pointCount << 1)) {
                match = false;
                break;
            }
        }
        return match;
    }

    /**
     * @return new offset
     * @see #multiPolygonWkbReverse(byte[], int)
     */
    protected static int polygonElementWkbReverse(final WkbType wkbType, final byte[] wkbArray, int offset
            , final int elementCount) {
        final boolean bigEndian = wkbArray[0] == 1;// reversed.

        for (int i = 0, linearCount; i < elementCount; i++) {
            linearCount = JdbdNumberUtils.readIntFromEndian(bigEndian, wkbArray, offset, 4);
            if (linearCount < 0) {
                throw createIllegalWkbLengthError(wkbArray.length, offset + Integer.toUnsignedLong(linearCount));
            }
            JdbdArrayUtils.reverse(wkbArray, offset, 4, 1); // reverse lineCount
            offset += 4;
            offset = lineStringElementReverse(wkbType, linearCount, wkbArray, offset);
        }
        return offset;
    }

    /**
     * @see #polygonWkbReverse(byte[], int)
     * @see #multiLineStringWkbReverse(byte[], int)
     */
    protected static int lineStringElementReverse(final WkbType wkbType, final int elementCount
            , final byte[] wkbArray, int offset) {
        final boolean bigEndian = wkbArray[0] == 1;// reversed.
        long needBytes = offset, pointCount;
        for (int i = 0; i < elementCount; i++) {
            needBytes += 4;
            if (wkbArray.length < needBytes) {
                throw createWkbLengthError(wkbType.wktType, wkbArray.length, needBytes);
            }
            if (bigEndian) {
                pointCount = JdbdNumberUtils.readIntFromBigEndian(wkbArray, offset, 4)
                        & JdbdNumberUtils.MAX_UNSIGNED_INT;
            } else {
                pointCount = JdbdNumberUtils.readIntFromLittleEndian(wkbArray, offset, 4)
                        & JdbdNumberUtils.MAX_UNSIGNED_INT;
            }
            needBytes += (pointCount << 4);

            if (wkbArray.length < needBytes) {
                throw createWkbLengthError(wkbType.wktType, wkbArray.length, needBytes);
            }
            JdbdArrayUtils.reverse(wkbArray, offset, 4, 1);
            offset += 4;
            JdbdArrayUtils.reverse(wkbArray, offset, 8, (int) pointCount << 1);
            offset += (pointCount << 4);
        }
        return offset;
    }

    protected static void writePointsAsWkt(final boolean bigEndian, final boolean pointText
            , final BufferWrapper inWrapper, final BufferWrapper outWrapper) {

        final ByteBuffer inBuffer = inWrapper.buffer, outBuffer = outWrapper.buffer;
        final byte[] inArray = inWrapper.bufferArray, outArray = outWrapper.bufferArray;
        final int inLimit = inBuffer.limit(), outLimit = outBuffer.limit();

        final boolean firstBuffer = Character.isLetter(outArray[0]);
        int inPosition = inBuffer.position(), outPosition = outBuffer.position();
        byte[] xBytes, yBytes;
        double x, y;
        for (int writeNeedBytes, i = 0; ; i++) {
            if (inLimit - inPosition < 16) {
                break;
            }
            x = JdbdNumberUtils.readDoubleFromEndian(bigEndian, inArray, inPosition, 8);
            y = JdbdNumberUtils.readDoubleFromEndian(bigEndian, inArray, inPosition + 8, 8);
            xBytes = Double.toString(x).getBytes(StandardCharsets.US_ASCII);
            yBytes = Double.toString(y).getBytes(StandardCharsets.US_ASCII);

            writeNeedBytes = xBytes.length + 1 + yBytes.length;
            if (!firstBuffer || i > 0) {
                writeNeedBytes += 1;
            }
            if (pointText) {
                writeNeedBytes += 2;
            }

            if (outLimit - outPosition < writeNeedBytes) {
                break;
            }


            inPosition += 16;

            if (firstBuffer) {
                if (i > 0) {
                    outArray[outPosition++] = (byte) ',';
                }
            } else {
                outArray[outPosition++] = (byte) ',';
            }

            if (pointText) {
                outArray[outPosition++] = (byte) '(';
            }

            for (byte b : xBytes) {
                outArray[outPosition++] = b;
            }
            outArray[outPosition++] = (byte) ' ';
            for (byte b : yBytes) {
                outArray[outPosition++] = b;
            }
            if (pointText) {
                outArray[outPosition++] = (byte) ')';
            }


        }
        inBuffer.position(inPosition);
        outBuffer.position(outPosition);

    }


    public static byte[] lineStringPathToWkbFromPath(final Path wktPath, final long offset, final boolean bigEndian
            , final Path wkbPath) throws IOException {

        final boolean wkbPathExists;
        wkbPathExists = Files.exists(wkbPath, LinkOption.NOFOLLOW_LINKS);

        try (FileChannel in = FileChannel.open(wktPath, StandardOpenOption.READ)) {
            final long hasBytes;
            hasBytes = handleOffset(in, offset);
            final BufferWrapper inWrapper = new BufferWrapper((int) Math.min(hasBytes, 2048));
            final ByteBuffer inBuffer = inWrapper.buffer;
            //1.read wkt type.
            final int typeLength = WkbType.LINE_STRING.wktType.length();
            boolean validate = false;
            for (int readLength; (readLength = in.read(inBuffer)) > 0; ) {
                if (readLength < typeLength) {
                    throw createWktFormatError(WkbType.LINE_STRING.name());
                }
                inBuffer.flip();
                if (readWktType(inWrapper, WkbType.LINE_STRING.name())) {
                    validate = true;
                    break;
                }

            }
            if (!validate) {
                throw createWktFormatError(WkbType.LINE_STRING.name());
            }

            try (FileChannel out = FileChannel.open(wkbPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
                // 2. parse and write wkb temp file.
                return writeLineStringWkbToPath(in, out, bigEndian, inWrapper);
            }
        } catch (Throwable e) {
            if (wkbPathExists) {
                JdbdStreamUtils.truncateIfExists(wkbPath, 0L);
            } else {
                Files.deleteIfExists(wkbPath);
            }
            if (e instanceof Error) {
                throw (Error) e;
            } else if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException(e.getMessage(), e);
            }

        }

    }

    /*################################## blow protected method ##################################*/

    /**
     * @return new offset.
     * @see #reverseWkb(byte[], int)
     */
    protected static int geometryWkbReverse(final WkbType wkbType, final byte[] wkbArray, int offset) {
        switch (wkbType) {
            case POINT:
                offset = pointWkbReverse(wkbArray, offset);
                break;
            case LINE_STRING:
                offset = lineStringWkbReverse(wkbArray, offset);
                break;
            case POLYGON:
                offset = polygonWkbReverse(wkbArray, offset);
                break;
            case TRIANGLE:
                offset = triangleWkbReverse(wkbArray, offset);
                break;
            case POLYHEDRAL_SURFACE:
                offset = polyhedralSurfaceWkbReverse(wkbArray, offset);
                break;
            case TIN:
                offset = tinWkbReverse(wkbArray, offset);
                break;
            case MULTI_POINT:
                offset = multiPointWkbReverse(wkbArray, offset);
                break;
            case MULTI_LINE_STRING:
                offset = multiLineStringWkbReverse(wkbArray, offset);
                break;
            case MULTI_POLYGON:
                offset = multiPolygonWkbReverse(wkbArray, offset);
                break;
            case GEOMETRY_COLLECTION:
                offset = geometryCollectionWkbReverse(wkbArray, offset);
                break;
            case GEOMETRY:
            case CIRCULAR_STRING:
            case COMPOUND_CURVE:
            case CURVE_POLYGON:
            case MULTI_CURVE:
            case MULTI_SURFACE:
            case CURVE:
            case SURFACE:
                throw new IllegalArgumentException(String.format("%s not supported type[%s]."
                        , WkbType.GEOMETRY, wkbType.wktType));
            default:
                throw JdbdExceptions.createUnknownEnumException(wkbType);
        }

        return offset;
    }

    /**
     * @return first:true big-endian,second : pointSize
     */
    protected static Pair<Boolean, Integer> readWkbHead(final byte[] wkbBytes, int offset, final int expectWkbType) {
        if (wkbBytes.length < 9) {
            throw new IllegalArgumentException(String.format("wkbBytes length[%s] less than 9.", wkbBytes.length));
        }
        if (wkbBytes.length - offset < 9) {
            throw new IllegalArgumentException(String.format("wkbBytes length[%s] - offset less than 9."
                    , wkbBytes.length));
        }
        // 1. below parse byteOrder,wkbType,pointSize
        final int wkbType, pointSize;
        final byte byteOrder = wkbBytes[offset++];
        if (byteOrder == 0) {
            // big-endian
            wkbType = JdbdNumberUtils.readIntFromBigEndian(wkbBytes, offset, 4);
            if (wkbType != expectWkbType) {
                throw new IllegalArgumentException(String.format("WKB-TYPE[%s] isn't [%s]"
                        , wkbType, expectWkbType));
            }
            offset += 4;
            pointSize = JdbdNumberUtils.readIntFromBigEndian(wkbBytes, offset, 4);
        } else if (byteOrder == 1) {
            // little-endian
            wkbType = JdbdNumberUtils.readIntFromLittleEndian(wkbBytes, offset, 4);
            if (wkbType != expectWkbType) {
                throw new IllegalArgumentException(String.format("WKB-TYPE[%s] isn't [%s]"
                        , wkbType, expectWkbType));
            }
            offset += 4;
            pointSize = JdbdNumberUtils.readIntFromLittleEndian(wkbBytes, offset, 4);
        } else {
            throw createIllegalByteOrderError(byteOrder);
        }
        return new Pair<>(byteOrder == 0, pointSize);
    }

    /**
     * @return false , more read , true read success.
     * @see #lineStringPathToWkbFromPath(Path, long, boolean, Path)
     */
    protected static boolean readWktType(final BufferWrapper inWrapper, final String wktType)
            throws IllegalArgumentException {

        final ByteBuffer inBuffer = inWrapper.buffer;
        final byte[] inArray = inWrapper.bufferArray;

        final int typeLength = wktType.length();
        int inLimit, inPosition, headerIndex;

        inLimit = inBuffer.limit();
        for (inPosition = inBuffer.position(); inPosition < inLimit; inPosition++) {
            if (!Character.isWhitespace(inArray[inPosition])) {
                break;
            }
        }
        if (inLimit - inPosition < typeLength) {
            inBuffer.position(inPosition);
            return false;
        }
        headerIndex = inPosition;
        for (int i = 0; i < typeLength; i++, inPosition++) {
            if (inArray[inPosition] != wktType.charAt(i)) {
                throw createWktFormatError(wktType);
            }
        }
        for (; inPosition < inLimit; inPosition++) {
            if (!Character.isWhitespace(inArray[inPosition])) {
                break;
            }
        }
        if (inLimit - inPosition < 1) {
            inBuffer.position(headerIndex);
            return false;
        }
        if (inArray[inPosition] != '(') {
            throw createWktFormatError(wktType);
        }
        inBuffer.position(inPosition + 1);
        return true;
    }

    /**
     * @return count of points.
     * @see #lineStringPathToWkbFromPath(Path, long, boolean, Path)
     */
    protected static int readAndWritePoints(final boolean bigEndian, final int coordinates, final boolean pointText
            , final BufferWrapper inWrapper, final BufferWrapper outWrapper)
            throws IllegalArgumentException {

        if (coordinates < 2 || coordinates > 4) {
            throw new IllegalArgumentException(String.format("coordinates[%s] error.", coordinates));
        }
        final byte[] inArray = inWrapper.bufferArray, outArray = outWrapper.bufferArray;
        final ByteBuffer inBuffer = inWrapper.buffer, outBuffer = outWrapper.buffer;

        final int inLimit = inBuffer.limit(), outLimit = outBuffer.limit();

        int codePoint, pointCount = 0, inPosition = inBuffer.position(), outPosition = outBuffer.position();
        topFor:
        for (int tempOutPosition, tempInPosition, pointEndIndex; inPosition < inLimit; ) {
            codePoint = inArray[inPosition];
            if (Character.isWhitespace(codePoint)) {
                inPosition++;
                continue;
            }
            if (outLimit - outPosition < (coordinates << 3)) {
                break;
            }
            tempInPosition = inPosition;
            tempOutPosition = outPosition;
            if (pointText) {
                if (codePoint != '(') {
                    throw new IllegalArgumentException("Not found '(' for point text.");
                }
                tempInPosition++;
            }

            //parse coordinates and write to outArray.
            for (int coor = 1, startIndex, endIndex = tempInPosition - 1, p; coor <= coordinates; coor++) {
                startIndex = -1;
                for (p = endIndex + 1; p < inLimit; p++) {
                    if (!Character.isWhitespace(inArray[p])) {
                        startIndex = p;
                        break;
                    }
                }
                if (startIndex < 0) {
                    break topFor;
                }
                endIndex = -1;
                for (p = startIndex + 1; p < inLimit; p++) {
                    if (coor == coordinates) {
                        // last coordinate.
                        codePoint = inArray[p];
                        if (pointText) {
                            if (codePoint == ')' || Character.isWhitespace(codePoint)) {
                                endIndex = p;
                                break;
                            }
                        } else if (codePoint == ',' || codePoint == ')' || Character.isWhitespace(codePoint)) {
                            endIndex = p;
                            break;
                        }
                    } else if (Character.isWhitespace(inArray[p])) {
                        endIndex = p;
                        break;
                    }
                }
                if (endIndex < 0) {
                    if (p - startIndex > 24) {
                        // non-double number
                        byte[] nonDoubleBytes = Arrays.copyOfRange(inArray, startIndex, p);
                        throw createNonDoubleError(new String(nonDoubleBytes));
                    }
                    break topFor;
                }
                double d = Double.parseDouble(new String(Arrays.copyOfRange(inArray, startIndex, endIndex)));
                JdbdNumberUtils.doubleToEndian(bigEndian, d, outArray, tempOutPosition);
                tempOutPosition += 8;
                tempInPosition = endIndex;
            }// parse coordinates and write to outArray.
            // below find point end index
            pointEndIndex = -1;
            for (int parenthesisCount = 0; tempInPosition < inLimit; tempInPosition++) {
                codePoint = inArray[tempInPosition];
                if (Character.isWhitespace(codePoint)) {
                    continue;
                }
                if (codePoint == ')') {
                    parenthesisCount++;
                    if (pointText) {
                        if (parenthesisCount == 2) {
                            pointEndIndex = tempInPosition;
                            break;
                        }
                    } else {
                        pointEndIndex = tempInPosition;
                        break;
                    }
                } else if (codePoint == ',') {
                    if (pointText && parenthesisCount == 0) {
                        throw new IllegalArgumentException("point text not close.");
                    }
                    pointEndIndex = tempInPosition;
                    break;
                } else {
                    throw new IllegalArgumentException(String.format("point end with %s.", (char) codePoint));
                }
            }
            if (pointEndIndex < 0) {
                break;
            }
            outPosition = tempOutPosition;
            inPosition = pointEndIndex + 1;
            pointCount++;
            if (inArray[pointEndIndex] == ')') {
                break;
            }
        }
        inBuffer.position(inPosition);
        outBuffer.position(outPosition);

        return pointCount;
    }

    /**
     * @return temp file md5.
     */
    protected static byte[] writeLineStringWkbToPath(final FileChannel in, final FileChannel out
            , final boolean bigEndian, final BufferWrapper inWrapper)
            throws IOException, IllegalArgumentException {

        final byte[] inArray = inWrapper.bufferArray;
        final ByteBuffer inBuffer = inWrapper.buffer;

        final BufferWrapper outWrapper = new BufferWrapper(inArray.length);
        final byte[] outArray = outWrapper.bufferArray;
        final ByteBuffer outBuffer = outWrapper.buffer;

        final MessageDigest digest = JdbdDigestUtils.createMd5Digest();
        //1. write wkb header
        // 0 is  placeholder of element count.
        writeWkbPrefix(bigEndian, WkbType.LINE_STRING.code, 0, outArray, 0);
        outBuffer.position(9);
        //2. read and write points.
        long elementCount = 0L;
        boolean lineStringEnd = false;
        for (int endIndex; in.read(inBuffer) > 0; ) {
            inBuffer.flip();
            elementCount += readAndWritePoints(bigEndian, 2, false, inWrapper, outWrapper);
            endIndex = inBuffer.position() - 1;
            if (endIndex > -1 && inBuffer.get(endIndex) == ')') {
                lineStringEnd = true;
                outBuffer.flip();
                digest.update(outArray, 0, outBuffer.limit());
                out.write(outBuffer);
                outBuffer.clear();
                break;
            }
            if (outBuffer.remaining() < 16) {
                outBuffer.flip();
                digest.update(outArray, 0, outBuffer.limit());
                out.write(outBuffer);
                outBuffer.clear();
            }
            JdbdBufferUtils.cumulate(inBuffer, true);

        }
        if (!lineStringEnd) {
            throw new IllegalArgumentException("LineString not end.");
        }
        if (elementCount < 2 || elementCount > JdbdNumberUtils.MAX_UNSIGNED_INT) {
            throw new IllegalArgumentException(String.format("LineString length[%s] not in[2,%s]"
                    , elementCount, JdbdNumberUtils.MAX_UNSIGNED_INT));
        }
        // 3. write element count.
        out.position(5L); // to element count position.
        JdbdNumberUtils.intToLittleEndian((int) elementCount, outArray, 0, 4);
        outBuffer.position(4);
        outBuffer.flip();
        digest.update(outArray, 0, outBuffer.limit());
        out.write(outBuffer);

        return digest.digest();
    }

    /**
     * @return byte count of {@link FileChannel} hold.
     */
    protected static long handleOffset(FileChannel in, final long offset) throws IOException {
        final long hasBytes;
        if (offset > 0L) {
            hasBytes = in.size() - offset;
            if (hasBytes > 0L) {
                in.position(offset);
            }
        } else {
            hasBytes = in.size();
        }
        if (hasBytes < 9L) {
            throw new IOException("Not found WKB.");
        }
        return hasBytes;
    }


    protected static void writeWkbPrefix(final boolean bigEndian, final int wkbType, final int elementCount
            , final byte[] wkbBytes, int offset) {
        if (wkbBytes.length < 9) {
            throw new IllegalArgumentException(String.format("wkbBytes length[%s] less than 9", wkbBytes.length));
        }
        if (wkbBytes.length - offset < 9) {
            throw new IllegalArgumentException(String.format("wkbBytes (length[%s]-offset[%s]) less than 9"
                    , wkbBytes.length, offset));
        }
        if (bigEndian) {
            wkbBytes[offset++] = 0;
            JdbdNumberUtils.intToBigEndian(wkbType, wkbBytes, offset, 4);
            offset += 4;
            JdbdNumberUtils.intToBigEndian(elementCount, wkbBytes, offset, 4);
        } else {
            wkbBytes[offset++] = 1;
            JdbdNumberUtils.intToLittleEndian(wkbType, wkbBytes, offset, 4);
            offset += 4;
            JdbdNumberUtils.intToLittleEndian(elementCount, wkbBytes, offset, 4);
        }

    }


    /*################################## blow private method ##################################*/


    private static IllegalArgumentException createWktFormatError(String wktType) {
        return new IllegalArgumentException(String.format("Not %s WKT format.", wktType));
    }


    protected static IllegalArgumentException createNonDoubleError(String nonDouble) {
        return new IllegalArgumentException(String.format("%s isn't double number.", nonDouble));
    }


}
