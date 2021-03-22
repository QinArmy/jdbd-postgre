package io.jdbd.vendor.geometry;

import io.jdbd.type.geometry.Geometry;
import io.jdbd.type.geometry.GeometryFactory;
import io.jdbd.type.geometry.LineString;
import io.jdbd.type.geometry.Point;
import io.jdbd.vendor.util.JdbdBufferUtils;
import io.jdbd.vendor.util.JdbdDigestUtils;
import io.jdbd.vendor.util.JdbdNumberUtils;
import org.qinarmy.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public abstract class DefaultGeometryFactory implements GeometryFactory {

    protected DefaultGeometryFactory() {
        throw new UnsupportedOperationException();
    }

    static final int MAX_ARRAY_LENGTH = (1 << 30);

    static final long MAX_UNSIGNED_INT = 0xFFFF_FFFFL;

    private static final Logger LOG = LoggerFactory.getLogger(DefaultGeometryFactory.class);


    public final Point point(double x, double y) {
        return (x == 0.0D && y == 0.0D) ? DefaultPoint.ZERO : new DefaultPoint(x, y);
    }

    public LineString line(Point one, Point two) {
        return MemoryLineString.line(one, two);
    }

    public LineString lineString(List<Point> pointList) {
        return MemoryLineString.create(pointList);
    }

    @Override
    public LineString lineStringFromWkt(String wkt, int offset) {
        return null;
    }

    public Geometry geometryFromWkb(final byte[] wkbBytes, final int offset)
            throws IllegalArgumentException {
        if (wkbBytes.length < 5) {
            throw createWkbFormatError(wkbBytes.length, 5);
        }
        if (wkbBytes.length - offset < 5) {
            throw createOffsetError(offset, wkbBytes.length);
        }

        final byte byteOrder = wkbBytes[offset];
        final int wkbType;
        if (byteOrder == 0) {
            // big-endian
            wkbType = JdbdNumberUtils.readIntFromBigEndian(wkbBytes, offset + 1);
        } else if (byteOrder == 1) {
            // little-endian
            wkbType = JdbdNumberUtils.readIntFromLittleEndian(wkbBytes, offset + 1, 4);
        } else {
            throw createIllegalByteOrderError(byteOrder);
        }
        final Geometry geometry;
        switch (wkbType) {
            case Point.WKB_TYPE_POINT:
                geometry = pointFromWkb(wkbBytes, offset);
                break;
            case LineString.WKB_TYPE_LINE_STRING:
                geometry = lineStringFromWkb(wkbBytes, offset);
                break;
            default:
                throw createUnknownWkbTypeError(wkbType);
        }
        return geometry;
    }

    /**
     * <p>
     * Point WKB format:
     *     <ol>
     *         <li>Byte order,1 byte,{@code 0x00}(big-endian) or {@code 0x01}(little-endian)</li>
     *         <li>WKB typ,4 bytes,must be {@link Point#WKB_TYPE_POINT}</li>
     *         <li>X coordinate,8 bytes,double number</li>
     *         <li>Y coordinate,8 bytes,double number</li>
     *     </ol>
     * </p>
     */
    public Point pointFromWkb(final byte[] wkbBytes, int offset) throws IllegalArgumentException {
        if (wkbBytes.length < Point.WKB_BYTES) {
            throw createWkbFormatError(wkbBytes.length, Point.WKB_BYTES);
        }
        if (wkbBytes.length - offset < Point.WKB_BYTES) {
            throw createOffsetError(offset, wkbBytes.length);
        }

        final int wkbType;
        final byte byteOrder = wkbBytes[offset++];
        if (byteOrder == 0) {
            // big-endian
            wkbType = JdbdNumberUtils.readIntFromBigEndian(wkbBytes, offset, 4);
            if (wkbType != Point.WKB_TYPE_POINT) {
                throw new IllegalArgumentException(String.format("WKB-TYPE[%s] isn't point[1]", wkbType));
            }
        } else if (byteOrder == 1) {
            // little-endian
            wkbType = JdbdNumberUtils.readIntFromLittleEndian(wkbBytes, offset, 4);
            if (wkbType != Point.WKB_TYPE_POINT) {
                throw new IllegalArgumentException(String.format("WKB-TYPE[%s] isn't point[1]", wkbType));
            }
        } else {
            throw createIllegalByteOrderError(byteOrder);
        }
        offset += 4;
        return doPointFromWkb(wkbBytes, byteOrder == 0, offset);
    }

    /**
     * @see #pointFromWkb(byte[], int)
     */
    static Point doPointFromWkb(final byte[] wkbBytes, final boolean bigEndian, int offset) {
        final long xBits, yBits;
        if (bigEndian) {
            xBits = JdbdNumberUtils.readLongFromBigEndian(wkbBytes, offset, 8);
            offset += 8;
            yBits = JdbdNumberUtils.readLongFromBigEndian(wkbBytes, offset, 8);
        } else {
            xBits = JdbdNumberUtils.readLongFromLittleEndian(wkbBytes, offset, 8);
            offset += 8;
            yBits = JdbdNumberUtils.readLongFromLittleEndian(wkbBytes, offset, 8);
        }
        return point(Double.longBitsToDouble(xBits), Double.longBitsToDouble(yBits));
    }

    /**
     * @see PathLineString#writeAsWkt(IoConsumer)
     */
    static void doPointsWkbToWkt(final byte[] wkbBytes, final boolean bigEndian, final StringBuilder builder
            , final boolean comma) {
        if ((wkbBytes.length & 0xF) != 0) {
            // mean length % 16 != 0
            throw new IllegalArgumentException("wkbBytes error.");
        }

        long xBits, yBits;
        for (int i = 0; i < wkbBytes.length; i += 16) {
            if (comma || i > 0) {
                builder.append(",");
            }
            if (bigEndian) {
                xBits = JdbdNumberUtils.readLongFromBigEndian(wkbBytes, i, 8);
                yBits = JdbdNumberUtils.readLongFromBigEndian(wkbBytes, i + 8, 8);
            } else {
                xBits = JdbdNumberUtils.readLongFromLittleEndian(wkbBytes, i, 8);
                yBits = JdbdNumberUtils.readLongFromLittleEndian(wkbBytes, i + 8, 8);
            }
            builder.append(Double.longBitsToDouble(xBits))
                    .append(" ")
                    .append(Double.longBitsToDouble(yBits));

        }

    }

    /**
     * <p>
     * eg: {@code POINT(0.0 0.1)}
     * </p>
     */
    @Override
    public Point pointFromWkt(final String wkt, int offset) throws IllegalArgumentException {
        final String startMarker = "POINT(", endMarker = ")";
        if (!wkt.startsWith(startMarker) || !wkt.endsWith(endMarker)) {
            throw createWktFormatError(null, wkt);
        }
        final int index = wkt.indexOf(" ", startMarker.length());
        if (index < 0) {
            throw createWktFormatError(null, wkt);
        }

        try {
            final double x, y;
            x = Double.parseDouble(wkt.substring(startMarker.length(), index));
            y = Double.parseDouble(wkt.substring(index + 1, wkt.length() - 1));
            return point(x, y);
        } catch (NumberFormatException e) {
            throw createWktFormatError(e, wkt);
        }
    }

    @Override
    public LineString lineStringFromWkb(final byte[] wkbBytes, int offset) {
        final Pair<Boolean, Integer> pair;
        pair = readWkbHead(wkbBytes, offset, LineString.WKB_TYPE_LINE_STRING);
        offset += 9;

        final boolean bigEndian = pair.getFirst();
        final int pointSize = pair.getSecond();

        // 2. below parse point list.
        final int end = offset + (pointSize * 16);
        if (wkbBytes.length < end) {
            throw createWkbFormatError(wkbBytes.length, end);
        }

        List<Point> pointList = new ArrayList<>(pointSize);
        // each  point need a comma.
        int textLength = pointSize - 1;
        Point point;
        for (int i = 0; i < pointSize; i++) {
            point = doPointFromWkb(wkbBytes, bigEndian, offset);
            if (textLength > 0) {
                textLength += point.getPointTextLength();
            }
            pointList.add(point);
            offset += 16;
        }
        return MemoryLineString.unsafeLineString(pointList, textLength);
    }


    public static LineString lineStringFromWkt(final String wkt) {
        final String startMarker = "LINESTRING(", endMarker = ")";
        if (!wkt.startsWith(startMarker) || !wkt.endsWith(endMarker)) {
            throw createWktFormatError(null, wkt);
        }

        String pointsSegment = wkt.substring(startMarker.length(), wkt.length() - 1);
        final String[] pairArray = pointsSegment.split(",");
        if (pairArray.length < 2 || pairArray.length > MemoryLineString.BOUNDARY_POINT_LIST_SIZE) {
            throw createSmallLineSizeError(pairArray.length);
        }

        try {
            List<Point> pointList = new ArrayList<>(pairArray.length);
            String pair;
            double x, y;
            Point point;
            // each  point need a comma.
            int textLength = pairArray.length - 1;
            for (int i = 0, spaceIndex; i < pairArray.length; i++) {
                pair = pairArray[i];
                spaceIndex = pair.indexOf(' ');
                if (spaceIndex < 0) {
                    throw createWktFormatError(null, wkt);
                }
                x = Double.parseDouble(pair.substring(0, spaceIndex));
                y = Double.parseDouble(pair.substring(spaceIndex + 1));
                point = point(x, y);
                if (textLength > 0) {
                    textLength += point.getPointTextLength();
                }
                pointList.add(point);
            }
            return MemoryLineString.unsafeLineString(pointList, textLength);
        } catch (NumberFormatException e) {
            throw createWktFormatError(e, wkt);
        }
    }

    public static LineString lineStringFromWkbPath(final Path path, final long offset) throws IOException {
        try (FileChannel in = FileChannel.open(path, StandardOpenOption.READ)) {
            long hasBytes;
            hasBytes = handleOffset(in, offset);
            final byte[] wkbArray = new byte[9];
            if (in.read(ByteBuffer.wrap(wkbArray)) != wkbArray.length) {
                throw new IOException(createWkbFormatError(hasBytes, wkbArray.length));
            }
            final Pair<Boolean, Integer> pair;
            pair = readWkbHead(wkbArray, 0, LineString.WKB_TYPE_LINE_STRING);
            return memoryLineStringFromWkbPath(in, pair.getFirst(), pair.getSecond());
        } catch (IllegalArgumentException | IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException(e.getMessage(), e);
        }

    }

    /*################################## blow packet static method ##################################*/

    /**
     * @see PathLineString#fromWktPath(Path, long)
     */
    static void readWktType(final FileChannel in, ByteBuffer buffer, byte[] bufferArray, final String wktType)
            throws IOException, IllegalArgumentException {

        final int typeLength = wktType.length();
        for (int limit, position, headerIndex; ; ) {
            if (in.read(buffer) < typeLength) {
                throw createNoWktHeaderError(wktType);
            }

            buffer.flip();

            limit = buffer.limit();
            for (position = buffer.position(); position < limit; position++) {
                if (!Character.isWhitespace(bufferArray[position])) {
                    break;
                }
            }
            if (limit - position < typeLength) {
                buffer.position(position);
                JdbdBufferUtils.cumulate(buffer, false);
                continue;
            }
            headerIndex = position;
            for (int i = 0; i < typeLength; i++, position++) {
                if (bufferArray[position] != wktType.charAt(i)) {
                    throw createWktTypeNotMatch(wktType);
                }
            }
            for (; position < limit; position++) {
                if (!Character.isWhitespace(bufferArray[position])) {
                    break;
                }
            }
            if (limit - position < 1) {
                buffer.position(headerIndex);
                JdbdBufferUtils.cumulate(buffer, true);
                continue;
            }
            if (bufferArray[position] != '(') {
                throw createNoWktHeaderError(wktType);
            }
            buffer.position(position + 1);
            break;
        }

    }

    /**
     * @param outBuffer always out little endian .
     * @see PathLineString#fromWktPath(Path, long)
     */
    static int readAndWritePoints(final boolean pointText, final int coordinates, ByteBuffer inBuffer, byte[] inArray
            , ByteBuffer outBuffer, byte[] outArray) throws IllegalArgumentException {

        if (coordinates < 2 || coordinates > 4) {
            throw new IllegalArgumentException(String.format("coordinates[%s] error.", coordinates));
        }
        final int inLimit = inBuffer.limit(), outLimit = outBuffer.limit();

        int codePoint, pointCount = 0, inPosition = inBuffer.position(), outPosition = outBuffer.position();
        topFor:
        for (int tempOutPosition, tempInPosition, pointEndIndex; inPosition < inLimit; ) {
            codePoint = inArray[inPosition];
            if (Character.isWhitespace(codePoint)) {
                inPosition++;
                continue;
            }
            if (outLimit - outPosition < coordinates << 3) {
                break;
            }
            tempInPosition = inPosition;
            tempOutPosition = outPosition;
            if (pointText) {
                if (codePoint != '(') {
                    throw createWktFormatErrorWithDetail("Not found '(' for point text.");
                }
                tempInPosition++;
            }

            //parse coordinates and write to outArray.
            for (int i = 1, startIndex, endIndex = inPosition - 1, tempEndIndex; i <= coordinates; i++) {
                startIndex = -1;
                for (tempInPosition = endIndex + 1; tempInPosition < inLimit; tempInPosition++) {
                    if (!Character.isWhitespace(inArray[tempInPosition])) {
                        startIndex = tempInPosition;
                        break;
                    }
                }
                if (startIndex < 0) {
                    break topFor;
                }
                endIndex = -1;
                for (tempInPosition = startIndex + 1; tempInPosition < inLimit; tempInPosition++) {
                    if (i == coordinates) {
                        // last coordinate.
                        codePoint = inArray[tempInPosition];
                        if (pointText) {
                            if (codePoint == ')' || Character.isWhitespace(codePoint)) {
                                endIndex = tempInPosition;
                            }
                        } else if (codePoint == ',' || codePoint == ')' || Character.isWhitespace(codePoint)) {
                            endIndex = tempInPosition;
                        }
                    } else if (Character.isWhitespace(inArray[tempInPosition])) {
                        endIndex = tempInPosition;
                        break;
                    }
                }
                if (endIndex < 0) {
                    if (tempInPosition - startIndex > 24) {
                        // non-double number
                        byte[] nonDoubleBytes = Arrays.copyOfRange(inArray, startIndex, tempInPosition);
                        throw DefaultGeometryFactory.createNonDoubleError(new String(nonDoubleBytes));
                    }
                    break topFor;
                }
                double d = Double.parseDouble(new String(Arrays.copyOfRange(inArray, startIndex, endIndex)));
                JdbdNumberUtils.doubleToEndian(false, d, outArray, tempOutPosition);
                tempOutPosition += 8;
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
                        throw createWktFormatErrorWithDetail("point text not close.");
                    }
                    pointEndIndex = tempInPosition;
                    break;
                } else {
                    throw createWktFormatErrorWithDetail(String.format("point end with %s.", (char) codePoint));
                }
            }
            if (pointEndIndex < 0) {
                break;
            }
            outPosition = tempOutPosition;
            inPosition = pointEndIndex + 1;
            pointCount++;
            if (inArray[pointEndIndex] == ')') {
                // invoker
                break;
            }
        }
        inBuffer.position(inPosition);
        outBuffer.position(outPosition);

        return pointCount;
    }

    /**
     * @return byte count of {@link FileChannel} hold.
     * @see #lineStringFromWkbPath(Path, long)
     */
    static long handleOffset(FileChannel in, final long offset) throws IOException {
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

    /**
     * @see #lineStringFromWkbPath(Path, long)
     */
    static LineString memoryLineStringFromWkbPath(final FileChannel in, final boolean bigEndian, final int pointSize)
            throws IOException {
        if (pointSize < 0 || pointSize > MAX_ARRAY_LENGTH) {
            throw new IllegalArgumentException("pointSize too large.");
        }

        final List<Point> pointList = new ArrayList<>(pointSize);
        int textLength;
        textLength = readLineStringPoints(in, bigEndian, pointSize, pointList::add, JdbdDigestUtils.createMd5Digest());
        return MemoryLineString.unsafeLineString(pointList, textLength);
    }


    /**
     * @return length of text.
     * @see #memoryLineStringFromWkbPath(FileChannel, boolean, int)
     * @see PathLineString#pointStream()
     */
    static int readLineStringPoints(final FileChannel in, final boolean bigEndian, final int pointSize
            , final Consumer<Point> pointConsumer, final MessageDigest digest) throws IOException {

        // each  point need a comma.
        final int[] textLength = new int[]{pointSize - 1};
        final IoConsumer<byte[]> bytesConsumer = wkb -> {
            Point point;
            for (int offset = 0; offset < wkb.length; offset += 16) {
                point = doPointFromWkb(wkb, bigEndian, offset);
                if (textLength[0] > 0) {
                    textLength[0] += point.getPointTextLength();
                }
                pointConsumer.accept(point);
            }
        };
        readPointsWkbAndConsumer(in, pointSize, bytesConsumer, digest, false);
        return textLength[0];
    }


    /**
     * @return first:true big-endian,second : pointSize
     * @see #lineStringFromWkb(byte[], int)
     */
    static Pair<Boolean, Integer> readWkbHead(final byte[] wkbBytes, int offset, final int expectWkbType) {
        if (wkbBytes.length < 9) {
            throw createWkbFormatError(wkbBytes.length, 9);
        }
        if (wkbBytes.length - offset < 9) {
            throw createOffsetError(offset, wkbBytes.length);
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


    static void pointAsWkb(final Point point, final boolean bigEndian, final byte[] wkbBytes, int offset)
            throws IllegalArgumentException {
        if (wkbBytes.length < Point.WKB_BYTES) {
            throw createWkbFormatError(wkbBytes.length, Point.WKB_BYTES);
        }
        if (wkbBytes.length - offset < Point.WKB_BYTES) {
            throw createOffsetError(offset, wkbBytes.length);
        }

        if (bigEndian) {
            wkbBytes[offset++] = 0;
            JdbdNumberUtils.intToBigEndian(Point.WKB_TYPE_POINT, wkbBytes, offset);
        } else {
            wkbBytes[offset++] = 1;
            JdbdNumberUtils.intToLittleEndian(Point.WKB_TYPE_POINT, wkbBytes, offset, 4);
        }
        offset += 4;
        doPointAsWkb(point.getX(), point.getY(), bigEndian, wkbBytes, offset);
    }

    /**
     * @see #pointAsWkb(Point, boolean, byte[], int)
     * @see #lineStringAsWkb(List, boolean, byte[], int)
     */
    static void doPointAsWkb(final double x, final double y, final boolean bigEndian, final byte[] wkbBytes
            , int offset) {
        if (bigEndian) {
            JdbdNumberUtils.longToBigEndian(Double.doubleToLongBits(x), wkbBytes, offset, 8);
            offset += 8;
            JdbdNumberUtils.longToBigEndian(Double.doubleToLongBits(y), wkbBytes, offset, 8);
        } else {
            JdbdNumberUtils.longToLittleEndian(Double.doubleToLongBits(x), wkbBytes, offset, 8);
            offset += 8;
            JdbdNumberUtils.longToLittleEndian(Double.doubleToLongBits(y), wkbBytes, offset, 8);
        }
    }


    /**
     * @param consumer byte[] length is multiple of 16.
     * @see PathLineString#writeAsWkb(boolean, IoConsumer)
     */
    static void readPointsWkbAndConsumer(final FileChannel in, final int pointCount
            , final IoConsumer<byte[]> consumer, final MessageDigest digest, final boolean copyArray)
            throws IOException {
        final long startTime = System.currentTimeMillis();
        final boolean infoEnabled = LOG.isInfoEnabled();

        final long needBytes = (pointCount & DefaultGeometryFactory.MAX_UNSIGNED_INT) << 4;
        final byte[] bufferArray = new byte[2048];
        final ByteBuffer buffer = ByteBuffer.wrap(bufferArray);

        long readBytes = 0L;
        final int multiple = 0xFFFF_F;
        for (int i = 0, readLength; readBytes < needBytes; ) {
            readLength = (int) Math.min(bufferArray.length, needBytes - readBytes);
            if ((readLength & 0xF) != 0) {
                // mean length % 16 != 0
                throw DefaultGeometryFactory.createWkbFormatError(readBytes, 9L + needBytes);
            }
            if (readLength < bufferArray.length) {
                buffer.limit(readLength);
            }

            if (in.read(buffer) != readLength) {
                throw DefaultGeometryFactory.createWkbFormatError(readBytes, 9L + needBytes);
            }
            if (copyArray || readLength < bufferArray.length) {
                consumer.next(Arrays.copyOfRange(bufferArray, 0, readLength));
            } else {
                consumer.next(bufferArray);
            }

            digest.update(bufferArray, 0, readLength);
            readBytes += readLength;
            // clear buffer for next.
            buffer.clear();
            i++;
            if (infoEnabled && (i & multiple) == 0) {
                LOG.info("Read large points process {}%.", (readBytes / (double) needBytes) * 100);
                i = 0;
            }


        }
        if (infoEnabled && needBytes > multiple) {
            LOG.info("Read large points process 100% ,cost {}ms", System.currentTimeMillis() - startTime);
        }


    }


    static int get16BufferLength(final long needBytes) {
        int bufferLength = (int) Math.max(Runtime.getRuntime().freeMemory() >> 10, 2048);
        bufferLength = (int) Math.min(needBytes, bufferLength);
        return bufferLength;
    }

    /**
     * @see #readPointsWkbAndConsumer(FileChannel, int, IoConsumer, MessageDigest, boolean)
     */
    static void notPointEndian(final byte[] wkbArray) {
        notPointEndian(wkbArray, 0);
    }

    /**
     * @see #readPointsWkbAndConsumer(FileChannel, int, IoConsumer, MessageDigest, boolean)
     * @see #notPointEndian(byte[])
     */
    static void notPointEndian(final byte[] wkbArray, final int offset) {
        if (((wkbArray.length - offset) & 0xF) != 0) {
            // mean length % 16 != 0
            throw new IllegalArgumentException("(wkbArray.length - offset) isn't multiple of 16.");
        }
        byte temp;
        for (int i = offset; i < wkbArray.length; i += 8) {
            for (int end = i + 4, left = i, right = i + 7; left < end; left++, right--) {
                temp = wkbArray[left];
                wkbArray[left] = wkbArray[right];
                wkbArray[right] = temp;
            }
        }

    }

    static StringBuilder pointAsWkt(Point point, StringBuilder builder) {
        return builder.append("POINT(")
                .append(point.getX())
                .append(" ")
                .append(point.getY())
                .append(")");
    }

    static StringBuilder lineStringAsWkt(final MemoryLineString lineString, final StringBuilder builder) {
        final List<Point> pointList = lineString.pointList();
        final int size = pointList.size();
        builder.append("LINESTRING(");
        Point point;
        for (int i = 0; i < size; i++) {
            if (i > 0) {
                builder.append(",");
            }
            point = pointList.get(i);
            builder.append(point.getX())
                    .append(" ")
                    .append(point.getY());
        }
        return builder.append(")");
    }

    static void lineStringAsWkb(final List<Point> pointList, final boolean bigEndian, final byte[] wkbBytes
            , int offset) throws IllegalArgumentException {
        final int size = pointList.size();
        if (size < 2) {
            throw createSmallLineSizeError(size);
        }
        final int wkbTotalLength = 9 + (size * 16);
        if (wkbBytes.length < wkbTotalLength) {
            throw createWkbFormatError(wkbBytes.length, wkbTotalLength);
        }
        if (wkbBytes.length - offset < wkbTotalLength) {
            throw createOffsetError(offset, wkbBytes.length);
        }

        writeWkbPrefix(bigEndian, LineString.WKB_TYPE_LINE_STRING, size, wkbBytes, offset);

        offset += 9;
        Point point;
        for (int i = 0; i < size; i++) {
            point = pointList.get(i);
            doPointAsWkb(point.getX(), point.getY(), bigEndian, wkbBytes, offset);
            offset += 16;
        }

    }


    static byte[] createWkbPrefix(final boolean bigEndian, final int wkbType, final int elementCount) {
        byte[] wkbBytes = new byte[9];
        writeWkbPrefix(bigEndian, wkbType, elementCount, wkbBytes, 0);
        return wkbBytes;
    }

    /**
     * @see #lineStringAsWkb(List, boolean, byte[], int)
     * @see #createWkbPrefix(boolean, int, int)
     */
    static void writeWkbPrefix(final boolean bigEndian, final int wkbType, final int elementCount
            , final byte[] wkbBytes, int offset) {
        if (wkbBytes.length < 9) {
            throw createWkbFormatError(wkbBytes.length, 9);
        }
        if (wkbBytes.length - offset < 9) {
            throw createOffsetError(offset, wkbBytes.length);
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

    private static IllegalArgumentException createOffsetError(final int offset, final int rightBound) {
        return new IllegalArgumentException(String.format("offset[%s] not in [0,%s).", offset, rightBound));
    }

    static IllegalArgumentException createWkbFormatError(long wkbBytesLength, long minLength) {
        return new IllegalArgumentException(
                String.format("WKB length[%s] less than min length[%s].", wkbBytesLength, minLength));
    }

    private static IllegalArgumentException createUnknownWkbTypeError(int wkbType) {
        return new IllegalArgumentException(String.format("Unknown WKB-Type[%s]", wkbType));
    }

    static IllegalArgumentException createIllegalByteOrderError(byte byteOrder) {
        return new IllegalArgumentException(String.format("Illegal byte order[%s]", byteOrder));
    }

    static IllegalArgumentException createWktTypeNotMatch(String wktType) {
        return new IllegalArgumentException(String.format("WKT isn't %s.", wktType));
    }

    static IllegalArgumentException createIllegalLineStringElementCountError(long elementCount) {
        return new IllegalArgumentException(String.format("WKT[%s] element count[%s] not in [2,%s]."
                , Constant.LINESTRING, elementCount, JdbdNumberUtils.MAX_UNSIGNED_INT));
    }

    static IllegalArgumentException createWktFormatError(@Nullable Throwable cause, String wkt) {
        IllegalArgumentException e;
        String message = String.format("WKT format[%s] error.", wkt);
        if (cause == null) {
            e = new IllegalArgumentException(message);
        } else {
            e = new IllegalArgumentException(message, cause);
        }
        return e;
    }

    static IllegalArgumentException createWktFormatErrorWithDetail(String detail) {
        return new IllegalArgumentException(String.format("WKT format error,%s", detail));
    }

    static IllegalArgumentException createNonDoubleError(String nonDouble) {
        return new IllegalArgumentException(String.format("%s isn't double number.", nonDouble));
    }

    static IllegalArgumentException createNoWktHeaderError(String wktType) {
        return new IllegalArgumentException(String.format("Not found key words[%s].", wktType));
    }

    private static IllegalArgumentException createSmallLineSizeError(int pointSize) {
        return new IllegalArgumentException(String.format(
                "%s point size[%s] must in [2,%s],please use %s ."
                , MemoryLineString.class.getSimpleName()
                , pointSize
                , MemoryLineString.BOUNDARY_POINT_LIST_SIZE
                , LineString.class.getName()));
    }


}
