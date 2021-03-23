package io.jdbd.vendor.util;

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

public abstract class GeometryConvertUtils {

    protected GeometryConvertUtils() {
        throw new UnsupportedOperationException();
    }


    public static byte[] pointToWkb(final String pointWkt, final boolean bigEndian) {
        String startMarker = "POINT(", endMarker = ")";
        if (!pointWkt.startsWith(startMarker) || !pointWkt.endsWith(endMarker)) {
            throw createWktFormatError(Geometries.POINT);
        }
        String[] coordinateArray = pointWkt.substring(startMarker.length(), pointWkt.length() - 1).split(" ");
        if (coordinateArray.length != 2) {
            throw createWktFormatError(Geometries.POINT);
        }
        double x, y;
        x = Double.parseDouble(coordinateArray[0]);
        y = Double.parseDouble(coordinateArray[1]);

        byte[] wkbArray = new byte[Geometries.WKB_POINT_BYTES];
        int offset = 0;
        if (bigEndian) {
            wkbArray[offset++] = 0;
            JdbdNumberUtils.intToBigEndian(Geometries.WKB_POINT, wkbArray, offset, 4);
            offset += 4;
            JdbdNumberUtils.doubleToEndian(true, x, wkbArray, offset);
            offset += 8;
            JdbdNumberUtils.doubleToEndian(true, y, wkbArray, offset);
        } else {
            wkbArray[offset++] = 1;
            JdbdNumberUtils.intToLittleEndian(Geometries.WKB_POINT, wkbArray, offset, 4);
            offset += 4;
            JdbdNumberUtils.doubleToEndian(false, x, wkbArray, offset);
            offset += 8;
            JdbdNumberUtils.doubleToEndian(false, y, wkbArray, offset);
        }
        return wkbArray;
    }


    public static void pointWkbReverse(final byte[] wkbArray) {
        if (wkbArray.length != Geometries.WKB_POINT_BYTES) {
            throw createWkbLengthError(Geometries.POINT, wkbArray.length, Geometries.WKB_POINT_BYTES);
        }

        checkByteOrder(wkbArray[0]);

        wkbArray[0] ^= 1;
        int offset = 1;
        JdbdArrayUtils.reverse(wkbArray, offset, 4, 1);
        offset += 4;
        JdbdArrayUtils.reverse(wkbArray, offset, 8, 2);

    }

    public static String pointToWkt(final byte[] wkbArray) {
        if (wkbArray.length != Geometries.WKB_POINT_BYTES) {
            throw createWkbLengthError(Geometries.POINT, wkbArray.length, Geometries.WKB_POINT_BYTES);
        }
        final byte byteOrder = checkByteOrder(wkbArray[0]);
        int offset = 1;
        final int wkbType;
        final double x, y;
        if (byteOrder == 0) {
            wkbType = JdbdNumberUtils.readIntFromBigEndian(wkbArray, offset, 4);
            if (wkbType != Geometries.WKB_POINT) {
                throw createWkbTypeNotMatchError(Geometries.POINT, wkbType);
            }
            offset += 4;
            x = Double.longBitsToDouble(JdbdNumberUtils.readLongFromBigEndian(wkbArray, offset, 8));
            offset += 8;
            y = Double.longBitsToDouble(JdbdNumberUtils.readLongFromBigEndian(wkbArray, offset, 8));
        } else {
            wkbType = JdbdNumberUtils.readIntFromLittleEndian(wkbArray, offset, 4);
            if (wkbType != Geometries.WKB_POINT) {
                throw createWkbTypeNotMatchError(Geometries.POINT, wkbType);
            }
            offset += 4;
            x = Double.longBitsToDouble(JdbdNumberUtils.readLongFromLittleEndian(wkbArray, offset, 8));
            offset += 8;
            y = Double.longBitsToDouble(JdbdNumberUtils.readLongFromLittleEndian(wkbArray, offset, 8));
        }
        return String.format("POINT(%s %s)", x, y);
    }

    public static byte[] lineStringToWkb(final String wkt, final boolean bigEndian) {

        final BufferWrapper inWrapper = new BufferWrapper(wkt.getBytes(StandardCharsets.US_ASCII));
        //1.read wkt type.
        if (!readWktType(inWrapper, Geometries.LINE_STRING)) {
            throw createWktFormatError(Geometries.LINE_STRING);
        }
        final ByteBuffer inBuffer = inWrapper.buffer;
        if (!inBuffer.hasRemaining()) {
            throw createWktFormatError(Geometries.LINE_STRING);
        }

        try (ByteArrayOutputStream out = new ByteArrayOutputStream(inWrapper.bufferArray.length)) {
            final BufferWrapper outWrapper = new BufferWrapper(inWrapper.bufferArray.length);
            writeWkbPrefix(bigEndian, Geometries.WKB_LINE_STRING, 0, outWrapper.bufferArray, 0);
            out.write(outWrapper.bufferArray, 0, 9);

            int elementCount = 0;
            while (inBuffer.hasRemaining()) {
                elementCount += readAndWritePoints(bigEndian, 2, false, inWrapper, outWrapper);
                outWrapper.buffer.flip();
                out.write(outWrapper.bufferArray, 0, outWrapper.buffer.limit());

                if (inBuffer.get(inBuffer.position() - 1) == ')') {
                    break;
                }
            }
            if (elementCount < 2) {
                throw new IllegalArgumentException("LineString elementCount must great than 2.l");
            }
            // write elementCount
            byte[] wkbArray = out.toByteArray();
            if (bigEndian) {
                JdbdNumberUtils.intToBigEndian(elementCount, wkbArray, 5, 4);
            } else {
                JdbdNumberUtils.intToLittleEndian(elementCount, wkbArray, 5, 4);
            }
            return wkbArray;
        } catch (IOException e) {
            // no bug ,never here.
            throw new IllegalStateException(e.getMessage(), e);
        }

    }

    public static String lineStringToWkt(final byte[] wkbArray) {

        return null;
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
            final int typeLength = Geometries.LINE_STRING.length();
            boolean validate = false;
            for (int readLength; (readLength = in.read(inBuffer)) > 0; ) {
                if (readLength < typeLength) {
                    throw createWktFormatError(Geometries.LINE_STRING);
                }
                inBuffer.flip();
                if (readWktType(inWrapper, Geometries.LINE_STRING)) {
                    validate = true;
                    break;
                }

            }
            if (!validate) {
                throw createWktFormatError(Geometries.LINE_STRING);
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
     * @see #lineStringPathToWkbFromPath(Path, long, boolean, Path)
     */
    protected static boolean readWktType(final BufferWrapper inWrapper, final String wktType)
            throws IllegalArgumentException {

        final ByteBuffer buffer = inWrapper.buffer;
        final byte[] bufferArray = inWrapper.bufferArray;

        final int typeLength = wktType.length();
        int limit, position, headerIndex;

        limit = buffer.limit();
        for (position = buffer.position(); position < limit; position++) {
            if (!Character.isWhitespace(bufferArray[position])) {
                break;
            }
        }
        if (limit - position < typeLength) {
            buffer.position(position);
            JdbdBufferUtils.cumulate(buffer, false);
            return false;
        }
        headerIndex = position;
        for (int i = 0; i < typeLength; i++, position++) {
            if (bufferArray[position] != wktType.charAt(i)) {
                throw createWktFormatError(wktType);
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
            return false;
        }
        if (bufferArray[position] != '(') {
            throw createWktFormatError(wktType);
        }
        buffer.position(position + 1);
        JdbdBufferUtils.cumulate(buffer, true);
        return true;
    }

    /**
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
            for (int i = 1, startIndex, endIndex = tempInPosition - 1; i <= coordinates; i++) {
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
                        throw createNonDoubleError(new String(nonDoubleBytes));
                    }
                    break topFor;
                }
                double d = Double.parseDouble(new String(Arrays.copyOfRange(inArray, startIndex, endIndex)));
                JdbdNumberUtils.doubleToEndian(bigEndian, d, outArray, tempOutPosition);
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
        writeWkbPrefix(bigEndian, Geometries.WKB_LINE_STRING, 0, outArray, 0);
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

    static byte checkByteOrder(byte byteOrder) {
        if (byteOrder != 0 && byteOrder != 1) {
            throw createIllegalByteOrderError(byteOrder);
        }
        return byteOrder;
    }


    private static IllegalArgumentException createIllegalByteOrderError(byte byteOrder) {
        return new IllegalArgumentException(String.format("Illegal byteOrder[%s].", byteOrder));
    }


    private static IllegalArgumentException createWkbLengthError(String type, int length, int exceptLength) {
        return new IllegalArgumentException(String.format("WKB length[%s] and %s except length[%s] not match."
                , length, type, exceptLength));
    }


    private static IllegalArgumentException createWktFormatError(String wktType) {
        return new IllegalArgumentException(String.format("Not %s WKT format.", wktType));
    }

    private static IllegalArgumentException createWkbTypeNotMatchError(String type, int wkbType) {
        return new IllegalArgumentException(String.format("WKB type[%s] and %s not match.", wkbType, type));
    }

    protected static IllegalArgumentException createNonDoubleError(String nonDouble) {
        return new IllegalArgumentException(String.format("%s isn't double number.", nonDouble));
    }

    private static final class BufferWrapper {

        private final byte[] bufferArray;

        private final ByteBuffer buffer;

        private BufferWrapper(int arrayLength) {
            this.bufferArray = new byte[arrayLength];
            this.buffer = ByteBuffer.wrap(this.bufferArray);
        }

        private BufferWrapper(byte[] bytes) {
            this.bufferArray = bytes;
            this.buffer = ByteBuffer.wrap(this.bufferArray);
        }

    }


}
