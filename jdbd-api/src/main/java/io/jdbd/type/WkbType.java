package io.jdbd.type;

import io.jdbd.lang.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

/**
 * @see <a href="https://www.ogc.org/standards/sfa">Simple Feature Access - Part 1: Common Architecture PDF</a>
 */
public enum WkbType implements CodeEnum {

    GEOMETRY(Constant.GEOMETRY),
    POINT(Constant.POINT),
    LINE_STRING(Constant.LINE_STRING),
    POLYGON(Constant.POLYGON),

    MULTI_POINT(Constant.MULTI_POINT),
    MULTI_LINE_STRING(Constant.MULTI_LINE_STRING),
    MULTI_POLYGON(Constant.MULTI_POLYGON),
    GEOMETRY_COLLECTION(Constant.GEOMETRY_COLLECTION),

    CIRCULAR_STRING(Constant.CIRCULAR_STRING), // future use
    COMPOUND_CURVE(Constant.COMPOUND_CURVE), // future use
    CURVE_POLYGON(Constant.CURVE_POLYGON), // future use
    MULTI_CURVE(Constant.MULTI_CURVE),

    MULTI_SURFACE(Constant.MULTI_SURFACE),
    CURVE(Constant.CURVE),
    SURFACE(Constant.SURFACE),
    POLYHEDRAL_SURFACE(Constant.POLYHEDRAL_SURFACE),

    TIN(Constant.TIN),
    TRIANGLE(Constant.TRIANGLE);


    private static final Map<Integer, WkbType> CODE_MAP = CodeEnum.getCodeMap(WkbType.class);


    public final int code;

    public final String wktType;

    WkbType(int code) {
        this.code = code;
        this.wktType = wktTypeName(name());
    }

    @Override
    public int code() {
        return this.code;
    }

    @Override
    public String display() {
        return this.wktType;
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
