package io.jdbd.mysql.protocol.client;

import io.jdbd.ResultRow;
import io.jdbd.ResultRowMeta;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.protocol.EofPacket;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.OkPacket;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.FluxSink;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.BiFunction;


final class CommandStatementTask extends AbstractStatementTask {

    static CommandStatementTask create(StatementTaskAdjutant taskAdjutant, String command) {
        return new CommandStatementTask(taskAdjutant, command, 1);
    }

    static CommandStatementTask create(StatementTaskAdjutant taskAdjutant, List<String> commandList) {
        StringBuilder builder = new StringBuilder();
        final int size = commandList.size();
        for (int i = 0; i < size; i++) {
            if (i > 0) {
                builder.append(";");
            }
            builder.append(commandList.get(0));
        }
        return new CommandStatementTask(taskAdjutant, builder.toString(), commandList.size());
    }

    private final String command;

    private final int expectedResultCount;

    // non-volatile ,because all modify in netty EventLoop .
    private Path localPath;

    // non-volatile ,because all modify in netty EventLoop .
    private DecoderType decoderType;

    // non-volatile ,because all modify in netty EventLoop .
    private TextResultDecodePhase textResultDecodePhase;

    // non-volatile ,because all modify in netty EventLoop .
    private MySQLRowMeta rowMeta;


    private CommandStatementTask(StatementTaskAdjutant taskAdjutant, String command, int expectedResultCount) {
        super(taskAdjutant);
        this.command = command;
        this.expectedResultCount = expectedResultCount;
    }


    @Override
    public boolean decode(final ByteBuf cumulateBuf) {
        if (PacketUtils.hasOnePacket(cumulateBuf)) {
            return false;
        }
        boolean taskEnd;
        final DecoderType decoderType = this.decoderType;
        if (decoderType != null) {
            switch (decoderType) {
                case TEXT_RESULT:
                    taskEnd = decodeTextResult(cumulateBuf);
                    break;
                case LOCAL_INFILE:
                    taskEnd = decodeLocalInfileResult(cumulateBuf);
                    break;
                default:
                    throw MySQLExceptionUtils.createUnknownEnumException(decoderType);
            }
        } else {
            taskEnd = decodeOneResultSet(cumulateBuf);
        }
        return taskEnd;
    }


    @Nullable
    @Override
    public ByteBuf moreSendPacket() {
        // always null
        return null;
    }

    @Nullable
    @Override
    public Path moreSendFile() {
        Path path = this.localPath;
        if (path != null) {
            this.localPath = null;
        }
        return path;
    }





    /*################################## blow packet method ##################################*/

    @Override
    ByteBuf internalStart() {
        int initialCapacity = this.taskAdjutant.obtainMaxBytesPerCharClient() * this.command.length();
        ByteBuf byteBuf = this.taskAdjutant.createPacketBuffer(initialCapacity);

        byteBuf.writeByte(PacketUtils.COM_QUERY_HEADER)
                .writeBytes(this.command.getBytes(this.taskAdjutant.obtainCharsetClient()))
        ;
        return byteBuf;
    }

    /*################################## blow private method ##################################*/

    /**
     * @return true:task end.
     */
    private boolean decodeOneResultSet(final ByteBuf cumulateBuf) {
        final int negotiatedCapability = this.taskAdjutant.obtainNegotiatedCapability();
        final ComQueryResponse response = detectComQueryResponseType(cumulateBuf, negotiatedCapability);
        boolean taskEnd;
        switch (response) {
            case ERROR:
                Charset charsetResults = this.taskAdjutant.obtainCharsetResults();
                ErrorPacket error;
                error = PacketDecoders.decodeErrorPacket(cumulateBuf, negotiatedCapability, charsetResults);
                emitErrorPacket(error);
                taskEnd = true;
                break;
            case OK:
                int payloadLength = PacketUtils.readInt3(cumulateBuf);
                updateSequenceId(PacketUtils.readInt1(cumulateBuf));
                OkPacket okPacket = OkPacket.readPacket(cumulateBuf.readSlice(payloadLength), this.negotiatedCapability);
                emitUpdateOkPacket(okPacket); // emit dml sql result set.
                taskEnd = (okPacket.getStatusFags() & ClientProtocol.SERVER_MORE_RESULTS_EXISTS) == 0;
                break;
            case LOCAL_INFILE_REQUEST:
                this.decoderType = DecoderType.LOCAL_INFILE;
                sendLocalFile(cumulateBuf);
                taskEnd = false;
                break;
            case TEXT_RESULT:
                this.decoderType = DecoderType.TEXT_RESULT;
                this.textResultDecodePhase = TextResultDecodePhase.META;
                taskEnd = decodeTextResult(cumulateBuf);
                break;
            default:
                throw MySQLExceptionUtils.createUnknownEnumException(response);
        }
        return taskEnd;
    }

    private boolean decodeLocalInfileResult(ByteBuf cumulateBuffer) {

        return false;
    }

    private void sendLocalFile(final ByteBuf cumulateBuffer) {
        int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        this.sequenceId = PacketUtils.readInt1(cumulateBuffer);
        String filePath;
        filePath = PacketUtils.readStringEof(cumulateBuffer, payloadLength, this.taskAdjutant.obtainCharsetResults());
        // task manager will send.
        this.localPath = Paths.get(filePath);
    }

    private boolean decodeTextResult(ByteBuf cumulateBuffer) {
        boolean taskEnd = false;
        final TextResultDecodePhase phase = this.textResultDecodePhase;
        switch (phase) {
            case META:
                if (hasMetaPacketLength(cumulateBuffer, this.negotiatedCapability)) {
                    this.textResultDecodePhase = TextResultDecodePhase.ROWS;
                    this.rowMeta = MySQLRowMeta.from(readResultColumnMetas(cumulateBuffer), this.taskAdjutant.obtainCustomCollationMap());
                }
                break;
            case ROWS:
                // task end ,if read error packet
                taskEnd = decodeMultiRowData(cumulateBuffer);
                break;
            case TERMINATOR:
                this.decoderType = null;
                this.textResultDecodePhase = null;
                break;
            default:
                throw MySQLExceptionUtils.createUnknownEnumException(phase);
        }
        return taskEnd;
    }

    /**
     * @return true: read error packet.
     */
    private boolean decodeMultiRowData(ByteBuf cumulateBuffer) {
        final int originalReaderIndex = cumulateBuffer.readerIndex(), writeIndex = cumulateBuffer.writerIndex();
        final boolean clientDeprecateEof = (this.negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) != 0;
        int readerIndex = originalReaderIndex;
        boolean rowPhaseEnd = false;
        ErrorPacket error = null;
        out:
        for (int readableBytes, packetLength, payloadLength; ; ) {
            readableBytes = writeIndex - readerIndex;
            if (readableBytes < PacketUtils.HEADER_SIZE) {
                break;
            }
            payloadLength = PacketUtils.getInt3(cumulateBuffer, readerIndex);
            packetLength = PacketUtils.HEADER_SIZE + payloadLength;
            if (readableBytes < packetLength) {
                break;
            }
            switch (PacketUtils.getInt1(cumulateBuffer, readerIndex + PacketUtils.HEADER_SIZE)) {
                case ErrorPacket.ERROR_HEADER:
                    error = PacketDecoders.decodeErrorPacket(cumulateBuffer, this.negotiatedCapability
                            , this.taskAdjutant.obtainCharsetResults());
                    // error terminator
                    break out;
                case EofPacket.EOF_HEADER:
                    if (clientDeprecateEof && payloadLength < PacketUtils.ENC_3_MAX_VALUE) {
                        //OK terminator
                        rowPhaseEnd = true;
                        break out;
                    } else if (!clientDeprecateEof && payloadLength < 6) {
                        // EOF terminator
                        rowPhaseEnd = true;
                        break out;
                    }
                default:
                    readerIndex += packetLength;

            }
        }

        if (error != null) {
            emitErrorPacket(error);
            return true;
        }
        if (readerIndex > originalReaderIndex) {
            if (ignoreEmit()) {
                cumulateBuffer.skipBytes(readerIndex - originalReaderIndex); // occur error ,skip packet.
            } else {
                emitMultiRow(cumulateBuffer.readSlice(readerIndex - originalReaderIndex));
            }
        }
        if (rowPhaseEnd) {
            this.textResultDecodePhase = TextResultDecodePhase.TERMINATOR;
            this.rowMeta = null;
        }
        return false;
    }

    /**
     * @see #decodeMultiRowData(ByteBuf)
     */
    private void emitMultiRow(ByteBuf multiRowBuffer) {
        int payloadLength;
        int sequenceId = -1;
        int payloadIndex;
        final MySQLRowMeta rowMeta = this.rowMeta;
        final MySQLColumnMeta[] columnMetas = rowMeta.columnMetas;
        final StatementTaskAdjutant adjutant = this.taskAdjutant;
        final FluxSink<Object> sink = obtainFluxSink();
        final BiFunction<ResultRow, ResultRowMeta, ?> rowDecoder = obtainRowDecoder();

        while (multiRowBuffer.isReadable()) {
            payloadLength = PacketUtils.readInt3(multiRowBuffer);
            sequenceId = PacketUtils.readInt1(multiRowBuffer);

            payloadIndex = multiRowBuffer.readerIndex();

            Object[] columnValues = new Object[columnMetas.length];
            MySQLColumnMeta columnMeta;
            for (int i = 0; i < columnMetas.length; i++) {
                columnMeta = columnMetas[i];
                columnValues[i] = ColumnParsers.parseColumn(multiRowBuffer, columnMeta, adjutant);
            }
            try {
                Object rowData = rowDecoder.apply(MySQLResultRow.from(columnValues, rowMeta, adjutant), rowMeta);
                if (rowData == null) {
                    Throwable e = new NullPointerException("Result row decoder return null.");
                    setTerminateError(e);
                    sink.error(e);
                    multiRowBuffer.readerIndex(multiRowBuffer.writerIndex()); // release buffer
                    return;
                } else {
                    sink.next(rowData);
                }
            } catch (Throwable e) {
                setTerminateError(e);
                sink.error(e);
                multiRowBuffer.readerIndex(multiRowBuffer.writerIndex()); // release buffer
                return;
            }
            multiRowBuffer.readerIndex(payloadIndex + payloadLength);// to next packet.
        }
        if (sequenceId > -1) {
            updateSequenceId(sequenceId);
        }
    }


    private MySQLColumnMeta[] readResultColumnMetas(ByteBuf cumulateBuffer) {
        int payloadLength = PacketUtils.readInt3(cumulateBuffer);

        int sequenceId = PacketUtils.readInt1(cumulateBuffer);

        int payloadIndex = cumulateBuffer.readerIndex();

        final byte metadataFollows;
        final boolean hasOptionalMeta = (this.negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0;
        if (hasOptionalMeta) {
            metadataFollows = cumulateBuffer.readByte();
        } else {
            metadataFollows = -1;
        }
        final int columnCount = PacketUtils.readLenEncAsInt(cumulateBuffer);
        // to next packet
        cumulateBuffer.readerIndex(payloadIndex + payloadLength);

        // below column meta packet
        MySQLColumnMeta[] columnMetas = new MySQLColumnMeta[columnCount];


        if (!hasOptionalMeta || metadataFollows == 1) {
            final Charset metaCharset = this.taskAdjutant.obtainCharsetResults();
            final Properties properties = this.taskAdjutant.obtainProperties();
            for (int i = 0; i < columnCount; i++) {
                payloadLength = PacketUtils.readInt3(cumulateBuffer);
                sequenceId = PacketUtils.readInt1(cumulateBuffer);
                payloadIndex = cumulateBuffer.readerIndex();

                columnMetas[i] = MySQLColumnMeta.readFor41(cumulateBuffer, metaCharset, properties);
                cumulateBuffer.readerIndex(payloadIndex + payloadLength); // to next packet.
            }
        }
        if ((negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) == 0) {
            payloadLength = PacketUtils.readInt3(cumulateBuffer);
            sequenceId = PacketUtils.readInt1(cumulateBuffer);
            EofPacket.readPacket(cumulateBuffer, negotiatedCapability);
            cumulateBuffer.readerIndex(payloadIndex + payloadLength); // to next packet.
        }

        this.sequenceId = sequenceId; // update sequenceId
        return columnMetas;
    }





    /*################################## blow static method ##################################*/

    /**
     * invoke this method after invoke {@link PacketUtils#hasOnePacket(ByteBuf)}.
     *
     * @see #decode(ByteBuf)
     */
    static ComQueryResponse detectComQueryResponseType(final ByteBuf cumulateBuf, final int negotiatedCapability) {
        int readerIndex = cumulateBuf.readerIndex();
        final int payloadLength = PacketUtils.getInt3(cumulateBuf, readerIndex);
        // skip header
        readerIndex += PacketUtils.HEADER_SIZE;
        ComQueryResponse responseType;
        final boolean metadata = (negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0;

        switch (PacketUtils.getInt1(cumulateBuf, readerIndex++)) {
            case 0:
                if (metadata && PacketUtils.obtainLenEncIntByteCount(cumulateBuf, readerIndex) + 1 == payloadLength) {
                    responseType = ComQueryResponse.TEXT_RESULT;
                } else {
                    responseType = ComQueryResponse.OK;
                }
                break;
            case ErrorPacket.ERROR_HEADER:
                responseType = ComQueryResponse.ERROR;
                break;
            case PacketUtils.LOCAL_INFILE:
                responseType = ComQueryResponse.LOCAL_INFILE_REQUEST;
                break;
            default:
                responseType = ComQueryResponse.TEXT_RESULT;

        }
        return responseType;
    }

    static boolean hasMetaPacketLength(final ByteBuf cumulateBuf, final int negotiatedCapability) {

        if (cumulateBuf.readableBytes() < 9) {
            return false;
        }
        final int startReaderIndex = cumulateBuf.readerIndex();

        cumulateBuf.skipBytes(PacketUtils.HEADER_SIZE);
        // 1. metadata_follows
        final byte metadataFollows;
        if ((negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0) {
            // 0: RESULTSET_METADATA_NONE , 1:RESULTSET_METADATA_FULL
            metadataFollows = cumulateBuf.readByte();
        } else {
            metadataFollows = -1;
        }
        // 2. column_count
        final int columnCount = PacketUtils.readLenEncAsInt(cumulateBuf);
        int actualColumnCount = 0;
        if ((negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) == 0 || metadataFollows == 1) {
            // 3. Field metadata
            for (int readableBytes; actualColumnCount < columnCount; actualColumnCount++) {
                readableBytes = cumulateBuf.readableBytes();
                if (readableBytes < PacketUtils.HEADER_SIZE) {
                    break;
                }
                int packetLength;
                packetLength = PacketUtils.HEADER_SIZE + PacketUtils.getInt3(cumulateBuf, cumulateBuf.readerIndex());
                if (readableBytes < packetLength) {
                    break;
                }
                cumulateBuf.skipBytes(packetLength);
            }
        } else {
            // reset reader index
            cumulateBuf.readerIndex(startReaderIndex);
            throw MySQLExceptionUtils.createFatalIoException("Result no column meta.close connection.");
        }

        if (actualColumnCount == columnCount && (negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) == 0) {
            // 4. End of metadata
            if (PacketUtils.hasOnePacket(cumulateBuf)) {
                int payloadLength = PacketUtils.readInt3(cumulateBuf);
                cumulateBuf.skipBytes(PacketUtils.HEADER_SIZE); // skip header
                if (EofPacket.isEofPacket(cumulateBuf)) {
                    cumulateBuf.skipBytes(payloadLength);
                } else {
                    // reset reader index
                    cumulateBuf.readerIndex(startReaderIndex);
                    throw MySQLExceptionUtils.createFatalIoException("ResultSet no EOF.close connection.");
                }
            }
        }

        // below obtain decodedBuf
        final int endReaderIndex = cumulateBuf.readerIndex();
        // reset reader index
        cumulateBuf.readerIndex(startReaderIndex);
        return actualColumnCount == columnCount;
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html">Protocol::COM_QUERY Response</a>
     */
    enum ComQueryResponse {
        OK,
        ERROR,
        TEXT_RESULT,
        LOCAL_INFILE_REQUEST
    }

    enum DecoderType {
        TEXT_RESULT,
        LOCAL_INFILE
    }

    enum TextResultDecodePhase {
        META,
        ROWS,
        TERMINATOR
    }
}
