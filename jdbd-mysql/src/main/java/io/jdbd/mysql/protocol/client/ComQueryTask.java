package io.jdbd.mysql.protocol.client;

import io.jdbd.ResultRow;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.protocol.EofPacket;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.OkPacket;
import io.jdbd.mysql.protocol.TerminatorPacket;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.jdbd.vendor.ReactorMultiResults;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.FluxSink;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;


final class ComQueryTask extends MySQLCommandTask {

    static ReactorMultiResults command(MySQLTaskAdjutant taskAdjutant, String command) {
        return new ComQueryTask(taskAdjutant, command, 1);
    }

    static ReactorMultiResults commands(MySQLTaskAdjutant taskAdjutant, List<String> commandList) {
        StringBuilder builder = new StringBuilder();
        final int size = commandList.size();
        for (int i = 0; i < size; i++) {
            if (i > 0) {
                builder.append(";");
            }
            builder.append(commandList.get(0));
        }
        return new ComQueryTask(taskAdjutant, builder.toString(), commandList.size());
    }


    private static final Logger LOG = LoggerFactory.getLogger(ComQueryTask.class);

    private final String command;


    // non-volatile ,because all modify in netty EventLoop .
    private Path localPath;

    // non-volatile ,because all modify in netty EventLoop .
    private DecoderType decoderType;

    // non-volatile ,because all modify in netty EventLoop .
    private TextResultDecodePhase textResultDecodePhase;

    // non-volatile ,because all modify in netty EventLoop .
    private MySQLRowMeta rowMeta;


    private ComQueryTask(MySQLTaskAdjutant taskAdjutant, String command, int expectedResultCount) {
        super(taskAdjutant, expectedResultCount);
        this.command = command;
    }


    @Nullable
    @Override
    public ByteBuf moreSendPacket() {
        // always null
        return null;
    }


    @Override
    @Nullable
    public final Path moreSendFile() {
        if (!this.executorAdjutant.inEventLoop()) {
            throw new IllegalStateException("moreSendFile() isn't in EventLoop.");
        }
        Path path = this.localPath;
        if (path != null) {
            this.localPath = null;
        }
        return path;
    }


    /*################################## blow protected method ##################################*/


    @Override
    protected boolean internalDecode(final ByteBuf cumulateBuf) {
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




    /*################################## blow packet method ##################################*/

    @Override
    protected ByteBuf internalStart() {
        int initialCapacity = this.executorAdjutant.obtainMaxBytesPerCharClient() * this.command.length();
        ByteBuf byteBuf = this.executorAdjutant.createPacketBuffer(initialCapacity);

        byteBuf.writeByte(PacketUtils.COM_QUERY_HEADER)
                .writeBytes(this.command.getBytes(this.executorAdjutant.obtainCharsetClient()))
        ;
        return byteBuf;
    }

    /*################################## blow private method ##################################*/

    /**
     * @return true:task end.
     */
    private boolean decodeOneResultSet(final ByteBuf cumulateBuf) {
        final ComQueryResponse response = detectComQueryResponseType(cumulateBuf, negotiatedCapability);
        boolean taskEnd;
        switch (response) {
            case ERROR:
                Charset charsetResults = this.executorAdjutant.obtainCharsetResults();
                cumulateBuf.skipBytes(PacketUtils.HEADER_SIZE);
                ErrorPacket error;
                error = ErrorPacket.readPacket(cumulateBuf, this.negotiatedCapability, charsetResults);
                emitErrorPacket(MySQLExceptionUtils.createErrorPacketException(error)); //emit error packet
                taskEnd = true;
                break;
            case OK:
                int payloadLength = PacketUtils.readInt3(cumulateBuf);
                updateSequenceId(PacketUtils.readInt1(cumulateBuf));
                OkPacket okPacket;
                okPacket = OkPacket.readPacket(cumulateBuf.readSlice(payloadLength), this.negotiatedCapability);
                emitUpdateResult(MySQLResultStates.from(okPacket)); // emit dml sql result set.
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
        final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        updateSequenceId(PacketUtils.readInt1(cumulateBuffer));

        final int payloadIndex = cumulateBuffer.readerIndex();
        boolean taskEnd;
        final int type = PacketUtils.getInt1(cumulateBuffer, cumulateBuffer.readerIndex());
        switch (type) {
            case ErrorPacket.ERROR_HEADER:
                ErrorPacket error = ErrorPacket.readPacket(cumulateBuffer
                        , this.negotiatedCapability, this.executorAdjutant.obtainCharsetResults());
                emitErrorPacket(MySQLExceptionUtils.createErrorPacketException(error));
                taskEnd = true;
                break;
            case EofPacket.EOF_HEADER:
            case OkPacket.OK_HEADER:
                OkPacket ok = OkPacket.readPacket(cumulateBuffer, negotiatedCapability);
                emitUpdateResult(MySQLResultStates.from(ok));
                taskEnd = (ok.getStatusFags() & ClientProtocol.SERVER_MORE_RESULTS_EXISTS) == 0;
                break;
            default:
                throw MySQLExceptionUtils.createFatalIoException("LOCAL INFILE Data response type[%s] unknown.", type);
        }
        cumulateBuffer.readerIndex(payloadIndex + payloadLength); // to next packet
        return taskEnd;
    }

    /**
     * @see #decodeOneResultSet(ByteBuf)
     */
    private void sendLocalFile(final ByteBuf cumulateBuffer) {
        int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        updateSequenceId(PacketUtils.readInt1(cumulateBuffer));
        String filePath;
        filePath = PacketUtils.readStringEof(cumulateBuffer, payloadLength, this.executorAdjutant.obtainCharsetResults());
        // task executor will send.
        this.localPath = Paths.get(filePath);
    }

    private boolean decodeTextResult(ByteBuf cumulateBuffer) {
        boolean taskEnd = false;
        final TextResultDecodePhase phase = this.textResultDecodePhase;
        switch (phase) {
            case META:
                MySQLColumnMeta[] columnMetas = readResultColumnMetas(cumulateBuffer);
                if (columnMetas.length == 0) {
                    break;
                }
                this.rowMeta = MySQLRowMeta.from(columnMetas, this.executorAdjutant.obtainCustomCollationMap());
                this.textResultDecodePhase = TextResultDecodePhase.ROWS;
                if (!PacketUtils.hasOnePacket(cumulateBuffer)) {
                    break;
                }
                taskEnd = decodeMultiRowData(cumulateBuffer);
                if (taskEnd
                        || !PacketUtils.hasOnePacket(cumulateBuffer)
                        || this.textResultDecodePhase != TextResultDecodePhase.TERMINATOR) {
                    break;
                }
                taskEnd = emitResultSetTerminator(cumulateBuffer);
                break;
            case ROWS:
                // task end ,if read error packet
                taskEnd = decodeMultiRowData(cumulateBuffer);
                if (taskEnd
                        || !PacketUtils.hasOnePacket(cumulateBuffer)
                        || this.textResultDecodePhase != TextResultDecodePhase.TERMINATOR) {
                    break;
                }
                taskEnd = emitResultSetTerminator(cumulateBuffer);
                break;
            case TERMINATOR:
                taskEnd = emitResultSetTerminator(cumulateBuffer);
                break;
            default:
                throw MySQLExceptionUtils.createUnknownEnumException(phase);
        }
        return taskEnd;
    }

    /**
     * @return true: read error packet, invoker must end task
     * @see #decodeTextResult(ByteBuf)
     */
    private boolean decodeMultiRowData(ByteBuf cumulateBuffer) {
        final boolean clientDeprecateEof = (this.negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) != 0;
        boolean rowPhaseEnd = false;
        int sequenceId = -1;
        final FluxSink<ResultRow> sink = obtainQueryResultSink();

        final MySQLRowMeta rowMeta = this.rowMeta;
        out:
        for (int readableBytes, packetLength, payloadLength, packetStartIndex; ; ) {
            readableBytes = cumulateBuffer.readableBytes();
            if (readableBytes < PacketUtils.HEADER_SIZE) {
                break;
            }
            packetStartIndex = cumulateBuffer.readerIndex(); //record packet start index
            payloadLength = PacketUtils.getInt3(cumulateBuffer, packetStartIndex);

            packetLength = PacketUtils.HEADER_SIZE + payloadLength;
            if (readableBytes < packetLength) {
                break;
            }
            switch (PacketUtils.getInt1(cumulateBuffer, packetStartIndex + PacketUtils.HEADER_SIZE)) {
                case ErrorPacket.ERROR_HEADER:
                    // error terminator
                    Charset charsetResults = this.executorAdjutant.obtainCharsetResults();
                    ErrorPacket error;
                    error = ErrorPacket.readPacket(cumulateBuffer, this.negotiatedCapability, charsetResults);
                    emitErrorPacket(MySQLExceptionUtils.createErrorPacketException(error));
                    return true;
                case EofPacket.EOF_HEADER:
                    if (clientDeprecateEof && payloadLength < PacketUtils.ENC_3_MAX_VALUE) {
                        //OK terminator
                        rowPhaseEnd = true;
                        break out;
                    } else if (!clientDeprecateEof && payloadLength < 6) {
                        // EOF terminator
                        rowPhaseEnd = true;
                        break out;
                    } else {
                        throw MySQLExceptionUtils.createFatalIoException("MySQL server send error ResultSet terminator.");
                    }
                default:
                    cumulateBuffer.skipBytes(3);
                    sequenceId = PacketUtils.readInt1(cumulateBuffer);
                    if (sink.isCancelled()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("downstream cancel, skip ResultRow");
                        }
                    } else {
                        sink.next(decodeOneRow(cumulateBuffer, rowMeta)); // emit one row
                    }
                    cumulateBuffer.readerIndex(packetStartIndex + packetLength); // to next packet
            }
        }
        if (sequenceId > -1) {
            updateSequenceId(sequenceId);
        }
        if (rowPhaseEnd) {
            this.textResultDecodePhase = TextResultDecodePhase.TERMINATOR;
            this.rowMeta = null;
        }
        return false;
    }

    private ResultRow decodeOneRow(ByteBuf cumulateBuffer, MySQLRowMeta rowMeta) {
        MySQLColumnMeta[] columnMetas = rowMeta.columnMetas;
        MySQLColumnMeta columnMeta;
        Object[] columnValueArray = new Object[columnMetas.length];
        final MySQLTaskAdjutant taskAdjutant = this.executorAdjutant;

        for (int i = 0; i < columnMetas.length; i++) {
            columnMeta = columnMetas[i];
            columnValueArray[i] = ColumnParsers.parseColumn(cumulateBuffer, columnMeta, taskAdjutant);
        }
        return MySQLResultRow.from(columnValueArray, rowMeta, taskAdjutant);
    }


    /**
     * @return true: task end
     * @see #decodeTextResult(ByteBuf)
     */
    private boolean emitResultSetTerminator(ByteBuf cumulateBuffer) {
        if (this.decoderType != DecoderType.TEXT_RESULT
                || this.textResultDecodePhase != TextResultDecodePhase.TERMINATOR) {
            throw new IllegalStateException(String.format("decoderType[%s] and textResultDecodePhase[%s] error."
                    , this.decoderType, this.textResultDecodePhase));
        }

        final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        updateSequenceId(PacketUtils.readInt1(cumulateBuffer));
        final int payloadIndex = cumulateBuffer.readerIndex();

        final TerminatorPacket terminator;
        if ((this.negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) != 0) {
            // ok terminator
            OkPacket ok = OkPacket.readPacket(cumulateBuffer, this.negotiatedCapability);
            emitRowTerminator(MySQLResultStates.from(ok)); // emit
            terminator = ok;
        } else {
            // eof terminator
            EofPacket eof = EofPacket.readPacket(cumulateBuffer, this.negotiatedCapability);
            emitRowTerminator(MySQLResultStates.from(eof));
            terminator = eof;
        }
        cumulateBuffer.readerIndex(payloadIndex + payloadLength);// to next packet

        this.decoderType = null;
        this.textResultDecodePhase = null;
        return (terminator.getStatusFags() & ClientProtocol.SERVER_MORE_RESULTS_EXISTS) == 0;
    }

    /**
     * @return columnMeta or empty {@link MySQLColumnMeta}
     * @see #decodeTextResult(ByteBuf)
     */
    private MySQLColumnMeta[] readResultColumnMetas(ByteBuf cumulateBuffer) {
        final int originalStartIndex = cumulateBuffer.readerIndex();

        int packetStartIndex = cumulateBuffer.readerIndex();
        int packetLength = PacketUtils.HEADER_SIZE + PacketUtils.readInt3(cumulateBuffer);

        int sequenceId = PacketUtils.readInt1(cumulateBuffer);

        final byte metadataFollows;
        final boolean hasOptionalMeta = (this.negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0;
        if (hasOptionalMeta) {
            metadataFollows = cumulateBuffer.readByte();
        } else {
            metadataFollows = -1;
        }
        final int columnCount = PacketUtils.readLenEncAsInt(cumulateBuffer);

        cumulateBuffer.readerIndex(packetStartIndex + packetLength);  // to next packet

        // below column meta packet
        MySQLColumnMeta[] columnMetas = new MySQLColumnMeta[columnCount];
        int receiveColumnCount = 0;
        if (!hasOptionalMeta || metadataFollows == 1) {
            final Charset metaCharset = this.executorAdjutant.obtainCharsetResults();
            final Properties properties = this.executorAdjutant.obtainProperties();
            for (int i = 0, readableBytes; i < columnCount; i++) {
                readableBytes = cumulateBuffer.readableBytes();
                if (readableBytes < PacketUtils.HEADER_SIZE) {
                    break;
                }
                packetStartIndex = cumulateBuffer.readerIndex();//recode payload start index
                packetLength = PacketUtils.HEADER_SIZE + PacketUtils.readInt3(cumulateBuffer);

                if (readableBytes < packetLength) {
                    cumulateBuffer.readerIndex(packetStartIndex);
                    break;
                }
                sequenceId = PacketUtils.readInt1(cumulateBuffer);

                columnMetas[i] = MySQLColumnMeta.readFor41(cumulateBuffer, metaCharset, properties);
                cumulateBuffer.readerIndex(packetStartIndex + packetLength); // to next packet.
                receiveColumnCount++;
            }
        }
        if ((this.negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) == 0) {
            if (PacketUtils.hasOnePacket(cumulateBuffer)) {
                packetStartIndex = cumulateBuffer.readerIndex();
                packetLength = PacketUtils.HEADER_SIZE + PacketUtils.readInt3(cumulateBuffer);
                sequenceId = PacketUtils.readInt1(cumulateBuffer);

                EofPacket.readPacket(cumulateBuffer, this.negotiatedCapability);
                cumulateBuffer.readerIndex(packetStartIndex + packetLength); // to next packet.
            } else {
                receiveColumnCount = 0; // need cumulate buffer
            }
        }

        if (receiveColumnCount == columnCount) {
            updateSequenceId(sequenceId);// update sequenceId
        } else {
            cumulateBuffer.readerIndex(originalStartIndex);  // need cumulate buffer
            columnMetas = MySQLColumnMeta.EMPTY;
        }
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
