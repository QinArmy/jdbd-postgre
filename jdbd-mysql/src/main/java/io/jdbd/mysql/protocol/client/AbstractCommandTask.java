package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.Result;
import io.jdbd.vendor.result.ResultSink;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Base class of All MySQL command phase communication task.
 *
 * @see ComQueryTask
 * @see QuitTask
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase.html">Command Phase</a>
 */
abstract class AbstractCommandTask extends MySQLTask implements StmtTask {

    final Logger log = LoggerFactory.getLogger(getClass());

    final ResultSink sink;

    final int negotiatedCapability;

    private final ResultSetReader resultSetReader;

    private int sequenceId = -1;

    private int resultIndex;

    private boolean downstreamCanceled;

    AbstractCommandTask(TaskAdjutant adjutant, final ResultSink sink) {
        super(adjutant, sink::error);
        this.sink = sink;
        this.negotiatedCapability = adjutant.capability();
        this.resultSetReader = createResultSetReader();
    }

    /*################################## blow StmtTask method ##################################*/


    @Override
    public final boolean isCancelled() {
        final boolean isCanceled;
        if (this.downstreamCanceled || hasError()) {
            isCanceled = true;
        } else if (this.sink.isCancelled()) {
            log.trace("Downstream cancel subscribe.");
            this.downstreamCanceled = isCanceled = true;
        } else {
            isCanceled = false;
        }
        log.trace("Read command response,isCanceled:{}", isCanceled);
        return isCanceled;
    }

    @Override
    public final void next(final Result result) {
        this.sink.next(result);
    }

    @Override
    public final void addErrorToTask(Throwable error) {
        addError(error);
    }

    @Override
    public final TaskAdjutant adjutant() {
        return this.adjutant;
    }

    @Override
    public final void updateSequenceId(final int sequenceId) {
        if (sequenceId < 0) {
            this.sequenceId = -1;
        } else {
            this.sequenceId = sequenceId & 0xFF;
        }
    }


    @Override
    public final boolean readResultStateWithReturning(ByteBuf cumulateBuffer, Supplier<Integer> resultIndexes) {

        return false;
    }

    @Override
    public final int getAndIncrementResultIndex() {
        return this.resultIndex++;
    }


    public final int obtainSequenceId() {
        return this.sequenceId;
    }


    public final int addAndGetSequenceId() {
        int sequenceId = ++this.sequenceId;
        if (sequenceId > 0xFF) {
            sequenceId &= 0xFF;
            this.sequenceId = sequenceId;
        }
        return sequenceId;
    }

    abstract void handleReadResultSetEnd();

    abstract ResultSetReader createResultSetReader();

    /**
     * @return true: will invoke {@link #executeNextGroup()}
     */
    abstract boolean hasMoreGroup();

    /**
     * @return true : send failure,task end.
     * @see #hasMoreGroup()
     */
    abstract boolean executeNextGroup();

    abstract boolean executeNextFetch();


    final void readErrorPacket(final ByteBuf cumulateBuffer) {
        final int payloadLength = Packets.readInt3(cumulateBuffer);
        updateSequenceId(Packets.readInt1AsInt(cumulateBuffer)); //  sequence_id
        final ErrorPacket error;
        error = ErrorPacket.read(cumulateBuffer.readSlice(payloadLength)
                , this.negotiatedCapability, this.adjutant.obtainCharsetError());
        addError(MySQLExceptions.createErrorPacketException(error));
    }

    /**
     * @return true:task end
     */
    final boolean readResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        final boolean taskEnd;
        switch (this.resultSetReader.read(cumulateBuffer, serverStatusConsumer)) {
            case END_ONE_ERROR: {
                taskEnd = true;
            }
            break;
            case MORE_FETCH: {
                handleReadResultSetEnd();
                if (this.isCancelled()) {
                    taskEnd = true;
                } else {
                    taskEnd = executeNextFetch();
                }
            }
            break;
            case MORE_CUMULATE: {
                taskEnd = false;
            }
            break;
            case NO_MORE_RESULT: {
                handleReadResultSetEnd();
                if (hasMoreGroup()) {
                    taskEnd = executeNextGroup();
                } else {
                    taskEnd = true;
                }
            }
            break;
            case MORE_RESULT: {
                handleReadResultSetEnd();
                taskEnd = false;
            }
            break;
            default: {
                throw new IllegalStateException("Unknown ResultSetReader.States instance.");
            }

        }
        return taskEnd;
    }


    /**
     * @return true: task end.
     */
    final boolean readUpdateResult(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        final int payloadLength = Packets.readInt3(cumulateBuffer);
        updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));
        final OkPacket ok;
        ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
        serverStatusConsumer.accept(ok);

        final int resultIndex = getAndIncrementResultIndex(); // must increment result index.
        final boolean noMoreResult = !ok.hasMoreResult();

        final boolean taskEnd;
        if (this.isCancelled()) {
            taskEnd = noMoreResult;
        } else {
            // emit update result.
            this.sink.next(MySQLResultStates.fromUpdate(resultIndex, ok));
            if (noMoreResult && hasMoreGroup()) {
                taskEnd = executeNextGroup();
            } else {
                taskEnd = noMoreResult;
            }
        }
        return taskEnd;
    }


    @Override
    protected final boolean canDecode(ByteBuf cumulateBuffer) {
        return Packets.hasOnePacket(cumulateBuffer);
    }


}
