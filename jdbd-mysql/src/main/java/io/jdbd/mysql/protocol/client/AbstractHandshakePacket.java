package io.jdbd.mysql.protocol.client;


import io.jdbd.mysql.protocol.MySQLPacket;
import io.jdbd.mysql.protocol.ServerVersion;



abstract class AbstractHandshakePacket implements MySQLPacket {

   private final short protocolVersion;

   private final ServerVersion serverVersion;

   private final long threadId;

    AbstractHandshakePacket(short protocolVersion, ServerVersion serverVersion, long threadId) {
      this.protocolVersion = protocolVersion;
      this.serverVersion = serverVersion;
      this.threadId = threadId;
   }

    public short getProtocolVersion() {
        return this.protocolVersion;
    }

    public ServerVersion getServerVersion() {
        return this.serverVersion;
    }

    public long getThreadId() {
        return this.threadId;
    }


}
