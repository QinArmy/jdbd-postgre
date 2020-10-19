package io.jdbd.mysql.protocol.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jdbd.mysql.protocol.MySQLPacket;
import io.jdbd.mysql.protocol.ProtocolTests;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.net.URI;
import java.net.URL;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientProtocolTests {

    private static final Logger LOG = LoggerFactory.getLogger(ClientProtocolTests.class);

    @Test
    public void reactorTcpClient() throws Exception {
        final Thread thread = Thread.currentThread();
        TcpClient.create()
                // .doOnConnected(this::connectMySQL)
                .doOnDisconnected(this::disConnectMySQL)
                .host("localhost")
                .port(3306)
                .connect()
                .flatMap(this::connectMySQL)
                .block(Duration.ofSeconds(30L));


      /*  try {
            Thread.sleep(10L * 1000L);
        } catch (InterruptedException e) {
          if( Thread.interrupted()){
              LOG.info("clear interrupt flag");
          }
        }*/


    }

    @Test
    public void simpleTest() throws Exception {


    }

    private Mono<Connection> connectMySQL(Connection connection) {
        LOG.info("连接成功");
        final AtomicInteger len = new AtomicInteger(-1);
        final AtomicInteger actualLen = new AtomicInteger(0);

        return connection.inbound().receive()
                .bufferUntil(byteBuf -> {
                    if (len.get() < 0) {
                        len.set(DataTypeUtils.getInt3(byteBuf, 0));
                    }
                    return actualLen.addAndGet(byteBuf.readableBytes()) >= len.get();
                })
                .elementAt(0)
                .map(ByteBufferUtils::mergeByteBuf)
                .map(this::handleHandShakes)
                .then(Mono.just(connection));


    }


    private void disConnectMySQL(Connection connection) {

    }

    private HandshakeV10Packet handleHandShakes(ByteBuf byteBuf) {
        LOG.info("cap:{}", byteBuf.capacity());
        LOG.info("readable bytes:{}", byteBuf.readableBytes());
        int payloadLen = DataTypeUtils.readInt3(byteBuf);
        short sequenceId = DataTypeUtils.readInt1(byteBuf);
        LOG.info("payloadLen:{},sequenceId:{}", payloadLen, sequenceId);
        short version = DataTypeUtils.readInt1(byteBuf);
        LOG.info("protocol version:{}", version);
        if (version != 10) {
            throw new RuntimeException("version error");
        }
        HandshakeV10Packet handshake = HandshakeV10Packet.readHandshake(byteBuf);
        ObjectMapper mapper = new ObjectMapper();
        try {
            LOG.info("handshake :{}", mapper.writeValueAsString(handshake));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return handshake;
    }

    @Test
    public void connect() throws Exception {
        URI uri = URI.create("jdbc:mysql://localhost:3306/army");
        LOG.info("uri:{},host:{}", uri, uri.getHost());
        ClientProtocol clientProtocol = ClientProtocolImpl.getInstance(uri, "", "");
        Object msg = clientProtocol.connect()
                .block();
        ObjectMapper mapper = new ObjectMapper();
        LOG.info("handshake :{}", mapper.writeValueAsString(msg));

    }

}
