package io.jdbd.mysql.protocol.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jdbd.mysql.protocol.MySQLPacket;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
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
                //.flatMap(this::connectMySQL)
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
    public void handshake() throws Exception {
        String url = "jdbc:mysql://localhost:3306/army";
        Map<String, String> properties = new HashMap<>();
        properties.put("user", "army_w");
        properties.put("password", "army123");
        MySQLPacket mySQLPacket = ClientProtocolImpl.getInstance(MySQLUrl.getInstance(url, properties))
                .flatMap(ClientProtocol::handshake)
                .block();
        LOG.info("handshake packet:\n {}", mySQLPacket);
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
                        len.set(PacketUtils.getInt3(byteBuf, 0));
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
        int payloadLen = PacketUtils.readInt3(byteBuf);
        short sequenceId = PacketUtils.readInt1(byteBuf);
        LOG.info("payloadLen:{},sequenceId:{}", payloadLen, sequenceId);
        short version = PacketUtils.readInt1(byteBuf);
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


}
