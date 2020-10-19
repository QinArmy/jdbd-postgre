package io.jdbd.mysql.protocol;

import io.jdbd.mysql.protocol.client.DataTypeUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.math.BigInteger;
import java.time.Duration;

public class ProtocolTests {

    private static final Logger LOG = LoggerFactory.getLogger(ProtocolTests.class);

    @Test
    public void netty() throws Exception {
        int port = 8900;
        EventLoopGroup bossGroup = new NioEventLoopGroup(); // (1)
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap(); // (2)
            b.group(bossGroup, workerGroup)
                    .channel(EpollServerSocketChannel.class) // (3)
                    .childHandler(new ChannelInitializer<SocketChannel>() { // (4)
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(null);
                        }
                    })
                    .handler(null)
                    .option(ChannelOption.SO_BACKLOG, 128)          // (5)
                    .childOption(ChannelOption.SO_KEEPALIVE, true); // (6)

            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(port).sync(); // (7)

            // Wait until the server socket is closed.
            // In this example, this does not happen, but you can do that to gracefully
            // shut down your server.
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    @Test
    public void nioServer() {
    }

    @Test
    public void loginMySQL() {
        LOG.info("test");
        byte byteNum = (byte) 0xff;
        int num = byteNum & 0xff;
        LOG.info("byteNum:{},num:{}", byteNum, num);
    }

    @Test
    public void simpleTest() {

        BigInteger big = DataTypeUtils.convertInt8ToBigInteger(Long.MIN_VALUE);
        LOG.info("max:{},big:{}", Long.MAX_VALUE, big);
    }

    @Test
    public void reactorHttpClient() {
        Flux.just(1/*, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12*/)
                .bufferUntil(num -> num == 8)
                .elementAt(0)
                .doOnNext(list -> LOG.info("list:{}", list))
                .then()
                .block()
        ;
    }



}
