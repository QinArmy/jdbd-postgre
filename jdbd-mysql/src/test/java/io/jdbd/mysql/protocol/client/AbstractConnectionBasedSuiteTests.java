package io.jdbd.mysql.protocol.client;

import io.netty.channel.EventLoopGroup;
import reactor.netty.resources.LoopResources;

public abstract class AbstractConnectionBasedSuiteTests {

    private final static EventLoopGroup EVENT_LOOP_GROUP = LoopResources.create("jdbd-mysql", 20, false)
            .onClient(true);


    protected static EventLoopGroup getEventLoopGroup() {
        return EVENT_LOOP_GROUP;
    }


}
