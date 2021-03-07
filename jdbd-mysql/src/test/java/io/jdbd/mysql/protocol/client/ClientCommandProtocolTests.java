package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.vendor.conf.HostInfo;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import reactor.netty.resources.LoopResources;

import java.util.*;

public class ClientCommandProtocolTests {

    private static final Logger LOG = LoggerFactory.getLogger(ClientCommandProtocolTests.class);

    @BeforeClass
    public void createCommandProtocol(ITestContext context) {
        Map<String, String> properties = new HashMap<>();
        properties.put("allowMultiQueries", "true");

        HostInfo<PropertyKey> hostInfo = MySQLUrlUtils.build(properties).getHostList().get(0);
        EventLoopGroup eventLoopGroup = LoopResources.create("jdbd-mysql").onClient(true);

        ClientCommandProtocol commandProtocol = ClientCommandProtocolImpl.create(hostInfo, eventLoopGroup)
                .block();
        Assert.assertNotNull(commandProtocol, "commandProtocol");
        context.setAttribute("commandProtocol", commandProtocol);
    }

    @AfterClass
    public void closeClientCommandProtocol(ITestContext context) {
        obtainProtocol(context)
                .closeGracefully()
                .block();
    }


    @Test
    public void multiCommand(ITestContext context) {
        List<String> list = new ArrayList<>();
        list.add("UPDATE u_user AS u SET  u.nick_name= concat(u.nick_name,'2') limit 3");
        list.add("SELECT * ROM u_user as u");
        list.add("SELECT u.id FROM u_user as u");

        StringBuilder builder = new StringBuilder();
        final int size = list.size();
        for (int i = 0; i < size; i++) {
            if (i > 0) {
                builder.append(";");
            }
            builder.append(list.get(i));
        }

        final ClientCommandProtocol p = obtainProtocol(context);

    }


    private ClientCommandProtocol obtainProtocol(ITestContext context) {
        ClientCommandProtocol protocol = (ClientCommandProtocol) context.getAttribute("commandProtocol");
        return Objects.requireNonNull(protocol, "protocol");
    }

}
