package io.jdbd.postgre.protocol.client;

import java.util.Map;

class ConnectionWrapper {

    final ConnectionManager connectionManager;

    final Map<String, String> initializedParamMap;

    ConnectionWrapper(ConnectionManager connectionManager, Map<String, String> initializedParamMap) {
        this.connectionManager = connectionManager;
        this.initializedParamMap = initializedParamMap;
    }


}
