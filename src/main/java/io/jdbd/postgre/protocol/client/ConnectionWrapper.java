package io.jdbd.postgre.protocol.client;

import java.util.Map;

class ConnectionWrapper {

    final ProtocolManager sessionManager;

    final Map<String, String> initializedParamMap;

    ConnectionWrapper(ProtocolManager sessionManager, Map<String, String> initializedParamMap) {
        this.sessionManager = sessionManager;
        this.initializedParamMap = initializedParamMap;
    }


}
