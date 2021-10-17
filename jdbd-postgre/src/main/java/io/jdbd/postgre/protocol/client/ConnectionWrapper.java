package io.jdbd.postgre.protocol.client;

import java.util.Map;

class ConnectionWrapper {

    final SessionManager sessionManager;

    final Map<String, String> initializedParamMap;

    ConnectionWrapper(SessionManager sessionManager, Map<String, String> initializedParamMap) {
        this.sessionManager = sessionManager;
        this.initializedParamMap = initializedParamMap;
    }


}
