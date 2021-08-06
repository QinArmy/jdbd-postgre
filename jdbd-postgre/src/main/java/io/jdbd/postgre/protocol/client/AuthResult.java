package io.jdbd.postgre.protocol.client;

import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.Map;

final class AuthResult {

    /** a unmodified map */
    final Map<String, String> serverStatusMap;

    final BackendKeyData backendKeyData;

    final TxStatus txStatus;

    final NoticeMessage noticeMessage;

    AuthResult(Map<String, String> serverStatusMap, BackendKeyData backendKeyData
            , TxStatus txStatus, @Nullable NoticeMessage noticeMessage) {
        this.serverStatusMap = Collections.unmodifiableMap(serverStatusMap);
        this.backendKeyData = backendKeyData;
        this.txStatus = txStatus;
        this.noticeMessage = noticeMessage;
    }


}
