package io.jdbd.mysql.protocol.client;

import io.jdbd.vendor.CommTaskExecutorAdjutant;

 interface MySQLTaskAdjutant extends CommTaskExecutorAdjutant, ClientProtocolAdjutant {


     MySQLCommTaskExecutor obtainCommTaskExecutor();

 }
