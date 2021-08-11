package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.config.PostgreHost;
import io.jdbd.vendor.task.ITaskAdjutant;

import java.nio.charset.Charset;

interface TaskAdjutant extends ITaskAdjutant {


    PostgreHost obtainHost();

    long processId();

    Charset clientCharset();

}
