package com.berry.clickhouse.tcp.client.protocol.listener;

import com.berry.clickhouse.tcp.client.log.Logger;
import com.berry.clickhouse.tcp.client.log.LoggerFactory;
import com.berry.clickhouse.tcp.client.protocol.ProgressResponse;

public class DefaultProgressListener implements ProgressListener {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultProgressListener.class);

    private DefaultProgressListener() {
    }

    public static DefaultProgressListener create() {
        return new DefaultProgressListener();
    }

    @Override
    public void onProgress(ProgressResponse progressResponse) {
        LOG.info("DefaultProgressListener: ".concat(progressResponse.toString()));
    }
}
