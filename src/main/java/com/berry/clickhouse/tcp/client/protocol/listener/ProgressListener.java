package com.berry.clickhouse.tcp.client.protocol.listener;

import com.berry.clickhouse.tcp.client.protocol.ProgressResponse;

public interface ProgressListener {
    void onProgress(ProgressResponse progressResponse);
}
