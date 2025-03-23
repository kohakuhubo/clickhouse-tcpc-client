package com.berry.clickhouse.tcp.client.ssl;

import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;

/**
 * PermissiveTrustManager类实现了X509TrustManager接口
 * 该类允许所有的SSL证书，不进行任何验证
 */
public class PermissiveTrustManager implements X509TrustManager {

    @Override
    public void checkClientTrusted(X509Certificate[] x509Certificates, String s) {
        // 允许所有客户端证书
    }

    @Override
    public void checkServerTrusted(X509Certificate[] x509Certificates, String s) {
        // 允许所有服务器证书
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0]; // 返回空数组，表示接受所有颁发者
    }
}
