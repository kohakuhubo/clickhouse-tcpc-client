package com.berry.clickhouse.tcp.client.ssl;

import com.berry.clickhouse.tcp.client.NativeClient;
import com.berry.clickhouse.tcp.client.log.Logger;
import com.berry.clickhouse.tcp.client.log.LoggerFactory;
import com.berry.clickhouse.tcp.client.settings.ClickHouseClientConfig;
import com.berry.clickhouse.tcp.client.settings.KeyStoreConfig;
import com.berry.clickhouse.tcp.client.settings.SettingKey;

import javax.net.ssl.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.*;
import java.security.cert.CertificateException;

/**
 * SSLContextBuilder类用于构建SSL上下文
 * 根据ClickHouse配置和密钥库配置初始化SSL上下文
 */
public class SSLContextBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(NativeClient.class); // 日志记录器

    private ClickHouseClientConfig config; // ClickHouse配置
    private KeyStoreConfig keyStoreConfig; // 密钥库配置

    /**
     * 构造函数，初始化SSLContextBuilder
     * 
     * @param config ClickHouse配置
     */
    public SSLContextBuilder(ClickHouseClientConfig config) {
        this.config = config;
        this.keyStoreConfig = new KeyStoreConfig(
                (String) config.settings().get(SettingKey.keyStoreType),
                (String) config.settings().get(SettingKey.keyStorePath),
                (String) config.settings().get(SettingKey.keyStorePassword)
        );
    }

    /**
     * 获取SSL上下文
     * 
     * @return 初始化后的SSLContext
     * @throws NoSuchAlgorithmException 找不到指定的算法
     * @throws KeyStoreException 密钥库异常
     * @throws IOException IO异常
     * @throws CertificateException 证书异常
     * @throws UnrecoverableKeyException 无法恢复的密钥异常
     * @throws KeyManagementException 密钥管理异常
     */
    public SSLContext getSSLContext() throws NoSuchAlgorithmException, KeyStoreException, IOException, CertificateException, UnrecoverableKeyException, KeyManagementException {
        SSLContext sslContext = SSLContext.getInstance("TLS"); // 创建TLS SSL上下文
        TrustManager[] trustManager = null; // 信任管理器
        KeyManager[] keyManager = null; // 密钥管理器
        SecureRandom secureRandom = new SecureRandom(); // 安全随机数生成器
        String sslMode = config.sslMode(); // 获取SSL模式
        LOG.debug("Client SSL mode: '" + sslMode + "'"); // 记录SSL模式

        switch (sslMode) {
            case "disabled":
                trustManager = new TrustManager[]{new PermissiveTrustManager()}; // 禁用SSL验证
                keyManager = new KeyManager[]{};
                break;
            case "verify_ca":
                KeyStore keyStore = KeyStore.getInstance(keyStoreConfig.getKeyStoreType()); // 获取密钥库实例
                keyStore.load(Files.newInputStream(Paths.get(keyStoreConfig.getKeyStorePath()).toFile().toPath()),
                        keyStoreConfig.getKeyStorePassword().toCharArray()); // 加载密钥库

                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore, keyStoreConfig.getKeyStorePassword().toCharArray()); // 初始化密钥管理器

                TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init(keyStore); // 初始化信任管理器

                trustManager = trustManagerFactory.getTrustManagers(); // 获取信任管理器
                keyManager = keyManagerFactory.getKeyManagers(); // 获取密钥管理器
                break;
            default:
                throw new IllegalArgumentException("Unknown SSL mode: '" + sslMode + "'"); // 抛出未知SSL模式异常
        }

        sslContext.init(keyManager, trustManager, secureRandom); // 初始化SSL上下文
        return sslContext; // 返回SSL上下文
    }
}
