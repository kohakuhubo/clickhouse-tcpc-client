package com.berry.clickhouse.tcp.client.settings;

/**
 * KeyStoreConfig类用于配置密钥库的设置
 * 包括密钥库类型、路径和密码
 */
public class KeyStoreConfig {
    private String keyStoreType; // 密钥库类型
    private String keyStorePath; // 密钥库路径
    private String keyStorePassword; // 密钥库密码

    public KeyStoreConfig() {
    }

    /**
     * KeyStoreConfig构造函数
     * 
     * @param keyStoreType 密钥库类型
     * @param keyStorePath 密钥库路径
     * @param keyStorePassword 密钥库密码
     */
    public KeyStoreConfig(String keyStoreType, String keyStorePath, String keyStorePassword) {
        this.keyStoreType = keyStoreType;
        this.keyStorePath = keyStorePath;
        this.keyStorePassword = keyStorePassword;
    }

    public String getKeyStorePath() {
        return this.keyStorePath; // 返回密钥库路径
    }

    public String getKeyStorePassword() {
        return this.keyStorePassword; // 返回密钥库密码
    }

    public String getKeyStoreType() {
        return this.keyStoreType; // 返回密钥库类型
    }
}
