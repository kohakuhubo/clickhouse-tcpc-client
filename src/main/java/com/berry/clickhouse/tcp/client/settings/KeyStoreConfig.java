package com.berry.clickhouse.tcp.client.settings;

public class KeyStoreConfig {
    private String keyStoreType;
    private String keyStorePath;
    private String keyStorePassword;

    public KeyStoreConfig() {
    }

    public KeyStoreConfig(String keyStoreType, String keyStorePath, String keyStorePassword) {
        this.keyStoreType = keyStoreType;
        this.keyStorePath = keyStorePath;
        this.keyStorePassword = keyStorePassword;
    }

    public String getKeyStorePath() {
        return this.keyStorePath;
    }

    public String getKeyStorePassword() {
        return this.keyStorePassword;
    }

    public String getKeyStoreType() {
        return this.keyStoreType;
    }
}
