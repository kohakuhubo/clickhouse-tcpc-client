package com.berry.clickhouse.tcp.client.serde;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;

/**
 * SettingType接口定义了序列化和反序列化设置的类型
 * 
 * @param <T> 可序列化的类型
 */
public interface SettingType<T extends Serializable> {

    /**
     * 获取Java类类型
     * 
     * @return Java类类型
     */
    Class<T> javaClass();

    /**
     * 从URL查询参数反序列化
     * 
     * @param queryParameter 查询参数
     * @return 反序列化后的对象
     */
    T deserializeURL(String queryParameter);

    /**
     * 序列化设置
     * 
     * @param serializer 二进制序列化器
     * @param value 要序列化的值
     * @throws IOException 如果序列化失败
     */
    void serializeSetting(BinarySerializer serializer, T value) throws IOException;

    // 各种类型的设置实现
    SettingType<Long> Int64 = new SettingType<Long>() {
        @Override
        public Class<Long> javaClass() {
            return Long.class; // 返回Long类
        }

        @Override
        public Long deserializeURL(String queryParameter) {
            return Long.valueOf(queryParameter); // 从查询参数反序列化
        }

        @Override
        public void serializeSetting(BinarySerializer serializer, Long value) throws IOException {
            serializer.writeVarInt(value); // 序列化为变长整数
        }
    };

    SettingType<Integer> Int32 = new SettingType<Integer>() {
        @Override
        public Class<Integer> javaClass() {
            return Integer.class; // 返回Integer类
        }

        @Override
        public Integer deserializeURL(String queryParameter) {
            return Integer.valueOf(queryParameter); // 从查询参数反序列化
        }

        @Override
        public void serializeSetting(BinarySerializer serializer, Integer value) throws IOException {
            serializer.writeVarInt(value); // 序列化为变长整数
        }
    };

    SettingType<Float> Float32 = new SettingType<Float>() {
        @Override
        public Class<Float> javaClass() {
            return Float.class; // 返回Float类
        }

        @Override
        public Float deserializeURL(String queryParameter) {
            return Float.valueOf(queryParameter); // 从查询参数反序列化
        }

        @Override
        public void serializeSetting(BinarySerializer serializer, Float value) throws IOException {
            serializer.writeUTF8StringBinary(String.valueOf(value)); // 序列化为UTF-8字符串
        }
    };

    SettingType<String> UTF8 = new SettingType<String>() {
        @Override
        public Class<String> javaClass() {
            return String.class; // 返回String类
        }

        @Override
        public String deserializeURL(String queryParameter) {
            return queryParameter; // 直接返回查询参数
        }

        @Override
        public void serializeSetting(BinarySerializer serializer, String value) throws IOException {
            serializer.writeUTF8StringBinary(String.valueOf(value)); // 序列化为UTF-8字符串
        }
    };

    SettingType<Boolean> Bool = new SettingType<Boolean>() {
        @Override
        public Class<Boolean> javaClass() {
            return Boolean.class; // 返回Boolean类
        }

        @Override
        public Boolean deserializeURL(String queryParameter) {
            return Boolean.valueOf(queryParameter); // 从查询参数反序列化
        }

        @Override
        public void serializeSetting(BinarySerializer serializer, Boolean value) throws IOException {
            serializer.writeVarInt(Boolean.TRUE.equals(value) ? 1 : 0); // 序列化为变长整数
        }
    };

    SettingType<Duration> Seconds = new SettingType<Duration>() {
        @Override
        public Class<Duration> javaClass() {
            return Duration.class; // 返回Duration类
        }

        @Override
        public Duration deserializeURL(String queryParameter) {
            return Duration.ofSeconds(Long.parseLong(queryParameter)); // 从查询参数反序列化
        }

        @Override
        public void serializeSetting(BinarySerializer serializer, Duration value) throws IOException {
            serializer.writeVarInt(value.getSeconds()); // 序列化为变长整数
        }
    };

    SettingType<Duration> Milliseconds = new SettingType<Duration>() {
        @Override
        public Class<Duration> javaClass() {
            return Duration.class; // 返回Duration类
        }

        @Override
        public Duration deserializeURL(String queryParameter) {
            return Duration.ofMillis(Long.parseLong(queryParameter)); // 从查询参数反序列化
        }

        @Override
        public void serializeSetting(BinarySerializer serializer, Duration value) throws IOException {
            serializer.writeVarInt(value.toMillis()); // 序列化为变长整数
        }
    };

    SettingType<Character> Char = new SettingType<Character>() {
        @Override
        public Class<Character> javaClass() {
            return Character.class; // 返回Character类
        }

        @Override
        public Character deserializeURL(String queryParameter) {
            return queryParameter.charAt(0); // 从查询参数反序列化
        }

        @Override
        public void serializeSetting(BinarySerializer serializer, Character value) throws IOException {
            serializer.writeUTF8StringBinary(String.valueOf(value)); // 序列化为UTF-8字符串
        }
    };
}
