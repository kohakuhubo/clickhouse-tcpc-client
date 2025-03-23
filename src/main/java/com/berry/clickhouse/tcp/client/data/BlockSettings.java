package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;

/**
 * BlockSettings类用于管理数据块的设置
 * 包含与数据块相关的配置选项
 */
public class BlockSettings {
    private final Setting[] settings; // 设置数组

    /**
     * 构造函数，初始化BlockSettings
     * 
     * @param settings 设置数组
     */
    public BlockSettings(Setting[] settings) {
        this.settings = settings;
    }

    /**
     * 将设置写入二进制序列化器
     * 
     * @param serializer 二进制序列化器
     * @throws IOException 如果写入过程中发生I/O错误
     */
    public void writeTo(BinarySerializer serializer) throws IOException {
        for (Setting setting : settings) {
            serializer.writeVarInt(setting.num); // 写入设置编号

            if (Boolean.class.isAssignableFrom(setting.clazz)) {
                serializer.writeBoolean((Boolean) setting.defaultValue); // 写入布尔值
            } else if (Integer.class.isAssignableFrom(setting.clazz)) {
                serializer.writeInt((Integer) setting.defaultValue); // 写入整数值
            }
        }
        serializer.writeVarInt(0); // 结束标记
    }

    /**
     * 从二进制反序列化器读取设置
     * 
     * @param deserializer 二进制反序列化器
     * @return 读取的BlockSettings实例
     * @throws IOException 如果读取过程中发生I/O错误
     */
    public static BlockSettings readFrom(BinaryDeserializer deserializer) throws IOException {
        return new BlockSettings(readSettingsFrom(1, deserializer)); // 读取设置
    }

    /**
     * 从反序列化器读取设置
     * 
     * @param currentSize 当前大小
     * @param deserializer 二进制反序列化器
     * @return 读取的设置数组
     * @throws IOException 如果读取过程中发生I/O错误
     */
    private static Setting[] readSettingsFrom(int currentSize, BinaryDeserializer deserializer) throws IOException {
        long num = deserializer.readVarInt(); // 读取设置编号

        for (Setting setting : Setting.defaultValues()) {
            if (setting.num == num) {
                if (Boolean.class.isAssignableFrom(setting.clazz)) {
                    Setting receiveSetting = new Setting(setting.num, deserializer.readBoolean()); // 读取布尔值
                    Setting[] settings = readSettingsFrom(currentSize + 1, deserializer);
                    settings[currentSize - 1] = receiveSetting; // 设置值
                    return settings;
                } else if (Integer.class.isAssignableFrom(setting.clazz)) {
                    Setting receiveSetting = new Setting(setting.num, deserializer.readInt()); // 读取整数值
                    Setting[] settings = readSettingsFrom(currentSize + 1, deserializer);
                    settings[currentSize - 1] = receiveSetting; // 设置值
                    return settings;
                }
            }
        }
        return new Setting[currentSize - 1]; // 返回设置数组
    }

    /**
     * 设置类，定义具体的设置项
     */
    public static class Setting {
        public static final Setting IS_OVERFLOWS = new Setting(1, false); // 溢出设置
        public static final Setting BUCKET_NUM = new Setting(2, -1); // 桶数量设置

        /**
         * 获取默认设置值
         * 
         * @return 默认设置数组
         */
        public static Setting[] defaultValues() {
            return new Setting[] {IS_OVERFLOWS, BUCKET_NUM}; // 返回默认设置
        }

        private final int num; // 设置编号
        private final Class<?> clazz; // 设置值的类型
        private final Object defaultValue; // 默认值

        /**
         * 构造函数，初始化设置
         * 
         * @param num 设置编号
         * @param defaultValue 默认值
         */
        public Setting(int num, Object defaultValue) {
            this.num = num;
            this.defaultValue = defaultValue;
            this.clazz = defaultValue.getClass(); // 设置值的类型
        }
    }
}
