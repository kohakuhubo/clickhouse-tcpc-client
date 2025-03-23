package com.berry.clickhouse.tcp.client.log;

/**
 * Util类提供了一些实用的静态方法
 * 包括安全获取系统属性和报告错误的方法
 */
public final class Util {

    private Util() {
    }

    /**
     * 安全获取系统属性
     * 
     * @param key 属性名称
     * @return 属性值，如果未找到则返回null
     * @throws IllegalArgumentException 如果输入为null
     */
    public static String safeGetSystemProperty(String key) {
        if (key == null)
            throw new IllegalArgumentException("null input");

        String result = null;
        try {
            result = System.getProperty(key);
        } catch (SecurityException sm) {
            // ignore
        }
        return result;
    }

    /**
     * 安全获取布尔类型的系统属性
     * 
     * @param key 属性名称
     * @return true如果属性值为"true"，false否则
     */
    public static boolean safeGetBooleanSystemProperty(String key) {
        String value = safeGetSystemProperty(key);
        if (value == null)
            return false;
        else
            return value.equalsIgnoreCase("true");
    }

    private static final class ClassContextSecurityManager extends SecurityManager {
        protected Class<?>[] getClassContext() {
            return super.getClassContext();
        }
    }

    private static ClassContextSecurityManager SECURITY_MANAGER;
    private static boolean SECURITY_MANAGER_CREATION_ALREADY_ATTEMPTED = false;

    private static ClassContextSecurityManager getSecurityManager() {
        if (SECURITY_MANAGER != null)
            return SECURITY_MANAGER;
        else if (SECURITY_MANAGER_CREATION_ALREADY_ATTEMPTED)
            return null;
        else {
            SECURITY_MANAGER = safeCreateSecurityManager();
            SECURITY_MANAGER_CREATION_ALREADY_ATTEMPTED = true;
            return SECURITY_MANAGER;
        }
    }

    private static ClassContextSecurityManager safeCreateSecurityManager() {
        try {
            return new ClassContextSecurityManager();
        } catch (SecurityException sm) {
            return null;
        }
    }

    /**
     * 获取调用类的Class对象
     * 
     * @return 调用类的Class对象
     */
    public static Class<?> getCallingClass() {
        ClassContextSecurityManager securityManager = getSecurityManager();
        if (securityManager == null)
            return null;
        Class<?>[] trace = securityManager.getClassContext();
        String thisClassName = Util.class.getName();

        int i;
        for (i = 0; i < trace.length; i++) {
            if (thisClassName.equals(trace[i].getName()))
                break;
        }

        if (i >= trace.length || i + 2 >= trace.length) {
            throw new IllegalStateException("Failed to find its caller in the stack; " + "this should not happen");
        }

        return trace[i + 2];
    }

    /**
     * 报告错误信息
     * 
     * @param msg 错误消息
     * @param t 异常信息
     */
    static public void report(String msg, Throwable t) {
        System.err.println(msg);
        System.err.println("Reported exception:");
        t.printStackTrace();
    }

    /**
     * 报告信息
     * 
     * @param msg 信息消息
     */
    static public void report(String msg) {
        System.err.println("LOG: " + msg);
    }
}
