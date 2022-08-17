package io.moquette.broker.subscriptions;

/**
 * Description: HandleTimeUtil
 *
 * @author Lizexin
 * @date 2022-08-15 13:17
 */
public class HandleTimeUtil {

    private static final ThreadLocal<Long> THREAD_LOCAL = new ThreadLocal<>();

    public static Long getHandleTime() {
        return THREAD_LOCAL.get();
    }

    public static void setHandleTime(Long handleTime) {
        THREAD_LOCAL.set(handleTime);
    }

    private HandleTimeUtil() {
    }
}
