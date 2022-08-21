package io.moquette.broker.subscriptions;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Description: HandleTimeUtil
 *
 * @author Lizexin
 * @date 2022-08-15 13:17
 */
public class HandleTimeUtil {

    private static final Map<String, ThreadLocal<Long>> TASK_HANDLE_TIME_MAP = new HashMap<>();

    public static void addHandleTime(String taskName, Long handleTime) {
        ThreadLocal<Long> taskHandleTime = TASK_HANDLE_TIME_MAP.computeIfAbsent(taskName, new Function<String, ThreadLocal<Long>>() {
            @Override
            public ThreadLocal<Long> apply(String s) {
                ThreadLocal<Long> taskHandleTime = new ThreadLocal<>();
                taskHandleTime.set(0L);
                return taskHandleTime;
            }
        });
        taskHandleTime.set(taskHandleTime.get() + handleTime);
    }

    public static Long getHandleTime(String taskName) {
        return TASK_HANDLE_TIME_MAP.get(taskName).get();
    }

    public static void clearHandleTime(String taskName) {
        TASK_HANDLE_TIME_MAP.remove(taskName);
    }

    public static void printResult() {
        TASK_HANDLE_TIME_MAP.forEach(
                (taskName, taskHandleTime) -> System.out.print(taskName + " handleTime is " + TimeUnit.NANOSECONDS.toSeconds(taskHandleTime.get()) + "s; ")
        );
        System.out.println();
    }


    private HandleTimeUtil() {
    }
}
