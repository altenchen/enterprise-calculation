package storm.constant;

import org.jetbrains.annotations.NotNull;

/**
 * @author: xzp
 * @date: 2018-07-06
 * @description:
 */

public final class RedisConstant {

    private RedisConstant() {
    }

    public static final class Select {

        @NotNull
        public static final String OK = "OK";
    }

    public static final class HashSet {

        @NotNull
        public static final Long UPDATE = 0L;

        @NotNull
        public static final Long CREATE = 1L;
    }

    public static final class HashSetNotExist {

        @NotNull
        public static final Long NOTHING = 0L;

        @NotNull
        public static final Long CREATE = 1L;
    }
}
