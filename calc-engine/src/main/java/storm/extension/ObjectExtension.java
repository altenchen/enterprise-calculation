package storm.extension;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storm.util.function.SupplierE;

import java.util.function.Supplier;

/**
 * @author: xzp
 * @date: 2018-09-30
 * @description:
 */
public final class ObjectExtension {

    @Contract(value = "!null, _ -> param1; null, _ -> param2", pure = true)
    public static <T> T defaultIfNull(
        @Nullable final T object,
        @NotNull final T defaultValue) {

        return object != null ? object : defaultValue;
    }

    @Contract("!null, _ -> param1")
    public static <T> T defaultIfNull(
        @Nullable final T object,
        @NotNull final Supplier<? extends T> defaultValue) {

        return object != null ? object : defaultValue.get();
    }

    @Contract("!null, _ -> param1")
    public static <T, E extends Exception> T defaultIfNull(
        @Nullable final T object,
        @NotNull final SupplierE<? extends T, ? extends E> defaultValue) throws E {

        return object != null ? object : defaultValue.get();
    }
}
