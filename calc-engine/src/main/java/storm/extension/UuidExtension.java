package storm.extension;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;
import java.util.UUID;

/**
 * @author: xzp
 * @date: 2018-12-14
 * @description:
 */
public final class UuidExtension {

    @Nullable
    public static String toStringWithoutDashes(@Nullable final UUID that) {
        return Optional
            .ofNullable(that)
            .map(uuid -> {
                final long mostSignificantBits = uuid.getMostSignificantBits();
                final long leastSignificantBits = uuid.getLeastSignificantBits();
                return (digits(mostSignificantBits >> 32, 8) +
                    digits(mostSignificantBits >> 16, 4) +
                    digits(mostSignificantBits, 4) +
                    digits(leastSignificantBits >> 48, 4) +
                    digits(leastSignificantBits, 12));
            })
            .orElse(null);
    }

    @NotNull
    private static String digits(long val, int digits) {
        long hi = 1L << (digits * 4);
        return Long.toHexString(hi | (val & (hi - 1))).substring(1);
    }
}
