package storm.util.function;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * @author xzp
 *
 * Represents an operation that accepts a single input argument and returns no
 * result. Unlike most other functional interfaces, {@code ConsumerE} is expected
 * to operate via side-effects.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #accept(Object)}.
 *
 * @param <T> the type of the input to the operation
 * @param <E> the type of the exception of the function
 *
 * @since 1.8
 */
@FunctionalInterface
public interface ConsumerE<T, E extends Exception> {

    /**
     * Performs this operation on the given argument.
     *
     * @param t the input argument
     * @throws E the function exception
     */
    void accept(final T t) throws E;

    /**
     * Returns a composed {@code ConsumerE} that performs, in sequence, this
     * operation followed by the {@code after} operation. If performing either
     * operation throws an exception, it is relayed to the caller of the
     * composed operation.  If performing this operation throws an exception,
     * the {@code after} operation will not be performed.
     *
     * @param after the operation to perform after this operation
     * @return a composed {@code ConsumerE} that performs in sequence this
     * operation followed by the {@code after} operation
     * @throws NullPointerException if {@code after} is null
     */
    default ConsumerE<? extends T, ? super E> andThen(
        @NotNull final ConsumerE<? super T, ? extends E> after) {
        Objects.requireNonNull(after);

        return (final T t) -> {
            accept(t);
            after.accept(t);
        };
    }
}
