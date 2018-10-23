package storm.util.function;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * @author xzp
 *
 * Represents an operation that accepts two input arguments and returns no
 * result.  This is the two-arity specialization of {@link ConsumerE}.
 * Unlike most other functional interfaces, {@code BiConsumerE} is expected
 * to operate via side-effects.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #accept(Object, Object)}.
 *
 * @param <T> the type of the first argument to the operation
 * @param <U> the type of the second argument to the operation
 * @param <E> the type of the exception of the function
 *
 * @see ConsumerE
 * @since 1.8
 */
@FunctionalInterface
public interface BiConsumerE<T, U, E extends Exception> {

    /**
     * Performs this operation on the given arguments.
     *
     * @param t the first input argument
     * @param u the second input argument
     * @throws E the function exception
     */
    void accept(final T t, final U u) throws E;

    /**
     * Returns a composed {@code BiConsumerE} that performs, in sequence, this
     * operation followed by the {@code after} operation. If performing either
     * operation throws an exception, it is relayed to the caller of the
     * composed operation.  If performing this operation throws an exception,
     * the {@code after} operation will not be performed.
     *
     * @param after the operation to perform after this operation
     * @return a composed {@code BiConsumerE} that performs in sequence this
     * operation followed by the {@code after} operation
     * @throws NullPointerException if {@code after} is null
     */
    default BiConsumerE<? extends T, ? extends U, ? super E> andThen(
        @NotNull final BiConsumerE<? super T, ? super U, ? extends E> after) {
        Objects.requireNonNull(after);

        return (final T l, final U r) -> {
            accept(l, r);
            after.accept(l, r);
        };
    }
}
