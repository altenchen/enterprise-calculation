package storm.util.function;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * @author xzp
 * 虽然都可以用高阶函数科里化来做, 但是没有函数这么直观, 所以加了这么个函数式接口.
 *
 * Represents an operation that accepts two input arguments and returns no
 * result.  This is the three-arity specialization of {@link ConsumerE}.
 * Unlike most other functional interfaces, {@code TeConsumerE} is expected
 * to operate via side-effects.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #accept(Object, Object, Object)}.
 *
 * @param <T> the type of the first argument to the operation
 * @param <U> the type of the second argument to the operation
 * @param <F> the type of the third argument to the function
 * @param <E> the type of the exception of the function
 *
 * @see TeConsumerE
 * @since 1.8
 */
@FunctionalInterface
public interface TeConsumerE<T, U, F, E extends Exception> {

    /**
     * Performs this operation on the given arguments.
     *
     * @param t the first input argument
     * @param u the second input argument
     * @param f the third function argument
     * @throws E the function exception
     */
    void accept(final T t, final U u, final F f) throws E;

    /**
     * Returns a composed {@code TeConsumerE} that performs, in sequence, this
     * operation followed by the {@code after} operation. If performing either
     * operation throws an exception, it is relayed to the caller of the
     * composed operation.  If performing this operation throws an exception,
     * the {@code after} operation will not be performed.
     *
     * @param after the operation to perform after this operation
     * @return a composed {@code TeConsumerE} that performs in sequence this
     * operation followed by the {@code after} operation
     * @throws NullPointerException if {@code after} is null
     */
    default TeConsumerE<? extends T, ? extends U, ? extends F, ? super E> andThen(
        @NotNull final TeConsumerE<? super T, ? super U, ? super F, ? extends E> after) {
        Objects.requireNonNull(after);

        return (final T t, final U u, final F f) -> {
            accept(t, u, f);
            after.accept(t, u, f);
        };
    }

    /**
     * Returns a composed function that first applies this function to
     * its input, and then applies the {@code after} function to the result.
     * If evaluation of either function throws an exception, it is relayed to
     * the caller of the composed function.
     *
     * @param <V> the type of output of the {@code after} function, and of the
     *           composed function
     * @param after the function to apply after this function is applied
     * @return a composed function that first applies this function and then
     * applies the {@code after} function
     * @throws NullPointerException if after is null
     */
    default <V> TeFunctionE<? extends T, ? extends U, ? extends F, ? super V, ? super E> andThen(
        @NotNull final TeFunctionE<? super T, ? super U, ? super F, ? extends V, ? extends E> after) {

        Objects.requireNonNull(after);

        return (final T t, final U u, final F f) -> {
            accept(t, u, f);
            return after.apply(t, u, f);
        };
    }
}
