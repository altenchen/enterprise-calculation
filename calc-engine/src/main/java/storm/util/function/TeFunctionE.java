package storm.util.function;


import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * @author xzp
 * 虽然都可以用高阶函数科里化来做, 但是没有函数这么直观, 所以加了这么个函数式接口.
 *
 * Represents a function that accepts three arguments and produces a result.
 * This is the three-arity specialization of {@link FunctionE}.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #apply(Object, Object, Object)}.
 *
 * @param <T> the type of the first argument to the function
 * @param <U> the type of the second argument to the function
 * @param <F> the type of the third argument to the function
 * @param <R> the type of the result of the function
 * @param <E> the type of the exception of the function
 *
 * @see TeFunctionE
 * @since 1.8
 */
@FunctionalInterface
public interface TeFunctionE<T, U, F, R, E extends Exception> {

    /**
     * Applies this function to the given arguments.
     *
     * @param t the first function argument
     * @param u the second function argument
     * @param f the third function argument
     * @return the function result
     * @throws E the function exception
     */
    R apply(final T t, final U u, final F f) throws E;

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
        @NotNull final FunctionE<? super R, ? extends V, ? extends E> after) {
        Objects.requireNonNull(after);

        return (final T t, final U u, final F f) -> after.apply(apply(t, u, f));
    }

    /**
     * Returns a composed {@code BiConsumer} that performs, in sequence, this
     * operation followed by the {@code after} operation. If performing either
     * operation throws an exception, it is relayed to the caller of the
     * composed operation.  If performing this operation throws an exception,
     * the {@code after} operation will not be performed.
     *
     * @param after the operation to perform after this operation
     * @return a composed {@code BiConsumer} that performs in sequence this
     * operation followed by the {@code after} operation
     * @throws NullPointerException if {@code after} is null
     */
    default TeConsumerE<? extends T, ? extends U, ? extends F, ? super E> andThen(
        @NotNull final ConsumerE<? super R, ? extends E> after) {
        Objects.requireNonNull(after);

        return (final T t, final U u, final F f) -> after.accept(apply(t, u, f));
    }
}
