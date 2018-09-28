package storm.util.function;

/**
 * @author xzp
 *
 * Represents a supplier of results.
 *
 * <p>There is no requirement that a new or distinct result be returned each
 * time the supplier is invoked.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #get()}.
 *
 * @param <T> the type of results supplied by this supplier
 * @param <E> the type of the exception of the function
 *
 * @since 1.8
 */
@FunctionalInterface
public interface SupplierE<T, E extends Exception> {

    /**
     * Gets a result.
     *
     * @return a result
     * @throws E the function exception
     */
    T get() throws E;
}
