package nherald.indigo.store.uow;

/**
 * Capable of wrapping a transaction class with a wrapper
 */
@FunctionalInterface
public interface WrapTransaction<T extends Transaction>
{
    /**
     * Takes a transaction and wraps it in a new wrapper instance
     * @param <T> the type of the wrapper
     * @param transaction transaction to wrap
     * @return wrapped transaction
     */
    T wrap(Transaction transaction);
}
