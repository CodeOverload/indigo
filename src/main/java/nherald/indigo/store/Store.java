package nherald.indigo.store;

import nherald.indigo.store.uow.Consumer;
import nherald.indigo.store.uow.Transaction;
import nherald.indigo.store.uow.WrapTransaction;

/**
 * Stores items to persistent storage.
 *
 * Implementations are not intended to be thread safe
 */
public interface Store extends StoreReadOps, StoreListOps
{
    <T extends Transaction> void transaction(Consumer<T> runnable,
        WrapTransaction<T> wrapFunction);
}
