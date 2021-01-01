package nherald.indigo.store;

import nherald.indigo.store.uow.Consumer;
import nherald.indigo.store.uow.Transaction;
import nherald.indigo.store.uow.WrapTransaction;

/**
 * Stores entities (documents/objects/etc) to persistent storage.
 *
 * Implementations are not intended to be thread safe
 */
public interface Store extends StoreReadOps
{
    <T extends Transaction> void transaction(Consumer<T> runnable,
        WrapTransaction<T> wrapFunction);
}
