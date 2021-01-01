package nherald.indigo.store;

import java.util.Collection;
import java.util.List;

import nherald.indigo.store.uow.Consumer;
import nherald.indigo.store.uow.Transaction;

/**
 * Stores entities (documents/objects/etc) to persistent storage.
 *
 * Implementations are not intended to be thread safe
 */
public interface Store extends StoreReadOps
{
    <T> List<T> get(String namespace, Collection<String> ids, Class<T> itemType);

    void transaction(Consumer<Transaction> runnable);
}
