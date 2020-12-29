package nherald.indigo.store;

import java.util.Collection;
import java.util.List;

import nherald.indigo.uow.Transaction;

/**
 * Stores entities (documents/objects/etc) to persistent storage.
 *
 * Implementations are not intended to be thread safe
 */
public interface Store
{
    <T> T get(String namespace, String id, Class<T> itemType);

    <T> List<T> get(String namespace, Collection<String> ids, Class<T> itemType);

    boolean exists(String namespace, String id);

    <T> void put(String namespace, String id, T entity, Transaction transaction);

    void delete(String namespace, String id, Transaction transaction);

    Transaction transaction();
}
