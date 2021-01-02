package nherald.indigo.store.uow;

import nherald.indigo.store.StoreReadOps;

/**
 * A single transaction. Different Store implementations have different
 * transaction support. Firestore supports transactions fully (each
 * transaction is an atomic operation, and it detects whether any of the
 * objects touched by the transaction have been concurrently modified by
 * another transaction)
 *
 * <p>The following apply when using transactions:
 * <ul>
 * <li>Read operations (get/exist) are run immediately, but update operations
 * (put/delete) may be batched up and run at the end of the transaction.
 * Depending on the implementation, earlier updates on the same entity may be
 * ignored
 * <li>As a result of batching the updates, entity objects passed in may be
 * cached. If those objects are modified before they've been applied to the
 * database, the modifications may be applied to the database
 * <li>Don't attempt to use a read operation (get/exists) on an entity that has
 * been updated earlier in the transaction
 * <li>Transactions may be re-run, so don't update application state within a
 * transaction runnable
 * </ul>
 */
public interface Transaction extends StoreReadOps
{
    /**
     * Stores the supplied entity. If an entity already exists with the
     * specified id, it will be overwritten. Otherwise a new entry will
     * be created with the specified id
     * @param namespace namespace
     * @param entityId entity id
     * @param entity entity
     */
    <T> void put(String namespace, String entityId, T entity);

    /**
     * Deletes the entity with the specified id from storage
     * @param namespace namespace
     * @param entityId entity id
     */
    void delete(String namespace, String entityId);
}
