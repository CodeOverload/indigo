package nherald.indigo.store.uow;

/**
 * A single transaction. Different Store implementations have different
 * transaction support; e.g. Firestore supports transactions fully (they're run
 * as an atomic operation, and if any of the objects read are updated by
 * another process in the meantime the transaction is retried). The File
 * Store implementation doesn't currently support transactions.
 *
 * Note that implementations may batch updates. Meaning that if you modify
 * an entity after you've passed it in, but before it's been committed, those
 * changes may be persisted on commit. This is a trade off for performance
 */
public interface Transaction
{
    /**
     * Fetch a single entity
     * @param <T> the entity type to deserialise
     * @param namespace namespace
     * @param entityId entity id
     * @param entityType entity type
     * @return the entity, or null if the entity doesn't exist
     */
    <T> T get(String namespace, String entityId, Class<T> entityType);

    /**
     * Determine if an entity exists
     * @param namespace namespace
     * @param entityId entity id
     * @return true if the entity with this id is stored, false otherwise
     */
    boolean exists(String namespace, String entityId);

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
