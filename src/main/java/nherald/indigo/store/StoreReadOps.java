package nherald.indigo.store;

public interface StoreReadOps
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
}
