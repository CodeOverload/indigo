package nherald.indigo.store;

import java.util.List;

public interface StoreReadOps
{
    /**
     * Fetch a single entity
     * @param <T> the entity type to deserialise
     * @param namespace namespace
     * @param id entity id
     * @param entityType entity type
     * @return the entity, or null if the entity doesn't exist
     */
    <T> T get(String namespace, String id, Class<T> entityType);

    /**
     * Fetch multiple entities
     * @param <T> the entity type
     * @param namespace namespace
     * @param ids entity ids
     * @param entityType entity type
     * @return the entities. If an entity doesn't exist, a null will
     * be returned in the returned list at the same position as the
     * id in the ids list
     */
    <T> List<T> get(String namespace, List<String> ids, Class<T> entityType);

    /**
     * Determine if an entity exists
     * @param namespace namespace
     * @param id entity id
     * @return true if the entity with this id is stored, false otherwise
     */
    boolean exists(String namespace, String id);
}
