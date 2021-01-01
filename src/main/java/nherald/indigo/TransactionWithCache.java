package nherald.indigo;

import java.util.HashMap;
import java.util.Map;

import nherald.indigo.store.uow.Transaction;

/**
 * Transaction wrapper that caches operations. This allows us to maintain state
 * throughout the transaction, and build on changes made earlier. Caching at
 * the transaction level reduces a lot of the complexity elsewhere; e.g. we
 * don't need separate caches for index segments, index contents, the entities
 * info object etc. All of these ultimately come from the store, so wrapping
 * the store transaction makes more sense as it's a lot cleaner
 */
public class TransactionWithCache implements Transaction
{
    private final Transaction transaction;

    /**
     * Map of entity id to the entity for that id. A null value indicates that
     * the entity doesn't exist
     */
    private final Map<MapKey, Object> cache;

    public TransactionWithCache(Transaction transaction)
    {
        this.transaction = transaction;
        this.cache = new HashMap<>(401);
    }

    @Override
    public <T> T get(String namespace, String entityId, Class<T> entityType)
    {
        final MapKey key = new MapKey(namespace, entityId);

        T entity = getFromCache(key);

        if (entity != null) return entity;

        entity = transaction.get(namespace, entityId, entityType);

        // Will add null to the map if the entity didn't exist
        cache.put(key, entity);

        return entity;
    }

    @Override
    public boolean exists(String namespace, String entityId)
    {
        final MapKey key = new MapKey(namespace, entityId);

        if (cache.containsKey(key))
        {
            return cache.get(key) != null;
        }

        return transaction.exists(namespace, entityId);
    }

    @Override
    public <T> void put(String namespace, String entityId, T entity)
    {
        final MapKey key = new MapKey(namespace, entityId);

        cache.put(key, entity);

        transaction.put(namespace, entityId, entity);
    }

    @Override
    public void delete(String namespace, String entityId)
    {
        final MapKey key = new MapKey(namespace, entityId);

        cache.put(key, null);

        transaction.delete(namespace, entityId);
    }

    @SuppressWarnings("unchecked")
    private <T> T getFromCache(MapKey key)
    {
        return (T) cache.get(key);
    }

    private static class MapKey
    {
        private final String namespace;
        private final String entityId;

        public MapKey(String namespace, String entityId)
        {
            this.namespace = namespace;
            this.entityId = entityId;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((entityId == null) ? 0 : entityId.hashCode());
            result = prime * result + ((namespace == null) ? 0 : namespace.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            MapKey other = (MapKey) obj;
            if (entityId == null) {
                if (other.entityId != null)
                    return false;
            } else if (!entityId.equals(other.entityId))
                return false;
            if (namespace == null) {
                if (other.namespace != null)
                    return false;
            } else if (!namespace.equals(other.namespace))
                return false;
            return true;
        }
    }
}
