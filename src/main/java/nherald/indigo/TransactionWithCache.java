package nherald.indigo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    public <T> List<T> get(String namespace, List<String> ids, Class<T> entityType)
    {
        // Create slots for the results, each containing the id and index
        final List<ResultSlot<T>> slots = ids.stream()
            .map(id -> new ResultSlot<T>(namespace, id))
            .collect(Collectors.toList());

        // Determine which entities aren't cached
        final List<ResultSlot<T>> notCached = slots.stream()
            .filter(slot -> !slot.isCached())
            .collect(Collectors.toList());

        // Fetch those that aren't
        if (!notCached.isEmpty())
        {
            final List<String> notCachedIds = notCached.stream()
                .map(ResultSlot::getEntityId)
                .collect(Collectors.toList());

            final List<T> entities = transaction.get(namespace, notCachedIds, entityType);

            for (int i = 0; i < entities.size(); ++i)
            {
                final ResultSlot<T> slot = notCached.get(i);

                slot.setResult(entities.get(i));

                // Cache the newly fetched entry
                cache.put(slot.getCacheKey(), entities.get(i));
            }
        }

        return slots.stream()
            .map(ResultSlot::getResult)
            .collect(Collectors.toList());
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

    private class ResultSlot<T>
    {
        private final String entityId;
        private final MapKey cacheKey;
        private final boolean isCached;
        private T result;

        public ResultSlot(String namespace, String entityId)
        {
            this.entityId = entityId;
            this.cacheKey = new MapKey(namespace, entityId);

            this.isCached = cache.containsKey(cacheKey);

            if (isCached)
            {
                result = getFromCache(cacheKey);
            }
        }

        public String getEntityId()
        {
            return entityId;
        }

        public boolean isCached()
        {
            return isCached;
        }

        public T getResult()
        {
            return result;
        }

        public void setResult(T result)
        {
            this.result = result;
        }

        public MapKey getCacheKey()
        {
            return cacheKey;
        }
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
