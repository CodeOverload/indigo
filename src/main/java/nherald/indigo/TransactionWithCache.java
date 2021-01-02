package nherald.indigo;

import java.util.Arrays;
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
    private final Map<EntityId, Object> cache;

    public TransactionWithCache(Transaction transaction)
    {
        this.transaction = transaction;
        this.cache = new HashMap<>(401);
    }

    @Override
    public <T> T get(String namespace, String entityId, Class<T> entityType)
    {
        return get(namespace, Arrays.asList(entityId), entityType).get(0);
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
        final EntityId key = new EntityId(namespace, entityId);

        if (cache.containsKey(key))
        {
            return cache.get(key) != null;
        }

        return transaction.exists(namespace, entityId);
    }

    @Override
    public <T> void put(String namespace, String entityId, T entity)
    {
        final EntityId key = new EntityId(namespace, entityId);

        cache.put(key, entity);

        transaction.put(namespace, entityId, entity);
    }

    @Override
    public void delete(String namespace, String entityId)
    {
        final EntityId key = new EntityId(namespace, entityId);

        cache.put(key, null);

        transaction.delete(namespace, entityId);
    }

    private class ResultSlot<T>
    {
        private final String entityId;
        private final EntityId cacheKey;
        private final boolean isCached;
        private T result;

        public ResultSlot(String namespace, String entityId)
        {
            this.entityId = entityId;
            this.cacheKey = new EntityId(namespace, entityId);

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

        public EntityId getCacheKey()
        {
            return cacheKey;
        }

        @SuppressWarnings("unchecked")
        private T getFromCache(EntityId key)
        {
            return (T) cache.get(key);
        }
    }
}
