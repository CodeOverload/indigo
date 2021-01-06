package nherald.indigo.store.uow;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import nherald.indigo.store.ItemId;

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
     * Map of item id to the item for that id. A null value indicates that
     * the item doesn't exist
     */
    private final Map<ItemId, Object> cache;

    public TransactionWithCache(Transaction transaction)
    {
        this.transaction = transaction;
        this.cache = new HashMap<>(401);
    }

    @Override
    public <T> T get(String namespace, String id, Class<T> type)
    {
        return get(namespace, Arrays.asList(id), type).get(0);
    }

    @Override
    public <T> List<T> get(String namespace, List<String> ids, Class<T> type)
    {
        // Create slots for the results, each containing the id and index
        final List<ResultSlot<T>> slots = ids.stream()
            .map(id -> new ResultSlot<T>(namespace, id))
            .collect(Collectors.toList());

        // Determine which items aren't cached
        final List<ResultSlot<T>> notCached = slots.stream()
            .filter(slot -> !slot.isCached())
            .collect(Collectors.toList());

        // Fetch those that aren't
        if (!notCached.isEmpty())
        {
            final List<String> notCachedIds = notCached.stream()
                .map(ResultSlot::getItemId)
                .collect(Collectors.toList());

            final List<T> items = transaction.get(namespace, notCachedIds, type);

            for (int i = 0; i < items.size(); ++i)
            {
                final ResultSlot<T> slot = notCached.get(i);

                slot.setResult(items.get(i));

                // Cache the newly fetched entry
                cache.put(slot.getCacheKey(), items.get(i));
            }
        }

        return slots.stream()
            .map(ResultSlot::getResult)
            .collect(Collectors.toList());
    }

    @Override
    public boolean exists(String namespace, String id)
    {
        final ItemId key = new ItemId(namespace, id);

        if (cache.containsKey(key))
        {
            return cache.get(key) != null;
        }

        return transaction.exists(namespace, id);
    }

    @Override
    public <T> void put(String namespace, String id, T item)
    {
        final ItemId key = new ItemId(namespace, id);

        cache.put(key, item);

        transaction.put(namespace, id, item);
    }

    @Override
    public void delete(String namespace, String id)
    {
        final ItemId key = new ItemId(namespace, id);

        cache.put(key, null);

        transaction.delete(namespace, id);
    }

    private class ResultSlot<T>
    {
        private final ItemId itemId;
        private final boolean isCached;
        private T result;

        public ResultSlot(String namespace, String id)
        {
            this.itemId = new ItemId(namespace, id);

            this.isCached = cache.containsKey(itemId);

            if (isCached)
            {
                result = getFromCache(itemId);
            }
        }

        public String getItemId()
        {
            return itemId.getId();
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

        public ItemId getCacheKey()
        {
            return itemId;
        }

        @SuppressWarnings("unchecked")
        private T getFromCache(ItemId key)
        {
            return (T) cache.get(key);
        }
    }
}
