package nherald.indigo.store;

import java.util.List;

public interface StoreReadOps
{
    /**
     * Fetch a single item
     * @param <T> the item type to deserialise
     * @param namespace namespace
     * @param id item id
     * @param type item type
     * @return the item, or null if the item doesn't exist
     */
    <T> T get(String namespace, String id, Class<T> type);

    /**
     * Fetch multiple items
     * @param <T> the item type
     * @param namespace namespace
     * @param ids item ids
     * @param type item type
     * @return the items. If an item doesn't exist, a null will
     * be returned in the returned list at the same position as the
     * id in the ids list
     */
    <T> List<T> get(String namespace, List<String> ids, Class<T> type);

    /**
     * Determine if an item exists
     * @param namespace namespace
     * @param id item id
     * @return true if the item with this id is stored, false otherwise
     */
    boolean exists(String namespace, String id);
}
