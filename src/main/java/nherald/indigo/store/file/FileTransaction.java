package nherald.indigo.store.file;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nherald.indigo.store.uow.Transaction;

public class FileTransaction implements Transaction
{
    private static final Logger logger = LoggerFactory.getLogger(FileTransaction.class);

    private final FileStore store;

    private final Map<String, Update> pending = new HashMap<>(101);

    public FileTransaction(FileStore store)
    {
        this.store = store;
    }

    public <T> T get(String namespace, String id, Class<T> type)
    {
        return store.get(namespace, id, type);
    }

    public <T> List<T> get(String namespace, List<String> id, Class<T> type)
    {
        return store.get(namespace, id, type);
    }

    @Override
    public boolean exists(String namespace, String id)
    {
        return store.exists(namespace, id);
    }

    @Override
    public <T> void put(String namespace, String id, T item)
    {
        pending.put(getMapKey(namespace, id), () ->
            store.put(namespace, id, item)
        );
    }

    @Override
    public void delete(String namespace, String id)
    {
        pending.put(getMapKey(namespace, id), () ->
            store.delete(namespace, id)
        );
    }

    /**
     * Adds an update to the transaction, to update in one go when finished. Notes:
     *
     * - If an item is added more than once, only the last version added
     *   will be saved
     *
     * - Updates may be run in any order, regardless of the order they were added.
     *   All items are stored independently on the filesystem, with no hard
     *   dependencies between each other, so it shouldn't matter which order
     *   they're saved in
     */
    public void add(String namespace, String id, Update update)
    {
        pending.put(getMapKey(namespace, id), update);
    }

    void commit()
    {
        pending.entrySet()
            .stream()
            .forEach(this::commitChange);
    }

    private String getMapKey(String namespace, String id)
    {
        return new StringBuilder(50)
            .append(namespace)
            .append("/")
            .append(id)
            .toString();
    }

    private void commitChange(Entry<String, Update> update)
    {
        logger.info("Commit: {}", update.getKey());

        update.getValue().run();
    }

    public static interface Update
    {
        void run();
    }
}
