package nherald.indigo.store.file;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nherald.indigo.uow.BatchUpdate;

public class FileBatchUpdate implements BatchUpdate
{
    private static final Logger logger = LoggerFactory.getLogger(FileBatchUpdate.class);

    private final Map<String, Update> pending = new HashMap<>(101);

    /**
     * Adds an update to the batch, to update in one go when finished. Notes:
     *
     * - If an entity is added more than once, only the last version added
     *   will be saved
     *
     * - Updates may be run in any order, regardless of the order they were added.
     *   All entities are stored independently on the filesystem, with no hard
     *   dependencies between each other, so it shouldn't matter which order
     *   they're saved in
     */
    public void add(String namespace, String entityId, Update update)
    {
        pending.put(getMapKey(namespace, entityId), update);
    }

    @Override
    public void commit()
    {
        pending.entrySet()
            .stream()
            .forEach(this::commit);
    }

    private String getMapKey(String namespace, String entityId)
    {
        return new StringBuilder(50)
            .append(namespace)
            .append("/")
            .append(entityId)
            .toString();
    }

    private void commit(Entry<String, Update> update)
    {
        logger.info("Commit: {}", update.getKey());

        update.getValue().run();
    }

    public static interface Update
    {
        void run();
    }
}
