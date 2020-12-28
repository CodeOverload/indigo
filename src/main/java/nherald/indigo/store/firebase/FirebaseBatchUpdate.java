package nherald.indigo.store.firebase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nherald.indigo.store.StoreException;
import nherald.indigo.store.firebase.db.FirebaseBatch;
import nherald.indigo.store.firebase.db.FirebaseDatabase;
import nherald.indigo.store.firebase.db.FirebaseDocumentId;
import nherald.indigo.uow.BatchUpdate;

/**
 * Single-use batch update transaction
 *
 * Notes for methods that apply updates (e.g. FirebaseBatchUpdate#put)
 *
 * - If an update to is applied to an entity more than once, only the last will be
 *   applied to the database. There's no point in applying earlier updates when the
 *   later one will overwrite it. Additionally, Firestore doesn't allow multiple
 *   writes within the same second on the same object
 *
 * - Updates may be run in any order, regardless of the order they were added.
 *   All entities are stored independently on the filesystem with no hard
 *   dependencies between each other, so it shouldn't matter which order they're
 *   saved in
 */
public class FirebaseBatchUpdate implements BatchUpdate
{
    private static final Logger logger = LoggerFactory.getLogger(FirebaseBatchUpdate.class);

    private static final int BATCH_SIZE = 500;

    private final FirebaseDatabase database;

    private final Map<String, Update> pending = new HashMap<>(203);

    public FirebaseBatchUpdate(FirebaseDatabase database)
    {
        this.database = database;
    }

    public <T> void put(String namespace, String entityId, T entity)
    {
        final FirebaseDocumentId docId = new FirebaseDocumentId(namespace, entityId);

        final String mapKey = getMapKey(namespace, entityId);

        pending.put(mapKey, batch -> batch.set(docId, entity));
    }

    public void delete(String namespace, String entityId)
    {
        final FirebaseDocumentId docId = new FirebaseDocumentId(namespace, entityId);

        final String mapKey = getMapKey(namespace, entityId);

        pending.put(mapKey, batch -> batch.delete(docId));
    }

    @Override
    public void commit()
    {
        final AtomicInteger count = new AtomicInteger();

        pending.entrySet()
            .stream()
            // Split the map into chunks, of no more than BATCH_SIZE updates in each
            .collect(Collectors.groupingBy(e -> count.getAndIncrement() / BATCH_SIZE))
            .values()
            .forEach(this::commitChunk);
    }

    private String getMapKey(String namespace, String entityId)
    {
        return new StringBuilder(50)
            .append(namespace)
            .append("/")
            .append(entityId)
            .toString();
    }

    private void commitChunk(List<Entry<String, Update>> chunk)
    {
        final FirebaseBatch batch = database.batch();

        chunk.stream()
            .forEach(e -> apply(e.getKey(), e.getValue(), batch));

        logger.info("Committing batch of size {}", chunk.size());
        try
        {
            batch.commit();
        }
        catch (InterruptedException | ExecutionException ex)
        {
            throw new StoreException("Error committing batch", ex);
        }
    }

    private void apply(String key, Update update, FirebaseBatch batch)
    {
        logger.info("Adding update to batch: {}", key);

        update.apply(batch);
    }

    @FunctionalInterface
    private static interface Update
    {
        void apply(FirebaseBatch batch);
    }
}
