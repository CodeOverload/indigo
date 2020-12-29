package nherald.indigo.store.firebase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nherald.indigo.store.firebase.db.FirebaseRawTransaction;
import nherald.indigo.store.firebase.db.FirebaseDocumentId;
import nherald.indigo.uow.Transaction;

/**
 * Single-use transaction
 *
 * <p>Update operations (put/delete) will be batched up and run at the end.
 * This is a Firestore restriction; all updates must appear after any get/read
 * operations
 *
 * <p>If an update to is applied to an entity more than once, only the last will be
 * applied to the database. There's no point in applying earlier updates when the
 * later one will overwrite it. Additionally, Firestore doesn't allow multiple
 * writes within the same second on the same object
 *
 * <p>Updates may be run in any order, regardless of the order they were added.
 * All entities are stored independently on the filesystem with no hard
 * dependencies between each other, so it shouldn't matter which order they're
 * saved in
 */
public class FirebaseTransaction implements Transaction
{
    private static final Logger logger = LoggerFactory.getLogger(FirebaseTransaction.class);

    private static final int BATCH_SIZE = 500;

    private final FirebaseRawTransaction transaction;

    private final Map<String, Update> pending = new HashMap<>(203);

    public FirebaseTransaction(FirebaseRawTransaction transaction)
    {
        this.transaction = transaction;
    }

    public <T> void put(String namespace, String entityId, T entity)
    {
        final FirebaseDocumentId docId = new FirebaseDocumentId(namespace, entityId);

        final String mapKey = getMapKey(namespace, entityId);

        pending.put(mapKey, () -> transaction.set(docId, entity));
    }

    public void delete(String namespace, String entityId)
    {
        final FirebaseDocumentId docId = new FirebaseDocumentId(namespace, entityId);

        final String mapKey = getMapKey(namespace, entityId);

        pending.put(mapKey, () -> transaction.delete(docId));
    }

    void flush()
    {
        final AtomicInteger count = new AtomicInteger();

        // TODO We aren't creating transactions ourselves anymore, so can't create a new transaction
        //      for each batch of 500. Need to implement a solution to handle this
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
        chunk.stream()
            .forEach(e -> apply(e.getKey(), e.getValue()));

        logger.info("Committing batch of size {}", chunk.size());
    }

    private void apply(String key, Update update)
    {
        logger.info("Adding update to batch: {}", key);

        update.apply();
    }

    @FunctionalInterface
    private static interface Update
    {
        void apply();
    }
}
