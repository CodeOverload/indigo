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

import nherald.indigo.store.firebase.db.FirebaseRawTransaction;
import nherald.indigo.store.uow.Transaction;
import nherald.indigo.store.StoreException;
import nherald.indigo.store.firebase.db.FirebaseDocument;
import nherald.indigo.store.firebase.db.FirebaseDocumentId;

/**
 * Single-use transaction
 *
 * <p>In a firestore transaction, all read operations (if any) must be run
 * prior to the updates. To adhere to this, update operations (put/delete)
 * will be batched up by this class and applied at the end.
 *
 * <p>Firestore doesn't allow multiple writes to the same object within the
 * same second. To avoid this, if an update is applied to an object more
 * than once, only the last update to that object will be applied to the
 * database. Later updates take precedence, so earlier operations should not
 * matter. E.g. if object A is updated to version A1, then later updated to
 * A2, the final state is A2 and therefore A1 is redundant. There is however
 * an edge case which counters this; if a new object N is added, and then
 * later deleted, the delete will be applied to the database but not the add
 * operation. As N was new, the database delete will fail because that object
 * didn't exist in the database. This isn't a case we envisage needing to
 * handle (it's not needed for the Entities logic), but may need to be beared
 * in mind for future changes
 *
 * <p>There's no guarantee that objects (relative to each other) will be
 * updated in the database in the order the updates were applied
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

    @Override
    public <T> T get(String namespace, String entityId, Class<T> entityType)
    {
        final FirebaseDocumentId docId = new FirebaseDocumentId(namespace, entityId);

        try
        {
            final FirebaseDocument doc = transaction.get(docId);

            if (!doc.exists()) return null;

            return doc.asObject(entityType);
        }
        catch (InterruptedException | ExecutionException ex)
        {
            throw new StoreException(String.format("Error getting %s/%s", namespace, entityId), ex);
        }
    }

    @Override
    public boolean exists(String namespace, String entityId)
    {
        final FirebaseDocumentId docId = new FirebaseDocumentId(namespace, entityId);

        try
        {
            final FirebaseDocument doc = transaction.get(docId);

            return doc.exists();
        }
        catch (InterruptedException | ExecutionException ex)
        {
            throw new StoreException(String.format("Error getting %s/%s", namespace, entityId), ex);
        }
    }

    @Override
    public <T> void put(String namespace, String entityId, T entity)
    {
        final FirebaseDocumentId docId = new FirebaseDocumentId(namespace, entityId);

        final String mapKey = getMapKey(namespace, entityId);

        pending.put(mapKey, () -> transaction.set(docId, entity));
    }

    @Override
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
