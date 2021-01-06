package nherald.indigo.store.firebase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nherald.indigo.store.firebase.db.FirebaseRawTransaction;
import nherald.indigo.store.uow.Transaction;
import nherald.indigo.EntityId;
import nherald.indigo.store.StoreException;
import nherald.indigo.store.TooManyWritesException;
import nherald.indigo.store.firebase.db.FirebaseRawDocumentId;

/**
 * Single-use transaction
 *
 * <p>In a firestore transaction, all read operations (if any) must be run
 * prior to updates. To adhere to this, update operations (put/delete)
 * will be batched up by this class and applied at the end. Read operations
 * (get/exists) however will be run immediately. This means that operations
 * won't always be applied in the same order they were requested. This
 * would be a problem, e.g. if a delete() was done followed by an exists()
 * on the same object. Requested order would be; delete(x), exists(x),
 * whereas the run order would be: exists(x), delete(x). The exists() call
 * will return true because the delete() wasn't run before it. These
 * transaction instances must not be used in this way; an exception will be
 * thrown when trying to do read operation after an update on the same object.
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
public class FirebaseTransaction extends FirebaseReadOps implements Transaction
{
    private static final Logger logger = LoggerFactory.getLogger(FirebaseTransaction.class);

    /** The maximum number of write operations permitted by Firestore */
    private static final int MAX_WRITES = 500;

    private final FirebaseRawTransaction transaction;

    private final Map<EntityId, Update> pending = new HashMap<>(203);

    public FirebaseTransaction(FirebaseRawTransaction transaction)
    {
        super(transaction);

        this.transaction = transaction;
    }

    @Override
    public <T> T get(String namespace, String id, Class<T> itemType)
    {
        throwIfPreviouslyUpdated(namespace, id);

        return super.get(namespace, id, itemType);
    }

    @Override
    public <T> List<T> get(String namespace, List<String> ids, Class<T> itemType)
    {
        ids.forEach(id -> throwIfPreviouslyUpdated(namespace, id));

        return super.get(namespace, ids, itemType);
    }

    @Override
    public boolean exists(String namespace, String id)
    {
        throwIfPreviouslyUpdated(namespace, id);

        return super.exists(namespace, id);
    }

    @Override
    public <T> void put(String namespace, String entityId, T entity)
    {
        final FirebaseRawDocumentId docId = new FirebaseRawDocumentId(namespace, entityId);

        final EntityId mapKey = getMapKey(namespace, entityId);

        addToPending(mapKey, () -> transaction.set(docId, entity));
    }

    @Override
    public void delete(String namespace, String entityId)
    {
        final FirebaseRawDocumentId docId = new FirebaseRawDocumentId(namespace, entityId);

        final EntityId mapKey = getMapKey(namespace, entityId);

        addToPending(mapKey, () -> transaction.delete(docId));
    }

    void flush()
    {
        pending.entrySet()
            .stream()
            .forEach(e -> apply(e.getKey(), e.getValue()));
    }

    private EntityId getMapKey(String namespace, String entityId)
    {
        return new EntityId(namespace, entityId);
    }

    private void throwIfPreviouslyUpdated(String namespace, String entityId)
    {
        final EntityId key = getMapKey(namespace, entityId);

        if (pending.containsKey(key))
        {
            final String message = String.format("Document %s/%s has a "
                + "pending update. Read operations aren't allowed after "
                + "updates", namespace, entityId);

            throw new StoreException(message);
        }
    }

    private void addToPending(EntityId mapKey, Update update)
    {
        pending.put(mapKey, update);

        throwIfTooManyUpdates();
    }

    private void throwIfTooManyUpdates()
    {
        if (pending.size() <= MAX_WRITES) return;

        final String message = String.format("The number of update operations "
            + "in this transaction exceeds the maximum of %s", MAX_WRITES);

        throw new TooManyWritesException(message);
    }

    private void apply(EntityId key, Update update)
    {
        logger.info("Applying update {}/{}", key.getNamespace(), key.getId());

        update.apply();
    }

    @FunctionalInterface
    private static interface Update
    {
        void apply();
    }
}
