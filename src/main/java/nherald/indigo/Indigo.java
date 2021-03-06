package nherald.indigo;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import nherald.indigo.helpers.IdHelpers;
import nherald.indigo.index.IndicesManager;
import nherald.indigo.store.Store;
import nherald.indigo.store.StoreException;
import nherald.indigo.store.uow.Consumer;
import nherald.indigo.store.uow.Transaction;
import nherald.indigo.store.uow.TransactionWithCache;

public class Indigo<T extends Entity>
{
    private static final String INFO_ID = "info";

    private static final String NAMESPACE = "entities";

    private final Class<T> entityType;

    private final IndicesManager<T> indices;

    private final Store store;

    Indigo(Class<T> entityType, IndicesManager<T> indices,
        Store store)
    {
        this.entityType = entityType;
        this.indices = indices;
        this.store = store;
    }

    Class<T> getEntityType()
    {
        return entityType;
    }

    IndicesManager<T> getIndicesManager()
    {
        return indices;
    }

    Store getStore()
    {
        return store;
    }

    public T get(long id)
    {
        return store.get(NAMESPACE, IdHelpers.asString(id), entityType);
    }

    public List<T> get(List<Long> ids)
    {
        return store.get(NAMESPACE, IdHelpers.asStrings(ids), entityType);
    }

    public Collection<Long> list()
    {
        return store.list(NAMESPACE)
            .stream()
            .filter(id -> id.matches("[0-9]+"))
            .map(Long::parseLong)
            .collect(Collectors.toList());
    }

    public Collection<Long> search(String indexId, String word)
    {
        return indices.search(indexId, word);
    }

    /**
     * Saves an entity to the database. If the entity already has a id, this will either;
     * a) update the entity with that id, or b) save it to that id if and entity doesn't
     * already exist with that id. Otherwise it will save it to a new auto-generated id
     * @param entity the entity
     */
    public void put(T entity)
    {
        put(Arrays.asList(entity));
    }

    public void put(Collection<T> entities)
    {
        runTransaction(transaction -> put(entities, transaction));
    }

    void runTransaction(Consumer<Transaction> runnable)
    {
        // Wrap the store transaction with a cachable wrapper. Note that
        // the store may re-run transactions (e.g. if there were conflicting
        // updates from another process), so need to start with a new cache
        // each time; each transaction must not update application state
        store.transaction(runnable, TransactionWithCache::new);
    }

    private void put(Collection<T> entities, Transaction transaction)
    {
        final EntitiesInfo info = loadInfo(transaction);

        for (T entity : entities)
        {
            // This is a new entity
            if (entity.getId() == null)
            {
                entity.setId(info.generateId());
            }
            // Existing entity
            else
            {
                // Remove from all indices as we don't want them containing stale data
                indices.removeEntity(entity.getId(), transaction);
            }

            transaction.put(NAMESPACE, IdHelpers.asString(entity.getId()), entity);

            indices.addEntity(entity, transaction);
        }

        transaction.put(NAMESPACE, INFO_ID, info);
    }

    public void delete(long id)
    {
        runTransaction(transaction -> delete(id, transaction));
    }

    private void delete(long id, Transaction transaction)
    {
        final String stringId = IdHelpers.asString(id);

        if (!transaction.exists(NAMESPACE, stringId))
        {
            throw new StoreException(String.format("Entity %s doesn't exist", id));
        }

        transaction.delete(NAMESPACE, stringId);

        indices.removeEntity(id, transaction);
    }

    private EntitiesInfo loadInfo(Transaction transaction)
    {
        final EntitiesInfo storedInfo = transaction.get(NAMESPACE, INFO_ID, EntitiesInfo.class);

        if (storedInfo != null) return storedInfo;

        return new EntitiesInfo();
    }
}
