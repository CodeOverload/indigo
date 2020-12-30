package nherald.indigo;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import nherald.indigo.index.Index;
import nherald.indigo.index.terms.BasicTokeniser;
import nherald.indigo.store.Store;
import nherald.indigo.store.StoreException;
import nherald.indigo.uow.Transaction;

public class Entities<T extends Entity>
{
    private static final String INFO_ID = "info";

    private static final String NAMESPACE = "entities";

    private final Class<T> entityType;

    private final Collection<Index<T>> indices;

    private final Store store;

    public Entities(Class<T> entityType, Collection<Index<T>> indices,
        Store store)
    {
        this.entityType = entityType;
        this.store = store;

        this.indices = indices;
    }

    public T get(long id)
    {
        return store.get(NAMESPACE, asString(id), entityType);
    }

    public Collection<T> get(Collection<Long> ids)
    {
        final Collection<String> strIds = ids.stream()
            .map(this::asString)
            .collect(Collectors.toList());

        return store.get(NAMESPACE, strIds, entityType);
    }

    public Collection<Long> search(String indexId, String word)
    {
        final Optional<Index<T>> index = indices.stream()
            .filter(i -> i.getId().equals(indexId))
            .findFirst();

        if (!index.isPresent())
        {
            throw new StoreException(String.format("Index %s doesn't exist", indexId));
        }

        return index.get().get(word);
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
        store.transaction(transaction -> put(entities, transaction));
    }

    private void put(Collection<T> entities, Transaction transaction)
    {
        final EntitiesInfo info = loadInfo(transaction);

        entities.stream()
            .forEach(e -> {
                final long id;
                // This is a new entity
                if (e.getId() == null)
                {
                    id = info.generateId();
                    e.setId(id);
                }
                // Existing entity
                else
                {
                    // The entity itself doesn't need deleted, but we need to remove
                    // it from all indices as we don't want them containing stale data
                    id = e.getId();
                    delete(id, transaction);
                }

                transaction.put(NAMESPACE, asString(id), e);

                addToIndices(e, transaction);
            });

        transaction.put(NAMESPACE, INFO_ID, info);
    }

    public void delete(long id)
    {
        store.transaction(transaction -> delete(id, transaction));
    }

    private void delete(long id, Transaction transaction)
    {
        if (!transaction.exists(NAMESPACE, asString(id)))
        {
            throw new StoreException(String.format("Entity %s doesn't exist", id));
        }

        transaction.delete(NAMESPACE, asString(id));

        indices.stream()
            .forEach(index -> index.remove(id, transaction));
    }

    private String asString(long id)
    {
        return id + "";
    }

    private EntitiesInfo loadInfo(Transaction transaction)
    {
        final EntitiesInfo storedInfo = transaction.get(NAMESPACE, INFO_ID, EntitiesInfo.class);

        if (storedInfo != null) return storedInfo;

        return new EntitiesInfo();
    }

    private void addToIndices(T entity, Transaction transaction)
    {
        indices.stream()
            .forEach(index -> addToIndex(entity, index, transaction));
    }

    private void addToIndex(T entity, Index<T> index, Transaction transaction)
    {
        final BasicTokeniser tokeniser = new BasicTokeniser();

        final String text = index.getTarget().getTextFromEntity(entity);

        final List<String> words = tokeniser.tokenise(text);

        index.add(words, entity.getId(), transaction);
    }
}
