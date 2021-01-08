package nherald.indigo.index;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import nherald.indigo.Entity;
import nherald.indigo.index.terms.BasicTokeniser;
import nherald.indigo.store.StoreException;
import nherald.indigo.store.uow.Transaction;

public class IndicesManager<T extends Entity>
{
    private final Collection<Index<T>> indices;

    public IndicesManager(Collection<Index<T>> indices)
    {
        this.indices = indices;
    }

    Collection<Index<T>> getIndices()
    {
        return indices;
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

    public void addEntity(T entity, Transaction transaction)
    {
        indices.stream()
            .forEach(index -> addEntity(entity, index, transaction));
    }

    private void addEntity(T entity, Index<T> index, Transaction transaction)
    {
        final BasicTokeniser tokeniser = new BasicTokeniser();

        final String text = index.getTarget().getTextFromEntity(entity);

        final List<String> words = tokeniser.tokenise(text);

        index.add(words, entity.getId(), transaction);
    }

    public void removeEntity(long id, Transaction transaction)
    {
        indices.stream()
            .forEach(index -> index.remove(id, transaction));
    }
}
