package nherald.indigo.index;

import nherald.indigo.Entity;
import nherald.indigo.index.terms.BasicWordFilter;
import nherald.indigo.index.terms.WordFilter;
import nherald.indigo.store.StoreException;
import nherald.indigo.store.StoreReadOps;

public class IndexBuilder<T extends Entity>
{
    private String id;
    private IndexTarget<T> target;
    private WordFilter wordFilter;
    private StoreReadOps store;

    public IndexBuilder(String id)
    {
        if (id == null)
        {
            throw new StoreException("Index id is null");
        }

        this.id = id;
    }

    public IndexBuilder<T> target(IndexTarget<T> target)
    {
        this.target = target;
        return this;
    }

    public IndexBuilder<T> wordFilter(WordFilter wordFilter)
    {
        this.wordFilter = wordFilter;
        return this;
    }

    public IndexBuilder<T> store(StoreReadOps store)
    {
        this.store = store;
        return this;
    }

    public Index<T> build()
    {
        if (store == null)
        {
            throw new StoreException("No store specified");
        }

        if (target == null)
        {
            throw new StoreException("No target specified");
        }

        if (wordFilter == null)
        {
            wordFilter = new BasicWordFilter(false);
        }

        return new Index<>(id, target, wordFilter, store);
    }
}
