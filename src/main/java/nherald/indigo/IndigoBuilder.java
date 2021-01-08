package nherald.indigo;

import java.util.ArrayList;
import java.util.List;

import nherald.indigo.index.Index;
import nherald.indigo.index.IndicesManager;
import nherald.indigo.store.Store;
import nherald.indigo.store.StoreException;

public class IndigoBuilder<T extends Entity>
{
    private final Class<T> entityType;
    private Store store;
    private final List<Index<T>> indices;

    public IndigoBuilder(Class<T> entityType)
    {
        this.entityType = entityType;
        this.indices = new ArrayList<>();
    }

    public IndigoBuilder<T> store(Store store)
    {
        this.store = store;
        return this;
    }

    public IndigoBuilder<T> addIndex(Index<T> index)
    {
        indices.add(index);
        return this;
    }

    public Indigo<T> build()
    {
        if (store == null)
        {
            throw new StoreException("No store specified");
        }

        final IndicesManager<T> indicesManager = new IndicesManager<>(indices);

        return new Indigo<>(entityType, indicesManager, store);
    }
}
