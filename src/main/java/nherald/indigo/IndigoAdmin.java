package nherald.indigo;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import nherald.indigo.index.IndicesManager;

/**
 * Admin operations. These are in a separate class to make it clear that they
 * should only be run when your app is offline. Firestore has a limit of 500
 * write operations per transaction. These operations tend to exceed that,
 * meaning the whole operation is run as a batch of separate transactions
 * and therefore not a single atomic operation
 */
public class IndigoAdmin<T extends Entity>
{
    private final Indigo<T> entities;
    private final IndicesManager<T> indices;

    public IndigoAdmin(Indigo<T> entities, IndicesManager<T> indices)
    {
        this.entities = entities;
        this.indices = indices;
    }

    /**
     * Regenerates all of the search indices. The existing indices in the
     * underlying storage should be manually deleted before running this,
     * otherwise some of the old index state will persist and lead to
     * inconsistencies
     * @param maxBatchSize the maximum number of entities to write per
     * transaction. Note that the Firestore maximum number of document writes
     * is 500. Each entity is stored as a separate document, however the
     * document itself won't be written but the associated index documents
     * will. The default index stores all words per 2 character prefix in a
     * single document, so it depends on your data as to how many unique 2
     * character prefixes you have, and therefore how many entities should be
     * done per batch to keep within the limit. This should be improved to
     * automatically batch up the updates based on the number of documents,
     * because at the moment you need either keep the batch size at a suitably
     * low guess or use trial and error
     */
    public void regenIndices(int maxBatchSize)
    {
        final AtomicInteger count = new AtomicInteger();

        entities.list()
            .stream()
            // Split the entity ids list into chunks, of no more than
            // maxBatchSize entities each
            .collect(
                Collectors.groupingBy(entityId ->
                    count.getAndIncrement() / maxBatchSize)
            )
            .values()
            // Add each batch of ids to the index separately
            .forEach(this::regenIndicesFor);
    }

    private void regenIndicesFor(List<Long> entityIds)
    {
        final List<T> entitiesBatch = entities.get(entityIds);

        entities.runTransaction(transaction ->
            entitiesBatch.forEach(entity ->
                indices.addEntity(entity, transaction)
            )
        );
    }
}
