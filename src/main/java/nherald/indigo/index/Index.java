package nherald.indigo.index;

import java.util.Collection;
import java.util.Set;

import nherald.indigo.Entity;
import nherald.indigo.index.terms.WordFilter;
import nherald.indigo.store.IdValidator;
import nherald.indigo.store.StoreReadOps;
import nherald.indigo.store.uow.Transaction;

/**
 * Represents an index, which stores key value pairs. Typically words/prefixes/values, which map
 * to document ids
 *
 * This is a cache backed by underlying storage. Note that this will split the index over multiple
 * files (segments) in the storage so that no one file is too big, with each file storing the
 * keys with a particular prefix.
 *
 * As segments are often updated many times during a single update (e.g. the word 'tomato'
 * produces the ngrams [tom, toma, tomat, tomato], which all have the same prefix), this caches
 * the segments in memory and updates the in-memory copy each time. Updates can be flushed
 * to storage by committing the transaction
 *
 * As a result of caching, these objects should be as short-lived as possible, otherwise the cache
 * will eventually go out of sync with the storage. Don't use this across different requests
 */
public class Index<T extends Entity>
{
    private static final String NAMESPACE = "indices";

    private final String id;
    private final IndexTarget<T> target;
    private final WordFilter wordFilter;
    private final StoreReadOps store;

    public Index(String id, IndexTarget<T> target,
        WordFilter wordFilter, StoreReadOps store)
    {
        this.id = id;
        this.target = target;
        this.wordFilter = wordFilter;
        this.store = store;
    }

    public String getId()
    {
        return id;
    }

    public IndexTarget<T> getTarget()
    {
        return target;
    }

    public Set<Long> get(String word)
    {
        final IndexSegment segment = getSegmentForWord(word, store);

        return segment.get(word);
    }

    public void add(Collection<String> words, long entityId, Transaction transaction)
    {
        words.stream()
            .flatMap(wordFilter::process)
            .forEach(prefix -> add(prefix, entityId, transaction));
    }

    private void add(String prefix, long entityId, Transaction transaction)
    {
        final IndexSegment segment = getSegmentForWord(prefix, transaction);

        segment.add(prefix, entityId);

        final String segmentId = getSegmentId(prefix);

        transaction.put(NAMESPACE, getStoreId(segmentId), segment);

        // Update the contents
        final Contents contents = getContents(transaction);
        contents.add(entityId, segmentId);
        saveContents(contents, transaction);
    }

    public void remove(long entityId, Transaction transaction)
    {
        final Contents contents = getContents(transaction);

        final Set<String> segmentIds = contents.get(entityId);

        segmentIds.stream()
            .forEach(segmentId -> {
                final IndexSegment segment = getSegmentById(segmentId, transaction);
                segment.remove(entityId);

                transaction.put(NAMESPACE, getStoreId(segmentId), segment);
            });

        contents.remove(entityId);
        saveContents(contents, transaction);
    }

    private String getSegmentId(String word)
    {
        return word.substring(0, 2);
    }

    private IndexSegment getSegmentForWord(String word, StoreReadOps transaction)
    {
        final String segmentId = getSegmentId(word);

        IdValidator.check(segmentId);

        return getSegmentById(segmentId, transaction);
    }

    private String getStoreId(String segmentId)
    {
        return String.format("%s-%s", getId(), segmentId);
    }

    private IndexSegment getSegmentById(String segmentId, StoreReadOps transaction)
    {
        final String filename = String.format("%s-%s", getId(), segmentId);

        // Load from persistent storage if it's saved
        final IndexSegment loadedSegment = transaction.get(NAMESPACE, filename, IndexSegment.class);

        if (loadedSegment != null) return loadedSegment;

        // Otherwise create a new segment
        return new IndexSegment();
    }

    private Contents getContents(Transaction transaction)
    {
        final String storeId = getContentsId();

        final Contents loadedContents = transaction.get(NAMESPACE, storeId, Contents.class);

        if (loadedContents != null) return loadedContents;

        // Create a new instance if not
        return new Contents();
    }

    private void saveContents(Contents contents, Transaction transaction)
    {
        transaction.put(NAMESPACE, getContentsId(), contents);
    }

    private String getContentsId()
    {
        return String.format("%s-contents", getId());
    }
}
