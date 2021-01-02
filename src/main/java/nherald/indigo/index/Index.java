package nherald.indigo.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
        final List<String> filteredWords = words.stream()
            .flatMap(wordFilter::process)
            .collect(Collectors.toList());

        // Determine which segments we need, and fetch them all in one go
        final List<String> storeIds = new ArrayList<>(
            filteredWords.stream()
                .map(this::getSegmentId)
                .map(this::getStoreId)
                .collect(Collectors.toCollection(LinkedHashSet::new))
        );

        final List<IndexSegment> segments = transaction.get(NAMESPACE,
            storeIds, IndexSegment.class);

        final Map<String, IndexSegment> segmentMap = new HashMap<>();

        for (int i = 0; i < storeIds.size(); ++i)
        {
            final String storeId = storeIds.get(i);
            IndexSegment segment = segments.get(i);

            if (segment == null)
            {
                segment = new IndexSegment();
            }

            segmentMap.put(storeId, segment);
        }

        final Contents contents = getContents(transaction);

        filteredWords.forEach(word -> {
            final String segmentId = getSegmentId(word);
            final String storeId = getStoreId(segmentId);

            final IndexSegment segment = segmentMap.get(storeId);
            segment.add(word, entityId);

            contents.add(entityId, segmentId);
        });

        // Update each of the segments in the store
        storeIds.forEach(storeId -> {
            transaction.put(NAMESPACE, storeId, segmentMap.get(storeId));
        });

        saveContents(contents, transaction);
    }

    public void remove(long entityId, Transaction transaction)
    {
        final Contents contents = getContents(transaction);

        // Determine which segments this entity is in, using the contents
        final List<String> storeIds = contents.get(entityId)
            .stream()
            .map(this::getStoreId)
            .collect(Collectors.toList());

        // Fetch them in one go
        final List<IndexSegment> segments = transaction.get(NAMESPACE,
            storeIds, IndexSegment.class);

        // Remove the entity from each of them
        for (int i = 0; i < storeIds.size(); ++i)
        {
            final String storeId = storeIds.get(i);
            final IndexSegment segment = segments.get(i);

            segment.remove(entityId);

            // Store the updated segment
            transaction.put(NAMESPACE, storeId, segment);
        }

        // Update the contents accordingly
        contents.remove(entityId);
        saveContents(contents, transaction);
    }

    private String getSegmentId(String word)
    {
        final String segmentId = word.substring(0, 2);
        return IdValidator.check(segmentId);
    }

    private IndexSegment getSegmentForWord(String word, StoreReadOps transaction)
    {
        final String segmentId = getSegmentId(word);

        return getSegmentById(segmentId, transaction);
    }

    private String getStoreId(String segmentId)
    {
        return String.format("%s-%s", getId(), segmentId);
    }

    private IndexSegment getSegmentById(String segmentId, StoreReadOps transaction)
    {
        final String storeId = getStoreId(segmentId);

        // Load from persistent storage if it's saved
        final IndexSegment loadedSegment = transaction.get(NAMESPACE, storeId, IndexSegment.class);

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
