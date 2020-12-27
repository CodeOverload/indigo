package nherald.indigo.index;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import nherald.indigo.Entity;
import nherald.indigo.index.terms.WordFilter;
import nherald.indigo.store.IdValidator;
import nherald.indigo.store.Store;
import nherald.indigo.uow.BatchUpdate;

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
 * to storage by committing the batch
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
    private final Store store;

    private final Map<String, IndexSegment> cache = new HashMap<>(201);

    private Contents cachedContents;

    public Index(String id, IndexTarget<T> target,
        WordFilter wordFilter, Store store)
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
        final IndexSegment segment = getSegmentForWord(word);

        return segment.get(word);
    }

    public void add(Collection<String> words, long entityId, BatchUpdate batch)
    {
        words.stream()
            .flatMap(wordFilter::process)
            .forEach(prefix -> add(prefix, entityId, batch));
    }

    private void add(String prefix, long entityId, BatchUpdate batch)
    {
        final IndexSegment segment = getSegmentForWord(prefix);

        segment.add(prefix, entityId);

        final String segmentId = getSegmentId(prefix);

        store.put(NAMESPACE, getStoreId(segmentId), segment, batch);

        // Update the contents
        final Contents contents = getContents();
        contents.add(entityId, segmentId);
        saveContents(contents, batch);
    }

    public void remove(long entityId, BatchUpdate batch)
    {
        final Contents contents = getContents();

        final Set<String> segmentIds = contents.get(entityId);

        segmentIds.stream()
            .forEach(segmentId -> {
                final IndexSegment segment = getSegmentById(segmentId);
                segment.remove(entityId);

                store.put(NAMESPACE, getStoreId(segmentId), segment, batch);
            });

        contents.remove(entityId);
        saveContents(contents, batch);
    }

    private String getSegmentId(String word)
    {
        return word.substring(0, 2);
    }

    private IndexSegment getSegmentForWord(String word)
    {
        final String segmentId = getSegmentId(word);

        IdValidator.check(segmentId);

        return getSegmentById(segmentId);
    }

    private IndexSegment getSegmentById(String segmentId)
    {
        IndexSegment segment = cache.get(segmentId);

        // Already cached
        if (segment != null)
        {
            return segment;
        }

        segment = load(segmentId);

        // Cache it so that updates accumulate
        cache.put(segmentId, segment);

        return segment;
    }

    private String getStoreId(String segmentId)
    {
        return String.format("%s-%s", getId(), segmentId);
    }

    private IndexSegment load(String segmentId)
    {
        final String filename = String.format("%s-%s", getId(), segmentId);

        // Load from persistent storage if it's saved
        final IndexSegment loadedSegment = store.get(NAMESPACE, filename, IndexSegment.class);

        if (loadedSegment != null) return loadedSegment;

        // Otherwise create a new segment
        return new IndexSegment();
    }

    private Contents getContents()
    {
        // Already loaded
        if (cachedContents != null) return cachedContents;

        // Load from persistent storage
        cachedContents = loadContents();

        return cachedContents;
    }

    private Contents loadContents()
    {
        final String storeId = getContentsId();

        final Contents loadedContents = store.get(NAMESPACE, storeId, Contents.class);

        if (loadedContents != null) return loadedContents;

        // Create a new instance if not
        return new Contents();
    }

    private void saveContents(Contents contents, BatchUpdate batch)
    {
        store.put(NAMESPACE, getContentsId(), contents, batch);
    }

    private String getContentsId()
    {
        return String.format("%s-contents", getId());
    }
}
