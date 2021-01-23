package nherald.indigo.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import nherald.indigo.Entity;
import nherald.indigo.helpers.IdHelpers;
import nherald.indigo.helpers.MapHelpers;
import nherald.indigo.index.terms.WordFilter;
import nherald.indigo.index.terms.WordSelector;
import nherald.indigo.store.StoreException;
import nherald.indigo.store.StoreReadOps;
import nherald.indigo.store.uow.Transaction;

/**
 * Represents an index, which stores the words and the ids of the entities
 * that contain those words.
 *
 * <p>Example:
 * <ul>
 * <li>tiger -> 4
 * <li>panther -> 60, 43
 * </ul>
 *
 * <p>Which denotes that entity 4 contains the word 'tiger', and entities
 * 60 and 43 contain the word 'panther'
 *
 * <p>The WordSelector dictates which words are looked up in the index for a
 * given search term. A prefixing word selector (PrefixWordSelector) will allow
 * the index to look up any words in the index that are prefixed by the given
 * search term. An exact word selector (ExactWordSelector) means that the
 * search term should match the words in the index exactly.
 *
 * <p>The index will be split over multiple documents (segments) in the store
 * so that no one document is too big. A segment is just a subset of the
 * index, storing the words with a particular prefix
 */
public class Index<T extends Entity>
{
    private static final String NAMESPACE = "indices";

    /**
     * The index is split into segments to reduce the size of the documents in
     * the index (Firestore enforces a maximum document size). Each segment
     * contains the words that start with a particular prefix; the prefix
     * length determines what length word prefix is used when splitting into
     * segments.
     *
     * <p>For example, given the words "pins", "pinstripe" and "parts". If the
     * prefix length is 2, words will be divided into "pi" and "pa" segments.
     * If the prefix length is 3; "pin" and "par"
     */
    private static final int PREFIX_LENGTH = 2;

    private final String id;
    private final IndexTarget<T> target;
    private final WordFilter wordFilter;
    private final WordSelector wordSelector;
    private final StoreReadOps store;

    Index(String id, IndexTarget<T> target, WordFilter wordFilter,
        WordSelector wordSelector, StoreReadOps store)
    {
        this.id = id;
        this.target = target;
        this.wordFilter = wordFilter;
        this.wordSelector = wordSelector;
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

    WordFilter getWordFilter()
    {
        return wordFilter;
    }

    WordSelector getWordSelector()
    {
        return wordSelector;
    }

    StoreReadOps getStore()
    {
        return store;
    }

    public Set<Long> get(String word)
    {
        if (word == null || word.length() < PREFIX_LENGTH)
        {
            throw new StoreException("Search term is not long enough");
        }

        final IndexSegmentData segmentData = getSegmentForWord(word, store);

        final IndexSegment segment = new IndexSegment(segmentData, wordSelector);

        return segment.get(word);
    }

    public void add(Collection<String> words, long entityId, Transaction transaction)
    {
        final List<String> filteredWords = words.stream()
            .flatMap(wordFilter::process)
            .collect(Collectors.toList());

        // Determine which segments we need, and fetch them all in one go
        final List<String> segmentIds = new ArrayList<>(
            filteredWords.stream()
                .map(this::getSegmentId)
                .collect(Collectors.toCollection(LinkedHashSet::new))
        );

        final Map<String, IndexSegmentData> segmentMap
            = getSegmentsById(segmentIds, transaction);

        final Contents contents = getContents(transaction);

        // Add each word to the corresponding segment, and to the contents
        filteredWords.forEach(word -> {
            final String segmentId = getSegmentId(word);

            segmentMap.get(segmentId)
                .add(word, entityId);

            contents.add(entityId, segmentId);
        });

        // Save each of the updated segments
        segmentMap.entrySet()
            .forEach(entry -> {
                transaction.put(NAMESPACE, getStoreId(entry.getKey()),
                    entry.getValue());
            });

        saveContents(contents, transaction);
    }

    public void remove(long entityId, Transaction transaction)
    {
        final Contents contents = getContents(transaction);

        // Determine which segments this entity is in, using the contents
        final List<String> segmentIds = new ArrayList<>(contents.get(entityId));

        // Fetch them in one go
        final Map<String, IndexSegmentData> segments = getSegmentsById(segmentIds,
            transaction);

        segments.entrySet().forEach(entry -> {
            final IndexSegmentData segment = entry.getValue();

            segment.remove(entityId);

            // Save the updated segment
            final String segmentId = entry.getKey();
            transaction.put(NAMESPACE, getStoreId(segmentId), segment);
        });

        // Save the contents accordingly
        contents.remove(entityId);
        saveContents(contents, transaction);
    }

    private String getSegmentId(String word)
    {
        final String segmentId = word.substring(0, PREFIX_LENGTH);
        return IdHelpers.validate(segmentId);
    }

    private IndexSegmentData getSegmentForWord(String word, StoreReadOps transaction)
    {
        final String segmentId = getSegmentId(word);

        return getSegmentById(segmentId, transaction);
    }

    private String getStoreId(String segmentId)
    {
        return String.format("%s-%s", getId(), segmentId);
    }

    private IndexSegmentData getSegmentById(String segmentId, StoreReadOps transaction)
    {
        final String storeId = getStoreId(segmentId);

        // Load from persistent storage if it's saved
        final IndexSegmentData loadedSegment = transaction.get(NAMESPACE, storeId, IndexSegmentData.class);

        if (loadedSegment != null) return loadedSegment;

        // Otherwise create a new segment
        return new IndexSegmentData();
    }

    /**
     * Fetches a list of segments from the store
     * @param segmentIds segment ids
     * @param transaction store transaction
     * @return map of segments, keyed by segment id. An empty segment will be
     * returned for each id that wasn't in the store
     */
    private Map<String, IndexSegmentData> getSegmentsById(List<String> segmentIds,
        Transaction transaction)
    {
        final List<String> storeIds = segmentIds.stream()
            .map(this::getStoreId)
            .collect(Collectors.toList());

        final List<IndexSegmentData> segments = transaction.get(NAMESPACE,
            storeIds, IndexSegmentData.class);

        return MapHelpers.asMap(segmentIds, segments,
            segmentId -> new IndexSegmentData());
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
