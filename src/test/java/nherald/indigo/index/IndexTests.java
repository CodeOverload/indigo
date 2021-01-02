package nherald.indigo.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

import nherald.indigo.index.terms.WordFilter;
import nherald.indigo.store.StoreException;
import nherald.indigo.store.StoreReadOps;
import nherald.indigo.store.uow.Transaction;
import nherald.indigo.utils.TestEntity;

@ExtendWith(MockitoExtension.class)
class IndexTests
{
    private static final String NAMESPACE = "indices";

    private static final List<IndexSegment> listContainingNull
        = Arrays.asList((IndexSegment) null);

    private WordFilter wordFilter;

    @Mock
    private Transaction transaction;

    @Mock
    private StoreReadOps store;

    private Index<TestEntity> subject;

    @BeforeEach
    void before()
    {
        // Very basic implementation that should suffice for most tests
        wordFilter = word -> Stream.of(word.toLowerCase());

        subject = new Index<>("name", entity -> "", wordFilter, store);
    }

    @Test
    void get_usesCorrectSegment_whenTermInIndex()
    {
        final IndexSegment segment = createSegment("pantha", List.of(4l, 7l));
        when(store.get(NAMESPACE, "name-pa", IndexSegment.class)).thenReturn(segment);

        subject.get("pantha");

        verify(store).get(NAMESPACE, "name-pa", IndexSegment.class);
    }

    @Test
    void get_returnsCorrectIds_whenTermInIndex()
    {
        final IndexSegment segment = createSegment("pantha", List.of(4l, 7l));
        when(store.get(NAMESPACE, "name-pa", IndexSegment.class)).thenReturn(segment);

        final Set<Long> actual = subject.get("pantha");

        final Set<Long> expected = Set.of(4l, 7l);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void get_returnsNull_whenTermNotInIndex_andSegmentHasBeenStored()
    {
        // Another word is contained in the stored segment, but not the one we're going to look up
        final IndexSegment segment = createSegment("pans", List.of(4l, 7l));
        when(store.get(NAMESPACE, "name-pa", IndexSegment.class)).thenReturn(segment);

        final Set<Long> actual = subject.get("pantha");

        final Set<Long> expected = Set.of();

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void get_returnsNull_whenTermNotInIndex_andSegmentNotAlreadyStored()
    {
        // The pa segment hasn't been stored
        when(store.get(NAMESPACE, "name-pa", IndexSegment.class)).thenReturn(null);

        final Set<Long> actual = subject.get("pantha");

        final Set<Long> expected = Set.of();

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void get_throwsOnInvalidTerm()
    {
        Assertions.assertThrows(StoreException.class, () -> {
            subject.get("p.name");
        });
    }

    @Test
    void add_storesCorrectSegment_whenSegmentNotAlreadyStored()
    {
        when(transaction.get(NAMESPACE, List.of("name-pa"), IndexSegment.class))
            .thenReturn(listContainingNull);

        subject.add(List.of("pantha"), 46l, transaction);

        final IndexSegment expectedSegment = createSegment("pantha", List.of(46l));

        verify(transaction).put(NAMESPACE, "name-pa", expectedSegment);
    }

    @Test
    void add_updatesSegmentCorrectly_whenSegmentAlreadyStored()
    {
        // Already has ids 2 and 7 stored for this word
        final IndexSegment segment = createSegment("pantha", List.of(2l, 7l));
        when(transaction.get(NAMESPACE, List.of("name-pa"), IndexSegment.class))
            .thenReturn(List.of(segment));

        // This should add id 46 to that list
        subject.add(List.of("pantha"), 46l, transaction);

        final IndexSegment expectedSegment = createSegment("pantha", List.of(2l, 7l, 46l));

        verify(transaction).put(NAMESPACE, "name-pa", expectedSegment);
    }

    @Test
    void add_updatesSegmentCorrectly_whenMultipleWordsInSameSegment()
    {
        final IndexSegment paSegment = createSegment("pantha", List.of(1l));
        final IndexSegment tiSegment = createSegment("tiger", List.of(2l));
        when(transaction.get(NAMESPACE, List.of("name-pa", "name-ti"), IndexSegment.class))
            .thenReturn(List.of(paSegment, tiSegment));

        // Add two words that are in the same segment
        subject.add(List.of("pantha", "tiger", "parakeet"), 46l, transaction);

        IndexSegment expectedSegment = createSegment("pantha", List.of(1l, 46l));
        expectedSegment.add("parakeet", 46l);
        verify(transaction).put(NAMESPACE, "name-pa", expectedSegment);

        expectedSegment = createSegment("tiger", List.of(2l, 46l));
        verify(transaction).put(NAMESPACE, "name-ti", expectedSegment);
    }

    @Test
    void add_updatesSegmentCorrectly_whenSegmentAlreadyStored_andIdAlreadyStoredAgainstWord()
    {
        // Already has ids 2 and 7 stored for this word
        final IndexSegment segment = createSegment("pantha", List.of(2l, 7l));
        when(transaction.get(NAMESPACE, List.of("name-pa"), IndexSegment.class))
            .thenReturn(List.of(segment));

        // We're adding 7 again, so should still have just 2 and 7 saved back to the store
        subject.add(List.of("pantha"), 7l, transaction);

        final IndexSegment expectedSegment = createSegment("pantha", List.of(2l, 7l));

        verify(transaction).put(NAMESPACE, "name-pa", expectedSegment);
    }

    @Test
    void add_updatesSegmentCorrectlyOnSuccessiveUpdates()
    {
        IndexSegment segment = createSegment("pantha", List.of(2l, 7l));
        when(transaction.get(NAMESPACE, List.of("name-pa"), IndexSegment.class))
            .thenReturn(List.of(segment));

        // First add 8
        subject.add(List.of("pantha"), 8l, transaction);

        IndexSegment expectedSegment = createSegment("pantha", List.of(2l, 7l, 8l));
        verify(transaction).put(NAMESPACE, "name-pa", expectedSegment);

        reset(transaction);
        when(transaction.get(NAMESPACE, List.of("name-pa"), IndexSegment.class))
            .thenReturn(List.of(expectedSegment));

        // Then add 10
        subject.add(List.of("pantha"), 10l, transaction);

        expectedSegment = createSegment("pantha", List.of(2l, 7l, 8l, 10l));
        verify(transaction).put(NAMESPACE, "name-pa", expectedSegment);
    }

    @Test
    void add_handlesUpdatesToDifferentSegmentsCorrectly()
    {
        when(transaction.get(NAMESPACE, List.of("name-pa"), IndexSegment.class))
            .thenReturn(listContainingNull);

        // First add 8 to pantha
        subject.add(List.of("pantha"), 8l, transaction);

        IndexSegment expectedSegment = createSegment("pantha", List.of(8l));
        verify(transaction).put(NAMESPACE, "name-pa", expectedSegment);

        reset(transaction);

        when(transaction.get(NAMESPACE, List.of("name-ti"), IndexSegment.class))
            .thenReturn(listContainingNull);

        // Then add 3 to tiger
        subject.add(List.of("tiger"), 3l, transaction);

        expectedSegment = createSegment("tiger", List.of(3l));
        verify(transaction).put(NAMESPACE, "name-ti", expectedSegment);
    }

    @Test
    void add_updatesMultipleSegmentsCorrectly_whenNoSegmentsStored()
    {
        final List<String> segmentIds = List.of("name-pa", "name-ti", "name-ch");
        final List<IndexSegment> segments = Arrays.asList(null, null, null);

        when(transaction.get(NAMESPACE, segmentIds, IndexSegment.class))
            .thenReturn(segments);

        // This should create a new segment for each prefix, and add id 46 to each of them
        subject.add(List.of("pantha", "tiger", "cheetah"), 46l, transaction);

        // 'pa' segment
        {
            final IndexSegment segment = createSegment("pantha", List.of(46l));
            verify(transaction).put(NAMESPACE, "name-pa", segment);
        }

        // 'ti' segment
        {
            final IndexSegment segment = createSegment("tiger", List.of(46l));
            verify(transaction).put(NAMESPACE, "name-ti", segment);
        }

        // 'ch' segment
        {
            final IndexSegment segment = createSegment("cheetah", List.of(46l));
            verify(transaction).put(NAMESPACE, "name-ch", segment);
        }
    }

    @Test
    void add_updatesMultipleSegmentsCorrectly_whenAllSegmentsStored()
    {
        final List<String> segmentIds = List.of("name-pa", "name-ti", "name-ch");

        final List<IndexSegment> segments = Arrays.asList(
            createSegment("pantha", List.of(2l, 7l)),
            createSegment("tiger", List.of(1l, 8l, 7l)),
            createSegment("cheetah", List.of(8l))
        );

        when(transaction.get(NAMESPACE, segmentIds, IndexSegment.class))
            .thenReturn(segments);

        // This should add id 46 to each of the segments
        subject.add(List.of("pantha", "tiger", "cheetah"), 46l, transaction);

        // 'pa' segment
        {
            final IndexSegment segment = createSegment("pantha", List.of(2l, 7l, 46l));
            verify(transaction).put(NAMESPACE, "name-pa", segment);
        }

        // 'ti' segment
        {
            final IndexSegment segment = createSegment("tiger", List.of(1l, 8l, 7l, 46l));
            verify(transaction).put(NAMESPACE, "name-ti", segment);
        }

        // 'ch' segment
        {
            final IndexSegment segment = createSegment("cheetah", List.of(8l, 46l));
            verify(transaction).put(NAMESPACE, "name-ch", segment);
        }
    }

    @Test
    void add_updatesMultipleSegmentsCorrectly_whenSomeSegmentsStored()
    {
        final List<String> segmentIds = List.of("name-pa", "name-ti", "name-ch");

        final List<IndexSegment> segments = Arrays.asList(
            createSegment("pantha", List.of(2l, 7l)),
            null, // The ti segment isn't stored
            createSegment("cheetah", List.of(8l))
        );

        when(transaction.get(NAMESPACE, segmentIds, IndexSegment.class))
            .thenReturn(segments);

        // This should add id 46 to each of the segments
        subject.add(List.of("pantha", "tiger", "cheetah"), 46l, transaction);

        // 'pa' segment
        {
            final IndexSegment segment = createSegment("pantha", List.of(2l, 7l, 46l));
            verify(transaction).put(NAMESPACE, "name-pa", segment);
        }

        // 'ti' segment - new segment, so should just contain the new entity (46)
        {
            final IndexSegment segment = createSegment("tiger", List.of(46l));
            verify(transaction).put(NAMESPACE, "name-ti", segment);
        }

        // 'ch' segment
        {
            final IndexSegment segment = createSegment("cheetah", List.of(8l, 46l));
            verify(transaction).put(NAMESPACE, "name-ch", segment);
        }
    }

    @Test
    void add_addsAllWordsFromTheFilter()
    {
        when(transaction.get(NAMESPACE, List.of("name-pa", "name-ra"), IndexSegment.class))
            .thenReturn(Arrays.asList(null, null));

        // Hypothetical filter that always returns the same two words
        wordFilter = word -> Stream.of("pancetta", "ravioli");

        subject = new Index<>("name", entity -> "", wordFilter, store);

        subject.add(List.of("pantha"), 8l, transaction);

        // Both words should be added to the relevant segment
        IndexSegment expectedSegment = createSegment("pancetta", List.of(8l));
        verify(transaction).put(NAMESPACE, "name-pa", expectedSegment);

        expectedSegment = createSegment("ravioli", List.of(8l));
        verify(transaction).put(NAMESPACE, "name-ra", expectedSegment);
    }

    @Test
    void add_updatesContents_whenContentsNotStored()
    {
        when(transaction.get(NAMESPACE, List.of("name-pa"), IndexSegment.class))
            .thenReturn(listContainingNull);

        subject.add(List.of("pantha"), 8l, transaction);

        final Contents expectedContents = new Contents();
        expectedContents.add(8, "pa");

        verify(transaction).put(NAMESPACE, "name-contents", expectedContents);
    }

    @Test
    void add_updatesContents_whenContentsIsStored()
    {
        final IndexSegment segment = createSegment("pantha", List.of(4l));
        when(transaction.get(NAMESPACE, List.of("name-pa"), IndexSegment.class))
            .thenReturn(List.of(segment));

        // Entity 4 is already in segment 'pa', as per the current stored contents
        final Contents storedContents = new Contents();
        storedContents.add(4, "pa");
        when(transaction.get(NAMESPACE, "name-contents", Contents.class))
            .thenReturn(storedContents);

        // Now add 8, which contains the word 'pantha'
        subject.add(List.of("pantha"), 8l, transaction);

        // 8 should now also be associated with the 'pa' segment, in addition to 4
        final Contents expectedContents = new Contents();
        expectedContents.add(4, "pa");
        expectedContents.add(8, "pa");

        verify(transaction).put(NAMESPACE, "name-contents", expectedContents);
    }

    @Test
    void remove_removesFromAllRelevantSegmentsAndContents()
    {
        Contents storedContents = new Contents();
        storedContents.add(4, "pa");
        storedContents.add(5, "pa");
        storedContents.add(6, "pa");
        storedContents.add(5, "ta");
        storedContents.add(4, "ba");
        when(transaction.get(NAMESPACE, "name-contents", Contents.class))
            .thenReturn(storedContents);

        final List<IndexSegment> storedSegments = new ArrayList<>();
        // 'pa' segment
        {
            final IndexSegment storedSegment = createSegment("pantha", List.of(4l, 6l, 5l));
            storedSegment.add("pans", 5);
            storedSegment.add("pans", 6);
            storedSegment.add("pairs", 5);

            storedSegments.add(storedSegment);
        }

        // 'ta' segment
        {
            final IndexSegment storedSegment = createSegment("tarragon", List.of(5l));

            storedSegments.add(storedSegment);
        }

        // All segments should be fetched in one go
        when(transaction.get(NAMESPACE, List.of("name-pa", "name-ta"), IndexSegment.class))
            .thenReturn(storedSegments);

        // Remove 5
        subject.remove(5, transaction);

        // All entries for 5 should be removed from the Contents
        storedContents = new Contents();
        storedContents.add(4, "pa");
        storedContents.add(6, "pa");
        storedContents.add(4, "ba");
        verify(transaction).put(NAMESPACE, "name-contents", storedContents);

        // And should be removed from only the segments it was in (we want removals/updates
        // to be efficient, and there could be hundreds of segments).

        // 'pa' segment
        {
            final IndexSegment storedSegment = createSegment("pantha", List.of(4l, 6l));
            storedSegment.add("pans", 6);
            verify(transaction).put(NAMESPACE, "name-pa", storedSegment);
        }

        // 'ta' segment
        {
            // 5 was the only entry in this segment
            final IndexSegment storedSegment = createSegment("tarragon", List.of());
            verify(transaction).put(NAMESPACE, "name-ta", storedSegment);
        }

        // Should be unchanged as 5 wasn't in segment 'ba' originally
        verify(transaction, never()).get(NAMESPACE, "name-ba", IndexSegment.class); // Shouldn't make an attempt to load it
        verify(transaction, never()).put(eq(NAMESPACE), eq("name-ba"), any());
    }

    private static IndexSegment createSegment(String word, Collection<Long> ids)
    {
        final IndexSegment segment = new IndexSegment();

        ids.stream()
            .forEach(id -> segment.add(word, id));

        return segment;
    }
}
