package nherald.indigo.index;

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

import nherald.indigo.TestEntity;
import nherald.indigo.index.terms.WordFilter;
import nherald.indigo.store.Store;
import nherald.indigo.store.StoreException;
import nherald.indigo.uow.BatchUpdate;

@ExtendWith(MockitoExtension.class)
public class IndexTests
{
    private static final String NAMESPACE = "indices";

    private WordFilter wordFilter;

    @Mock
    private Store store;

    @Mock
    private BatchUpdate batch;

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
    void get_usesCacheOnSucessiveCalls()
    {
        final IndexSegment segment = createSegment("pantha", List.of(4l, 7l));
        when(store.get(NAMESPACE, "name-pa", IndexSegment.class)).thenReturn(segment);

        subject.get("pantha");

        verify(store).get(NAMESPACE, "name-pa", IndexSegment.class);
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
        subject.add(List.of("pantha"), 46l, batch);

        final IndexSegment expectedSegment = createSegment("pantha", List.of(46l));

        verify(store).put(NAMESPACE, "name-pa", expectedSegment, batch);
    }

    @Test
    void add_updatesSegmentCorrectly_whenSegmentAlreadyStored()
    {
        // Already has ids 2 and 7 stored for this word
        final IndexSegment segment = createSegment("pantha", List.of(2l, 7l));
        when(store.get(NAMESPACE, "name-pa", IndexSegment.class)).thenReturn(segment);

        // This should add id 46 to that list
        subject.add(List.of("pantha"), 46l, batch);

        final IndexSegment expectedSegment = createSegment("pantha", List.of(2l, 7l, 46l));

        verify(store).put(NAMESPACE, "name-pa", expectedSegment, batch);
    }

    @Test
    void add_updatesSegmentCorrectly_whenSegmentAlreadyStored_andIdAlreadyStoredAgainstWord()
    {
        // Already has ids 2 and 7 stored for this word
        final IndexSegment segment = createSegment("pantha", List.of(2l, 7l));
        when(store.get(NAMESPACE, "name-pa", IndexSegment.class)).thenReturn(segment);

        // We're adding 7 again, so should still have just 2 and 7 saved back to the store
        subject.add(List.of("pantha"), 7l, batch);

        final IndexSegment expectedSegment = createSegment("pantha", List.of(2l, 7l));

        verify(store).put(NAMESPACE, "name-pa", expectedSegment, batch);
    }

    @Test
    void add_updatesSegmentCorrectlyOnSuccessiveUpdates()
    {
        IndexSegment segment = createSegment("pantha", List.of(2l, 7l));
        when(store.get(NAMESPACE, "name-pa", IndexSegment.class)).thenReturn(segment);

        // First add 8
        subject.add(List.of("pantha"), 8l, batch);

        IndexSegment expectedSegment = createSegment("pantha", List.of(2l, 7l, 8l));
        verify(store).put(NAMESPACE, "name-pa", expectedSegment, batch);

        reset(store);

        // Then add 10
        subject.add(List.of("pantha"), 10l, batch);

        expectedSegment = createSegment("pantha", List.of(2l, 7l, 8l, 10l));
        verify(store).put(NAMESPACE, "name-pa", expectedSegment, batch);
    }

    @Test
    void add_handlesUpdatesToDifferentSegmentsCorrectly()
    {
        // First add 8 to pantha
        subject.add(List.of("pantha"), 8l, batch);

        IndexSegment expectedSegment = createSegment("pantha", List.of(8l));
        verify(store).put(NAMESPACE, "name-pa", expectedSegment, batch);

        reset(store);

        // Then add 3 to tiger
        subject.add(List.of("tiger"), 3l, batch);

        expectedSegment = createSegment("tiger", List.of(3l));
        verify(store).put(NAMESPACE, "name-ti", expectedSegment, batch);
    }

    @Test
    void add_handlesUpdatesToTheSameSegmentCorrectly()
    {
        // First add 8 to pantha
        subject.add(List.of("pantha"), 8l, batch);

        IndexSegment expectedSegment = createSegment("pantha", List.of(8l));
        verify(store).put(NAMESPACE, "name-pa", expectedSegment, batch);

        reset(store);

        // Then add 3 to pancetta
        subject.add(List.of("pancetta"), 3l, batch);

        expectedSegment = createSegment("pantha", List.of(8l));
        expectedSegment.add("pancetta", 3l);

        verify(store).put(NAMESPACE, "name-pa", expectedSegment, batch);
    }

    @Test
    void add_addsAllWordsFromTheFilter()
    {
        // Hypothetical filter that always returns the same two words
        wordFilter = word -> Stream.of("pancetta", "ravioli");

        subject = new Index<>("name", entity -> "", wordFilter, store);

        subject.add(List.of("pantha"), 8l, batch);

        // Both words should be added to the relevant segment
        IndexSegment expectedSegment = createSegment("pancetta", List.of(8l));
        verify(store).put(NAMESPACE, "name-pa", expectedSegment, batch);

        expectedSegment = createSegment("ravioli", List.of(8l));
        verify(store).put(NAMESPACE, "name-ra", expectedSegment, batch);
    }

    @Test
    void add_updatesContents_whenContentsNotStored()
    {
        when(store.get(NAMESPACE, "name-pa", IndexSegment.class)).thenReturn(null);

        subject.add(List.of("pantha"), 8l, batch);

        final Contents expectedContents = new Contents();
        expectedContents.add(8, "pa");

        verify(store).put(NAMESPACE, "name-contents", expectedContents, batch);
    }

    @Test
    void add_updatesContents_whenContentsIsStored()
    {
        final IndexSegment segment = createSegment("pantha", List.of(4l));
        when(store.get(NAMESPACE, "name-pa", IndexSegment.class)).thenReturn(segment);

        // Entity 4 is already in segment 'pa', as per the current stored contents
        final Contents storedContents = new Contents();
        storedContents.add(4, "pa");
        when(store.get(NAMESPACE, "name-contents", Contents.class))
            .thenReturn(storedContents);

        // Now add 8, which contains the word 'pantha'
        subject.add(List.of("pantha"), 8l, batch);

        // 8 should now also be associated with the 'pa' segment, in addition to 4
        final Contents expectedContents = new Contents();
        expectedContents.add(4, "pa");
        expectedContents.add(8, "pa");

        verify(store).put(NAMESPACE, "name-contents", expectedContents, batch);
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
        when(store.get(NAMESPACE, "name-contents", Contents.class))
            .thenReturn(storedContents);

        IndexSegment storedSegment = createSegment("pantha", List.of(4l, 6l, 5l));
        storedSegment.add("pans", 5);
        storedSegment.add("pans", 6);
        storedSegment.add("pairs", 5);
        when(store.get(NAMESPACE, "name-pa", IndexSegment.class)).thenReturn(storedSegment);

        storedSegment = createSegment("tarragon", List.of(5l));
        when(store.get(NAMESPACE, "name-ta", IndexSegment.class)).thenReturn(storedSegment);

        // Remove 5
        subject.remove(5, batch);

        // All entries for 5 should be removed from the Contents
        storedContents = new Contents();
        storedContents.add(4, "pa");
        storedContents.add(6, "pa");
        storedContents.add(4, "ba");
        verify(store).put(NAMESPACE, "name-contents", storedContents, batch);

        // And should be removed from only the segments it was in (we want removals/updates
        // to be efficient, and there could be hundreds of segments)
        storedSegment = createSegment("pantha", List.of(4l, 6l));
        storedSegment.add("pans", 6);
        verify(store).put(NAMESPACE, "name-pa", storedSegment, batch);

        // 5 was the only entry in this segment
        storedSegment = createSegment("tarragon", List.of());
        verify(store).put(NAMESPACE, "name-ta", storedSegment, batch);

        // Should be unchanged as 5 wasn't in segment 'ba' originally
        verify(store, never()).get(NAMESPACE, "name-ba", IndexSegment.class); // Shouldn't make an attempt to load it
        verify(store, never()).put(eq(NAMESPACE), eq("name-ba"), any(), eq(batch));
    }

    private static IndexSegment createSegment(String word, Collection<Long> ids)
    {
        final IndexSegment segment = new IndexSegment();

        ids.stream()
            .forEach(id -> segment.add(word, id));

        return segment;
    }
}
