package nherald.indigo.index;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

import nherald.indigo.index.terms.WordSelector;

@ExtendWith(MockitoExtension.class)
class IndexSegmentTests
{
    @Mock
    private WordSelector selector;

    @Test
    void get_looksUpAllWordsFromSelector()
    {
        // Hypothetical selector that picks related words
        when(selector.select(eq("gopher"), any()))
            .thenReturn(Stream.of("gopher", "mole"));

        final IndexSegmentData data = mockData();
        final IndexSegment subject = new IndexSegment(data, selector);

        final List<Long> actual = sort(subject.get("gopher"));

        // These entities contained either of the words
        Assertions.assertEquals(List.of(3l, 5l, 7l), actual);
    }

    @Test
    void get_worksWhenWordIsntInIndex_andNoOtherWordsSpecifiedBySelector()
    {
        when(selector.select(eq("cat"), any()))
            // Word that isn't in the index
            .thenReturn(Stream.of("cat"));

        final IndexSegmentData data = mockData();
        final IndexSegment subject = new IndexSegment(data, selector);

        final List<Long> actual = sort(subject.get("cat"));

        // Nothing matches the word the selector returned
        Assertions.assertEquals(List.of(), actual);
    }

    @Test
    void get_worksWhenWordIsntInIndex_andOtherWordsSpecifiedBySelector()
    {
        // Hypothetical selector that picks related words
        when(selector.select(eq("gopher"), any()))
            // The first of these words isn't in the index
            .thenReturn(Stream.of("cat", "mole"));

        final IndexSegmentData data = mockData();
        final IndexSegment subject = new IndexSegment(data, selector);

        final List<Long> actual = sort(subject.get("gopher"));

        // Should still return the entities for the word that is in the index
        Assertions.assertEquals(List.of(7l), actual);
    }

    private IndexSegmentData mockData()
    {
        final IndexSegmentData data = new IndexSegmentData();

        data.add("gopher", 3);
        data.add("gopher", 5);

        data.add("tiger", 5);

        data.add("catfish", 1);

        data.add("mole", 7);

        return data;
    }

    private List<Long> sort(Collection<Long> collection)
    {
        return collection.stream()
            .sorted()
            .collect(Collectors.toList());
    }
}
