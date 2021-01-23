package nherald.indigo.index.terms;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import nherald.indigo.index.IndexSegmentData;

@ExtendWith(MockitoExtension.class)
class PrefixWordSelectorTests
{
    @Test
    void select_picksAllWordsThatMatchPrefix_whenMultipleMatchThePrefix()
    {
        final PrefixWordSelector subject = new PrefixWordSelector();

        final IndexSegmentData data = mockData();

        final List<String> actual = sort(subject.select("go", data));

        Assertions.assertEquals(List.of("goat", "goose", "gopher"), actual);
    }

    @Test
    void select_picksCorrectWord_whenOnlyOneMatchesThePrefix()
    {
        final PrefixWordSelector subject = new PrefixWordSelector();

        final IndexSegmentData data = mockData();

        final List<String> actual = sort(subject.select("ca", data));

        Assertions.assertEquals(List.of("catfish"), actual);
    }

    @Test
    void select_returnsEmptySet_whenNoWordsMatchThePrefix()
    {
        final PrefixWordSelector subject = new PrefixWordSelector();

        final IndexSegmentData data = mockData();

        final List<String> actual = sort(subject.select("pa", data));

        Assertions.assertEquals(List.of(), actual);
    }

    private IndexSegmentData mockData()
    {
        final IndexSegmentData data = new IndexSegmentData();

        data.add("gopher", 3);
        data.add("gopher", 5);

        data.add("tiger", 5);

        data.add("catfish", 1);

        data.add("goat", 7);

        data.add("goose", 22);

        return data;
    }

    private List<String> sort(Stream<String> words)
    {
        return words
            .sorted()
            .collect(Collectors.toList());
    }
}
