package nherald.indigo.index;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexSegmentTests
{
    private IndexSegment subject;

    @BeforeEach
    void before()
    {
        subject = new IndexSegment();

        subject.add("butterfly", 3);
        subject.add("ostrich", 3);

        subject.add("butterfly", 5);
        subject.add("eagle", 5);

        subject.add("gopher", 8);
    }

    @Test
    void get_returnsAllEntities_whenOneEntityContainWord()
    {
        final List<Long> actual = sort(subject.get("eagle"));

        Assertions.assertEquals(List.of(5l), actual);
    }

    @Test
    void get_returnsAllEntities_whenMultipleEntitiesContainWord()
    {
        final List<Long> actual = sort(subject.get("butterfly"));

        Assertions.assertEquals(List.of(3l, 5l), actual);
    }

    @Test
    void get_returnsNoEntities_whenNoEntitiesContainWord()
    {
        final List<Long> actual = sort(subject.get("weasel"));

        Assertions.assertEquals(Collections.emptyList(), actual);
    }

    @Test
    void add_addsEntity_whenWordNotInSegment()
    {
        subject.add("butterfly", 7);

        final List<Long> actual = sort(subject.get("butterfly"));

        Assertions.assertEquals(List.of(3l, 5l, 7l), actual);
    }

    @Test
    void add_addsEntity_whenWordAlreadyAssociatedWithEntity()
    {
        // 5 is already mapped to 'butterfly'
        subject.add("butterfly", 5);

        final List<Long> actual = sort(subject.get("butterfly"));

        Assertions.assertEquals(List.of(3l, 5l), actual);
    }

    @Test
    void add_addsEntity_whenWordInSegment()
    {
        subject.add("weasel", 3);

        final List<Long> actual = sort(subject.get("weasel"));

        Assertions.assertEquals(List.of(3l), actual);
    }

    @Test
    void remove_removesEntityFromAllWords_whenEntityInSegment()
    {
        subject.remove(5);

        List<Long> actual = sort(subject.get("butterfly"));
        Assertions.assertEquals(List.of(3l), actual);

        actual = sort(subject.get("eagle"));
        Assertions.assertEquals(Collections.emptyList(), actual);
    }

    @Test
    void remove_doesntFail_whenEntityNotInSegment()
    {
        subject.remove(10l);

        final List<Long> actual = sort(subject.get("butterfly"));

        Assertions.assertEquals(List.of(3l, 5l), actual);
    }

    @Test
    void getMap_returnsAllWordsAndEntities()
    {
        final Map<String, List<Long>> actualMap = subject.getMap();

        List<Long> actual = sort(actualMap.get("butterfly"));
        Assertions.assertEquals(List.of(3l, 5l), actual);

        actual = sort(actualMap.get("gopher"));
        Assertions.assertEquals(List.of(8l), actual);
    }

    private List<Long> sort(Collection<Long> collection)
    {
        return collection.stream()
            .sorted()
            .collect(Collectors.toList());
    }
}
