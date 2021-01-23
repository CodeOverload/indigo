package nherald.indigo.helpers;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

import static nherald.indigo.helpers.MapHelpers.asMap;

@ExtendWith(MockitoExtension.class)
class MapHelpersTests
{
    @Mock
    private Function<String, Long> defaultFunction;

    @Test
    void asMap_returnsCorrectMap_whenEqualSizedCollections()
    {
        final List<String> keys = List.of("b", "a", "cat");
        final List<Long> values = List.of(34l, 99l, 1l);

        final Map<String, Long> actual = asMap(keys, values, defaultFunction);

        final Map<String, Long> expected = new HashMap<>();
        expected.put("b", 34l);
        expected.put("a", 99l);
        expected.put("cat", 1l);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void asMap_usesDefault_whenValueIsNull()
    {
        final List<String> keys = List.of("b", "a", "cat");
        final List<Long> values = Arrays.asList(34l, null, null);

        when(defaultFunction.apply("a")).thenReturn(63l);
        when(defaultFunction.apply("cat")).thenReturn(61l);

        final Map<String, Long> actual = asMap(keys, values, defaultFunction);

        final Map<String, Long> expected = new HashMap<>();
        expected.put("b", 34l);
        expected.put("a", 63l);
        expected.put("cat", 61l);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void asMap_returnsEmptyMap_whenCollectionsAreEmpty()
    {
        final List<String> keys = Collections.emptyList();
        final List<Long> values = Collections.emptyList();

        final Map<String, Long> actual = asMap(keys, values, defaultFunction);

        final Map<String, Long> expected = new HashMap<>();

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void asMap_notEnoughKeys_throwsException()
    {
        final List<String> keys = List.of("b", "a");
        final List<Long> values = List.of(34l, 99l, 1l);

        Assertions.assertThrows(IllegalArgumentException.class,
            () -> asMap(keys, values, defaultFunction));
    }

    @Test
    void asMap_notEnoughValues_throwsException()
    {
        final List<String> keys = List.of("b", "a", "cat");
        final List<Long> values = List.of(34l, 99l);

        Assertions.assertThrows(IllegalArgumentException.class,
            () -> asMap(keys, values, defaultFunction));
    }
}
