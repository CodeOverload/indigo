package nherald.indigo.index.terms;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BasicWordFilterTests
{
    @Test
    void sanitise_removesSpecialCharacters()
    {
        final BasicWordFilter subject = new BasicWordFilter(false);

        final String actual = subject.sanitise("'o'nei%^!ll");
        final String expected = "oneill";

        Assertions.assertEquals(expected, actual);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "bo", // Too short
        "b", // Too short
        "150ml" // Starts with a number
    })
    void includeInSearch_filtersCorrectly(String input)
    {
        final BasicWordFilter subject = new BasicWordFilter(false);

        final boolean actual = subject.includeInSearch(input);

        Assertions.assertFalse(actual);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "hat", "pancetta", "area52"
    })
    void includeInSearch_doesntFilterValidTerms(String input)
    {
        final BasicWordFilter subject = new BasicWordFilter(false);

        final boolean actual = subject.includeInSearch(input);

        Assertions.assertTrue(actual);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "bo", // Too short
        "b", // Too short
        "150ml", // Starts with a number
        "him", // Is a stop word
        "one" // Is a stop word
    })
    void includeInIndex_filtersCorrectly(String input)
    {
        final BasicWordFilter subject = new BasicWordFilter(true);

        final boolean actual = subject.includeInIndex(input);

        Assertions.assertFalse(actual);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "him", // Is a stop word
        "one" // Is a stop word
    })
    void includeInIndex_doesntFilterStopWords_whenStopWordFilterDisabled(String input)
    {
        final BasicWordFilter subject = new BasicWordFilter(false);

        final boolean actual = subject.includeInIndex(input);

        Assertions.assertTrue(actual);
    }

    @Test
    void process_sanitise()
    {
        final BasicWordFilter subject = new BasicWordFilter(true);

        final List<String> actual = subject.process("o'neill")
            .collect(Collectors.toList());

        final List<String> expected = List.of("oneill");

        Assertions.assertEquals(expected, actual);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "o'n", // Currently longer than 2 chars, but after sanitise it won't be
        "150ml", // Starts with a number
        "three" // Stop word
    })
    void process_sanitiseAndFilter(String input)
    {
        final BasicWordFilter subject = new BasicWordFilter(true);

        final List<String> actual = subject.process(input)
            .collect(Collectors.toList());

        final List<String> expected = List.of();

        Assertions.assertEquals(expected, actual);
    }
}
