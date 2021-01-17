package nherald.indigo.index.terms;

import java.util.stream.Stream;

import nherald.indigo.index.IndexSegmentData;

/**
 * Selects which words to look up in the index for a particular search term
 */
@FunctionalInterface
public interface WordSelector
{
    /**
     * Selects the words to look up in the index for a particular search term
     * @param searchTerm search term
     * @param data segment data to search over
     * @return the words to look up in the index
     */
    Stream<String> select(String searchTerm, IndexSegmentData data);
}
