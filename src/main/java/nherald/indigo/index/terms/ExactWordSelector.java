package nherald.indigo.index.terms;

import java.util.stream.Stream;

import nherald.indigo.index.IndexSegmentData;

/**
 * Selects words in the index that match the search term exactly
 */
public class ExactWordSelector implements WordSelector
{
    @Override
    public Stream<String> select(String searchTerm, IndexSegmentData data)
    {
        return Stream.of(searchTerm);
    }
}
