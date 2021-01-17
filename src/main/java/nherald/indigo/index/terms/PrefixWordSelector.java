package nherald.indigo.index.terms;

import java.util.stream.Stream;

import nherald.indigo.index.IndexSegmentData;

/**
 * Selects all words in the index that are prefixed with the specified search
 * term
 */
public class PrefixWordSelector implements WordSelector
{
    @Override
    public Stream<String> select(String searchTerm, IndexSegmentData data)
    {
        return data.allWords()
            .stream()
            .filter(word -> word.startsWith(searchTerm));
    }
}
