package nherald.indigo.index;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import nherald.indigo.index.terms.WordSelector;

public class IndexSegment
{
    private final IndexSegmentData data;
    private final WordSelector selector;

    public IndexSegment(IndexSegmentData data, WordSelector selector)
    {
        this.data = data;
        this.selector = selector;
    }

    public Set<Long> get(String word)
    {
        // Look up all the words returned by the selector
        return selector.select(word, data)
            .map(data::get)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
    }
}
