package nherald.indigo.index;

import java.util.stream.Stream;

public interface IndexBehaviour
{
    public String sanitise(String word);

    public boolean includeInIndex(String word);

    public boolean includeInSearch(String word);

    public Stream<String> ngram(String word);
}
