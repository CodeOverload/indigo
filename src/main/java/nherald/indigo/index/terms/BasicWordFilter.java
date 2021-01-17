package nherald.indigo.index.terms;

import java.util.stream.Stream;

/**
 * Word filter/processor for words added to the index, that should cover a lot
 * of the basic use cases.
 *
 * This applies the following:
 *   1. Remove all non-alpha numeric characters
 *   2. Convert to lower case
 *   3. Filter words that are too short (&lt; 3 characters)
 *   4. Filter words that start with a number
 *   5. Filter stop words (this is optional; can be switched on/off via ctor)
 *
 * Example:
 *         INPUT: Tomato Bo O'Neill her 150ml
 *
 *         sanitise: tomato, bo, oneill, her, 150ml
 *         filter:
 *              filter words that are too short: tomato, oneill, her, 150ml
 *              filter words not starting with alpha: tomato, oneill, her
 *              filter stop words: tomato, oneill
 *
 * When searching an index built using a particular word filter, the same
 * transformations should be applied to the search terms. E.g. if the search
 * term isn't converted to lower case and has upper case characters, it won't
 * ever match anything in the index because the index only contains lower
 * case characters. There are some exceptions to this. For example, 'her' is
 * a stop word so won't be added to the index itself (if stop word filtering is
 * enabled). However, the word 'herta', has a prefix of 'her', so 'her' will
 * appear in the index for cases when the word 'herta' was used. Therefore, the
 * stop word filter should never be applied to search terms as it won't allow
 * the client to search for 'herta' in this case, using the prefix 'her'
 */
public class BasicWordFilter implements WordFilter
{
    private final boolean filterStopWords;

    public BasicWordFilter(boolean filterStopWords)
    {
        this.filterStopWords = filterStopWords;
    }

    public String sanitise(String word)
    {
        return word.replaceAll("[^a-zA-Z0-9]+", "")
            .toLowerCase();
    }

    public boolean includeInIndex(String word)
    {
        return includeInSearch(word) &&
            !(filterStopWords && StopWords.isStopWord(word));
    }

    public boolean includeInSearch(String word)
    {
        return isWordLongEnough(word) && wordStartsWithLetter(word);
    }

    private boolean isWordLongEnough(String word)
    {
        return word.length() > 2;
    }

    private boolean wordStartsWithLetter(String word)
    {
        // Note that this must be applied after isWordLongEnough
        return Character.isAlphabetic(word.charAt(0));
    }

    @Override
    public Stream<String> process(String word)
    {
        return Stream.of(word)
            .map(this::sanitise)
            .filter(this::includeInIndex);
    }
}
