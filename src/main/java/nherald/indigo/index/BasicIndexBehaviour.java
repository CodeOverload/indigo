package nherald.indigo.index;

import java.util.stream.Stream;

public class BasicIndexBehaviour implements IndexBehaviour
{
    /*
        When indexing   Tomato Bo O'Neill her ->
                            tokenise: Tomato, Bo, O'Neill, her
                            sanitise: tomato, bo, oneill, her
                            filter:
                                filter words that are too short: tomato, oneill, her
                                filter words not starting with alpha: tomato, oneill
                                filter stop words: tomato, oneill
                            ngram: tomato, tomat, toma, tom, oneill, oneil, onei, one

        When searching  o'neill 123reg Tomato bo ->
                            tokenise: o'neill, 123reg, Tomato, bo
                            sanitise: oneill, 123reg, tomato, bo
                            filter:
                                filter words that are too short: oneill, 123reg, tomato
                                filter words not starting with alpha: oneill, tomato
                                filter stop words: [NOT DONE, as e.g. 'her' could be the prefix of 'herta']
                            then search index separately for each of these terms; oneil, tomato  (no ngram)
    */

    @Override
    public String sanitise(String word)
    {
        return word.replaceAll("[^a-zA-Z0-9]+", "")
            .toLowerCase();
    }

    @Override
    public boolean includeInIndex(String word)
    {
        return includeInSearch(word) && !StopWords.isStopWord(word);
    }

    @Override
    public boolean includeInSearch(String word)
    {
        return isWordLongEnough(word) && wordStartsWithLetter(word);
    }

    @Override
    public Stream<String> ngram(String word)
    {
        final String[] result = new String[word.length() - 3 + 1];

        for (int i = 0; i < result.length; ++i)
        {
            result[i] = word.substring(0, i + 3);
        }

        return Stream.of(result);
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
}
