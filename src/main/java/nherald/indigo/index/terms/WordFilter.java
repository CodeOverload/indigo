package nherald.indigo.index.terms;

import java.util.stream.Stream;

/**
 * Processes words prior to them being added to the index. It's up
 * to implementations how words are handled; some may apply filters,
 * some may apply transformations, most will be a mixture of the two
 */
@FunctionalInterface
public interface WordFilter
{
    /**
     * Applies all filters and transformations to a word, to produce the terms
     * that should be added to the index for this word.
     *
     * Note that this can return an empty result as some words will be filtered
     * out (e.g. if a stop word filter is applied). Multiple strings can be
     * returned if an ngram is applied. Ngraming is where a word is split into
     * all its possible prefixes.
     *    E.g. train will result in: train, trai, tra, tr, t
     *
     * (Implementations are likely to ignore prefixes under a certain length to
     * keep the index small, and to make the results more meaningful)
     *
     * @param word word to process
     * @return the terms that should be added to the index for this word. May
     * be empty
     */
    Stream<String> process(String word);
}
