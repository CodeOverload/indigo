package nherald.indigo.index;

import java.util.Arrays;
import java.util.stream.Stream;

public class BasicTokeniser
{
    public Stream<String> tokenise(String phrase)
    {
        return Arrays.stream(phrase.split(" +"));
    }
}
