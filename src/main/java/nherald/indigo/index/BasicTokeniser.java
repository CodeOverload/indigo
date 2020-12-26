package nherald.indigo.index;

import java.util.List;

public class BasicTokeniser
{
    public List<String> tokenise(String phrase)
    {
        return List.of(phrase.split(" +"));
    }
}
