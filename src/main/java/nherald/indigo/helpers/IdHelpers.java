package nherald.indigo.helpers;

import java.util.List;
import java.util.stream.Collectors;

public class IdHelpers
{
    private IdHelpers() {}

    public static String asString(long id)
    {
        return id + "";
    }

    public static List<String> asStrings(List<Long> ids)
    {
        return ids.stream()
            .map(IdHelpers::asString)
            .collect(Collectors.toList());
    }
}
