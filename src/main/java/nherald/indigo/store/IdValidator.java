package nherald.indigo.store;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IdValidator
{
    private static final Pattern PATTERN = Pattern.compile("[a-zA-Z0-9_-]+");

    private IdValidator()
    {
    }

    public static String check(String id)
    {
        final Matcher matcher = PATTERN.matcher(id);

        if (matcher.matches()) return id;

        throw new StoreException("Invalid id " + id);
    }
}
