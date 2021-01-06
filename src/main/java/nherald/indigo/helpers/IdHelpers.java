package nherald.indigo.helpers;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import nherald.indigo.store.StoreException;

public class IdHelpers
{
    /** Valid pattern for Store ids */
    private static final Pattern VALID_PATTERN = Pattern.compile("[a-zA-Z0-9_-]+");

    private IdHelpers() {}

    /**
     * Converts a numerical id into a string
     * @param id numerical id
     * @return id as a string
     */
    public static String asString(long id)
    {
        return id + "";
    }

    /**
     * Converts numerical ids into strings
     * @param ids numerical ids
     * @return a list, of equal length as the input list, containing the ids
     * as strings
     */
    public static List<String> asStrings(List<Long> ids)
    {
        return ids.stream()
            .map(IdHelpers::asString)
            .collect(Collectors.toList());
    }

    /**
     * Validates that a store id is valid. As ids are used in filenames, firestore
     * document ids etc we want to ensure there aren't any bad characters in them
     * @param id id
     * @return the id, to allow chaining
     * @throws StoreException if the id isn't valid
     */
    public static String validate(String id)
    {
        final Matcher matcher = VALID_PATTERN.matcher(id);

        if (matcher.matches()) return id;

        throw new StoreException("Invalid id " + id);
    }
}
