package nherald.indigo.index;

import java.util.Collection;

import nherald.indigo.Entity;

public class IndexTestUtils
{
    public static <T extends Entity> Collection<Index<T>> getIndices(
        IndicesManager<T> manager)
    {
        return manager.getIndices();
    }
}
