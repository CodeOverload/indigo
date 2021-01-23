package nherald.indigo.helpers;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

public final class MapHelpers
{
    private MapHelpers()
    {
    }

    public static <K, V> Map<K, V> asMap(Collection<K> keys, Collection<V> values,
        Function<K, V> defaultFunction)
    {
        final Map<K, V> result = new HashMap<>();

        final Iterator<K> itrKeys = keys.iterator();
        final Iterator<V> itrValues = values.iterator();

        do
        {
            if (!itrKeys.hasNext())
            {
                if (itrValues.hasNext())
                {
                    throw new IllegalArgumentException("Not enough keys");
                }

                return result;
            }

            if (!itrValues.hasNext())
            {
                throw new IllegalArgumentException("Not enough values");
            }

            final K key = itrKeys.next();
            V value = itrValues.next();

            if (value == null)
            {
                value = defaultFunction.apply(key);
            }

            result.put(key, value);

        } while(true);
    }
}
