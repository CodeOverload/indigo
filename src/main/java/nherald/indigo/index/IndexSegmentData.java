package nherald.indigo.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class IndexSegmentData
{
    /**
     * Map of word (or prefix, in the case of ngram indexes) to the set of
     * entity ids that contain that word
     */
    private Map<String, Set<Long>> map;

    public IndexSegmentData()
    {
        map = new HashMap<>(1001);
    }

    /**
     * Gets a shallow copy of the underlying map. This is here for serialisation; we want both Jackson
     * (used for serialisation to json files) and Firestore to persist the underlying map
     * @return a clone of the underlying map
     */
    public Map<String, List<Long>> getMap()
    {
        return map.entrySet()
            .stream()
            .collect(Collectors.toMap(
                Entry::getKey,
                // Firestore doesn't support sets (complains that collections aren't supported), so convert to a list
                e -> new ArrayList<>(e.getValue()))
            );
    }

    public void setMap(Map<String, List<Long>> newMap)
    {
        // Do the inverse of IndexSegment#getMap - refer to the comments of that
        map = newMap.entrySet()
            .stream()
            .collect(Collectors.toMap(Entry::getKey, e -> new HashSet<>(e.getValue())));
    }

    public Set<Long> get(String word)
    {
        final Set<Long> result = map.get(word);

        return result != null ? result : Collections.emptySet();
    }

    public void add(String word, long entityId)
    {
        Set<Long> result = map.get(word);

        if (result == null)
        {
            result = new HashSet<>(21);
            map.put(word, result);
        }

        result.add(entityId);
    }

    public void remove(long entityId)
    {
        // Go through the entities mapped to each word, and remove any references to the specified entity id
        map.entrySet()
            .stream()
            // Collect and re-stream, as removeEntityFromPrefix removes entries from the map (avoids
            // concurrent modification exceptions)
            .collect(Collectors.toList())
            .stream()
            .forEach(e -> removeEntityFromPrefix(e, entityId));
    }

    private void removeEntityFromPrefix(Entry<String, Set<Long>> wordEntry, long entityId)
    {
        final Set<Long> entityIds = wordEntry.getValue();

        // Remove the entity id from list of entity ids for this word
        entityIds.remove(entityId);

        // If there are no entity ids containing this word, remove the entry for it from the map.
        // This saves space in the storage; if this wasn't done over time we'd end up with many empty sets
        // for words that are no longer used
        if (entityIds.isEmpty())
        {
            map.remove(wordEntry.getKey());
        }
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((map == null) ? 0 : map.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        IndexSegmentData other = (IndexSegmentData) obj;
        if (map == null) {
            if (other.map != null)
                return false;
        } else if (!map.equals(other.map))
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("IndexSegmentData [map=").append(map).append("]");
        return builder.toString();
    }

}
