package nherald.indigo.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tracks which segments of the index each entity id is in. This is essentially the
 * index in reverse, and allows us to quickly see which segments need updated when
 * an entity is updated or removed. In either case, we need to remove the old
 * entries from the index. We could open every single index segment, but there are
 * a lot of segments and most entities are only likely to be in a subset of them.
 */
public class Contents
{
    /** Map of entity id to the set of segment ids that entity is in */
    private Map<Long, Set<String>> map;

    public Contents()
    {
        map = new HashMap<>(2001);
    }

    /**
     * Adds an association between an entity and a segment
     * @param entityId entity id
     * @param segmentId segmentid
     */
    public void add(long entityId, String segmentId)
    {
        final Set<String> segments = getSegments(entityId);

        segments.add(segmentId);
    }

    /**
     * Get the set of segments associated with a particular entity
     * @param entityId entity id
     * @return the associated segments
     */
    public Set<String> get(long entityId)
    {
        return Collections.unmodifiableSet(getSegments(entityId));
    }

    /**
     * Completely removes all associations for a particular entity
     * @param entityId entity id
     */
    public void remove(long entityId)
    {
        map.remove(entityId);
    }

    /**
     * Gets a shallow copy of the underlying map. This is here for serialisation; we want both Jackson
     * (used for serialisation to json files) and Firestore to persist the underlying map
     * @return a clone of the underlying map
     */
    public Map<String, List<String>> getMap()
    {
        return map.entrySet()
            .stream()
            .collect(Collectors.toMap(
                // Firestore only supports string keys, so convert to strings
                e -> e.getKey() + "",
                // Also Firestore doesn't support sets (complains that collections aren't supported), so convert to a list
                e -> new ArrayList<>(e.getValue()))
            );
    }

    public void setMap(Map<String, List<String>> newMap)
    {
        // Do the inverse of Contents#getMap - refer to the comments of that
        map = newMap.entrySet()
            .stream()
            .collect(Collectors.toMap(
                e -> Long.parseLong(e.getKey()),
                e -> new HashSet<>(e.getValue())
            ));
    }

    public Set<String> getSegments(long entityId)
    {
        Set<String> segments = map.get(entityId);

        if (segments != null) return segments;

        segments = new HashSet<>(101);

        map.put(entityId, segments);

        return segments;
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
        Contents other = (Contents) obj;
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
        builder.append("Contents [map=").append(map).append("]");
        return builder.toString();
    }
}
