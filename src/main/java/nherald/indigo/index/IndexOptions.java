package nherald.indigo.index;

import nherald.indigo.Entity;

public class IndexOptions<T extends Entity>
{
    private final String id;
    private final IndexTarget<T> target;
    private final IndexBehaviour behaviour;

    public IndexOptions(String id, IndexTarget<T> target, IndexBehaviour behaviour)
    {
        this.id = id;
        this.target = target;
        this.behaviour = behaviour;
    }

    public String getId()
    {
        return id;
    }

    public IndexTarget<T> getTarget()
    {
        return target;
    }

    public IndexBehaviour getBehaviour()
    {
        return behaviour;
    }
}
