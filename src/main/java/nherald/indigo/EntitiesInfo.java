package nherald.indigo;

public class EntitiesInfo
{
    private long maxId;

    public EntitiesInfo()
    {
        this(0);
    }

    public EntitiesInfo(long maxId)
    {
        this.maxId = maxId;
    }

    public long getMaxId()
    {
        return maxId;
    }

    public long generateId()
    {
        return ++maxId;
    }
}
