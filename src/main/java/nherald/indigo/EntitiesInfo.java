package nherald.indigo;

public class EntitiesInfo
{
    private long maxId;

    public EntitiesInfo()
    {
        maxId = 0;
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
