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

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (maxId ^ (maxId >>> 32));
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
        EntitiesInfo other = (EntitiesInfo) obj;
        if (maxId != other.maxId)
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("EntitiesInfo [maxId=").append(maxId).append("]");
        return builder.toString();
    }
}
