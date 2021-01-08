package nherald.indigo;

import nherald.indigo.store.StoreException;

public class IndigoAdminBuilder<T extends Entity>
{
    private Indigo<T> indigo;

    public IndigoAdminBuilder<T> indigo(Indigo<T> indigo)
    {
        this.indigo = indigo;
        return this;
    }

    public IndigoAdmin<T> build()
    {
        if (indigo == null)
        {
            throw new StoreException("Indigo not specified");
        }

        return new IndigoAdmin<>(indigo, indigo.getIndicesManager());
    }
}
