package nherald.indigo.store.firebase.db;

public interface FirebaseDocument
{
    boolean exists();

    <T> T asObject(Class<T> type);
}
