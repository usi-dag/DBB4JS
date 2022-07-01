package SQL.sql_utils;

public final class MarkedRowWrapper<T> {
    private final T element;
    private boolean marked = false;

    public MarkedRowWrapper(T element) {
        this.element = element;
    }

    public boolean isMarked() {
        return marked;
    }

    public void mark() {
        marked = true;
    }

    public T element() {
        return element;
    }
}
