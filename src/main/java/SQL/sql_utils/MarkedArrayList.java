package SQL.sql_utils;

import java.util.ArrayList;

public final class MarkedArrayList<T> extends ArrayList<T> {
    private boolean marked = false;

    public boolean isMarked() {
        return marked;
    }

    public void mark() {
        marked = true;
    }
}
