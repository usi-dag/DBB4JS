package SQL.queries;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ20 implements TestQ{
    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query20().execute(tpchStreamDB);
        var imperative = new Query20Imperative().execute(tpchStreamDB);
        var joinMapMulti = new Query20JoinMapMulti().execute(tpchStreamDB);
        var sqlGroupBy = new Query20SqlGroupBy().execute(tpchStreamDB);
        var sqlGroupByAndJoinMapMulti = new Query20SqlGroupByAndJoinMapMulti().execute(tpchStreamDB);

        int size = stream.size();
        assertEquals(size, imperative.size());
        assertEquals(size, joinMapMulti.size());
        assertEquals(size, sqlGroupBy.size());
        assertEquals(size, sqlGroupByAndJoinMapMulti.size());

        var s = stream.toString();
        assertEquals(s, imperative.toString());
        assertEquals(s, joinMapMulti.toString());
        assertEquals(s, sqlGroupBy.toString());
        assertEquals(s, sqlGroupByAndJoinMapMulti.toString());
    }
}
