package SQL.queries;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ21 implements TestQ {

    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query21().execute(tpchStreamDB);
        var imperative = new Query21Imperative().execute(tpchStreamDB);
        var sqlGroupBy = new Query21SqlGroupBy().execute(tpchStreamDB);
        var joinMapMulti = new Query21JoinMapMulti().execute(tpchStreamDB);
        var sqlGroupByJoinMapMulti = new Query21SqlGroupByAndJoinMapMulti().execute(tpchStreamDB);

        int size = stream.size();
        assertEquals(size, imperative.size());
        assertEquals(size, sqlGroupBy.size());
        assertEquals(size, joinMapMulti.size());
        assertEquals(size, sqlGroupByJoinMapMulti.size());

        var s = stream.toString();
        assertEquals(s, imperative.toString());
        assertEquals(s, sqlGroupBy.toString());
        assertEquals(s, joinMapMulti.toString());
        assertEquals(s, sqlGroupByJoinMapMulti.toString());
    }
}
