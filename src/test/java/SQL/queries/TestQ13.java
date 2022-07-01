package SQL.queries;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ13 implements TestQ{

    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query13().execute(tpchStreamDB);
        var streamMapMulti = new Query13MapMulti().execute(tpchStreamDB);
        var streamSqlGroupBy = new Query13SqlGroupBy().execute(tpchStreamDB);
        var streamMapMultiSqlGroupBy = new Query13MapMultiSqlGroupBy().execute(tpchStreamDB);
        var imperative = new Query13Imperative().execute(tpchStreamDB);

        var s = stream.toString();
        assertEquals(s, streamMapMulti.toString());
        assertEquals(s, streamSqlGroupBy.toString());
        assertEquals(s, streamMapMultiSqlGroupBy.toString());
        assertEquals(s, imperative.toString());
    }
}
