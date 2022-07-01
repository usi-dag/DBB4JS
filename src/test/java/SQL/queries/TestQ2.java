package SQL.queries;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ2 implements TestQ {

    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query2().execute(tpchStreamDB);
        var imperative = new Query2Imperative().execute(tpchStreamDB);
        var joinMultiMap = new Query2JoinMapMulti().execute(tpchStreamDB);
        var singleFilter = new Query2SingleFilter().execute(tpchStreamDB);
        var singleFilterJoinMapMulti = new Query2SingleFilterAndJoinMapMulti().execute(tpchStreamDB);

        var s = stream.toString();
        assertEquals(s, imperative.toString());
        assertEquals(s, joinMultiMap.toString());
        assertEquals(s, singleFilter.toString());
        assertEquals(s, singleFilterJoinMapMulti.toString());
    }
}
