package SQL.queries;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ14 implements TestQ {

    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query14().execute(tpchStreamDB);
        var imperative = new Query14Imperative().execute(tpchStreamDB);
        var joinMapMulti = new Query14JoinMapMulti().execute(tpchStreamDB);
        var singleFilter = new Query14SingleFilter().execute(tpchStreamDB);
        var singleFilterJoinMapMulti = new Query14SingleFilterAndJoinMapMulti().execute(tpchStreamDB);


        // check size
        int size = stream.size();
        assertEquals(size, imperative.size());
        assertEquals(size, joinMapMulti.size());
        assertEquals(size, singleFilter.size());
        assertEquals(size, singleFilterJoinMapMulti.size());

        var s = stream.toString();
        assertEquals(s, imperative.toString());
        assertEquals(s, joinMapMulti.toString());
        assertEquals(s, singleFilter.toString());
        assertEquals(s, singleFilterJoinMapMulti.toString());
        // check entries

    }
}
