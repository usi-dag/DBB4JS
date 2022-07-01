package SQL.queries;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ4 implements TestQ{

    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query4().execute(tpchStreamDB);
        var imperative = new Query4Imperative().execute(tpchStreamDB);
        var partialResult = new Query4SqlGroupBy().execute(tpchStreamDB);

        int size = stream.size();
        // check size
        assertEquals(size, imperative.size());
        assertEquals(size, partialResult.size());

        // check entries
        var s = stream.toString();
        assertEquals(s, imperative.toString());
        assertEquals(s, partialResult.toString());

    }
}
