package SQL.sql_utils;

public final class StringMultiContains {

    private final String[] strings;

    private final int totalLen;

    public StringMultiContains(String... strings) {
        this.strings = strings;
        int len = 0;
        for (String s : strings) len += s.length();
        totalLen = len;
    }

    public boolean match(String input) {
        return input.length() >= totalLen && runBooleanWithString(input);
    }

    public boolean runBooleanWithString(String string) {
        int start = 0;
        for(String sub : strings) {
            int find = string.indexOf(sub, start);
            if(find == -1) {
                return false;
            }
            start = find + sub.length();
        }
        return true;
    }
}