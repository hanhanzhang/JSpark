package com.sdu.spark.launcher;

import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author hanhan.zhang
 * */
public class CommandBuilderUtils {

    static String join(String sep, String... elements) {
        return join(sep, Arrays.asList(elements));
    }

    static String join(String sep, List<String> elements) {
        StringBuilder sb = new StringBuilder();
        elements.forEach(e -> {
            if (e != null) {
                if (sb.length() > 0) {
                    sb.append(sep);
                }
                sb.append(e);
            }
        });
        return sb.toString();
    }

    static String firstNonEmpty(String... candidates) {
        for (String s : candidates) {
            if (!Strings.isNullOrEmpty(s)) {
                return s;
            }
        }
        return null;
    }


    /**
     * Parse a string as if it were a list of arguments, following bash semantics.
     * For example:
     *
     * Input: "\"ab cd\" efgh 'i \" j'"
     * Output: [ "ab cd", "efgh", "i \" j" ]
     */
    static List<String> parseOptionString(String s) {
        List<String> opts = new ArrayList<>();
        StringBuilder opt = new StringBuilder();
        boolean inOpt = false;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean escapeNext = false;

        // This is needed to detect when a quoted empty string is used as an argument ("" or '').
        boolean hasData = false;

        for (int i = 0; i < s.length(); i++) {
            int c = s.codePointAt(i);
            if (escapeNext) {
                opt.appendCodePoint(c);
                escapeNext = false;
            } else if (inOpt) {
                switch (c) {
                    case '\\':
                        if (inSingleQuote) {
                            opt.appendCodePoint(c);
                        } else {
                            escapeNext = true;
                        }
                        break;
                    case '\'':
                        if (inDoubleQuote) {
                            opt.appendCodePoint(c);
                        } else {
                            inSingleQuote = !inSingleQuote;
                        }
                        break;
                    case '"':
                        if (inSingleQuote) {
                            opt.appendCodePoint(c);
                        } else {
                            inDoubleQuote = !inDoubleQuote;
                        }
                        break;
                    default:
                        if (!Character.isWhitespace(c) || inSingleQuote || inDoubleQuote) {
                            opt.appendCodePoint(c);
                        } else {
                            opts.add(opt.toString());
                            opt.setLength(0);
                            inOpt = false;
                            hasData = false;
                        }
                }
            } else {
                switch (c) {
                    case '\'':
                        inSingleQuote = true;
                        inOpt = true;
                        hasData = true;
                        break;
                    case '"':
                        inDoubleQuote = true;
                        inOpt = true;
                        hasData = true;
                        break;
                    case '\\':
                        escapeNext = true;
                        inOpt = true;
                        hasData = true;
                        break;
                    default:
                        if (!Character.isWhitespace(c)) {
                            inOpt = true;
                            hasData = true;
                            opt.appendCodePoint(c);
                        }
                }
            }
        }

        checkArgument(!inSingleQuote && !inDoubleQuote && !escapeNext, "Invalid option string: %s", s);
        if (hasData) {
            opts.add(opt.toString());
        }
        return opts;
    }
}
