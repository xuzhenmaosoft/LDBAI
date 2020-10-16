/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */

package com.datapps.linkoopdb.worker.spi;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionUtils {

    static ThrowException<RuntimeException> throwAny = (ThrowException) new ThrowAny();

    public static void throwException(Throwable e) {
        throwAny.throwException(e);
    }

    interface ThrowException<E extends Throwable> {

        void throwException(Throwable e) throws E;
    }

    public static String exceptionToString(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);
        t.printStackTrace(pw);
        pw.flush();
        sw.flush();
        return sw.toString();
    }

    public static class ThrowAny implements ThrowException<Throwable> {

        @Override
        public void throwException(Throwable e) throws Throwable {
            throw e;
        }
    }
}
