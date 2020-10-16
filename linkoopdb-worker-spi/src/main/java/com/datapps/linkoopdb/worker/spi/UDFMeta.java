package com.datapps.linkoopdb.worker.spi;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

public class UDFMeta implements Serializable {

    private static final long serialVersionUID = -2081442930283757780L;

    String user;
    Set<FunctionInfo> methods = new HashSet<>();
    String jarDir;

    public static UDFMeta toUdfMeta(String user, String jarDir) {
        UDFMeta info = new UDFMeta();
        info.user = user;
        info.jarDir = jarDir;
        return info;
    }

    public void addFunctionInfo(Set<Pair<String, Boolean>> set) {
        for (Pair<String, Boolean> p : set) {
            methods.add(FunctionInfo.toFuncInfo(p.getLeft(), p.getRight()));
        }
    }

    public void addFunctionInfo(FunctionInfo info) {
        methods.add(info);
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getJarDir() {
        return jarDir;
    }

    public void setJarDir(String jarDir) {
        this.jarDir = jarDir;
    }

    public Set<FunctionInfo> getMethods() {
        return methods;
    }

    public static class FunctionInfo implements Serializable {

        String fullMethod;
        boolean tableFunction;

        public static FunctionInfo toFuncInfo(String fullMethod, boolean tableFunction) {
            FunctionInfo info = new FunctionInfo();
            info.fullMethod = fullMethod;
            info.tableFunction = tableFunction;
            return info;
        }

        public String getFullMethod() {
            return fullMethod;
        }

        public void setFullMethod(String fullMethod) {
            this.fullMethod = fullMethod;
        }

        public boolean isTableFunction() {
            return tableFunction;
        }

        public void setTableFunction(boolean tableFunction) {
            this.tableFunction = tableFunction;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FunctionInfo that = (FunctionInfo) o;
            return tableFunction == that.tableFunction
                && Objects.equals(fullMethod, that.fullMethod);
        }

        @Override
        public int hashCode() {

            return Objects.hash(fullMethod, tableFunction);
        }
    }
}
