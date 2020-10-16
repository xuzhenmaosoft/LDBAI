package com.datapps.linkoopdb.worker.spi.plan.expression;

import java.util.List;

import com.datapps.linkoopdb.jdbc.types.Type;

/**
 * Created by xzm on 2019/11/22.
 */
public class LdbPythonUdf extends RexCall {

    public String pythonMothedName;
    public  String pythonFullPath;
    public String functionName;
    public List<Type> parTypes;
    public Type returnType;

    public LdbPythonUdf(String functionName,String pythonFullPath,String  pythonMothedName, List<RexNode> operands, List<Type> parTypes, Type type) {
        super(SqlKind.PYTHONUDF, operands, type);
        this.functionName = functionName;
        this.pythonMothedName = pythonMothedName;
        this.parTypes = parTypes;
        this.pythonFullPath = pythonFullPath;
        this.returnType = type;
    }

    @Override
    public String simpleString() {
        return "PYTHONUDF: " + functionName + mkString(operands);
    }
}
