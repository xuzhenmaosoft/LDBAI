package com.datapps.linkoopdb.worker.spark.ai.evaluator;

import java.util.ArrayList;
import java.util.List;

import com.datapps.linkoopdb.worker.spark.ai.core.AiEvaluator;
import com.datapps.linkoopdb.worker.spark.ai.core.SparkFunctionContext;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.regression.GeneralizedLinearRegressionModel;
import org.apache.spark.ml.regression.GeneralizedLinearRegressionSummary;
import org.apache.spark.ml.util.DatasetUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class GlrEvaluator implements AiEvaluator {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"glr_statistic"};
    }

    @Override
    public LogicalPlan execute(SparkFunctionContext context) throws Exception {
        String method = context.getFunctionName();
        if (method.equalsIgnoreCase("glr_statistic")) {
            return glrStatistic(context);
        }
        return null;
    }

    private LogicalPlan glrStatistic(SparkFunctionContext context) {
        String[] parameters = context.getParameters();
        Object[] arguments = context.getArguments();
        ModelLogicPlan modelLogicPlan = (ModelLogicPlan) arguments[0];
        GeneralizedLinearRegressionModel model = (GeneralizedLinearRegressionModel) modelLogicPlan
            .getModel();
        Dataset dataset = Dataset.ofRows(context.getSparkSession(), (LogicalPlan) arguments[1]);
        dataset = dataset.withColumn("FEATURES", DatasetUtils.columnToVector(dataset, "FEATURES"));
        String param = arguments[2].toString();
        GeneralizedLinearRegressionSummary summary = model.evaluate(dataset);
        Object values = null;
        List<Row> rows = new ArrayList<>();
        StructType schema = new StructType(new StructField[]{
            new StructField("statistic", DataTypes.DoubleType, true, Metadata.empty())
        });
        Dataset<Row> dataFrame = null;
        //如果为true，则直接返回dataset
        boolean isreturn = false;
        if (param.equalsIgnoreCase("workingResiduals")) {
            dataFrame = summary.workingResiduals();
            isreturn = true;
        } else if (param.equalsIgnoreCase("devianceResiduals")) {
            dataFrame = summary.devianceResiduals();
            isreturn = true;
        } else if (param.equalsIgnoreCase("pearsonResiduals")) {
            dataFrame = summary.pearsonResiduals();
            isreturn = true;
        } else if (param.equalsIgnoreCase("responseResiduals")) {
            dataFrame = summary.responseResiduals();
            isreturn = true;
        } else if (param.equalsIgnoreCase("deviance")) {
            values = summary.deviance();
            rows.add(RowFactory.create((double) values));
        } else if (param.equalsIgnoreCase("dispersion")) {
            values = summary.dispersion();
            rows.add(RowFactory.create((double) values));
        } else if (param.equalsIgnoreCase("aic")) {
            values = summary.aic();
            rows.add(RowFactory.create((double) values));
        } else if (param.equalsIgnoreCase("degreesOfFreedom")) {
            values = summary.degreesOfFreedom();
            rows.add(RowFactory.create(Double.parseDouble(values.toString())));
        } else if (param.equalsIgnoreCase("residualDegreeOfFreedomNull")) {
            values = summary.residualDegreeOfFreedomNull();
            rows.add(RowFactory.create(Double.parseDouble(values.toString())));
        } else if (param.equalsIgnoreCase("nullDeviance")) {
            values = summary.nullDeviance();
            rows.add(RowFactory.create((double) values));
        }
        if (!isreturn) {
            dataFrame = context.getSparkSession()
                .createDataFrame(rows, schema);
        } else {
            dataFrame = dataFrame.withColumnRenamed(dataFrame.columns()[0], "statistic");
        }
        return dataFrame.logicalPlan();
    }


}
