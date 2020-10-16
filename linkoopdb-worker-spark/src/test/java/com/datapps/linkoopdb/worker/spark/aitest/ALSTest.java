package com.datapps.linkoopdb.worker.spark.aitest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datapps.linkoopdb.worker.spark.ai.Als;
import com.datapps.linkoopdb.worker.spark.ai.core.SparkFunctionContext;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import com.datapps.linkoopdb.worker.spi.plan.expression.SqlKind;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ALSTest {


    private Map ldbFunctionMap = new HashMap();
    private SqlKind alsTrain = SqlKind.ALS_TRAIN;
    private Object[] args = new Object[10];
    private String[] paras = new String[10];
    private SparkFunctionContext functionContext;
    private SparkSession sparkSession ;
    private ModelLogicPlan logicalPlan;
    private Als als;
    private Dataset<Row> testDataset;

    @Before
    public void init() throws Exception{
        SparkConf conf = new SparkConf().set("spark.sql.warehouse.dir", "d://spark-warehouse-dir");
        sparkSession = SparkSession.builder().config(conf).appName("alsTrain").master("local").getOrCreate();

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1, 2, 3.0f));
        rows.add(RowFactory.create(1, 2, 3.0f));
        rows.add(RowFactory.create(1, 2, 3.0f));
        rows.add(RowFactory.create(1, 2, 3.0f));

        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(new StructField("USERID", DataTypes.IntegerType, false, Metadata.empty()));
        structFields.add(new StructField("ITEMID", DataTypes.IntegerType, false, Metadata.empty()));
        structFields.add(new StructField("RATING", DataTypes.FloatType, false, Metadata.empty()));
        StructType schema = new StructType(structFields.toArray(new StructField[structFields.size()]));

        testDataset = sparkSession.createDataFrame(rows, schema);

        args[0] = testDataset.logicalPlan();
        args[1] = 10;
        args[2] = 0.1;
        args[3] = 10;
        args[4] = 10;
        args[5] = 10;
        args[6] = false;
        args[7] = 1.0;
        args[8] = false;

        paras[0] = "trainning_table";
        paras[1] = "maxIter";
        paras[2] = "regParam";
        paras[3] = "rank";
        paras[4] = "numUserBlocks";
        paras[5] = "numItemBlocks";
        paras[6] = "implicitPrefs";
        paras[7] = "alpha";
        paras[8] = "nonnegative";


        functionContext = new SparkFunctionContext(ldbFunctionMap, alsTrain,
            paras,args, this.sparkSession);

        als = new Als();
        logicalPlan = (ModelLogicPlan)als.execute(functionContext);


    }

    @Ignore
    public void testCreateModel() throws Exception{

    }

    @Test
    public void testDeployModel() throws Exception{
        args[0] = logicalPlan;
        args[1] = testDataset.logicalPlan();
        args[2] = 2;

        paras[0] = "model";
        paras[1] = "data";
        paras[2] = "num";

        functionContext = new SparkFunctionContext(ldbFunctionMap,alsTrain,
            paras,args,sparkSession);
        LogicalPlan recommend = als.recommend(functionContext);
        Dataset<Row> dataSet = Dataset.ofRows(sparkSession, recommend);
        List<Row> rows = dataSet.collectAsList();
        assert rows.size() != 0;
    }

}
