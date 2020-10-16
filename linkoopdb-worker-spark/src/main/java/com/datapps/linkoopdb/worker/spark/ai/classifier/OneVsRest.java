/**
 * This file is part of linkoopdb. <p> Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spark.ai.classifier;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import com.google.gson.Gson;

import com.datapps.linkoopdb.worker.spark.ai.core.SparkMLModelBase;
import com.datapps.linkoopdb.worker.spark.ai.core.model.ModelLogicPlan;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.Classifier;
import org.apache.spark.ml.classification.OneVsRestModel;
import org.apache.spark.sql.SparkSession;

public class OneVsRest extends SparkMLModelBase {

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String[] methods() {
        return new String[]{"onevsrest_train", "onevsrest_predict"};
    }


    @Override
    public ModelLogicPlan load(SparkSession session, String alias, String path) {
        return new ModelLogicPlan(session.read().parquet(path).alias(alias).logicalPlan(),
            OneVsRestModel.load(path));
    }


    public Classifier getClassifier(HashMap<String, Object> paraMap) throws Exception {
        String classifierStr = paraMap.get("classifier").toString();
        Class<?> aClass = Class.forName(classifierStr);
        Classifier classifier = (Classifier) aClass.newInstance();
        Set<Entry<String, Object>> entries = paraMap.entrySet();
        Iterator<Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Entry<String, Object> nextVal = iterator.next();
            String key = nextVal.getKey();
            Object value = nextVal.getValue();
            if (key.equalsIgnoreCase("classifier")) {
                continue;
            } else {
                boolean hasParam = classifier.hasParam(key);
                if (hasParam) {
                    if (value instanceof String) {
                        classifier.set(key, value);
                    } else if (value instanceof String[]) {
                        classifier.set(key, value.toString());
                    } else {
                        classifier.set(key, classifier.getParam(key).jsonDecode(value.toString()));

                    }
                }
            }
        }
        return classifier;
    }

    @Override
    public void setFittingParams(Object[] arguments, String[] parameters, PipelineStage pipelineStage) {
        try {
            for (int i = 1; i < parameters.length; i++) {
                if (arguments[i] != null && pipelineStage.hasParam(parameters[i])) {
                    String classiferPara = parameters[i];
                    if (classiferPara.equalsIgnoreCase("classifier")) {
                        String classifierValue = arguments[i].toString();
                        Gson gson = new Gson();
                        HashMap paraMap = gson.fromJson(classifierValue, HashMap.class);

                        Classifier classifier = getClassifier(paraMap);

                        pipelineStage.set(parameters[i], classifier);
                        continue;
                    }
                    pipelineStage.set(parameters[i], arguments[i]);
                }
            }
        } catch (Exception e) {
            // do nothing
        }
    }

    @Override
    public PipelineStage createSimplePipelineStage() {
        return new org.apache.spark.ml.classification.OneVsRest()
            .setFeaturesCol("FEATURES").setLabelCol("LABEL").setPredictionCol("PREDICTION");
    }
}
