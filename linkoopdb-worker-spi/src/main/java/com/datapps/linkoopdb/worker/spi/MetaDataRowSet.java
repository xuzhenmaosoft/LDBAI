package com.datapps.linkoopdb.worker.spi;

import java.util.List;

import com.google.gson.Gson;

import com.datapps.linkoopdb.jdbc.Row;
import com.datapps.linkoopdb.jdbc.types.RowType;
import com.datapps.linkoopdb.jdbc.types.Type;

/**
 * @author xingbu
 * @version 1.0
 *
 * created by　19-10-24 下午2:27
 */
public class MetaDataRowSet extends SimpleRowSet {

    public static final Type[] COL_TYPES = {Type.SQL_VARCHAR, Type.SQL_VARCHAR, Type.SQL_VARCHAR};
    public static final String[] COLUMNS = {"NAME", "VALUE", "DOC"};

    public MetaDataRowSet(Row[] rows) {
        super(
            new RowType(COL_TYPES),
            COLUMNS,
            rows
        );
    }

    public static MetaDataRowSet deserializeFromObjArray(String jsonData) {
        Object[] data = new Gson().fromJson(jsonData, Object[].class);
        Row[] rows = new Row[data.length];
        for (int i = 0; i < data.length; i++) {
            List datum = (List) data[i];
            Row row = new Row(datum.toArray(new Object[0]));
            rows[i] = row;
        }
        return new MetaDataRowSet(rows);
    }

    public static String serializeToObjArray(SimpleRowSet metaDataRowSet) {
        Row[] rows = metaDataRowSet.getRows();
        Object[] rowsSerialize = new Object[rows.length];
        for (int i = 0; i < rows.length; i++) {
            rowsSerialize[i] = rows[i].getData();
        }
        return new Gson().toJson(rowsSerialize);
    }

}
