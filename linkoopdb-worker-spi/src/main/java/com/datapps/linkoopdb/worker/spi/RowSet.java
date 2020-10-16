/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spi;

import com.datapps.linkoopdb.jdbc.navigator.RowIterator;
import com.datapps.linkoopdb.jdbc.types.RowType;

public interface RowSet extends RowIterator {

    RowType getRowType();

    String[] getColumns();
}
