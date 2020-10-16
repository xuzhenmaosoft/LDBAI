/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */

package com.datapps.linkoopdb.worker.spi;

import java.io.Serializable;

/**
 * @auther webber
 * @date 2019/8/8 11:22
 */
public class PallasDmlSymbol implements Serializable {

    private static final long serialVersionUID = 5308915112153042408L;
    // for pallas load-into
    public boolean isLoad = false;
    // for pallas delete or update in merge-into dml
    public boolean isDeleteOrUpdateInMerge = false;
    // for delete columns contain origin pk column, true if it does, and false if it does not
    public boolean isUpdatePk = false;

    public PallasDmlSymbol() {

    }

    public PallasDmlSymbol setDeleteOrUpdateInMerge(boolean deleteOrUpdateInMerge) {
        this.isDeleteOrUpdateInMerge = deleteOrUpdateInMerge;
        return this;
    }
}
