/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */

package com.datapps.linkoopdb.worker.spi;


//todo: use annation & function parameter mapping to reduce type mismatch
public interface LdbTableFunction {

    String version();

    String[] methods();

    //todo: define context class
    Object invoke(Object context) throws Exception;
}
