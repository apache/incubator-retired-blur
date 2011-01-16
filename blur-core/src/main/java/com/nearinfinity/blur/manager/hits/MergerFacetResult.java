package com.nearinfinity.blur.manager.hits;

import com.nearinfinity.blur.thrift.generated.FacetResult;
import com.nearinfinity.blur.utils.BlurExecutorCompletionService;
import com.nearinfinity.blur.utils.ForkJoin.Merger;

public class MergerFacetResult implements Merger<FacetResult> {

    @Override
    public FacetResult merge(BlurExecutorCompletionService<FacetResult> service) throws Exception {
        return null;
    }

}
