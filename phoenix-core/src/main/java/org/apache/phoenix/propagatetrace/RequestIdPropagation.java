package org.apache.phoenix.propagatetrace;


import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.schema.PRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

public class RequestIdPropagation {
    final static Logger logger = LoggerFactory.getLogger(RequestIdPropagation.class);

    public static void setInitialRequestId(PhoenixStatement stmt){
        Random rn = new Random();
        stmt.setRequestId(rn.nextInt());
        logRequestIdAssigned(stmt);
    }
    public static void propagateRequestId(PhoenixStatement src, MutationState.RowMutationState dest){
        dest.setRequestId(src.getRequestId());
        logRequestIdAssigned(dest);
    }
    public static void propagateRequestId(MutationState.RowMutationState src, PRow dest){
        dest.setRequestId((src.getRequestId()));
        logRequestIdAssigned(dest);
    }
    public static void propagateRequestId(PRow src, List<Mutation>dest){
        for(int it=0;it<dest.size();it++){
            dest.get(it).setId(Integer.toString(src.getRequestId()));
        }
    }

    public static int extractRequestId(PhoenixStatement stmt){
        return stmt.getRequestId();
    }

    public static int extractRequestId(MutationState.RowMutationState rowMutationState){
        return rowMutationState.getRequestId();
    }

    public static int extractRequestId(PRow row){
        return row.getRequestId();
    }

    public static void logRequestIdAssigned(PhoenixStatement stmt){
        logger.info("attached traceId {}. to query-statement {}.", RequestIdPropagation.extractRequestId(stmt),stmt);
    }

    public static void logRequestIdAssigned(MutationState.RowMutationState rowMutationState){
        logger.debug("attached traceId {}. to RowMutationState {}.", extractRequestId(rowMutationState),rowMutationState);
    }

    public static void logRequestIdAssigned(PRow row){
        logger.debug("attached traceId {}. to PRow Object ", extractRequestId(row),row);
    }

    public static void logRequestIdAssigned(Mutation mutation){
        logger.info("mutation {}. attached to mutation id {}.",mutation,mutation.getId());
    }

    public static  void logRequestIdAssigned(List<Mutation>batch){
        for(int it=0;it<batch.size();it++){
            logRequestIdAssigned(batch.get(it));
        }
    }
}
