package org.apache.phoenix.propagatetrace;


import java.sql.Timestamp;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.schema.PRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RequestIdPropagationPhoenix {
    final static Logger logger = LoggerFactory.getLogger(RequestIdPropagationPhoenix.class);
    private static long counter=-1;

    private static synchronized long getAndSetCounter(){
        counter++;
        if(counter<0)counter=0;
        return counter;
    }

    private static String createRequestId(PhoenixStatement stmt){
        String client;
        PhoenixConnection conn = stmt.getConnection();
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        if(stmt.getConnection().getTenantId()==null){
            client = conn.getQueryServices().getUser().getName();
        }
        else{
            client = conn.getTenantId().toString();
        }
        client=client+Long.toString(timestamp.getTime())+Long.toString(getAndSetCounter());
        return client;
    }

    public static void setInitialRequestId(PhoenixStatement stmt){
        stmt.setRequestId(createRequestId(stmt));
        logRequestIdAssigned(stmt);
    }

    public static void propagateRequestId(PhoenixStatement src, MutationState.RowMutationState dest){
        dest.setRequestId(src.getRequestId());
    }
    public static void propagateRequestId(MutationState.RowMutationState src, PRow dest){
        dest.setRequestId((src.getRequestId()));
    }
    public static void propagateRequestId(PRow src, List<Mutation>dest){
        for(int it=0;it<dest.size();it++){
            dest.get(it).setId(src.getRequestId());
        }
    }
    public static void propagateRequestId(PhoenixStatement src, Scan dest){
        dest.setId(src.getRequestId());
    }

    public static String extractRequestId(PhoenixStatement stmt){
        return stmt.getRequestId();
    }

    public static void logRequestIdAssigned(PhoenixStatement stmt){
        System.out.println("attached "+ RequestIdPropagationPhoenix.extractRequestId(stmt)+" to "+stmt.toString());
        logger.info("attached traceId {}. to query-statement {}.", RequestIdPropagationPhoenix.extractRequestId(stmt),stmt);
    }

    public static void logRequestIdAssigned(Mutation mutation){
        System.out.println("mutation corresponding to request id"+mutation.getId()+"propagated from Phoenix to Hbase side");
        logger.info("mutation corresponding to request id {}. propagated from Phoenix to HBase side",mutation.getId());
    }

    public static  void logRequestIdAssigned(List<Mutation>batch){
        for(int it=0;it<batch.size();it++){
            logRequestIdAssigned(batch.get(it));
        }
    }

    public static void logEndOfSelect(QueryPlan plan){
        if(plan.getContext().getScan().getId()!="" && plan.getContext().getScan().getId()!=null) {
            System.out.println("requestId " + plan.getContext().getScan().getId() + " propagated futher from Phoenix to HBase side for table " + plan.getTableRef()
                .getTable().getTableName());
            logger.debug("requestId {}. propagated futher from Phoenix to HBase side for table {}.",
                plan.getContext().getScan().getId(),plan.getTableRef().getTable().getTableName());
        }
    }

}
