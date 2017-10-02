package com.sdu.spark.scheduler;

import com.sdu.spark.rpc.RpcCallContext;
import com.sdu.spark.rpc.RpcEndPoint;
import com.sdu.spark.rpc.RpcEnv;
import com.sdu.spark.scheduler.OutputCommitCoordinationMessage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hanhan.zhang
 * */
public class OutputCommitCoordinatorEndPoint extends RpcEndPoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutputCommitCoordinatorEndPoint.class);

    private OutputCommitCoordinator outputCommitCoordinator;

    public OutputCommitCoordinatorEndPoint(RpcEnv rpcEnv, OutputCommitCoordinator outputCommitCoordinator) {
        super(rpcEnv);
        this.outputCommitCoordinator = outputCommitCoordinator;
    }

    @Override
    public void receive(Object msg) {
        if (msg instanceof StopCoordinator) {
            LOGGER.info("OutputCommitCoordinator stopped!");
            stop();
        }
    }

    @Override
    public void receiveAndReply(Object msg, RpcCallContext context) {
        if (msg instanceof AskPermissionToCommitOutput) {
            AskPermissionToCommitOutput toCommitOutput = (AskPermissionToCommitOutput) msg;
            context.reply(outputCommitCoordinator.handleAskPermissionToCommit(
                    toCommitOutput.stageId,
                    toCommitOutput.partition,
                    toCommitOutput.attemptNumber
            ));
        }
    }
}
