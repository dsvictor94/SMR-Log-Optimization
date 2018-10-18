package ch.usi.da.smr.statetransfer;

import java.util.Arrays;
import java.util.List;

import ch.usi.da.smr.PartitionManager;
import ch.usi.da.smr.log.LoggerInterface;
import ch.usi.da.smr.message.Command;
import ch.usi.da.smr.message.Message;

class StateTransferDummy implements StateTransferInterface {
    // restaura de si mesmo
    private LoggerInterface logger = null;

    @Override
    public void init(LoggerInterface logger, PartitionManager partitions, String token) {
        this.logger = logger;
    }

    @Override
    public Message restore(int ring, long from) {
        return logger.retrive(ring, from);
    }

    @Override
    public void close() {
    }
}