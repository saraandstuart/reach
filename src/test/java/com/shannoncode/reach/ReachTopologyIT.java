package com.shannoncode.reach;

import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.Testing;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.jdbc.bolt.JdbcLookupBolt;
import org.apache.storm.testing.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 * @author Stuart Shannon
 */
public class ReachTopologyIT
{
    private static final int NUMBER_OF_SUPERVISORS = 2;

    private static final String URL = "foo.com/blog/1";

    @Test
    public void topologyFlow()
    {
        //given
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(NUMBER_OF_SUPERVISORS);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);


        //when
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            public void run(ILocalCluster cluster) throws IOException
            {
                LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("reach");
                builder.addBolt(new ReachTopology.GetTweeters(), 4);
                builder.addBolt(new ReachTopology.GetFollowers(), 12)
                        .shuffleGrouping();
                builder.addBolt(new ReachTopology.PartialUniquer(), 6)
                        .fieldsGrouping(new Fields("id", "follower"));
                builder.addBolt(new ReachTopology.CountAggregator(), 3)
                        .fieldsGrouping(new Fields("id"));

                LocalDRPC drpc = new LocalDRPC();

                StormTopology topology = builder.createLocalTopology(drpc);

                MockedSources mockedSources = new MockedSources();
//                mockedSources.addMockData(USER_SPOUT, expectedSpoutValues);

                Config conf = new Config();
                conf.setNumWorkers(2);

                CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                completeTopologyParam.setMockedSources(mockedSources);
                completeTopologyParam.setStormConf(conf);

                Map result = Testing.completeTopology(cluster, topology, completeTopologyParam);

                //then

                drpc.shutdown();
            }
        });










    }
}
