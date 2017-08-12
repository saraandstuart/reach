package com.shannoncode.reach;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.jdbc.bolt.JdbcLookupBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.jdbc.mapper.JdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.testing.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

/**
 * @author Stuart Shannon
 */
public class TopologyExampleIT
{
    private static final String USER_SPOUT = "user-spout";
    private static final String LOOKUP_BOLT = "lookup-bolt";

    private static final int NUMBER_OF_SUPERVISORS = 2;

    protected static final String TABLE_NAME = "user";
    protected static final String JDBC_CONF = "jdbc.conf";

    private static final String SELECT_QUERY = "select dept_name from department, user_department "
                                               + "where department.dept_id = user_department.dept_id"
                                               + " and user_department.user_id = ?";

    private static final List<String> SETUP_SQL = Lists.newArrayList
            (
                "drop table if exists user",
                "drop table if exists department",
                "drop table if exists user_department",
                "create table if not exists user (user_id integer, user_name varchar(100), dept_name varchar(100), create_date date)",
                "create table if not exists department (dept_id integer, dept_name varchar(100))",
                "create table if not exists user_department (user_id integer, dept_id integer)",
                "insert into department values (1, 'R&D')",
                "insert into department values (2, 'Finance')",
                "insert into department values (3, 'HR')",
                "insert into department values (4, 'Sales')",
                "insert into user_department values (1, 1)",
                "insert into user_department values (2, 2)",
                "insert into user_department values (3, 3)",
                "insert into user_department values (4, 4)"
            );

    private ConnectionProvider connectionProvider;
    private JdbcLookupMapper jdbcLookupMapper;


    @Before
    public void setup()
    {
        Map map = Maps.newHashMap();
        map.put("dataSourceClassName","org.hsqldb.jdbc.JDBCDataSource");
        map.put("dataSource.url", "jdbc:hsqldb:mem:test");
        map.put("dataSource.user","SA");
        map.put("dataSource.password","");
        connectionProvider = new HikariCPConnectionProvider(map);
        connectionProvider.prepare();

        final int queryTimeoutSecs = 60;
        JdbcClient jdbcClient = new JdbcClient(connectionProvider, queryTimeoutSecs);
        for (String sql : SETUP_SQL) {
            jdbcClient.executeSql(sql);
        }

        Fields outputFields = new Fields("user_id", "user_name", "dept_name", "create_date");
        List<Column> queryParamColumns = Lists.newArrayList(new Column("user_id", Types.INTEGER));
        jdbcLookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);

    }

    @Test
    public void topologyFlow()
    {
        // given
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(NUMBER_OF_SUPERVISORS);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);


        final int userIdPeter = 1;
        final int userIdBob = 2;
        final int userIdAlice = 3;

        final long createDatePeter = 1502524800000L;
        final long createDateBob = 1502528400000L;
        final long createDateAlice = 1502532000000L;

        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {

            public void run(ILocalCluster cluster) throws IOException {
                TopologyBuilder builder = new TopologyBuilder();

                builder.setSpout(USER_SPOUT, new FeederSpout(new Fields("user_id", "user_name", "create_date")));

                builder.setBolt(LOOKUP_BOLT, new JdbcLookupBolt(connectionProvider, SELECT_QUERY, jdbcLookupMapper))
                        .shuffleGrouping(USER_SPOUT);

                StormTopology topology = builder.createTopology();

                MockedSources mockedSources = new MockedSources();

                mockedSources.addMockData(USER_SPOUT,
                        new Values(userIdPeter, "peter", createDatePeter),
                        new Values(userIdBob, "bob", createDateBob),
                        new Values(userIdAlice, "alice", createDateAlice));

                Config conf = new Config();
                conf.setNumWorkers(2);

                CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                completeTopologyParam.setMockedSources(mockedSources);
                completeTopologyParam.setStormConf(conf);

                Map result = Testing.completeTopology(cluster, topology, completeTopologyParam);

                List<Object> expectedSpoutTuples = new Values(
                        new Values(userIdPeter, "peter", createDatePeter),
                        new Values(userIdBob, "bob", createDateBob),
                        new Values(userIdAlice, "alice", createDateAlice));

                List<Object> actualSpoutTuples = Testing.readTuples(result, USER_SPOUT);

                List<Object> expectedBoltTuples = new Values(
                        new Values(userIdPeter, "peter", "R&D", createDatePeter),
                        new Values(userIdBob, "bob", "Finance", createDateBob),
                        new Values(userIdAlice, "alice", "HR", createDateAlice));

                List<Object> actualBoltTuples = Testing.readTuples(result, LOOKUP_BOLT);


                Assert.assertTrue(Testing.multiseteq(expectedSpoutTuples, actualSpoutTuples));

                Assert.assertTrue(Testing.multiseteq(expectedBoltTuples, actualBoltTuples));
            }
        });

    }



}
