
package quickstart;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class StreamEventJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 设置作业并行度为4
        //TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE input (\n" +
                "    id Int\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE print_table (\n" +
                "                id Int,\n" +
                "                t TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "                'connector' = 'kafka',\n" +
                "                'topic' = 'stream-event',\n" +
                "                'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "                'properties.group.id' = 'testGroup',\n" +
                "                'scan.startup.mode' = 'earliest-offset',\n" +
                "                'format' = 'csv'\n" +
                "        )");

        tableEnv.executeSql(
                "INSERT INTO print_table SELECT id, now() FROM input");

    }
}
