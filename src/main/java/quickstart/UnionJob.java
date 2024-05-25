
package quickstart;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UnionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 设置作业并行度为4
        //TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE stream_table (\n" +
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

        tableEnv.executeSql("CREATE TABLE batch_table (\n" +
                "                id Int,\n" +
                "                t TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "                'connector' = 'kafka',\n" +
                "                'topic' = 'batch-event',\n" +
                "                'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "                'properties.group.id' = 'testGroup',\n" +
                "                'scan.startup.mode' = 'earliest-offset',\n" +
                "                'format' = 'csv'\n" +
                "        )");

        tableEnv.executeSql("Create view union_table as\n" +
                "                select 'batch' as s ,id,t from batch_table\n" +
                "                union \n" +
                "                        select 'stream' as s,id,t from stream_table");

       tableEnv.executeSql("CREATE TABLE print_table (\n" +
                "  s  String,\n" +
                "  id Int,\n" +
                "  t TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        tableEnv.executeSql(
                "INSERT INTO print_table SELECT s ,id, t FROM union_table");

    }
}
