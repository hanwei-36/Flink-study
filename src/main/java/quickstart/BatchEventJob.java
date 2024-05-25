
package quickstart;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class BatchEventJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE input (\n" +
                "    id Int\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'number-of-rows' =  '500000',\n" +
                "  'rows-per-second' = '2'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE print_table (\n" +
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

        tableEnv.executeSql(
                "INSERT INTO print_table SELECT id, now() FROM input");

    }
}
