import com.berry.clickhouse.tcp.client.ClickHouseClient;
import com.berry.clickhouse.tcp.client.data.Block;
import com.berry.clickhouse.tcp.client.data.IColumn;
import com.berry.clickhouse.tcp.client.settings.ClickHouseConfig;
import com.berry.clickhouse.tcp.client.util.ByteConverter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;

public class ClickhouseClientInsertTest {

    public static void main(String[] args) throws Exception {

        ClickHouseConfig clickHouseConfig = ClickHouseConfig.Builder.builder()
                .host("127.0.0.1")
                .port(9000)
                .database("berry")
                .user("default")
                .password("honey")
                .connectionPoolTotal(20)
                .connectionPoolMaxIdle(10)
                .connectionPooMinIdle(20)
                .connectTimeout(Duration.ofSeconds(60))
                .queryTimeout(Duration.ofSeconds(60))
                .charset(StandardCharsets.UTF_8)
                .build();

        ClickHouseClient client = new ClickHouseClient.Builder()
                .clickHouseConfig(clickHouseConfig).build();
        //创建Block
        Block block = client.createBlock("all_data_types_v1");
        //获取id的Column并写入数据
        IColumn column = block.getColumn("id");
        byte[] bytes = ByteConverter.intToByteArray(1, true);
        column.writeInt(bytes, 0, bytes.length);
        bytes = ByteConverter.intToByteArray(2, true);
        column.writeInt(bytes, 0, bytes.length);

        //获取name的Column并写入数据
        column = block.getColumn("name");
        bytes = "Tom".getBytes(StandardCharsets.UTF_8);
        column.write(bytes, 0, bytes.length);
        bytes = "Jerry".getBytes(StandardCharsets.UTF_8);
        column.write(bytes, 0, bytes.length);

        //获取age的Column并写入数据
        column = block.getColumn("age");
        bytes = ByteConverter.intToByteArray(2, true);
        column.writeInt(bytes, 0, bytes.length);
        bytes = ByteConverter.intToByteArray(3, true);
        column.writeInt(bytes, 0, bytes.length);

        //获取is_active的Column并写入数据
        column = block.getColumn("salary");
        column.write(1.09F);
        column.write(2.09F);

        //获取is_active的Column并写入数据
        column = block.getColumn("is_active");
        bytes = ByteConverter.intToByteArray(1, true);
        column.writeInt(bytes, 0, bytes.length);
        bytes = ByteConverter.intToByteArray(0, true);
        column.writeInt(bytes, 0, bytes.length);

        //获取created_at的Column并写入数据
        column = block.getColumn("created_at");
        bytes = ByteConverter.intToByteArray((int) (System.currentTimeMillis() / 1000L), true);
        column.writeInt(bytes, 0, bytes.length);
        bytes = ByteConverter.intToByteArray((int) (System.currentTimeMillis() / 1000L), true);
        column.writeInt(bytes, 0, bytes.length);

        //获取created_date的Column并写入数据
        column = block.getColumn("created_date");
        column.write(LocalDate.now());
        column.write(LocalDate.now());

        //获取created_time的Column并写入数据
        column = block.getColumn("created_time");
        column.write(System.currentTimeMillis());
        column.write(System.currentTimeMillis());

        //获取data的Column并写入数据
        column = block.getColumn("data");
        column.write(new int[]{1, 2, 3});
        column.write(new int[]{4, 5, 6});

        //获取map_data的Column并写入数据
        column = block.getColumn("map_data");
        column.write(Map.of("fish", 1, "apple", 2));
        column.write(Map.of("cheese", 3, "banana", 4));

        //获取tuple_data的Column并写入数据
        column = block.getColumn("tuple_data");
        column.write(new Object[]{1, "sun"});
        column.write(new Object[]{1, "moon"});

        //获取nested_data.id的Column并写入数据
        column = block.getColumn("nested_data.id");
        column.write(new long[]{1, 2, 3});
        column.write(new long[]{1, 2, 3});

        //获取nested_data.value的Column并写入数据
        column = block.getColumn("nested_data.value");
        column.write(new String[]{"coffee", "egg", "toast"});
        column.write(new String[]{"cookie", "cake", "cheese"});

        //获取decimal_value的Column并写入数据
        column = block.getColumn("decimal_value");
        column.write(BigDecimal.valueOf(1.1));
        column.write(BigDecimal.valueOf(2.2));

        //获取uuid_value的Column并写入数据
        column = block.getColumn("uuid_value");
        column.write(UUID.randomUUID());
        column.write(UUID.randomUUID());

        //获取low_cardinality_value的Column并写入数据
        column = block.getColumn("low_cardinality_value");
        column.write("CN");
        column.write("US");

        //获取int8_value的Column并写入数据
        column = block.getColumn("int8_value");
        column.write((byte) 0);
        column.write((byte) 1);

        //获取int16_value的Column并写入数据
        column = block.getColumn("int16_value");
        column.writeInt(new byte[]{(byte) 10}, 0, 1, true);
        bytes = ByteConverter.shortToByteArray((short) -10, false);
        column.writeInt(bytes, 0, bytes.length, false);

        //获取int32_value的Column并写入数据
        bytes = ByteConverter.shortToByteArray((short) 11, true);
        column = block.getColumn("int32_value");
        column.writeInt(bytes, 0, bytes.length);
        bytes = ByteConverter.intToByteArray(-11, false);
        column.writeInt(bytes, 0, bytes.length, false);

        //获取int64_value的Column并写入数据
        bytes = ByteConverter.intToByteArray(11, true);
        column = block.getColumn("int64_value");
        column.writeInt(bytes, 0, bytes.length);
        bytes = ByteConverter.longToByteArray(-11, false);
        column.writeInt(bytes, 0, bytes.length, false);

        //获取uint16_value的Column并写入数据
        column = block.getColumn("uint16_value");
        column.write(Integer.MIN_VALUE);
        column.write(Integer.MAX_VALUE);

        //获取uint32_value的Column并写入数据
        column = block.getColumn("uint32_value");
        column.write(Long.MIN_VALUE);
        column.write(Long.MAX_VALUE);

        //获取uint32_value的Column并写入数据
        column = block.getColumn("uint64_value");
        column.write(new BigInteger("12345678901234567890"));
        column.write(new BigInteger("12345678901234567890"));

        //获取float64_value的Column并写入数据
        column = block.getColumn("float64_value");
        column.write(Double.MIN_VALUE);
        column.write(Double.MAX_VALUE);

        //获取date_time64_value的Column并写入数据
        column = block.getColumn("date_time64_value");
        column.write(ZonedDateTime.now());
        column.write(ZonedDateTime.now());

        //获取min_salary的Column并写入数据
        column = block.getColumn("min_salary");
        column.write(Float.MIN_VALUE);
        column.write(Float.MAX_VALUE);

        //获取max_salary的Column并写入数据
        column = block.getColumn("max_salary");
        column.write(Float.MIN_VALUE);
        column.write(Float.MAX_VALUE);

        //插入
        client.insert(block);
        //关闭
        client.close();
    }

}
