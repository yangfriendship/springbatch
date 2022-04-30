package me.youzheng.springbatch.batch.partition;

import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import lombok.Setter;
import me.youzheng.springbatch.batch.domain.ProductVo;
import me.youzheng.springbatch.batch.job.api.QueryGenerator;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

public class ProductPartitioner implements Partitioner {

    @Setter
    private DataSource dataSource;

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {

        ProductVo[] list = QueryGenerator.getProductList(this.dataSource);
        Map<String, ExecutionContext> result = new HashMap<>();

        int num = 0;
        for (int i = 0; i < list.length; i++) {
            ExecutionContext value = new ExecutionContext();
            result.put("partition" + (num++), value);
            value.put("product", list[i]);
        }
        return result;
    }
}
