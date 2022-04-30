package me.youzheng.springbatch.batch.job.api;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import me.youzheng.springbatch.batch.domain.ProductVo;
import me.youzheng.springbatch.batch.rowmapper.ProductRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

public class QueryGenerator {

    public static ProductVo[] getProductList(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        List<ProductVo> productVoList = jdbcTemplate.query("select type from product group by type",
            new ProductRowMapper() {
                @Override
                public ProductVo mapRow(ResultSet rs, int rowNum) throws SQLException {
                    return ProductVo.builder()
                        .type(rs.getString("type"))
                        .build();
                }
            });
        return productVoList.toArray(new ProductVo[]{});
    }

    public static Map<String, Object> getParameterForQuery(String parameter, String value) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(parameter, value);
        return parameters;
    }
}
