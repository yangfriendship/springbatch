package me.youzheng.springbatch.batch.rowmapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import me.youzheng.springbatch.batch.domain.ProductVo;
import org.springframework.jdbc.core.RowMapper;

public class ProductRowMapper implements RowMapper<ProductVo> {

    @Override
    public ProductVo mapRow(ResultSet rs, int rowNum) throws SQLException {

        return ProductVo.builder()
            .id(rs.getLong("id"))
            .name(rs.getString("name"))
            .type(rs.getString("type"))
            .price(rs.getInt("price"))
            .build();
    }
}
