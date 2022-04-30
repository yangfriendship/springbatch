package me.youzheng.springbatch.batch.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ProductVo {

    private Long id;
    private String name;
    private int price;
    private String type;

}
