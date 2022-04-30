package me.youzheng.springbatch.batch.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ApiRequestVo {

    private long id;
    private ProductVo productVo;
    private ApiResponseVo apiResponseVo;
}