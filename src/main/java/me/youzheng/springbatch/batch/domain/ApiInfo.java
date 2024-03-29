package me.youzheng.springbatch.batch.domain;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ApiInfo {

    private String url;
    private List<? extends ApiRequestVo> apiRequests;

}
