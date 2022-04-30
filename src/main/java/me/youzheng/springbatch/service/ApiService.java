package me.youzheng.springbatch.service;

import java.util.List;
import me.youzheng.springbatch.batch.domain.ApiRequestVo;
import me.youzheng.springbatch.batch.domain.ApiResponseVo;

public interface ApiService {

    ApiResponseVo service(List<? extends ApiRequestVo> requests);
}
