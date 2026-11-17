/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opengoofy.index12306.biz.userservice.config;

import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 用户注册场景的布隆过滤器配置
 * <p>
 * 用于拦截「用户名是否已被注册」的缓存穿透：注册查重时先用布隆过滤器判断，
 * 若判定「一定不存在」则直接放行，不再访问数据库，从而把数据库命中率压到误判率量级。
 * <p>
 * 位数组长度与哈希函数个数由 Redisson 依据 {@code expectedInsertions} 与
 * {@code falseProbability} 自动推导（约 {@code 1.44 × log2(1 / p)} bit 每元素），
 * 因此这两个参数需要按真实数据量评估，而不是沿用默认值。
 *
 * @author 你的名字
 */
@Configuration
@EnableConfigurationProperties(UserRegisterBloomFilterProperties.class)
public class RBloomFilterConfiguration {

    /**
     * 创建防止注册缓存穿透的布隆过滤器
     * <p>
     * 使用 {@code tryInit} 而非 {@code init}：前者在 Redis 中已存在同名过滤器时会直接复用、
     * 忽略本次参数；后者会强制重置配置，使已注册用户全部从过滤器中消失，进而允许重复注册。
     *
     * @param redissonClient                    Redisson 客户端
     * @param userRegisterBloomFilterProperties 过滤器配置（名称、预期插入量、误判率）
     * @return 由 Spring 容器管理的布隆过滤器实例
     */
    @Bean
    public RBloomFilter<String> userRegisterCachePenetrationBloomFilter(RedissonClient redissonClient,
                                                                       UserRegisterBloomFilterProperties userRegisterBloomFilterProperties) {
        RBloomFilter<String> cachePenetrationBloomFilter = redissonClient.getBloomFilter(userRegisterBloomFilterProperties.getName());
        cachePenetrationBloomFilter.tryInit(userRegisterBloomFilterProperties.getExpectedInsertions(),
                userRegisterBloomFilterProperties.getFalseProbability());
        return cachePenetrationBloomFilter;
    }
}
