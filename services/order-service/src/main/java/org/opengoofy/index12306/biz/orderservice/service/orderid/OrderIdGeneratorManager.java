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

package org.opengoofy.index12306.biz.orderservice.service.orderid;

import lombok.RequiredArgsConstructor;
import org.opengoofy.index12306.framework.starter.cache.DistributedCache;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * 订单号生成器
 * <p>
 * 采用「基因法」生成订单号，使订单在按 {@code user_id} 与按 {@code order_sn} 两个维度查询时
 * 都能落到同一个分片上，避免跨库广播。
 * <p>
 * 订单号结构：
 * <pre>
 *   {雪花 ID}{用户 ID 低 6 位}
 *   └─ 18 位 ┘└─ 固定 6 位 ─┘
 * </pre>
 * 其中「用户 ID 低 6 位」即所谓基因，两个分片键取同一段基因后哈希结果一致，
 * 因此 {@code hash(orderSn 低 6 位)} == {@code hash(userId 低 6 位)}。
 *
 * @author 你的名字
 */
@Component
@RequiredArgsConstructor
public final class OrderIdGeneratorManager implements InitializingBean {

    private final RedissonClient redissonClient;
    private final DistributedCache distributedCache;

    /**
     * 雪花 ID 生成器，节点号在应用启动时由 {@link #afterPropertiesSet()} 分配
     */
    private static DistributedIdGenerator DISTRIBUTED_ID_GENERATOR;

    /**
     * 生成全局唯一订单号
     * <p>
     * 订单号 = 雪花 ID + 用户 ID 低 6 位（定长 6 位，不足左补 0）。
     * <p>
     * 基因段必须定长，否则会出现路由错位：假设 {@code userId = 1000123}，
     * 低 6 位是 {@code "000123"}；若直接用 {@code String.valueOf(userId % 1000000)}
     * 只会得到 {@code "123"}，拼出的订单号末 6 位将混入雪花 ID 的数字，
     * 与 userId 的末 6 位不再一致，按订单号查询时就会落到错误的表上。
     * 因此这里用 {@code %06d} 强制补零。
     *
     * @param userId 用户 ID
     * @return 订单号，形如 {@code 1685000000000000362304}
     */
    public static String generateId(long userId) {
        return DISTRIBUTED_ID_GENERATOR.generateId() + String.format("%06d", userId % 1000000);
    }

    /**
     * 应用启动时分配雪花算法节点号
     * <p>
     * 多实例部署时，各实例通过 Redis 的原子自增获取互不相同的节点号，
     * 避免相同毫秒内生成重复 ID。节点号上限与分片总数保持一致（32）。
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        String lockKey = "distributed_id_generator_lock_key";
        RLock lock = redissonClient.getLock(lockKey);
        lock.lock();
        try {
            StringRedisTemplate instance = (StringRedisTemplate) distributedCache.getInstance();
            String generatorKey = "distributed_id_generator_config";
            long incremented = Optional.ofNullable(instance.opsForValue().increment(generatorKey)).orElse(0L);
            // 节点号取 32 位，与订单分片总数对齐
            int nodeMax = 32;
            if (incremented > nodeMax) {
                incremented = 0;
                instance.opsForValue().set(generatorKey, "0");
            }
            DISTRIBUTED_ID_GENERATOR = new DistributedIdGenerator(incremented);
        } finally {
            lock.unlock();
        }
    }
}
