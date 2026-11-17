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

package org.opengoofy.index12306.biz.orderservice.dao.algorithm;

import cn.hutool.core.collection.CollUtil;
import lombok.Getter;
import org.apache.shardingsphere.infra.util.exception.ShardingSpherePreconditions;
import org.apache.shardingsphere.sharding.api.sharding.complex.ComplexKeysShardingAlgorithm;
import org.apache.shardingsphere.sharding.api.sharding.complex.ComplexKeysShardingValue;
import org.apache.shardingsphere.sharding.exception.algorithm.sharding.ShardingAlgorithmInitializationException;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;

/**
 * 订单库级复合分片算法
 * <p>
 * 支持按 {@code user_id} 或 {@code order_sn} 路由到唯一数据源。两者借助「基因法」保证结果一致：
 * 订单号末 6 位与用户 ID 末 6 位为同一段字符，取哈希后必然落在同一分片，
 * 因此按订单号查询不会退化为跨库广播。
 * <p>
 * 分片规则：
 * <pre>
 *   slot = hash(基因) % shardingCount      // 0 ~ 31，全局槽位
 *   db   = slot / tableShardingCount       // 0 或 1
 * </pre>
 * 即 ds_0 持有 0~15 号表，ds_1 持有 16~31 号表，需与表级分片算法配合定位物理表。
 * <p>
 * {@link #init(Properties)} 在应用启动时调用一次；{@link #doSharding} 在每次 SQL 执行时调用，
 * 因此后者不应包含耗时操作。
 *
 * @author 你的名字
 */
public class OrderCommonDataBaseComplexAlgorithm implements ComplexKeysShardingAlgorithm {

    @Getter
    private Properties props;

    /**
     * 全局槽位总数，对应配置项 {@code sharding-count}
     */
    private int shardingCount;

    /**
     * 单库表数量，对应配置项 {@code table-sharding-count}；库数量 = shardingCount / tableShardingCount
     */
    private int tableShardingCount;

    private static final String SHARDING_COUNT_KEY = "sharding-count";
    private static final String TABLE_SHARDING_COUNT_KEY = "table-sharding-count";

    /**
     * 计算本次 SQL 的目标数据源
     * <p>
     * 优先使用 {@code user_id} 分片，缺失时退化到 {@code order_sn}；
     * 两者都缺失（例如按非分片键查询）时返回全部数据源，由上层承担广播代价。
     *
     * @param availableTargetNames 该逻辑表可用的数据源集合，如 {@code [ds_0, ds_1]}
     * @param shardingValue        ShardingSphere 从 SQL 的 WHERE 条件解析出的分片键值
     * @return 目标数据源集合，正常情况只包含一个元素
     */
    @Override
    public Collection<String> doSharding(Collection availableTargetNames, ComplexKeysShardingValue shardingValue) {
        // 使用 LinkedHashSet 去重并保持插入顺序，便于问题排查时对照
        Collection<String> result = new LinkedHashSet<>();

        Map<String, Collection<Comparable<?>>> columnValueMap = shardingValue.getColumnNameAndShardingValuesMap();

        // 未命中任何等值分片键（范围查询，或按非分片键查询），无法定位，只能全路由
        if (CollUtil.isEmpty(columnValueMap)) {
            return new LinkedHashSet<>(availableTargetNames);
        }

        Collection<Comparable<?>> userIds = columnValueMap.get("user_id");
        if (CollUtil.isNotEmpty(userIds)) {
            result.add("ds_" + resolveDbSuffix(userIds));
        } else {
            Collection<Comparable<?>> orderSns = columnValueMap.get("order_sn");
            if (CollUtil.isNotEmpty(orderSns)) {
                result.add("ds_" + resolveDbSuffix(orderSns));
            }
        }

        // 分片键存在但均非本算法支持的列时同样退化为全路由，避免返回空集合导致报错
        if (result.isEmpty()) {
            return new LinkedHashSet<>(availableTargetNames);
        }
        return result;
    }

    /**
     * 由分片键值推导库序号
     * <p>
     * 统一先转成字符串再截取低 6 位，保证 {@code String} 与 {@code Long} 两种入参走同一条哈希路径。
     * 若对两种类型分别取 hashCode，同一用户会算出不同分片，导致按订单号查询落错库。
     *
     * @param shardingValues 分片键值集合，取其中第一个值参与计算
     * @return 库序号，{@code "0"} 或 {@code "1"}
     */
    private String resolveDbSuffix(Collection<Comparable<?>> shardingValues) {
        Comparable<?> value = shardingValues.iterator().next();
        String keyStr = (value instanceof String) ? value.toString() : String.valueOf(value);
        // 防止异常短串导致下标越界
        String gene = keyStr.substring(Math.max(keyStr.length() - 6, 0));
        // slot 为全局槽位，再按单库表数折算为库序号
        return String.valueOf(hashShardingValue(gene) % shardingCount / tableShardingCount);
    }

    /**
     * 应用启动时由 ShardingSphere 调用，注入 yaml 中配置的分片参数
     *
     * @param props 配置项，包含 {@code sharding-count} 与 {@code table-sharding-count}
     */
    @Override
    public void init(Properties props) {
        this.props = props;
        shardingCount = getShardingCount(props);
        tableShardingCount = getTableShardingCount(props);
    }

    private int getShardingCount(final Properties props) {
        ShardingSpherePreconditions.checkState(props.containsKey(SHARDING_COUNT_KEY),
                () -> new ShardingAlgorithmInitializationException(getType(), "Sharding count cannot be null."));
        return Integer.parseInt(props.getProperty(SHARDING_COUNT_KEY));
    }

    private int getTableShardingCount(final Properties props) {
        ShardingSpherePreconditions.checkState(props.containsKey(TABLE_SHARDING_COUNT_KEY),
                () -> new ShardingAlgorithmInitializationException(getType(), "Table sharding count cannot be null."));
        return Integer.parseInt(props.getProperty(TABLE_SHARDING_COUNT_KEY));
    }

    /**
     * 将分片键归一化为非负长整型
     * <p>
     * 先提升为 {@code long} 再取绝对值：{@code Math.abs(Integer.MIN_VALUE)} 仍为负数，
     * 直接参与取模会得到非法的负数下标。
     */
    private long hashShardingValue(final Comparable<?> shardingValue) {
        return Math.abs((long) shardingValue.hashCode());
    }

    /**
     * 声明为自定义类实现，ShardingSphere 将依据配置中的 algorithmClassName 反射创建本类
     */
    @Override
    public String getType() {
        return "CLASS_BASED";
    }
}
