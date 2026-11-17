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
import com.google.common.base.Preconditions;
import lombok.Getter;
import org.apache.shardingsphere.sharding.api.sharding.complex.ComplexKeysShardingAlgorithm;
import org.apache.shardingsphere.sharding.api.sharding.complex.ComplexKeysShardingValue;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;

/**
 * 订单表级复合分片算法
 * <p>
 * 与 {@link OrderCommonDataBaseComplexAlgorithm} 配对使用，一次查询会先后调用两个算法：
 * 前者决定数据源，本类决定该数据源内的物理表。
 * <p>
 * 分片规则（与库级算法共用同一个哈希值，仅取模除数不同）：
 * <pre>
 *   slot = hash(基因) % shardingCount      // 0 ~ 31，全局表号
 *   db   = slot / tableShardingCount       // 由库级算法计算
 * </pre>
 * 因此 ds_0 只有 {@code t_order_0 ~ t_order_15}，ds_1 只有 {@code t_order_16 ~ t_order_31}。
 * 若此处误写成 {@code hash % 16}，表号恒小于 16，而 ds_1 中并不存在这些表，
 * 会直接查到空表。
 *
 * @author 你的名字
 */
public class OrderCommonTableComplexAlgorithm implements ComplexKeysShardingAlgorithm {

    @Getter
    private Properties props;

    /**
     * 全局表号上限，对应配置项 {@code sharding-count}
     */
    private int shardingCount;

    private static final String SHARDING_COUNT_KEY = "sharding-count";

    /**
     * 计算本次 SQL 的目标物理表
     * <p>
     * 优先使用 {@code user_id} 分片，缺失时退化到 {@code order_sn}；
     * 两者都缺失时返回全部物理表，由上层承担广播代价。
     *
     * @param availableTargetNames 该逻辑表可用的物理表集合，本方法不使用（表名由逻辑表名与表号拼接而来）
     * @param shardingValue        ShardingSphere 从 SQL 的 WHERE 条件解析出的分片键值
     * @return 目标物理表集合，正常情况只包含一个元素
     */
    @Override
    public Collection<String> doSharding(Collection availableTargetNames, ComplexKeysShardingValue shardingValue) {
        Collection<String> result = new LinkedHashSet<>();

        Map<String, Collection<Comparable<?>>> columnValueMap = shardingValue.getColumnNameAndShardingValuesMap();

        // 未命中任何等值分片键（范围查询，或按非分片键查询），无法定位，只能全路由
        if (CollUtil.isEmpty(columnValueMap)) {
            return new LinkedHashSet<>(availableTargetNames);
        }

        Collection<Comparable<?>> userIds = columnValueMap.get("user_id");
        if (CollUtil.isNotEmpty(userIds)) {
            result.add(buildTableName(shardingValue, userIds));
        } else {
            Collection<Comparable<?>> orderSns = columnValueMap.get("order_sn");
            if (CollUtil.isNotEmpty(orderSns)) {
                result.add(buildTableName(shardingValue, orderSns));
            }
        }

        // 分片键存在但均非本算法支持的列时同样退化为全路由，避免返回空集合导致报错
        if (result.isEmpty()) {
            return new LinkedHashSet<>(availableTargetNames);
        }
        return result;
    }

    /**
     * 由分片键值构造物理表名
     * <p>
     * 与库级算法保持完全相同的基因截取方式与哈希函数，只把取模除数换成 {@code shardingCount}，
     * 这样库与表的组合才始终落在真实存在的物理表上。
     *
     * @param shardingValue  分片上下文，用于获取逻辑表名
     * @param shardingValues 分片键值集合，取其中第一个值参与计算
     * @return 物理表名，形如 {@code t_order_27}
     */
    private String buildTableName(ComplexKeysShardingValue shardingValue, Collection<Comparable<?>> shardingValues) {
        Comparable<?> value = shardingValues.iterator().next();
        String keyStr = (value instanceof String) ? value.toString() : String.valueOf(value);
        // 防止异常短串导致下标越界
        String gene = keyStr.substring(Math.max(keyStr.length() - 6, 0));
        return shardingValue.getLogicTableName() + "_" + (hashShardingValue(gene) % shardingCount);
    }

    /**
     * 应用启动时由 ShardingSphere 调用，注入 yaml 中配置的分片参数
     *
     * @param props 配置项，包含 {@code sharding-count}
     */
    @Override
    public void init(Properties props) {
        this.props = props;
        shardingCount = getShardingCount(props);
    }

    private int getShardingCount(final Properties props) {
        Preconditions.checkArgument(props.containsKey(SHARDING_COUNT_KEY), "Sharding count cannot be null.");
        return Integer.parseInt(props.getProperty(SHARDING_COUNT_KEY));
    }

    /**
     * 将分片键归一化为非负长整型
     * <p>
     * 先提升为 {@code long} 再取绝对值：{@code Math.abs(Integer.MIN_VALUE)} 仍为负数，
     * 直接参与取模会得到非法的负数表号。
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
