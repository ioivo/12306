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

package org.opengoofy.index12306.biz.ticketservice.canal;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import lombok.RequiredArgsConstructor;
import org.opengoofy.index12306.biz.ticketservice.common.enums.CanalExecuteStrategyMarkEnum;
import org.opengoofy.index12306.biz.ticketservice.common.enums.SeatStatusEnum;
import org.opengoofy.index12306.biz.ticketservice.mq.event.CanalBinlogEvent;
import org.opengoofy.index12306.framework.starter.cache.DistributedCache;
import org.opengoofy.index12306.framework.starter.designpattern.strategy.AbstractExecuteStrategy;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.TRAIN_STATION_REMAINING_TICKET;

/**
 * 余票缓存失效处理器
 * <p>
 * 监听 {@code t_seat} 的 binlog，在座位状态变更后使对应区间的余票缓存失效，
 * 由下一次查询负责回源重建，以 Cache-Aside 模式保证缓存与数据库的最终一致。
 * <p>
 * 数据流：{@code UPDATE t_seat → binlog → Canal → RocketMQ → 本处理器 → DEL 余票缓存}
 * <p>
 * 这里选择「删除缓存」而非「更新缓存」：
 * <ul>
 *   <li>删除是幂等的，消息重复消费不会产生偏差</li>
 *   <li>无需依据变更前的状态推算增量，消息乱序也不会算错</li>
 * </ul>
 * 代价是缓存失效后首次查询需要回源聚合，该开销由数据库索引承担。
 *
 * @author 你的名字
 */
@Component
@RequiredArgsConstructor
public class TicketAvailabilityCacheUpdateHandler implements AbstractExecuteStrategy<CanalBinlogEvent, Void> {

    private final DistributedCache distributedCache;

    /**
     * 处理一批 t_seat 的变更事件
     * <p>
     * 仅处理「座位状态变为可售或已锁定」的记录：前者对应解锁回滚，后者对应锁座，
     * 两者都会改变可售数量；而变为已售出（已锁定 → 已售出）不影响可售数量，无需处理。
     * <p>
     * 一次购票会更新多个区间的多行记录，同一区间的余票缓存只需失效一次，因此对 key 去重。
     *
     * @param message Canal 推送的变更事件
     */
    @Override
    public void execute(CanalBinlogEvent message) {
        List<Map<String, Object>> dataList = message.getData();
        List<Map<String, Object>> oldList = message.getOld();
        // 消费线程长期驻留，需防止空数据导致异常中断消费
        if (CollUtil.isEmpty(dataList) || CollUtil.isEmpty(oldList)) {
            return;
        }

        List<Map<String, Object>> changedSeatList = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++) {
            if (i >= oldList.size()) {
                break;
            }
            Map<String, Object> newData = dataList.get(i);
            Map<String, Object> oldData = oldList.get(i);

            // old 中仅包含本次实际变更的列，没有该列说明座位状态未发生变化
            Object oldSeatStatus = oldData.get("seat_status");
            if (oldSeatStatus == null || StrUtil.isBlank(oldSeatStatus.toString())) {
                continue;
            }

            // 仅关注变为「可售」或「已锁定」的变更，变为「已售出」不影响可售数量
            Object newSeatStatusObj = newData.get("seat_status");
            if (newSeatStatusObj == null) {
                continue;
            }
            String newSeatStatus = newSeatStatusObj.toString();
            if (!StrUtil.equalsAny(newSeatStatus,
                    String.valueOf(SeatStatusEnum.AVAILABLE.getCode()),
                    String.valueOf(SeatStatusEnum.LOCKED.getCode()))) {
                continue;
            }

            changedSeatList.add(newData);
        }

        if (CollUtil.isEmpty(changedSeatList)) {
            return;
        }

        // 同一区间的多行变更归并到同一个 key，使用 Set 去重并保持顺序
        Set<String> cacheKeys = new LinkedHashSet<>(changedSeatList.size());
        for (Map<String, Object> each : changedSeatList) {
            String trainId = each.get("train_id").toString();
            String startStation = each.get("start_station").toString();
            String endStation = each.get("end_station").toString();
            cacheKeys.add(TRAIN_STATION_REMAINING_TICKET + trainId + "_" + startStation + "_" + endStation);
        }

        // 批量删除，一次网络往返；DEL 幂等，重复消费不会产生副作用
        StringRedisTemplate instance = (StringRedisTemplate) distributedCache.getInstance();
        instance.delete(cacheKeys);
    }

    /**
     * 声明本处理器负责的表名，用于策略路由
     *
     * @return 表名 {@code t_seat}
     */
    @Override
    public String mark() {
        return CanalExecuteStrategyMarkEnum.T_SEAT.getActualTable();
    }
}
