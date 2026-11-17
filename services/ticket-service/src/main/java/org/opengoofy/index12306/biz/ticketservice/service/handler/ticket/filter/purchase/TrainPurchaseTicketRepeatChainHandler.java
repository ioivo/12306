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

package org.opengoofy.index12306.biz.ticketservice.service.handler.ticket.filter.purchase;

import cn.hutool.core.collection.CollUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import lombok.RequiredArgsConstructor;
import org.opengoofy.index12306.biz.ticketservice.common.enums.TicketStatusEnum;
import org.opengoofy.index12306.biz.ticketservice.dao.entity.TicketDO;
import org.opengoofy.index12306.biz.ticketservice.dao.mapper.TicketMapper;
import org.opengoofy.index12306.biz.ticketservice.dto.domain.PurchaseTicketPassengerDetailDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.req.PurchaseTicketReqDTO;
import org.opengoofy.index12306.framework.starter.convention.exception.ClientException;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 购票流程过滤器之校验乘客是否重复购买
 * <p>
 * 同一乘车人在同一车次上只允许持有一张有效车票。若已存在待支付、已支付或已乘车的记录，
 * 视为重复购买并拒绝本次请求；已退票、已关闭、已改签的记录不再占用车次名额，允许重新购买。
 * <p>
 * 校验按乘车人维度而非下单账号维度进行：同一乘车人可能被不同账号代为购票，
 * 仅按账号过滤会漏判。
 * <p>
 * 该过滤器位于购票责任链的第 30 位，在参数校验与余票校验之后、执行加锁之前完成，
 * 使重复请求在进入选座与扣减环节前就被拦截。
 *
 * @author 你的名字
 */
@Component
@RequiredArgsConstructor
public class TrainPurchaseTicketRepeatChainHandler implements TrainPurchaseTicketChainFilter<PurchaseTicketReqDTO> {

    /**
     * 视为「有效车票」的状态。已退票、已关闭、已改签不在其中，允许重新购票
     */
    private static final List<Integer> VALID_TICKET_STATUS = List.of(
            TicketStatusEnum.UNPAID.getCode(),
            TicketStatusEnum.PAID.getCode(),
            TicketStatusEnum.BOARDED.getCode());

    private final TicketMapper ticketMapper;

    /**
     * 校验本次购票的乘车人是否已有该车次的有效车票
     *
     * @param requestParam 购票请求
     * @throws ClientException 存在重复购买时抛出
     */
    @Override
    public void handler(PurchaseTicketReqDTO requestParam) {
        List<String> passengerIds = requestParam.getPassengers().stream()
                .map(PurchaseTicketPassengerDetailDTO::getPassengerId)
                .distinct()
                .toList();
        if (CollUtil.isEmpty(passengerIds)) {
            return;
        }

        // 车次 + 乘车人 + 有效状态 三者共同确定「是否已持有该车次车票」
        LambdaQueryWrapper<TicketDO> queryWrapper = Wrappers.lambdaQuery(TicketDO.class)
                .eq(TicketDO::getTrainId, requestParam.getTrainId())
                .in(TicketDO::getPassengerId, passengerIds)
                .in(TicketDO::getTicketStatus, VALID_TICKET_STATUS);

        // 使用 selectList 而非 selectOne：一次提交多位乘车人时可能命中多条记录，
        // selectOne 在结果数大于 1 时会抛 TooManyResultsException
        List<TicketDO> existTickets = ticketMapper.selectList(queryWrapper);
        if (CollUtil.isNotEmpty(existTickets)) {
            throw new ClientException("乘车人已购买该车次车票，请勿重复提交");
        }
    }

    /**
     * 责任链执行顺序，数值越小越先执行
     */
    @Override
    public int getOrder() {
        return 30;
    }
}
