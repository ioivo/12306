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

package org.opengoofy.index12306.biz.userservice.service.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opengoofy.index12306.biz.userservice.common.enums.UserChainMarkEnum;
import org.opengoofy.index12306.biz.userservice.dao.entity.UserDO;
import org.opengoofy.index12306.biz.userservice.dao.entity.UserDeletionDO;
import org.opengoofy.index12306.biz.userservice.dao.entity.UserMailDO;
import org.opengoofy.index12306.biz.userservice.dao.entity.UserPhoneDO;
import org.opengoofy.index12306.biz.userservice.dao.entity.UserReuseDO;
import org.opengoofy.index12306.biz.userservice.dao.mapper.UserDeletionMapper;
import org.opengoofy.index12306.biz.userservice.dao.mapper.UserMailMapper;
import org.opengoofy.index12306.biz.userservice.dao.mapper.UserMapper;
import org.opengoofy.index12306.biz.userservice.dao.mapper.UserPhoneMapper;
import org.opengoofy.index12306.biz.userservice.dao.mapper.UserReuseMapper;
import org.opengoofy.index12306.biz.userservice.dto.req.UserDeletionReqDTO;
import org.opengoofy.index12306.biz.userservice.dto.req.UserLoginReqDTO;
import org.opengoofy.index12306.biz.userservice.dto.req.UserRegisterReqDTO;
import org.opengoofy.index12306.biz.userservice.dto.resp.UserLoginRespDTO;
import org.opengoofy.index12306.biz.userservice.dto.resp.UserQueryRespDTO;
import org.opengoofy.index12306.biz.userservice.dto.resp.UserRegisterRespDTO;
import org.opengoofy.index12306.biz.userservice.service.UserLoginService;
import org.opengoofy.index12306.biz.userservice.service.UserService;
import org.opengoofy.index12306.framework.starter.cache.DistributedCache;
import org.opengoofy.index12306.framework.starter.common.toolkit.BeanUtil;
import org.opengoofy.index12306.framework.starter.convention.exception.ClientException;
import org.opengoofy.index12306.framework.starter.convention.exception.ServiceException;
import org.opengoofy.index12306.framework.starter.designpattern.chain.AbstractChainContext;
import org.opengoofy.index12306.frameworks.starter.user.core.UserContext;
import org.opengoofy.index12306.frameworks.starter.user.core.UserInfoDTO;
import org.opengoofy.index12306.frameworks.starter.user.toolkit.JWTUtil;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.opengoofy.index12306.biz.userservice.common.constant.RedisKeyConstant.LOCK_USER_REGISTER;
import static org.opengoofy.index12306.biz.userservice.common.constant.RedisKeyConstant.USER_DELETION;
import static org.opengoofy.index12306.biz.userservice.common.constant.RedisKeyConstant.USER_REGISTER_REUSE_SHARDING;
import static org.opengoofy.index12306.biz.userservice.common.enums.UserRegisterErrorCodeEnum.HAS_USERNAME_NOTNULL;
import static org.opengoofy.index12306.biz.userservice.common.enums.UserRegisterErrorCodeEnum.MAIL_REGISTERED;
import static org.opengoofy.index12306.biz.userservice.common.enums.UserRegisterErrorCodeEnum.PHONE_REGISTERED;
import static org.opengoofy.index12306.biz.userservice.common.enums.UserRegisterErrorCodeEnum.USER_REGISTER_FAIL;
import static org.opengoofy.index12306.biz.userservice.toolkit.UserReuseUtil.hashShardingIdx;

/**
 * 用户登录接口实现
 * 公众号：马丁玩编程，回复：加群，添加马哥微信（备注：12306）获取项目资料
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class UserLoginServiceImpl implements UserLoginService {

    private final UserService userService;
    private final UserMapper userMapper;
    private final UserReuseMapper userReuseMapper;
    private final UserDeletionMapper userDeletionMapper;
    private final UserPhoneMapper userPhoneMapper;
    private final UserMailMapper userMailMapper;
    private final RedissonClient redissonClient;
    private final DistributedCache distributedCache;
    private final AbstractChainContext<UserRegisterReqDTO> abstractChainContext;
    private final RBloomFilter<String> userRegisterCachePenetrationBloomFilter;

    @Override
    public UserLoginRespDTO login(UserLoginReqDTO requestParam) {
        String usernameOrMailOrPhone = requestParam.getUsernameOrMailOrPhone();
        boolean mailFlag = false;
        // 时间复杂度最佳 O(1)。indexOf or contains 时间复杂度为 O(n)
        for (char c : usernameOrMailOrPhone.toCharArray()) {
            if (c == '@') {
                mailFlag = true;
                break;
            }
        }
        String username;
        if (mailFlag) {
            LambdaQueryWrapper<UserMailDO> queryWrapper = Wrappers.lambdaQuery(UserMailDO.class)
                    .eq(UserMailDO::getMail, usernameOrMailOrPhone);
            username = Optional.ofNullable(userMailMapper.selectOne(queryWrapper))
                    .map(UserMailDO::getUsername)
                    .orElseThrow(() -> new ClientException("用户名/手机号/邮箱不存在"));
        } else {
            LambdaQueryWrapper<UserPhoneDO> queryWrapper = Wrappers.lambdaQuery(UserPhoneDO.class)
                    .eq(UserPhoneDO::getPhone, usernameOrMailOrPhone);
            username = Optional.ofNullable(userPhoneMapper.selectOne(queryWrapper))
                    .map(UserPhoneDO::getUsername)
                    .orElse(null);
        }
        username = Optional.ofNullable(username).orElse(requestParam.getUsernameOrMailOrPhone());
        LambdaQueryWrapper<UserDO> queryWrapper = Wrappers.lambdaQuery(UserDO.class)
                .eq(UserDO::getUsername, username)
                .eq(UserDO::getPassword, requestParam.getPassword())
                .select(UserDO::getId, UserDO::getUsername, UserDO::getRealName);
        UserDO userDO = userMapper.selectOne(queryWrapper);
        if (userDO != null) {
            UserInfoDTO userInfo = UserInfoDTO.builder()
                    .userId(String.valueOf(userDO.getId()))
                    .username(userDO.getUsername())
                    .realName(userDO.getRealName())
                    .build();
            String accessToken = JWTUtil.generateAccessToken(userInfo);
            UserLoginRespDTO userLogin = UserLoginRespDTO.builder()
                    .userId(userInfo.getUserId())
                    .username(userInfo.getUsername())
                    .realName(userInfo.getRealName())
                    .accessToken(accessToken)
                    .build();
            distributedCache.put(accessToken, JSON.toJSONString(userLogin), 30, TimeUnit.MINUTES);
            return userLogin;
        }
        throw new ServiceException("账号不存在或密码错误");
    }

    @Override
    public UserLoginRespDTO checkLogin(String accessToken) {
        return distributedCache.get(accessToken, UserLoginRespDTO.class);
    }

    @Override
    public void logout(String accessToken) {
        if (StrUtil.isNotBlank(accessToken)) {
            distributedCache.delete(accessToken);
        }
    }

    /**
     * 校验用户名是否可用
     * <p>
     * 采用「布隆过滤器前置拦截 + 数据库兜底」两层校验，避免注册查重被无效用户名打穿：
     * 布隆过滤器判断「一定不存在」时直接放行，不访问数据库，
     * 仅在命中「可能存在」时才回查一次 t_user 确认。
     * <p>
     * 布隆过滤器无假阴性、有假阳性，因此只可据此判定「不存在」；
     * 判定「存在」时必须由数据库确认。按误判率 0.001 计算，
     * 数据库中约有 0.1% 的查重请求会被穿透，其余均在缓存层拦截。
     * <p>
     * 另需注意：布隆过滤器不支持删除，用户注销后其用户名仍会命中过滤器，
     * 此时会走到查库分支，因库中已无该记录而判定可用，语义正确，
     * 代价是该用户名每次查重会多一次数据库查询。
     *
     * @param username 待校验的用户名
     * @return {@code true} 表示用户名可用，{@code false} 表示已被占用
     */
    @Override
    public Boolean hasUsername(String username) {
        // 第一层：布隆过滤器判定「一定不存在」时直接放行，不访问数据库
        if (!userRegisterCachePenetrationBloomFilter.contains(username)) {
            return true;
        }
        // 第二层：过滤器判定「可能存在」时回查数据库确认，t_user 以 username 为分片键，此处为单分片查询
        Long count = userMapper.selectCount(
                Wrappers.lambdaQuery(UserDO.class)
                        .eq(UserDO::getUsername, username));
        return count == 0;
    }
    @Transactional(rollbackFor = Exception.class)
    @Override
    public UserRegisterRespDTO register(UserRegisterReqDTO requestParam) {
        abstractChainContext.handler(UserChainMarkEnum.USER_REGISTER_FILTER.name(), requestParam);
        RLock lock = redissonClient.getLock(LOCK_USER_REGISTER + requestParam.getUsername());
        boolean tryLock = lock.tryLock();
        if (!tryLock) {
            throw new ServiceException(HAS_USERNAME_NOTNULL);
        }
        try {
            try {
                int inserted = userMapper.insert(BeanUtil.convert(requestParam, UserDO.class));
                if (inserted < 1) {
                    throw new ServiceException(USER_REGISTER_FAIL);
                }
            } catch (DuplicateKeyException dke) {
                log.error("用户名 [{}] 重复注册", requestParam.getUsername());
                throw new ServiceException(HAS_USERNAME_NOTNULL);
            }
            UserPhoneDO userPhoneDO = UserPhoneDO.builder()
                    .phone(requestParam.getPhone())
                    .username(requestParam.getUsername())
                    .build();
            try {
                userPhoneMapper.insert(userPhoneDO);
            } catch (DuplicateKeyException dke) {
                log.error("用户 [{}] 注册手机号 [{}] 重复", requestParam.getUsername(), requestParam.getPhone());
                throw new ServiceException(PHONE_REGISTERED);
            }
            if (StrUtil.isNotBlank(requestParam.getMail())) {
                UserMailDO userMailDO = UserMailDO.builder()
                        .mail(requestParam.getMail())
                        .username(requestParam.getUsername())
                        .build();
                try {
                    userMailMapper.insert(userMailDO);
                } catch (DuplicateKeyException dke) {
                    log.error("用户 [{}] 注册邮箱 [{}] 重复", requestParam.getUsername(), requestParam.getMail());
                    throw new ServiceException(MAIL_REGISTERED);
                }
            }
            String username = requestParam.getUsername();
            userReuseMapper.delete(Wrappers.update(new UserReuseDO(username)));
            StringRedisTemplate instance = (StringRedisTemplate) distributedCache.getInstance();
            instance.opsForSet().remove(USER_REGISTER_REUSE_SHARDING + hashShardingIdx(username), username);
            // ================================================================
            // 写库成功后同步写入布隆过滤器。放在事务方法内、finally 之前，
            // 保证只有写库成功的用户名才会被加入；过滤器不支持删除，
            // 因此宁可偶发多加，也不能漏加，否则该用户名会被判定为「可用」。
            userRegisterCachePenetrationBloomFilter.add(username);

        } finally {
            lock.unlock();
        }
        return BeanUtil.convert(requestParam, UserRegisterRespDTO.class);
    }

    @Transactional(rollbackFor = Exception.class)
    @Override
    public void deletion(UserDeletionReqDTO requestParam) {
        String username = UserContext.getUsername();
        if (!Objects.equals(username, requestParam.getUsername())) {
            // 此处严谨来说，需要上报风控中心进行异常检测
            throw new ClientException("注销账号与登录账号不一致");
        }
        RLock lock = redissonClient.getLock(USER_DELETION + requestParam.getUsername());
        // 加锁为什么放在 try 语句外？https://www.yuque.com/magestack/12306/pu52u29i6eb1c5wh
        lock.lock();
        try {
            UserQueryRespDTO userQueryRespDTO = userService.queryUserByUsername(username);
            UserDeletionDO userDeletionDO = UserDeletionDO.builder()
                    .idType(userQueryRespDTO.getIdType())
                    .idCard(userQueryRespDTO.getIdCard())
                    .build();
            userDeletionMapper.insert(userDeletionDO);
            UserDO userDO = new UserDO();
            userDO.setDeletionTime(System.currentTimeMillis());
            userDO.setUsername(username);
            // MyBatis Plus 不支持修改语句变更 del_flag 字段
            userMapper.deletionUser(userDO);
            UserPhoneDO userPhoneDO = UserPhoneDO.builder()
                    .phone(userQueryRespDTO.getPhone())
                    .deletionTime(System.currentTimeMillis())
                    .build();
            userPhoneMapper.deletionUser(userPhoneDO);
            if (StrUtil.isNotBlank(userQueryRespDTO.getMail())) {
                UserMailDO userMailDO = UserMailDO.builder()
                        .mail(userQueryRespDTO.getMail())
                        .deletionTime(System.currentTimeMillis())
                        .build();
                userMailMapper.deletionUser(userMailDO);
            }
            distributedCache.delete(UserContext.getToken());
            userReuseMapper.insert(new UserReuseDO(username));
            StringRedisTemplate instance = (StringRedisTemplate) distributedCache.getInstance();
            instance.opsForSet().add(USER_REGISTER_REUSE_SHARDING + hashShardingIdx(username), username);
        } finally {
            lock.unlock();
        }
    }
}
