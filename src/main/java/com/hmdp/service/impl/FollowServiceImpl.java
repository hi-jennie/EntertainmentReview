package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.constants.RedisConstants;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author Jennie
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IUserService userService;

    /**
     * check if current user follow the user
     *
     * @param followUserId
     * @return
     */
    public Result isFollow(Long followUserId) {
        //获取登陆用户
        Long id = UserHolder.getUser().getId();
        //查询是否关注
        Long count = lambdaQuery()
                .eq(Follow::getUserId, id)
                .eq(Follow::getFollowUserId, followUserId)
                .count();
        return Result.ok(count > 0);

    }

    /**
     * follow or unfollow the user
     * 在redis 创建某个用户的follower 列表的时机： 当第一个用户尝试关注当前用户的时候
     *
     * @param followUserId
     * @param isFollow
     * @return
     */
    public Result follow(Long followUserId, Boolean isFollow) {
        Long id = UserHolder.getUser().getId();
        if (id.equals(followUserId)) {
            return Result.fail("Can't follow yourself");
        }
        if (isFollow) {
            // follow
            boolean exists = lambdaQuery()
                    .eq(Follow::getUserId, id)
                    .eq(Follow::getFollowUserId, followUserId)
                    .exists();
            if (exists) {
                return Result.ok(); // if already followed, do nothing
            }

            Follow follow = new Follow();
            follow.setFollowUserId(followUserId);
            follow.setUserId(id);
            boolean isSuccess = save(follow);
            if (isSuccess) {
                String key = RedisConstants.FOLLOWERS_KEY_PREFIX + followUserId;
                stringRedisTemplate.opsForSet().add(key, id.toString());
            }
        } else {
            // unfollow
            boolean isSuccess = remove(new LambdaQueryWrapper<Follow>()
                    .eq(Follow::getUserId, id)
                    .eq(Follow::getFollowUserId, followUserId)
            );
            if (isSuccess) {
                String key = RedisConstants.FOLLOWERS_KEY_PREFIX + followUserId;
                stringRedisTemplate.opsForSet().remove(key, id.toString());
            }
        }
        return Result.ok();
    }

    /**
     * find the common follow users
     *
     * @param id
     * @return
     */
    public Result followCommons(Long id) {
        String key1 = RedisConstants.FOLLOWERS_KEY_PREFIX + id;
        // get current user
        UserDTO user = UserHolder.getUser();
        String key2 = RedisConstants.FOLLOWERS_KEY_PREFIX + user.getId();
        Set<String> ids = stringRedisTemplate.opsForSet().intersect(key1, key2);
        if (ids == null || ids.isEmpty()) {
            // no intersection, so return an empty list
            return Result.ok(Collections.emptyList());
        }
        // convert ids string to long
        List<Long> userIds = ids.stream().map(Long::valueOf).collect(Collectors.toList());
        // get the corresponding users in DB by ids
        List<User> users = userService.listByIds(userIds);
        // convert user to userDTO
        List<UserDTO> collect = users.stream()
                .map(u -> BeanUtil.copyProperties(u, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(collect);

    }

    /**
     * add som user to redis for test
     */
    @PostConstruct
    public void init() {
        // put 10 users into redis
        Long followerId = 10L;
        for (long i = 0; i <= 6; i++) {
            User user = userService.getById(i);
            if (user != null) {
                boolean exist = this.count(new LambdaQueryWrapper<Follow>()
                        .eq(Follow::getUserId, followerId)
                        .eq(Follow::getFollowUserId, i)
                ) > 0;

                if (!exist) {
                    Follow follow = new Follow();
                    follow.setUserId(followerId);
                    follow.setFollowUserId(i);
                    this.save(follow);  // write to DB
                }
                String key = RedisConstants.FOLLOWERS_KEY_PREFIX + i;
                stringRedisTemplate.opsForSet().add(key, followerId.toString());
            }
        }
    }
}
