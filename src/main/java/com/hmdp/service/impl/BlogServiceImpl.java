package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.constants.SystemConstants;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.constants.RedisConstants.BLOG_LIKE_KEY_PREFIX;
import static com.hmdp.constants.RedisConstants.FEED_KEY_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author Jennie
 * @since 2021-12-22
 */
@Service
@Slf4j
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IBlogService blogService;
    @Resource
    private IUserService userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IFollowService followService;

    @Override
    public Result queryBlogById(Long id) {
        //  查询博客
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("blog does not exist");
        }

        // set the info about the user part
        setUserInfo(blog);
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = blogService.query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            setUserInfo(blog);
            isBlogLiked(blog); // this is to set the isLiked field
        });
        return Result.ok(records);
    }

    // this is more like toggle like status
    public Result updateLike(Long id) {
        // 查需redis，看当前用户是否点过赞了（查询用户id是否在点在列表里）
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("please log in first");
        }
        String key = BLOG_LIKE_KEY_PREFIX + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, user.getId().toString());
        if (score == null) {
            // 表明用户没有点过赞，可以点赞
            // 更新数据库当前blog的点赞数量
            boolean isSuccess = update().setSql("liked=liked+1").eq("id", id).update();
            // 把用户放进redis的点在列表里
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().add(BLOG_LIKE_KEY_PREFIX + id, user.getId().toString(), System.currentTimeMillis());
            }
        } else {
            // 表明用户已经点过赞了，则取消点赞
            boolean isSuccess = update().setSql("liked=liked-1").eq("id", id).update();
            // 把用户放进redis的点在列表里
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().remove(BLOG_LIKE_KEY_PREFIX + id, user.getId().toString());
            }
        }
        return Result.ok();
    }

    public Result queryBlogTop5Likes(Long id) {
        // 从redis中获取top 5 的userIds
        String key = BLOG_LIKE_KEY_PREFIX + id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 6);
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }

        // get the userId
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String joinedIds = StrUtil.join(",", ids);
        List<UserDTO> userDTOS = userService.lambdaQuery()
                .in(User::getId, ids)
                .last("order by field(id," + joinedIds + ")")
                .list()
                .stream().map(user ->
                        BeanUtil.copyProperties(user, UserDTO.class)
                ).collect(Collectors.toList());
        //返回
        return Result.ok(userDTOS);
    }

    private void setUserInfo(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        if (user == null) {
            //  万一blog指向的用户已经删除，可以直接设置一个假的用户
            blog.setName("未知用户");
            blog.setIcon(null);
            return;
        }
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    // 判断当前用户是否点赞过该blog
    private void isBlogLiked(Blog blog) {
        UserDTO user = UserHolder.getUser();
        // 如果用户未登录，可以直接返回，不用再查询了
        if (user == null) {
            return;
        }
        Long userId = user.getId();
        // 判断当前用户是否点过赞（判断当前用户是否在redis的zset里）
        String key = BLOG_LIKE_KEY_PREFIX + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }


    public Result saveBlog(Blog blog) {
        // 获取当前用户
        UserDTO currUser = UserHolder.getUser();
        blog.setUserId(currUser.getId());
        Boolean isSuccess = save(blog);
        if (!isSuccess) {
            return Result.fail("Fail to add blog");
        }

        // 获取当前用户的所有的follower，把blog id推送给他们
        List<Follow> follows = followService.lambdaQuery()
                .eq(Follow::getFollowUserId, currUser.getId())
                .list();

        // 把blog ID存进对应的粉丝的redis账户
        for (Follow follow : follows) {
            // get follower id
            Long userId = follow.getUserId();
            String key = FEED_KEY_PREFIX + userId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        return Result.ok(blog.getId());
    }


    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 获取当前用户
        UserDTO currUser = UserHolder.getUser();
        String key = FEED_KEY_PREFIX + currUser.getId();
        // 获取当前用用户的关注feed流，分页查询2条数据
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet().reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }

        List<Long> blogIds = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int currOffset = 1;
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            String blogId = typedTuple.getValue();
            blogIds.add(Long.valueOf(blogId));
            long time = typedTuple.getScore().longValue();
            if (time == minTime) {
                currOffset++;
            } else {
                minTime = time;
                currOffset = 1;
            }
        }
        // 根据blogIds 查询对应的blog
        List<Blog> blogList = new ArrayList<>();
        for (Long blogId : blogIds) {
            Blog blog = getById(blogId);
            setUserInfo(blog);
            isBlogLiked(blog);
            blogList.add(blog);
        }
        //封装 返回
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogList);
        scrollResult.setOffset(currOffset);
        scrollResult.setMinTime(minTime);
        return Result.ok(scrollResult);
    }
}
