package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.constants.SystemConstants;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.constants.RedisConstants.BLOG_LIKE_KEY_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author Jennie
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IBlogService blogService;
    @Resource
    private IUserService userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

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
}
