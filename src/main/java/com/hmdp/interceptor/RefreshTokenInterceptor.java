package com.hmdp.interceptor;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.constants.RedisConstants;
import com.hmdp.dto.UserDTO;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

// this is general interceptor, all the path will be intercepted to do something
// but actually, no one will be intercepted
@Slf4j
public class RefreshTokenInterceptor implements HandlerInterceptor {
    private StringRedisTemplate stringRedisTemplate;

    public RefreshTokenInterceptor(StringRedisTemplate redisTemplate) {
        this.stringRedisTemplate = redisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request,
                             HttpServletResponse response,
                             Object handler) throws Exception {
        // 1. get header to get token
        String token = request.getHeader("authorization");

        // 2. if token is null, let it pass, show that the user haven't logged in yet;
        if (StringUtils.isEmpty(token)) {
            //token not exist, haven't logged in yet;
            return true;
        }

        // 3. if token is not null, check if the token is valid
        // using this token to get the userDTO stored in redis, entries will return an empty map if nothing is found(if return null);
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(RedisConstants.LOGIN_USER_KEY + token);
        if (userMap.isEmpty()) {
            return true; // token过期了； token乱写的之类的
        }
        // 4. convert userMap to userDTO and save in ThreadLocal
        UserHolder.saveUser(BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false));
        log.info("current or me", UserHolder.getUser());

        // 5. refresh the token TTL
        stringRedisTemplate.expire(RedisConstants.LOGIN_USER_KEY + token, RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, @Nullable Exception ex) throws Exception {
        // remove the user from ThreadLocal
        UserHolder.removeUser();
    }
}
