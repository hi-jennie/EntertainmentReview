package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.constants.RedisConstants;
import com.hmdp.constants.SystemConstants;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.constants.RedisConstants.USER_SIGN_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author
 * @since 2021-12-22
 */
@Service
@Slf4j
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    public Result sendSMSCode(String phone, HttpSession session) {
        // 1. validate the format of the phone number
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("the phone format is invalid");
        }

        // 2. generate a code of 5 digits
        String code = RandomUtil.randomNumbers(6);

        // 3. save the code in session
        // session.setAttribute(SystemConstants.SMS_CODE, code);

        // refactor 3. save the code in redis
        stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY, code, RedisConstants.LOGIN_CODE_TTL, TimeUnit.SECONDS);

        // 4. send sms code, but here, assuming that I send it
        log.debug("send the SMS code {}", code);

        return Result.ok();
    }

    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 1. validate the format of the phone number
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            //手机号不符合
            return Result.fail("phone is not in correct format");
        }

        // 2 validate the SMS code in redis
        String cacheSMSCode = stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY);
        String code = loginForm.getCode();
        if (cacheSMSCode == null || !cacheSMSCode.equals(code)) {
            return Result.fail("SMS code is not correct");
        }
        // TODO or 2.2 validate the password

        // 3. find the phone already exist (using mybatis plus, convenient for single table) select * from user where phone = ?
        User user = query().eq("phone", loginForm.getPhone()).one(); // SELECT * FROM user WHERE phone = ? LIMIT 1;

        // 4. if user not exists, save the user in session
        if (user == null) {
            user = createUserWithPhone(loginForm.getPhone());
        }
        // 5. convert userDTO to map
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> map = BeanUtil.beanToMap(userDTO, new HashMap<>(), CopyOptions.create().ignoreNullValue().setFieldValueEditor((field, value) -> value.toString()));

        // 6. generate a token
        String token = UUID.randomUUID().toString(true);
        // 7. save token in redis
        stringRedisTemplate.opsForHash().putAll(RedisConstants.LOGIN_USER_KEY + token, map);
        // 8. set timeout for token
        stringRedisTemplate.expire(RedisConstants.LOGIN_USER_KEY + token, RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);
        return Result.ok(token);
    }

    /**
     * using bitmap to record the sign info
     * each month has its own bitmap
     *
     * @return
     */
    public Result sign() {
        Long userId = UserHolder.getUser().getId();
        LocalDateTime now = LocalDateTime.now();
        String yyyyMM = now.format(DateTimeFormatter.ofPattern("yyyyMM"));
        String key = USER_SIGN_KEY + yyyyMM + userId;
        //然后要知道我知道设置的是当前这个月的第几天的bit值，第几天的打卡
        int dayOfMonth = now.getDayOfMonth();
        // 写入redis
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);
        return Result.ok();
    }

    /**
     * 查询连续打卡的天数
     *
     * @return
     */
    public Result signCount() {
        Long userId = UserHolder.getUser().getId();
        LocalDateTime now = LocalDateTime.now();
        String yyyyMM = now.format(DateTimeFormatter.ofPattern("yyyyMM"));
        String key = USER_SIGN_KEY + yyyyMM + userId;
        //然后要知道我知道设置的是当前这个月的第几天的bit值，第几天的打卡
        int dayOfMonth = now.getDayOfMonth();
        // 获取对应的bit值,从 0 开始，要 dayOfMonth 位，无符号；由于可以设置多个子命令，所以List<Long> 会收集多个子命令的返回结果，目前只有一个命令，所以一个返回结果
        List<Long> result = stringRedisTemplate.opsForValue().bitField(key, BitFieldSubCommands.create().get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0));

        if (result == null || result.isEmpty()) {
            return Result.ok(0);
        }

        Long num = result.get(0); // 是一个十进制的值
        // String binaryString = Long.toBinaryString(num); 可以转成二进制字符串然后循环遍历，不适用位运算也可以
        int counter = 0;
        while (true) {
            if ((num & 1) == 1) {
                counter++;
            } else {
                break;
            }
            // 向右移一位
            num >>>= 1;
        }
        return Result.ok(counter);
    }


    /*
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 1.1 validate the SMS code
        String cacheSMSCode = (String) session.getAttribute(SystemConstants.SMS_CODE);
        String cacheSMSCode = stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY);/
        String code = loginForm.getCode();
        if (cacheSMSCode == null || !cacheSMSCode.equals(code)) {
            return Result.fail("SMS is not correct");
        }

        // 2. find the phone already exist (using mybatis plus, convenient for single table) select * from user where phone = ?
        User user = query().eq("phone", loginForm.getPhone()).one(); // SELECT * FROM user WHERE phone = ? LIMIT 1;

        // 3.1 if user not exists, save the user in session
        if (user == null) {
            user = createUserWithPhone(loginForm.getPhone());
        }

        // 4. save user in session
        session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));

        return Result.ok();
    }
     */

    private User createUserWithPhone(String phone) {
        User newUser = new User();
        newUser.setPhone(phone);
        newUser.setNickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        newUser.setCreateTime(LocalDateTime.now());
        newUser.setUpdateTime(LocalDateTime.now());
        baseMapper.insert(newUser); // baseMapper is UseMapper
        return newUser;
    }
}
