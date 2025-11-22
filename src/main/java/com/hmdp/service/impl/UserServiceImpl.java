package com.hmdp.service.impl;

import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;

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

    public Result sendSMSCode(String phone, HttpSession session) {
        // 1. validate the format of the phone number
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("the phone format is invalid");
        }

        // 2. 2generate a code of 5 digits
        String code = RandomUtil.randomNumbers(6);

        // 3. save the code in session
        session.setAttribute(SystemConstants.SMS_CODE, code);

        // 4. send sms code, but here, assuming that I send it
        log.debug("send the SMS code {}", code);

        return Result.ok();
    }

    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 1.1 validate the SMS code
        String cacheSMSCode = (String) session.getAttribute(SystemConstants.SMS_CODE);
        String code = loginForm.getCode();
        if (cacheSMSCode == null || !cacheSMSCode.equals(code)) {
            return Result.fail("SMS is not correct");
        }

        // TODO or 1.2 validate the password

        // 2. find the phone already exist (using mybatis plus, convenient for single table) select * from user where phone = ?
        User user = query().eq("phone", loginForm.getPhone()).one(); // SELECT * FROM user WHERE phone = ? LIMIT 1;

        // 3.1 if user not exists, save the user in session
        if (user == null) {
            user = createUserWithPhone(loginForm.getPhone());
        }

        // 4. save user in session
        session.setAttribute("user", user);

        return Result.ok();
    }

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
