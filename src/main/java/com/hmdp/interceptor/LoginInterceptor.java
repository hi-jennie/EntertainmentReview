package com.hmdp.interceptor;

import com.hmdp.entity.User;
import com.hmdp.utils.UserHolder;
import org.springframework.lang.Nullable;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class LoginInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request,
                             HttpServletResponse response,
                             Object handler) throws Exception {
        //  1. get Httpsession from request first
        HttpSession session = request.getSession();
        // 2. get use from httpsession
        Object user = session.getAttribute("user");

        // 3.1 if not exist, intercept(return false)
        if (user == null) {
            response.setStatus(401); // unauthorized
            return false;
        }

        // 3.2 if exists, save the user in ThreadLocal
        UserHolder.saveUser((User) user);
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, @Nullable Exception ex) throws Exception {
        // remove the user from ThreadLocal
        UserHolder.removeUser();
    }

}
