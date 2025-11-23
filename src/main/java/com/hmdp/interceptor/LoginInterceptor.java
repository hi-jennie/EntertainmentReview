package com.hmdp.interceptor;

import com.hmdp.dto.UserDTO;
import com.hmdp.utils.UserHolder;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

// This interceptor is straightforward: it only intercepts requests to APIs that require login.
public class LoginInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request,
                             HttpServletResponse response,
                             Object handler) throws Exception {
        // 1. get userDTO from threadLocal
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            response.setStatus(401);
            return false;
        }
        return true;
    }

    /*@Override
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
        UserHolder.saveUser((UserDTO) user);
        return true;
    }*/

}
