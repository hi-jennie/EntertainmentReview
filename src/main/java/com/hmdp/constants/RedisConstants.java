package com.hmdp.constants;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final long LOGIN_CODE_TTL = 30L;
    public static final long LOGIN_USER_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";
    public static final long CACHE_SHOP_TTL = 30L;
    public static final String LOCK_SHOP_KEY_PREFX = "lock:shop:";
    public static final long LOCK_SHOP_TTL = 30L;
}
