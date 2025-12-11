package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.constants.SystemConstants;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisData;
import jodd.util.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.constants.RedisConstants.*;

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
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    private static final ExecutorService CACHE_REBUILD_EXECUTOR =
            Executors.newFixedThreadPool(5);

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public Result queryById(Long id) {
        // Shop shop = queryWithPassThrough(id);
//        saveShopToRedis(id, CACHE_SHOP_TTL);
        Shop shop = queryWithMutex(id);
        if (shop == null) {
            return Result.fail("shop doesn't exist");
        }
        return Result.ok(shop);
    }

    /**
     * @param typeId
     * @param current 代表查第几页的内容
     * @param x
     * @param y
     * @return
     */
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 检查坐标是否为空，为空则正常查分页查询
        if (x == null || y == null) {
            Page<Shop> page = lambdaQuery()
                    .eq(Shop::getTypeId, typeId)
                    .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }
        // 计算分页参数
        int startIdx = (current - 1) * SystemConstants.MAX_PAGE_SIZE;
        int endIdx = current * SystemConstants.MAX_PAGE_SIZE;

        // 查redis 按距离排序， 分页
        String key = GEO_SHOPTYPE_PREFIX + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(
                key,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(endIdx));
        // limit只能从0 查到endIndex， 无法从startIdx截取，所以查出来之后，手动截取
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        // results 的size是<= endIdx 的, startIdx 表示要从当前查询中跳过的数量
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> contents = results.getContent();
        if (startIdx >= contents.size()) {
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = new ArrayList<>();
        Map<Long, Double> distanceMap = new HashMap<>();
        contents.stream().skip(startIdx).forEach(content -> {
            // content 里面寸了一个geoLocation和这个location到指定圆心的距离
            // 从geoLocation里面解析出shopId
            String shopId = content.getContent().getName();
            ids.add(Long.valueOf(shopId));
            // 解析距离
            Distance distance = content.getDistance();
            distanceMap.put(Long.valueOf(shopId), distance.getValue());
        });
        // 根据shopIds 查询出当前也的shop
        String join = StrUtil.join(",", ids);
        List<Shop> shopList = lambdaQuery().in(Shop::getId, ids).last("order by field(id," + join + ")").list();
        // 对每一个shop还要set 距离用户的距离
        for (Shop shop : shopList) {
            shop.setDistance(distanceMap.get(shop.getId()));
        }
        return Result.ok(shopList);
    }

    // 利用逻辑过期时间解决缓存击穿问题：本质问题是避免多线程同时重建缓存业务，本质是保证重建缓存业务的时候只有一个线程去做。
    private Shop queryWithLogicalExpire(Long id) {
        // 查缓存，查RedisData，里面存有过期时间, 存的时候也应该是RedisData）
        String shopKey = CACHE_SHOP_KEY + id;
        String redisDataJson = stringRedisTemplate.opsForValue().get(shopKey);

        // 未击中缓存，按理来说一般是肯定会击中缓存，活动商品都是提前存入缓存的（所以这里预设是redis有我们缓存的商铺，否则请求不到商铺信息）
        if (StringUtil.isBlank(redisDataJson)) {
            return null;
        }
        RedisData redisData = JSONUtil.toBean(redisDataJson, RedisData.class);
        // data的类型是Object， 默认会转成JSONObject，但是不能强转成Shop
        JSONObject jsonObject = (JSONObject) redisData.getData();
        Shop shop = BeanUtil.toBean(jsonObject, Shop.class);
        if (shop == null) {
            return null;
        }
        // 击中缓存则查询逻辑过期时间
        LocalDateTime expireTime = redisData.getExpireTime();
        // 未过期，返回当前数据
        if (expireTime.isAfter(LocalDateTime.now())) {
            return shop;
        }
        // 已过期，获取锁，开启新线程（使用线程池）重构缓存业务逻辑
        boolean notLocked = tryGetLock(LOCK_SHOP_KEY_PREFX + id);
        if (notLocked) {
            // 成功获取到锁, 查询数据库，缓存新的内容—— redisData
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    saveShopToRedis(id, CACHE_SHOP_TTL);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(LOCK_SHOP_KEY_PREFX + id);
                }
            });
        }

        // 未成功获取到锁，业务重建任务已经有人在做，返回过期数据就好
        return shop;
    }


    // 利用互斥锁的方式避免缓存击穿问题：热点key问题，默认key
    private Shop queryWithMutex(Long id) {
        //  查询redis
        String shopKey = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
        // 击中缓存，返回结果
        if (StringUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        // 击中缓存的空值，返回null， 默认查询的内容在数据库应当存在，不存在说明没参加活动
        if ("".equals(shopJson)) {
            return null;
        }

        // 缓存未击中， 重建缓存业务（mutex），持有锁去查询数据库
        String lockKey = LOCK_SHOP_KEY_PREFX + id;
        Shop shop = null;
        try {
            // a. 获取锁
            boolean notLocked = tryGetLock(lockKey);
            if (!notLocked) {
                // 未成功获取到锁，开始重建缓存业务， 说明有其他的线程获取到锁了，可以休眠一会，然后重新看看能不能呢个获取到缓存；
                Thread.sleep(2000);
                // 不建议使用递归容易导致栈溢出，最好使用循环多使用几次，看后面的循环实现。
                return queryWithMutex(id);

            }
            // b. 成功获取到锁，查询数据库，将结果存到缓存中，释放锁
            shop = this.getById(id);
            if (shop == null) {
                // 存入空值,返回空值，也是避免缓存穿透的方法
                stringRedisTemplate.opsForValue().set(shopKey, "", CACHE_SHOP_TTL, TimeUnit.MINUTES);
                return null;
            }
            // shop  不是null， 将shop存入缓存：先有问题——高并发下 TTL 一样，可能出现缓存同时失效——加随机过期时间，避免缓存雪崩
            stringRedisTemplate.opsForValue().set(shopKey, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 释放锁
            unlock(lockKey);
        }

        return shop;
    }

    // 利用缓存空值的方法解决缓存穿透的问题
    private Shop queryWithPassThrough(Long id) {
        // 查询redis
        String shopKey = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);

        // 击中缓存，返回结果
        if (StringUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //  击中缓存的空值，代表数据库没有，也返回，返回空值
        if ("".equals(shopJson)) {
            return null;
        }

        // 未击中，查询数据库
        Shop shop = this.getById(id);
        log.info("current shop {}", shop);

        // 数据库未击中，写入空值到redis，避免对不存在的内容反复的查询数据库，返回结果
        if (shop == null) {
            stringRedisTemplate.opsForValue().set(shopKey, "", CACHE_SHOP_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 数据库查询到内容，缓存到redis，返回shop
        stringRedisTemplate.opsForValue().set(shopKey, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    private boolean tryGetLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    private void saveShopToRedis(Long id, long l) {
        Shop shop = this.getById(id);
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(l));
        String json = JSONUtil.toJsonStr(redisData);
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, json);
    }

    /*private Shop queryWithMutex(Long id) {
        String shopKey = CACHE_SHOP_KEY + id;
        int retry = 0;
        while (retry < 5) {
            String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
            if (StringUtil.isNotBlank(shopJson)) return JSONUtil.toBean(shopJson, Shop.class);
            if ("".equals(shopJson)) return null;

            String lockKey = LOCK_SHOP_KEY_PREFX + id;
            if (tryGetLock(lockKey)) {
                try {
                    Shop shop = this.getById(id);
                    if (shop == null) {
                        stringRedisTemplate.opsForValue().set(shopKey, "", 5 + new Random().nextInt(5), TimeUnit.MINUTES);
                        return null;
                    }
                    stringRedisTemplate.opsForValue().set(shopKey, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL + new Random().nextInt(5), TimeUnit.MINUTES);
                    return shop;
                } finally {
                    unlock(lockKey);
                }
            } else {
                Thread.sleep(50 + new Random().nextInt(50)); // 随机短暂休眠
                retry++;
            }
        }
        // 重试多次失败，可选择直接查询数据库或返回异常
        return this.getById(id);
    }*/
}
