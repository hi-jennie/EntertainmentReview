package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisIdGenerator;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    // 类加载的时候加载的静态资源，lua脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    private static final BlockingQueue<VoucherOrder> blockingQueue = new ArrayBlockingQueue<>(1024 * 1024);

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private volatile boolean running = true;
    @Resource
    private IVoucherService voucherService;
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Autowired
    private RedisIdGenerator redisIdGenerator;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private RedissonClient redissonClient;

    /*@PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(() -> {
            while (true) {
                // 从阻塞队列取出来，然后向数据库去创建订单
                VoucherOrder voucherOrder = blockingQueue.take();
                // 创建订单
                handleVoucherOrder(voucherOrder);
            }
        });
    }*/

    // 现在监听redis 里面order queue的信息
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(() -> {
            String queueName = "stream.orders";
            while (running) {
                try {
                    //从消息队列中获取订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1")
                            , StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2))
                            , StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //判断消息时候获取成功
                    if (list == null || list.isEmpty()) {
                        //获取失败 没有消息 继续循环
                        continue;
                    }
                    //获取成功 解析消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    record.getId();
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //下单
                    handleVoucherOrder(voucherOrder);
                    //ack确认消息
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    e.printStackTrace();
                    handlePendingList();
                }
            }
        });
    }

    @PreDestroy
    public void destroy() {
        running = false;
        SECKILL_ORDER_EXECUTOR.shutdown(); // 也可以 shutdownNow()
    }

    private void handlePendingList() {
        String queueName = "stream.orders";
        while (running) {
            try {
                //从消息队列中获取订单信息
                List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", "c1")
                        , StreamReadOptions.empty().count(1)
                        , StreamOffset.create(queueName, ReadOffset.from("0"))
                );
                //判断消息时候获取成功
                if (list == null || list.isEmpty()) {
                    //获取失败 没有消息 继续循环
                    break;
                }
                //获取成功 解析消息
                MapRecord<String, Object, Object> record = list.get(0);
                Map<Object, Object> values = record.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                //下单
                handleVoucherOrder(voucherOrder);
                //ack确认消息
                stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    Thread.sleep(20);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    // 实现方法3:基于redis stream的实现方法
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户信息
        UserDTO user = UserHolder.getUser();
        // 准备orderId
        Long orderId = redisIdGenerator.nextId("order");
        // 执行lua脚本
        Long res = stringRedisTemplate.execute(SECKILL_SCRIPT
                , Collections.EMPTY_LIST
                , voucherId.toString()
                , user.getId().toString()
                , orderId.toString());
        int r = res.intValue();
        if (r != 0) {
            // r != 0 标识未执行lua脚本成功
            return Result.fail(r == 1 ? "out of stock" : "you have already placed an order for this voucher");
        }
        // 执行成功， 向前端返回orderId，请求结束
        return Result.ok(orderId);

    }

    // 实现方法1:基于JVM阻塞队列的实现方法，对redis 分布式锁的优化
    public Result seckillVoucher2(Long voucherId) {
        // 提前 将优惠卷信息和需要创建的order信息存入redis

        // 准备lua脚本需要的参数，此时需要voucherId和userId
        Long userId = UserHolder.getUser().getId();

        // 使用lua脚本执行是否有库存，是否重复下单的逻辑
        Long res = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.EMPTY_LIST, voucherId.toString(), userId.toString());
        //判断结果是否为0
        int r = res.intValue();
        if (r != 0) {
            //不为0 没有购买资格
            return Result.fail(r == 1 ? "out of stock" : "you have already placed an order for this voucher");
        }

        // this means order successfully, we can add the order to blockingQueue
        Long orderId = redisIdGenerator.nextId("order");
        VoucherOrder order = new VoucherOrder();
        order.setVoucherId(voucherId);
        order.setUserId(userId);
        order.setId(orderId);

        // add order into the blockingQueue, then the background thread will handle the order creation
        blockingQueue.add(order);
        return Result.ok(orderId);
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        // 创建锁对象, 这里其实可以不用再使用锁了，因为在脚本中已经保证了每个用户只能下单一次，但是以防万一，还是加上分布式锁
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        // 获取锁
        boolean isLock = lock.tryLock();

        if (!isLock) {
            // 获取锁失败，说明有其他线程在处理这个用户的订单
            throw new RuntimeException("发送未知错误");
        }

        try {
            // 在数据库创建订单，扣减库存
            createVoucherOrder(voucherOrder);
        } finally {
            //释放锁
            lock.unlock();
        }
    }

    @Transactional(rollbackFor = Exception.class)
    @Override
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //在数据库扣减库存
        boolean isSuccess = seckillVoucherService.update(
                new LambdaUpdateWrapper<SeckillVoucher>()
                        .eq(SeckillVoucher::getVoucherId, voucherOrder.getVoucherId())
                        .gt(SeckillVoucher::getStock, 0)
                        .setSql("stock=stock-1"));
        //在数据库创建订单
        this.save(voucherOrder);
    }

    // 实现方法1:基于redis分布式锁实现的方法
    public Result seckillVoucher1(Long voucherId) {
        // 查数据库,优惠卷是否存在
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        log.info("voucher: {}", voucher);
        // 不存在，返回错误，优惠券不存在
        if (voucher == null) {
            return Result.fail("voucher does not exist");
        }
        // 存在， 查看优惠券开始和结束时间
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("the voucher has not started yet");
        }
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("the voucher has expired");

        }
        // 处于有效期，查看库存
        if (voucher.getStock() < 1) {
            return Result.fail("the voucher is out of stock");
        }
        // 有库存，大概可以开始下单,锁住用户，避免在高并发下对同一用户多次下单
        Long userId = UserHolder.getUser().getId();
        // 2. 分布式锁实现版， 创建锁对象
        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        boolean isLock = lock.tryLock(1200);
        if (!isLock) {
            // 如果获取锁失败，代表已经有线程在处理当前用户的voucher order了
            return Result.fail("do not repeat the order");
        }

        // 获取锁成功，执行下单任务
        try {
            IVoucherOrderService voucherOrderService = (IVoucherOrderService) AopContext.currentProxy();
            return voucherOrderService.getResult(voucherId);
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }


//        1.单机系统下的锁
//        synchronized (userId.toString().intern()) {
//            // 获取当前类的代理对象，以保证事务能够生效
//            IVoucherOrderService voucherOrderService = (IVoucherOrderService) AopContext.currentProxy();
//            return voucherOrderService.getResult(voucherId);
//        }

    }

    /*
     * 为什么不把synchronized加在这个方法上？加在这个方法上会锁住整个类，相当于所有线程都只能排队执行这个方法，效率太低
     * 为什么把锁加在这个方法的外面？ 因为如果在在方法内部加锁和释放锁，那么在释放锁之后，提交事物之前，还是有可能出现并发问题
     * 所以把锁加在这个方法的外面，事物提交之后再释放锁，保证了数据的一致性
     */
    @Transactional
    public Result getResult(Long voucherId) {
        // 获取用户ID
        Long userId = UserHolder.getUser().getId();

        //  查看voucher_order表中是否存在该用户的订单
        Long count = lambdaQuery()
                .eq(VoucherOrder::getUserId, userId)
                .eq(VoucherOrder::getVoucherId, voucherId)
                .count();
        if (count > 0) {
            // 存在，说明已经下过单
            return Result.fail("you have already placed an order for this voucher");
        }

        //扣减库存，使用乐观锁的方式（版本号的方式，这里处理真正的数据库更新逻辑 stock > 0）
        UpdateWrapper<SeckillVoucher> updateWrapper = new UpdateWrapper<>();
        updateWrapper.eq("voucher_id", voucherId)
                .gt("stock", 0)
                .setSql("stock = stock - 1");
        boolean isSuccess = seckillVoucherService.update(updateWrapper);
        //  如果数据库语句未能执行成功，说明库存不足
        if (!isSuccess) {
            return Result.fail("the voucher is out of stock");
        }

        // 执行成功，说明两件事，库存充足，当前用户未下过单
        VoucherOrder voucherOrder = new VoucherOrder();
        Long orderId = redisIdGenerator.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setUserId(userId);
        voucherOrder.setCreateTime(LocalDateTime.now());
        // 保存订单
        this.save(voucherOrder);
        // 返回订单id
        return Result.ok(orderId);
    }
}
