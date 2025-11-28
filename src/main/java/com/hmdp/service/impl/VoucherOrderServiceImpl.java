package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisIdGenerator;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

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

    @Resource
    private IVoucherService voucherService;

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Autowired
    private RedisIdGenerator redisIdGenerator;


    public Result seckillVoucher(Long voucherId) {
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
        synchronized (userId.toString().intern()) {
            // 获取当前类的代理对象，以保证事务能够生效
            IVoucherOrderService voucherOrderService = (IVoucherOrderService) AopContext.currentProxy();
            return voucherOrderService.getResult(voucherId);
        }

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
