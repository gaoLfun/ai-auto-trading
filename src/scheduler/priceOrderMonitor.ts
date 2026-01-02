/**
 * ai-auto-trading - AI 加密货币自动交易系统
 * Copyright (C) 2025 losesky
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Found      const checkContract = this.exchangeClient.normalizeContract(order.symbol);
      const positions = await this.exchangeClient.getPositions();
      const positionExists = positions.some(p => 
        p.contract === checkContract && Math.abs(parseFloat(p.size || '0')) > 0
      );, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

/**
 * 条件单监控服务
 * 定期检测条件单触发情况，更新数据库状态，记录平仓交易
 */
import { createLogger } from "../utils/logger";
import { getChinaTimeISO } from "../utils/timeUtils";
import { getQuantoMultiplier } from "../utils/contractUtils";
import { FeeService } from "../services/feeService";
import { extractOrderId, createOrderIdMap } from "../utils/orderIdExtractor";
import type { Client } from "@libsql/client";
import type { IExchangeClient } from "../exchanges/IExchangeClient";

/**
 * 统一格式化成交数据，兼容币安和Gate.io
 */
function formatTradeRecord(trade: any): {
  id: string;
  price: string;
  size: string;
  fee: string;
  timestamp: number;
} {
  return {
    id: trade.id?.toString() || trade.orderId?.toString() || trade.tradeId?.toString() || '',
    price: trade.price?.toString() || trade.avgPrice?.toString() || trade.deal_price?.toString() || '0',
    size: trade.size?.toString() || trade.qty?.toString() || trade.amount?.toString() || '0',
    fee: trade.fee?.toString() || trade.commission?.toString() || trade.fee_amount?.toString() || '0',
    timestamp: Number(trade.timestamp || trade.time || trade.create_time || Date.now()),
  };
}

const logger = createLogger({
  name: "price-order-monitor",
  level: "info",
});

interface DBPriceOrder {
  id: number;
  order_id: string;
  symbol: string;
  side: 'long' | 'short';
  type: 'stop_loss' | 'take_profit';
  trigger_price: string;
  quantity: string;
  created_at: string;
}

export class PriceOrderMonitor {
  private checkInterval: NodeJS.Timeout | null = null;
  private isRunning = false;
  private feeService: FeeService;
  
  constructor(
    private dbClient: Client,
    private exchangeClient: IExchangeClient
  ) {
    this.feeService = new FeeService(exchangeClient);
  }

  /**
   * 启动监控服务
   */
  async start() {
    if (this.checkInterval) {
      logger.warn('条件单监控服务已在运行');
      return;
    }

    const intervalSeconds = parseInt(process.env.PRICE_ORDER_CHECK_INTERVAL || '30');
    logger.info(`🚀 启动条件单监控服务，检测间隔: ${intervalSeconds}秒`);
    logger.info(`📋 环境变量 PRICE_ORDER_CHECK_INTERVAL = ${process.env.PRICE_ORDER_CHECK_INTERVAL || '(未设置，使用默认30秒)'}`);

    // 立即执行第一次检测，捕获系统离线期间触发的条件单
    logger.info('立即执行首次检测，捕获系统离线期间的平仓事件...');
    await this.checkTriggeredOrders();

    // 定期执行
    this.checkInterval = setInterval(async () => {
      await this.checkTriggeredOrders();
    }, intervalSeconds * 1000);
  }

  /**
   * 停止监控服务
   */
  stop() {
    if (this.checkInterval) {
      clearInterval(this.checkInterval);
      this.checkInterval = null;
      logger.info('条件单监控服务已停止');
    }
  }

  /**
   * 检测已触发的条件单
   */
  private async checkTriggeredOrders() {
    if (this.isRunning) {
      logger.debug('⏭️  上一次检测尚未完成，跳过本次检测');
      return;
    }

    this.isRunning = true;
    const startTime = Date.now();
    
    try {
      // 1. 获取数据库中active的条件单
      const activeOrders = await this.getActiveOrdersFromDB();
      if (activeOrders.length === 0) {
        logger.debug('✅ 没有活跃的条件单需要检测');
        return;
      }

      // 🔧 过滤掉刚创建的条件单（30秒保护窗口）
      // 原因：币安测试网API响应慢，刚创建的条件单可能1-2分钟内查不到，避免误判为"消失"
      const now = Date.now();
      const GRACE_PERIOD_MS = 30 * 1000; // 30秒保护窗口（币安测试网需要更长时间）
      const ordersToCheck = activeOrders.filter(order => {
        const createdAt = new Date(order.created_at).getTime();
        const age = now - createdAt;
        if (age < GRACE_PERIOD_MS) {
          logger.debug(`⏳ 跳过刚创建的条件单: ${order.symbol} ${order.type} (创建时间: ${Math.floor(age/1000)}秒前)`);
          return false;
        }
        return true;
      });

      if (ordersToCheck.length === 0) {
        logger.debug('✅ 所有活跃条件单都在保护窗口内，无需检测');
        return;
      }

      logger.debug(`🔍 检测 ${ordersToCheck.length}/${activeOrders.length} 个活跃条件单（已过滤刚创建的）...`);

      // 2. 获取交易所的条件单
      let exchangeOrders: any[] = [];
      try {
        exchangeOrders = await this.exchangeClient.getPriceOrders();
        logger.debug(`📋 交易所返回 ${exchangeOrders.length} 个条件单`);
      } catch (error: any) {
        logger.warn('⚠️ 无法从交易所获取条件单列表，跳过本次检测（可能是API错误）:', error.message);
        return;
      }
      
      // 构建交易所订单映射表，使用统一的订单ID提取工具
      const exchangeOrderMap = createOrderIdMap(exchangeOrders);
      
      logger.debug(`🔑 交易所订单ID映射: [${Array.from(exchangeOrderMap.keys()).join(', ')}]`);

      // 3. 同时获取交易所实际持仓状态（关键补充）
      let exchangePositions: any[] = [];
      try {
        exchangePositions = await this.exchangeClient.getPositions();
      } catch (error: any) {
        logger.warn('⚠️ 无法获取交易所持仓信息:', error.message);
      }
      
      // 建立持仓映射：contract -> position
      const exchangePositionMap = new Map(
        exchangePositions
          .filter(p => Math.abs(parseFloat(p.size || '0')) > 0)
          .map(p => [p.contract, p])
      );

      // 4. 识别已触发的条件单
      // 🔧 核心优化：记录初始条件单状态，用于检测状态变化
      const initialOrderStates = new Map<string, boolean>(
        ordersToCheck.map(order => [order.order_id, exchangeOrderMap.has(order.order_id)])
      );
      
      for (const dbOrder of ordersToCheck) {
        try {
          const contract = this.exchangeClient.normalizeContract(dbOrder.symbol);
          let orderInExchange = exchangeOrderMap.has(dbOrder.order_id);
          const positionInExchange = exchangePositionMap.has(contract);
          const initialOrderState = initialOrderStates.get(dbOrder.order_id) || false;
          
          logger.debug(`🔍 检查条件单: ${dbOrder.symbol} ${dbOrder.type} ID=${dbOrder.order_id}, 在交易所=${orderInExchange}, 持仓存在=${positionInExchange}`);
          
          // 🔧 智能修复：如果数据库中的条件单ID在交易所不存在，
          // 但交易所有该合约的条件单，尝试同步更新数据库ID
          if (!orderInExchange && positionInExchange) {
            // 查找交易所中该合约的条件单
            const exchangeContractOrders = exchangeOrders.filter((o: any) => {
              const oContract = this.exchangeClient.normalizeContract(o.contract || o.symbol || '');
              return oContract === contract;
            });
            
            // 尝试根据类型匹配（stop_loss 或 take_profit）
            const matchingOrder = exchangeContractOrders.find((o: any) => {
              // Gate.io: rule=1表示>=触发(多单止盈/空单止损), rule=2表示<=触发(多单止损/空单止盈)
              // 从trigger规则推断订单类型
              if (o.trigger && o.trigger.rule !== undefined) {
                const isSellOrder = o.initial && parseFloat(o.initial.size || '0') < 0;
                const isLongPosition = dbOrder.side === 'long';
                
                if (dbOrder.type === 'stop_loss') {
                  // 止损: 多单用rule=2(<=), 空单用rule=1(>=)
                  return isLongPosition ? o.trigger.rule === 2 : o.trigger.rule === 1;
                } else if (dbOrder.type === 'take_profit') {
                  // 止盈: 多单用rule=1(>=), 空单用rule=2(<=)
                  return isLongPosition ? o.trigger.rule === 1 : o.trigger.rule === 2;
                }
              }
              return false;
            });
            
            if (matchingOrder) {
              // 使用统一的订单ID提取工具
              const newOrderId = extractOrderId(matchingOrder);
              if (newOrderId && newOrderId !== dbOrder.order_id) {
                logger.info(`🔄 检测到条件单ID不匹配，自动同步: ${dbOrder.order_id} → ${newOrderId}`);
                
                // 更新数据库中的条件单ID
                try {
                  await this.dbClient.execute({
                    sql: 'UPDATE price_orders SET order_id = ?, updated_at = ? WHERE order_id = ?',
                    args: [newOrderId, new Date().toISOString(), dbOrder.order_id]
                  });
                  
                  // 更新本地对象
                  dbOrder.order_id = newOrderId;
                  orderInExchange = true; // 现在在交易所中了
                  
                  logger.info(`✅ 条件单ID已同步更新到数据库`);
                } catch (updateError: any) {
                  logger.error(`❌ 更新条件单ID失败: ${updateError.message}`);
                }
              }
            }
          }
          
          // 🔧 核心改进：多层次触发检测逻辑
          // 
          // 检测条件：
          // 1. 条件单状态变化（从存在到不存在）- 最可靠的触发信号
          // 2. 条件单不存在 + 持仓不存在 - 确定触发
          // 3. 条件单不存在 + 持仓存在 + 有成交记录 - 触发中（等待持仓完全平仓）
          // 4. 条件单不存在 + 持仓存在 + 价格穿越触发线 - 可能触发（容错处理）
          
          if (!orderInExchange) {
            // 场景1：条件单消失
            let shouldHandle = false;
            let detectionReason = '';
            
            if (!positionInExchange) {
              // 1a. 订单没了，持仓也没了 - 确定触发
              shouldHandle = true;
              detectionReason = '条件单和持仓均已消失';
              logger.info(`🔍 ${dbOrder.symbol} ${detectionReason}，确认触发: ${dbOrder.order_id}`);
            } else {
              // 1b. 订单没了，但持仓还在 - 需要深入分析
              logger.debug(`🔍 ${dbOrder.symbol} 条件单已消失但持仓存在，深入分析: ${dbOrder.order_id}`);
              
              // 先检查是否有平仓成交记录
              const closeTrade = await this.findCloseTrade(dbOrder);
              
              if (closeTrade) {
                // 有成交记录 - 确认触发，持仓正在平仓中
                shouldHandle = true;
                detectionReason = '条件单消失且有平仓成交记录';
                logger.info(`🔍 ${dbOrder.symbol} ${detectionReason}: ${dbOrder.order_id}`);
              } else {
                // 没有成交记录 - 检查价格是否穿越触发线
                // 🔧 条件单监控使用实时价格（跳过缓存）以获得最新触发状态
                try {
                  const currentTicker = await this.exchangeClient.getFuturesTicker(contract, 2, { skipCache: true });
                  const currentPrice = parseFloat(currentTicker.last || '0');
                  const triggerPrice = parseFloat(dbOrder.trigger_price);
                  
                  let priceCrossed = false;
                  if (dbOrder.type === 'stop_loss') {
                    priceCrossed = dbOrder.side === 'long' 
                      ? currentPrice <= triggerPrice 
                      : currentPrice >= triggerPrice;
                  } else {
                    priceCrossed = dbOrder.side === 'long'
                      ? currentPrice >= triggerPrice
                      : currentPrice <= triggerPrice;
                  }
                  
                  if (priceCrossed) {
                    // 价格已穿越触发线 - 很可能触发了，但成交记录还没返回
                    shouldHandle = true;
                    detectionReason = `条件单消失且价格已穿越触发线(当前=${currentPrice.toFixed(2)}, 触发=${triggerPrice.toFixed(2)})`;
                    logger.info(`🔍 ${dbOrder.symbol} ${detectionReason}: ${dbOrder.order_id}`);
                  } else {
                    // 🔧 关键修复：价格未穿越 - 条件单被取消/过期，需要立即重建（特别是止损单）
                    detectionReason = '条件单消失但价格未穿越触发线，可能被手动取消或过期';
                    logger.warn(`⚠️ ${dbOrder.symbol} ${detectionReason}: ${dbOrder.order_id}`);
                    
                    // 🚨 如果是止损单失效，必须立即重建以保护持仓
                    if (dbOrder.type === 'stop_loss') {
                      logger.error(`🚨 严重风险：止损单失效且持仓仍存在！立即自动重建止损保护...`);
                      await this.recreateStopLossOrder(dbOrder);
                    } else {
                      // 止盈单被取消，仅记录日志（不影响风控）
                      logger.info(`${dbOrder.symbol} ${dbOrder.side} 止盈单被取消或过期，仅更新状态`);
                      await this.updateOrderStatus(dbOrder.order_id, 'cancelled');
                    }
                  }
                } catch (priceError: any) {
                  logger.warn(`获取价格失败，无法判断是否触发: ${priceError.message}`);
                  
                  // 🔧 即使无法获取价格，如果是止损单消失+持仓存在，也要重建
                  if (dbOrder.type === 'stop_loss') {
                    logger.error(`🚨 止损单失效但无法获取价格，保险起见仍重建止损保护...`);
                    await this.recreateStopLossOrder(dbOrder);
                  }
                }
              }
            }
            
            if (shouldHandle) {
              logger.info(`✅ 触发检测: ${dbOrder.symbol} ${dbOrder.type} - ${detectionReason}`);
              await this.handleTriggeredOrder(dbOrder);
            }
          }
        } catch (error: any) {
          logger.error(`处理条件单 ${dbOrder.order_id} 失败:`, error);
        }
      }
    } catch (error: any) {
      logger.error('❌ 检测条件单触发失败:', error);
    } finally {
      this.isRunning = false;
      const elapsedTime = Date.now() - startTime;
      logger.debug(`⏱️  本次条件单检测完成，耗时: ${elapsedTime}ms`);
    }
  }

  /**
   * 从数据库获取活跃的条件单
   * 🔧 修复：只获取 status='active' 且未被处理过的条件单
   */
  private async getActiveOrdersFromDB(): Promise<DBPriceOrder[]> {
    const result = await this.dbClient.execute({
      sql: `SELECT id, order_id, symbol, side, type, trigger_price, quantity, created_at
            FROM price_orders
            WHERE status = 'active'
            ORDER BY symbol, created_at DESC`
    });

    return result.rows.map(row => ({
      id: row.id as number,
      order_id: String(row.order_id), // 确保 order_id 是字符串
      symbol: row.symbol as string,
      side: row.side as 'long' | 'short',
      type: row.type as 'stop_loss' | 'take_profit',
      trigger_price: row.trigger_price as string,
      quantity: row.quantity as string,
      created_at: row.created_at as string
    }));
  }

  /**
   * 处理已触发的条件单
   * 🔧 修复:添加防重复处理检查
   */
  private async handleTriggeredOrder(order: DBPriceOrder) {
    logger.debug(`🔍 检查条件单: ${order.symbol} ${order.type} ${order.order_id}`);
    
    // 🔧 关键修复1：严格的幂等性检查 - 只检查trigger_order_id
    try {
      const existingEvent = await this.dbClient.execute({
        sql: `SELECT id FROM position_close_events WHERE trigger_order_id = ? LIMIT 1`,
        args: [order.order_id]
      });
      
      if (existingEvent.rows.length > 0) {
        logger.info(`⏭️ [幂等性] 条件单 ${order.order_id} 已被处理，跳过`);
        await this.updateOrderStatus(order.order_id, 'triggered');
        return;
      }
    } catch (checkError: any) {
      logger.warn(`幂等性检查失败: ${checkError.message}`);
    }
    
    // 🔧 关键修复2：检查近期平仓记录（扩大时间窗口到2分钟）
    // 注意：只有完全平仓才需要去重，分批平仓不应该跳过
    try {
      const recentCloseTime = new Date(Date.now() - 120 * 1000).toISOString(); // 2分钟
      const recentClose = await this.dbClient.execute({
        sql: `SELECT id, close_reason, created_at, trigger_order_id FROM position_close_events 
              WHERE symbol = ? AND side = ? AND created_at > ?
              ORDER BY created_at DESC LIMIT 1`,
        args: [order.symbol, order.side, recentCloseTime]
      });
      
      if (recentClose.rows.length > 0) {
        const lastClose = recentClose.rows[0];
        const closeReason = lastClose.close_reason as string;
        
        // 🔧 核心修复：区分完全平仓和部分平仓
        // - partial_close: 部分平仓，持仓仍存在，应继续处理后续条件单触发
        // - stop_loss_triggered/take_profit_triggered/manual_close等: 完全平仓，需要去重
        if (closeReason === 'partial_close') {
          logger.debug(`检测到近期分批平仓 (${closeReason})，但持仓可能仍存在，继续处理条件单触发`);
        } else {
          // 完全平仓类型：检查是否是同一个条件单触发
          const lastTriggerOrderId = lastClose.trigger_order_id as string;
          if (lastTriggerOrderId === order.order_id) {
            logger.info(`⏭️ [去重-幂等性] 条件单 ${order.order_id} 已被处理 (${closeReason})，跳过`);
            await this.updateOrderStatus(order.order_id, 'triggered');
            await this.cancelOppositeOrderInDB(order);
            return;
          } else {
            // 不同条件单但同一持仓的完全平仓，也应该跳过（可能是手动平仓或其他条件单）
            logger.info(`⏭️ [去重] ${order.symbol} ${order.side} 在2分钟内已完全平仓 (${closeReason})，跳过当前条件单`);
            await this.updateOrderStatus(order.order_id, 'cancelled');
            return;
          }
        }
      }
    } catch (checkError: any) {
      logger.warn(`近期平仓检查失败: ${checkError.message}`);
    }

    // ========================================
    // 阶段1: 检查交易所持仓状态
    // ========================================
    const checkContract = this.exchangeClient.normalizeContract(order.symbol);
    const positions = await this.exchangeClient.getPositions();
    const positionExists = positions.some(p => 
      p.contract === checkContract && Math.abs(parseFloat(p.size || '0')) > 0
    );
    
    // 如果持仓仍存在，说明条件单被取消而非触发
    if (positionExists) {
      logger.warn(`⚠️ ${order.symbol} ${order.side} 持仓仍存在，但条件单 ${order.order_id} (${order.type}) 已失效`);
      
      // 🔧 关键修复：如果是止损单被取消，立即重新创建以保护持仓
      if (order.type === 'stop_loss') {
        logger.error(`🚨 严重风险：止损单失效！立即重新创建止损保护...`);
        await this.recreateStopLossOrder(order);
      } else {
        // 止盈单被取消，仅记录日志（不影响风控）
        logger.info(`${order.symbol} ${order.side} 止盈单被取消，仅更新状态`);
        await this.updateOrderStatus(order.order_id, 'cancelled');
      }
      
      return;
    }

    // ========================================
    // 阶段2: 查询持仓信息（用于计算PnL）
    // ========================================
    let position = await this.getPositionInfo(order.symbol, order.side);
    let entryOrderId: string | null = null;
    
    // 如果数据库中有持仓记录，同时获取 entry_order_id
    if (position) {
      entryOrderId = position.entry_order_id as string | null;
    }
    
    // 如果数据库中没有持仓记录，尝试从开仓交易记录中查找
    if (!position) {
      logger.warn(`数据库中未找到 ${order.symbol} ${order.side} 的持仓信息，尝试从交易记录查找开仓信息...`);
      const openTrade = await this.findOpenTrade(order.symbol, order.side);
      if (openTrade) {
        // 使用开仓交易信息构建持仓对象
        position = {
          symbol: openTrade.symbol,
          side: openTrade.side,
          entry_price: openTrade.price,
          quantity: openTrade.quantity,
          leverage: openTrade.leverage,
        };
        logger.info(`✅ 从交易记录恢复持仓信息: ${order.symbol} @ ${position.entry_price}`);
      }
    }

    // 阶段2: 查找平仓交易（从交易所查询实际的成交记录）
    const closeTrade = await this.findCloseTrade(order);
    
    // ⚠️ 关键修复：如果交易所没有平仓记录，需要判断是真的触发还是被取消
    //    判断依据：检查交易所持仓是否还存在
    if (!closeTrade) {
      logger.warn(`⚠️ 未找到 ${order.symbol} 的平仓交易记录，检查交易所持仓状态...`);
      
      // 检查持仓是否还存在
      const checkContract = this.exchangeClient.normalizeContract(order.symbol);
      const positions = await this.exchangeClient.getPositions();
      const positionExists = positions.some(p => 
        p.contract === checkContract && Math.abs(parseFloat(p.size || '0')) > 0
      );
      
      if (positionExists) {
        // 持仓还在，说明条件单只是被取消了，不是触发
        logger.info(`${order.symbol} 持仓仍存在，条件单可能被手动取消`);
        
        await this.dbClient.execute('BEGIN TRANSACTION');
        try {
          await this.updateOrderStatus(order.order_id, 'cancelled');
          await this.dbClient.execute('COMMIT');
          logger.info(`✅ 条件单状态已更新为cancelled`);
        } catch (error: any) {
          await this.dbClient.execute('ROLLBACK');
          logger.error('❌ 更新条件单状态失败，已回滚:', error);
        }
        return;
      }
      
      // 🚨 严重错误：持仓不存在但未找到平仓交易记录
      // 这说明系统存在严重问题，不应该用估算数据掩盖
      logger.error(`🚨 严重错误: ${order.symbol} 条件单触发但未找到成交记录`);
      logger.error(`   - 条件单ID: ${order.order_id}`);
      logger.error(`   - 类型: ${order.type}`);
      logger.error(`   - 触发价: ${order.trigger_price}`);
      logger.error(`   - 创建时间: ${order.created_at}`);
      
      // 记录不一致状态到数据库，供后续人工排查
      const timestamp = new Date().toISOString();
      
      await this.dbClient.execute('BEGIN TRANSACTION');
      try {
        // 更新条件单状态为triggered（但标注为异常）
        await this.updateOrderStatus(order.order_id, 'triggered');
        await this.cancelOppositeOrderInDB(order);
        
        // 删除持仓记录（持仓已在交易所不存在）
        await this.dbClient.execute({
          sql: 'DELETE FROM positions WHERE symbol = ? AND side = ?',
          args: [order.symbol, order.side]
        });
        
        // 记录到不一致状态表
        await this.dbClient.execute({
          sql: `INSERT INTO inconsistent_states 
                (operation, symbol, side, exchange_success, db_success, 
                 exchange_order_id, error_message, created_at, resolved)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          args: [
            'price_order_triggered_no_trade',
            order.symbol,
            order.side,
            1,  // 交易所端已平仓（持仓不存在）
            0,  // 数据库无法完整记录（找不到成交数据）
            order.order_id,
            `条件单${order.type}触发，但未找到成交记录。触发价=${order.trigger_price}, 创建时间=${order.created_at}。请检查交易所API或扩大查询时间窗口。`,
            timestamp,
            0
          ]
        });
        
        await this.dbClient.execute('COMMIT');
        logger.error(`❌ 已记录不一致状态，请人工排查！`);
        
      } catch (error: any) {
        await this.dbClient.execute('ROLLBACK');
        logger.error('❌ 记录不一致状态失败:', error);
      }
      
      return;
    }
    
    const finalCloseTrade = closeTrade;

    // 阶段3: 确认有持仓信息才继续（如果既没有持仓也没有开仓记录，无法处理）
    if (!position) {
      logger.error(`❌ 无法获取 ${order.symbol} ${order.side} 的持仓信息，无法记录平仓事件`);
      // 即使无法记录详情，也要更新条件单状态
      await this.updateOrderStatus(order.order_id, 'triggered');
      await this.cancelOppositeOrderInDB(order);
      return;
    }

    // 阶段4: 确认是真实平仓，计算盈亏
    logger.info(`🔔 确认条件单触发: ${order.symbol} ${order.type}, 平仓价格: ${finalCloseTrade.price}`);
    
    // 格式化成交数据，兼容所有交易所
    const trade = formatTradeRecord(finalCloseTrade);
    
    // 计算盈亏
    const entryPrice = parseFloat(position.entry_price as string);
    const exitPrice = parseFloat(trade.price);
    // 🔧 关键修复：使用持仓记录中的原始数量，而非成交数量
    // Gate.io 成交记录的 size 字段可能不准确，应使用开仓时的数量
    const quantity = Math.abs(parseFloat(position.quantity as string));
    const leverage = parseInt(position.leverage as string) || 1;
    const contract = this.exchangeClient.normalizeContract(order.symbol);

    const grossPnl = await this.exchangeClient.calculatePnl(
      entryPrice,
      exitPrice,
      quantity,
      order.side,
      contract
    );
    
    // 🔧 核心优化：使用 FeeService 获取真实手续费
    const contractType = this.exchangeClient.getContractType(contract);
    const quantoMultiplier = await getQuantoMultiplier(contract);
    
    // 🔧 核心修复：正确计算名义价值
    // 无论U本位还是币本位，计算公式都是：名义价值 = 张数 * 合约乘数 * 价格
    // 例如：BTC_USDT (U本位)，每张 = 0.001 BTC，160张 * 0.001 * 89826.6 = 14372.256 USDT
    // 例如：BTC_USD (币本位)，每张 = 100 USD，160张 * 100 / 89826.6 = 0.178 BTC
    const openNotionalValue = quantity * quantoMultiplier * entryPrice;
    const closeNotionalValue = quantity * quantoMultiplier * exitPrice;
    
    const closeFeeResult = await this.feeService.getFee(trade.id, contract, closeNotionalValue);
    const closeFee = closeFeeResult.fee;
    
    // 获取开仓手续费（尝试从数据库中的开仓交易记录获取）
    let openFee: number;
    try {
      const openTradeResult = await this.dbClient.execute({
        sql: `SELECT fee FROM trades WHERE symbol = ? AND side = ? AND type = 'open' 
              ORDER BY timestamp DESC LIMIT 1`,
        args: [order.symbol, order.side]
      });
      
      if (openTradeResult.rows.length > 0 && openTradeResult.rows[0].fee) {
        openFee = parseFloat(openTradeResult.rows[0].fee as string);
        logger.debug(`使用数据库中的真实开仓手续费: ${openFee.toFixed(4)} USDT`);
      } else {
        // 后备方案：估算
        const openFeeResult = await this.feeService.estimateFee(openNotionalValue);
        openFee = openFeeResult.fee;
      }
    } catch (error: any) {
      logger.warn(`获取开仓手续费失败，使用估算: ${error.message}`);
      const openFeeResult = await this.feeService.estimateFee(openNotionalValue);
      openFee = openFeeResult.fee;
    }
    
    const totalFee = openFee + closeFee;
    const netPnl = grossPnl - totalFee;

    // 🔧 核心修复：盈亏百分比计算
    // 盈亏百分比 = (净盈亏 / 保证金) * 100
    // 保证金 = 持仓价值 / 杠杆
    // 持仓价值 = 张数 * 合约乘数 * 开仓价（无论U本位还是币本位都是这个公式）
    const positionValue = quantity * quantoMultiplier * entryPrice;
    const margin = positionValue / leverage;
    const pnlPercent = (netPnl / margin) * 100;
    
    logger.info(`💰 盈亏: 毛利=${grossPnl.toFixed(2)} USDT, 手续费=${totalFee.toFixed(2)} USDT, 净利=${netPnl.toFixed(2)} USDT (${pnlPercent.toFixed(2)}%)`);

    // 阶段5: 数据库事务操作
    const timestamp = new Date().toISOString();
    
    await this.dbClient.execute('BEGIN TRANSACTION');
    
    try {
      // ⭐️ 5.1 先删除持仓记录
      // 即使后续步骤失败，也不会误认为持仓存在
      await this.dbClient.execute({
        sql: 'DELETE FROM positions WHERE symbol = ? AND side = ?',
        args: [order.symbol, order.side]
      });
      logger.debug('✅ [事务] 步骤1: 持仓记录已删除');
      
      // ⭐️ 5.2 更新触发的条件单状态
      await this.updateOrderStatus(order.order_id, 'triggered');
      logger.debug('✅ [事务] 步骤2: 条件单状态已更新为triggered');
      
      // 5.3 取消反向条件单（数据库内操作）
      await this.cancelOppositeOrderInDB(order);
      logger.debug('✅ [事务] 步骤3: 反向条件单已取消');
      
      // 5.4 记录平仓交易
      await this.dbClient.execute({
        sql: `INSERT INTO trades 
              (order_id, symbol, side, type, price, quantity, leverage, pnl, fee, timestamp, status)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          trade.id || order.order_id,
          order.symbol,
          order.side,
          'close',
          exitPrice,
          quantity,
          leverage,
          netPnl,
          totalFee,
          timestamp,
          'filled'
        ]
      });
      logger.debug('✅ [事务] 步骤4: 交易记录已插入');
      
      // 5.5 记录平仓事件
      const closeReason = order.type === 'stop_loss' 
        ? 'stop_loss_triggered' 
        : 'take_profit_triggered';
      
      // 🔧 修复: order_id 统一存储实际平仓成交的订单ID，与 trades 表保持一致
      // trade.id: Gate.io的成交ID (短ID)，用于存储到 trades.order_id
      // order.order_id: 条件单ID，用于存储到 trigger_order_id
      const closeOrderId = trade.id || order.order_id; // 优先使用成交ID，与trades表保持一致
      
      await this.dbClient.execute({
        sql: `INSERT INTO position_close_events 
              (symbol, side, close_reason, trigger_type, trigger_price, close_price, 
               entry_price, quantity, leverage, pnl, pnl_percent, fee, 
               trigger_order_id, close_trade_id, order_id, position_order_id, created_at, processed)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          order.symbol, order.side, closeReason, 'exchange_order',
          parseFloat(order.trigger_price), exitPrice, entryPrice,
          quantity, leverage, netPnl, pnlPercent, totalFee,
          order.order_id, trade.id, closeOrderId, entryOrderId || null, timestamp, 1  // 已处理
        ]
      });
      logger.debug('✅ [事务] 步骤5: 平仓事件已记录');
      
      // 提交事务
      await this.dbClient.execute('COMMIT');
      logger.info(`✅ [事务] ${order.symbol} ${order.type} 触发处理完成`);
      
      // 📧 发送平仓提醒邮件
      try {
        const { emailAlertService } = await import("../utils/emailAlert.js");
        await emailAlertService.sendTradeNotification({
          type: 'close',
          symbol: order.symbol,
          side: order.side,
          quantity,
          price: exitPrice,
          leverage,
          entryPrice,
          exitPrice,
          pnl: netPnl,
          pnlPercent,
          fee: totalFee,
          closeReason: order.type === 'stop_loss' ? 'stop_loss' : 'take_profit',
          orderId: closeOrderId,
          timestamp,
        });
      } catch (emailError: any) {
        logger.warn(`发送平仓提醒邮件失败: ${emailError.message}`);
      }
      
    } catch (error: any) {
      // 回滚事务
      await this.dbClient.execute('ROLLBACK');
      logger.error('❌ [事务] 条件单触发处理失败，已回滚:', error);
      
      // ⚠️ 记录不一致状态
      try {
        await this.dbClient.execute({
          sql: `INSERT INTO inconsistent_states 
                (operation, symbol, side, exchange_success, db_success, 
                 exchange_order_id, error_message, created_at, resolved)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          args: [
            'price_order_triggered',
            order.symbol,
            order.side,
            1,  // 交易所已平仓
            0,  // 数据库记录失败
            order.order_id,
            error.message,
            timestamp,
            0
          ]
        });
        logger.warn('⚠️ 已记录不一致状态到数据库');
      } catch (recordError: any) {
        logger.error('❌ 记录不一致状态失败:', recordError);
      }
    }
  }

  /**
   * 取消反向条件单 (同时更新交易所和数据库)
   */
  private async cancelOppositeOrderInDB(triggeredOrder: DBPriceOrder) {
    const oppositeType = triggeredOrder.type === 'stop_loss' ? 'take_profit' : 'stop_loss';
    
    const result = await this.dbClient.execute({
      sql: `SELECT order_id FROM price_orders 
            WHERE symbol = ? AND side = ? AND type = ? AND status = 'active'
            LIMIT 1`,
      args: [triggeredOrder.symbol, triggeredOrder.side, oppositeType]
    });
    
    if (result.rows.length > 0) {
      const oppositeOrderId = result.rows[0].order_id as string;
      
      // 🔧 优化: 先尝试在交易所端取消（兼容币安和Gate.io）
      try {
        const contract = this.exchangeClient.normalizeContract(triggeredOrder.symbol);
        
        // 检查交易所类型并调用对应的取消方法
        const exchangeName = process.env.EXCHANGE_NAME?.toLowerCase() || 'gate';
        
        if (exchangeName === 'gate') {
          // Gate.io: 使用单个订单取消API
          const gateClient = this.exchangeClient as any;
          if (gateClient.futuresApi && typeof gateClient.futuresApi.cancelPriceTriggeredOrder === 'function') {
            await gateClient.futuresApi.cancelPriceTriggeredOrder(
              gateClient.settle,
              oppositeOrderId
            );
            logger.debug(`✅ 已在Gate.io交易所取消反向条件单: ${oppositeOrderId}`);
          }
        } else if (exchangeName === 'binance') {
          // 🔧 币安关键修复: 使用正确的API路径和参数格式
          const binanceClient = this.exchangeClient as any;
          if (binanceClient.privateRequest && typeof binanceClient.privateRequest === 'function') {
            // 币安要求symbol必须是大写且无下划线的格式 (如: ETHUSDT)
            const symbol = contract.replace('_', '').toUpperCase();
            
            // 使用正确的API端点: DELETE /fapi/v1/order
            // 参数: symbol (必需), orderId (必需)
            await binanceClient.privateRequest('/fapi/v1/order', {
              symbol,
              orderId: oppositeOrderId
            }, 'DELETE', 2);
            
            logger.debug(`✅ 已在Binance交易所取消反向条件单: ${oppositeOrderId} (symbol=${symbol})`);
          }
        }
      } catch (cancelError: any) {
        // 如果取消失败（订单可能已被触发或不存在），记录警告但继续更新数据库
        // 这是正常的：币安在止损触发时会自动取消止盈单
        const errorMsg = cancelError.message || String(cancelError);
        if (errorMsg.includes('Unknown order') || 
            errorMsg.includes('does not exist') ||
            errorMsg.includes('Order does not exist')) {
          logger.debug(`反向条件单已不在交易所（可能已被自动取消）: ${oppositeOrderId}`);
        } else {
          logger.warn(`⚠️ 交易所端取消反向条件单失败: ${errorMsg}`);
        }
      }
      
      // 更新数据库状态（无论交易所是否成功取消）
      await this.updateOrderStatus(oppositeOrderId, 'cancelled');
      logger.debug(`✅ 已更新反向条件单数据库状态为cancelled: ${oppositeOrderId}`);
    }
  }

  /**
   * 查找平仓交易记录
   * 🔧 核心修复：使用近期时间窗口而非条件单创建时间，避免查询范围过大
   */
  private async findCloseTrade(order: DBPriceOrder, retries: number = 3): Promise<any | null> {
    try {
      const contract = this.exchangeClient.normalizeContract(order.symbol);
      const currentTime = Date.now();
      const orderCreateTime = new Date(order.created_at).getTime();
      
      // 🔧 币安条件单触发后，成交记录可能有延迟，添加重试机制
      let trades: any[] = [];
      
      for (let attempt = 1; attempt <= retries; attempt++) {
        // 第一次尝试立即查询，后续尝试等待3秒
        if (attempt > 1) {
          logger.debug(`等待3秒后重试查询成交记录 (${attempt}/${retries})...`);
          await new Promise(resolve => setTimeout(resolve, 3000));
        }
        
        // 🔧 关键修复：只查询最近5分钟的交易，避免查询范围过大导致性能问题
        // 条件单触发到系统检测通常不会超过5分钟
        const searchWindowMs = 60 * 60 * 1000; // 1小时
        // const searchWindowMs = 5 * 60 * 1000; // 5分钟
        // const searchStartTime = Math.max(currentTime - searchWindowMs, orderCreateTime - 5000);
        const searchStartTime =  orderCreateTime - searchWindowMs;
        
        trades = await this.exchangeClient.getMyTrades(contract, 500, searchStartTime);
        
        const maxTimeWindowMs = 24 * 60 * 60 * 1000; // 24小时

        if (attempt === 1) {
          logger.debug(`查找 ${order.symbol} 平仓交易: 搜索起始=${new Date(searchStartTime).toISOString()}, 获取${trades.length}笔交易记录`);
        }

        // 查找所有符合条件的平仓交易
        const closeTrades = trades.filter(t => {
          // 交易时间必须在容差范围内
          const tradeTime = t.timestamp || t.create_time || 0;
          if (tradeTime < searchStartTime) {
            return false;
          }

          // 只检查条件单创建后24小时内的交易
          if (tradeTime - orderCreateTime > maxTimeWindowMs) {
            return false;
          }

          // 检查交易方向（平仓方向与持仓相反）
          const tradeSize = typeof t.size === 'number' ? t.size : parseFloat(t.size || '0');
          const isCloseTrade = (order.side === 'long' && tradeSize < 0) || 
                              (order.side === 'short' && tradeSize > 0);
          
          if (!isCloseTrade) return false;

          // 🔧 价格验证优化：放宽价格匹配条件，允许市价成交偏差
          // 止损单触发后通常以市价成交，可能与触发价有较大偏差（尤其是快速行情）
          const tradePrice = parseFloat(t.price);
          const triggerPrice = parseFloat(order.trigger_price);
          
          // 使用2%的价格容差，允许市价单的滑点
          const priceTolerancePercent = 2.0; // 2% 价格容差
          const priceTolerance = triggerPrice * (priceTolerancePercent / 100);

          let priceMatches = false;
          if (order.type === 'stop_loss') {
            // 止损：价格触及或穿越触发价即可能触发
            // 多单止损：价格下跌触发，成交价应 <= 触发价附近
            // 空单止损：价格上涨触发，成交价应 >= 触发价附近
            // 但考虑到市价单滑点，两个方向都给予容差
            if (order.side === 'long') {
              // 多单止损：允许成交价在触发价下方或上方2%范围内
              priceMatches = tradePrice >= triggerPrice - priceTolerance && 
                           tradePrice <= triggerPrice + priceTolerance;
            } else {
              // 空单止损：允许成交价在触发价下方或上方2%范围内
              priceMatches = tradePrice >= triggerPrice - priceTolerance && 
                           tradePrice <= triggerPrice + priceTolerance;
            }
          } else {
            // 止盈：同样放宽条件
            // 多单止盈：价格上涨触发
            // 空单止盈：价格下跌触发
            if (order.side === 'long') {
              priceMatches = tradePrice >= triggerPrice - priceTolerance && 
                           tradePrice <= triggerPrice + priceTolerance;
            } else {
              priceMatches = tradePrice >= triggerPrice - priceTolerance && 
                           tradePrice <= triggerPrice + priceTolerance;
            }
          }
          
          if (!priceMatches) return false;

          // 🔧 关键修复：数量验证 - 平仓数量不应超过条件单数量的110%
          // 允许10%的容差以应对部分成交和精度问题
          const absTradeSize = Math.abs(tradeSize);
          const expectedQuantity = parseFloat(order.quantity);
          const quantityTolerancePercent = 10; // 10% 数量容差
          const maxAllowedQuantity = expectedQuantity * (1 + quantityTolerancePercent / 100);
          
          if (absTradeSize > maxAllowedQuantity) {
            logger.debug(`⏭️ 跳过交易记录（数量异常）: 成交量=${absTradeSize}, 预期=${expectedQuantity}, 最大允许=${maxAllowedQuantity.toFixed(2)}`);
            return false;
          }
          
          return true;
        });

        if (closeTrades.length > 0) {
          // 找到了成交记录，选择最早的一笔
          const closeTrade = closeTrades.reduce((earliest, current) => {
            const currentTime = current.timestamp || current.create_time || 0;
            const earliestTime = earliest.timestamp || earliest.create_time || 0;
            return currentTime < earliestTime ? current : earliest;
          });

          const tradeTime = closeTrade.timestamp || closeTrade.create_time || 0;
          const minutesAgo = Math.floor((currentTime - tradeTime) / 60000);
          logger.debug(`✅ 找到平仓交易: 时间=${new Date(tradeTime).toISOString()}, 价格=${closeTrade.price}, 距今${minutesAgo}分钟`);

          return closeTrade;
        }
        
        // 🔍 调试：如果未找到，输出所有候选交易以便排查
        if (attempt === retries && trades.length > 0) {
          logger.warn(`❌ 未找到符合条件的平仓交易，输出调试信息:`);
          logger.warn(`   条件单信息: ${order.symbol} ${order.side} ${order.type}, 触发价=${order.trigger_price}, 创建时间=${new Date(orderCreateTime).toISOString()}`);
          logger.warn(`   搜索时间范围: ${new Date(searchStartTime).toISOString()} ~ 现在`);
          
          // 输出最近10笔交易的详细信息
          const recentTrades = trades.slice(0, 10);
          logger.warn(`   最近${recentTrades.length}笔交易:`);
          recentTrades.forEach((t, idx) => {
            const tradeTime = t.timestamp || t.create_time || 0;
            const tradeSize = typeof t.size === 'number' ? t.size : parseFloat(t.size || '0');
            const isCloseTrade = (order.side === 'long' && tradeSize < 0) || 
                                (order.side === 'short' && tradeSize > 0);
            logger.warn(`     [${idx + 1}] 时间=${new Date(tradeTime).toISOString()}, 价格=${t.price}, 数量=${tradeSize}, 方向=${isCloseTrade ? '平仓' : '开仓'}`);
          });
        }
        
        if (attempt < retries) {
          logger.debug(`第${attempt}次未找到成交记录，准备重试...`);
        }
      }

      logger.debug(`未找到 ${order.symbol} ${order.type} 的平仓交易记录 (已重试${retries}次)`);
      return null;
    } catch (error: any) {
      logger.error(`查找平仓交易失败:`, error);
      return null;
    }
  }

  /**
   * 查找开仓交易记录
   */
  private async findOpenTrade(symbol: string, side: 'long' | 'short'): Promise<any | null> {
    try {
      const result = await this.dbClient.execute({
        sql: `SELECT * FROM trades 
              WHERE symbol = ? 
              AND side = ? 
              AND type = 'open' 
              ORDER BY timestamp DESC 
              LIMIT 1`,
        args: [symbol, side]
      });

      return result.rows.length > 0 ? result.rows[0] : null;
    } catch (error: any) {
      logger.error(`查找开仓交易失败:`, error);
      return null;
    }
  }

  /**
   * 更新条件单状态
   */
  private async updateOrderStatus(orderId: string, status: 'triggered' | 'cancelled') {
    const now = new Date().toISOString();
    
    await this.dbClient.execute({
      sql: `UPDATE price_orders
            SET status = ?,
                updated_at = ?,
                triggered_at = ?
            WHERE order_id = ?`,
      args: [status, now, status === 'triggered' ? now : null, orderId]
    });

    logger.debug(`更新条件单状态: ${orderId} -> ${status}`);
  }

  /**
   * 取消反向条件单
   */
  private async cancelOppositeOrder(triggeredOrder: DBPriceOrder) {
    try {
      // 1. 查找反向条件单
      const oppositeType = triggeredOrder.type === 'stop_loss' ? 'take_profit' : 'stop_loss';
      
      const result = await this.dbClient.execute({
        sql: `SELECT * FROM price_orders 
              WHERE symbol = ? 
              AND side = ? 
              AND type = ? 
              AND status = 'active'
              LIMIT 1`,
        args: [triggeredOrder.symbol, triggeredOrder.side, oppositeType]
      });

      if (result.rows.length === 0) {
        logger.debug(`未找到 ${triggeredOrder.symbol} 的反向条件单`);
        return;
      }

      const opposite = result.rows[0];
      const oppositeOrderId = opposite.order_id as string;
      const contract = this.exchangeClient.normalizeContract(triggeredOrder.symbol);

      // 2. 取消交易所的条件单
      try {
        // 先尝试从交易所查询条件单，确认是否存在
        const exchangePriceOrders = await this.exchangeClient.getPriceOrders(contract);
        
        // 统一格式：确保有id字段（兼容币安和Gate.io）
        const normalizedOrders = exchangePriceOrders.map(o => ({
          ...o,
          id: o.id?.toString() || o.orderId?.toString() || o.order_id?.toString()
        }));
        
        const exchangeOrder = normalizedOrders.find(o => o.id === oppositeOrderId);
        
        if (exchangeOrder) {
          // 订单存在，执行取消
          if (this.exchangeClient.getExchangeName() === 'binance') {
            // 币安需要使用特定的取消条件单API
            await this.cancelBinanceConditionalOrder(oppositeOrderId, contract);
          } else {
            // Gate.io 直接使用 cancelOrder
            await this.exchangeClient.cancelOrder(oppositeOrderId);
          }
          logger.info(`✅ 已取消交易所条件单: ${contract} ${oppositeOrderId}`);
        } else {
          logger.debug(`交易所条件单 ${oppositeOrderId} 已不存在（可能已触发或取消），无需取消`);
        }
      } catch (error: any) {
        logger.warn(`⚠️ 取消交易所条件单失败: ${error.message}`);
      }

      // 3. 更新数据库状态（无论交易所取消是否成功，都要更新本地状态）
      await this.updateOrderStatus(oppositeOrderId, 'cancelled');
      
      logger.info(`✅ 已更新本地反向条件单状态为cancelled: ${oppositeOrderId}`);
    } catch (error: any) {
      logger.error(`取消反向条件单失败:`, error);
    }
  }

  /**
   * 取消币安的条件单
   */
  private async cancelBinanceConditionalOrder(orderId: string, symbol: string): Promise<void> {
    const exchangeClient = this.exchangeClient as any;
    
    try {
      // 币安的条件单取消需要 symbol 参数
      await exchangeClient.privateRequest('/fapi/v1/order', {
        symbol,
        orderId
      }, 'DELETE');
      
      logger.debug(`已取消币安条件单 ${orderId}`);
    } catch (error: any) {
      // 如果订单已经不存在，不应该抛出错误
      if (error.message?.includes('Unknown order') || 
          error.message?.includes('Order does not exist')) {
        logger.debug(`订单 ${orderId} 已不存在，无需取消`);
        return;
      }
      throw error;
    }
  }

  /**
   * 获取持仓信息
   */
  private async getPositionInfo(symbol: string, side: 'long' | 'short'): Promise<any | null> {
    try {
      const result = await this.dbClient.execute({
        sql: `SELECT * FROM positions WHERE symbol = ? AND side = ? LIMIT 1`,
        args: [symbol, side]
      });

      return result.rows.length > 0 ? result.rows[0] : null;
    } catch (error: any) {
      logger.error(`获取持仓信息失败:`, error);
      return null;
    }
  }

  /**
   * 记录平仓交易
   */
  private async recordCloseTrade(
    order: DBPriceOrder,
    closeTrade: any,
    position: any
  ) {
    try {
      // 格式化成交数据，兼容所有交易所
      const trade = formatTradeRecord(closeTrade);
      // 计算盈亏
      const entryPrice = parseFloat(position.entry_price as string);
      const exitPrice = parseFloat(trade.price);
      const quantity = Math.abs(parseFloat(trade.size));
      const leverage = parseInt(position.leverage as string);
      const contract = this.exchangeClient.normalizeContract(order.symbol);

      const pnl = await this.exchangeClient.calculatePnl(
        entryPrice,
        exitPrice,
        quantity,
        order.side,
        contract
      );

      // 计算盈亏百分比（考虑杠杆）
      const priceChange = order.side === 'long' 
        ? (exitPrice - entryPrice) / entryPrice 
        : (entryPrice - exitPrice) / entryPrice;
      const pnlPercent = priceChange * 100 * leverage;

      // 插入交易记录（timestamp是毫秒时间戳，转换为ISO 8601格式）
      // trade.timestamp 是UTC时间戳，直接转换为ISO格式即可
      const closeTimeISO = new Date(trade.timestamp).toISOString();
      
      logger.debug(`准备记录平仓交易: symbol=${order.symbol}, side=${order.side}, ` +
        `entry=${entryPrice}, exit=${exitPrice}, qty=${quantity}, pnl=${pnl.toFixed(2)}, ` +
        `time=${closeTimeISO}`);
      
      await this.dbClient.execute({
        sql: `INSERT INTO trades 
              (order_id, symbol, side, type, price, quantity, leverage, pnl, fee, timestamp, status)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          trade.id,
          order.symbol,
          order.side,
          'close',
          trade.price,
          quantity,
          leverage,
          pnl,
          trade.fee,
          closeTimeISO,
          'filled'
        ]
      });
      
      logger.info(`✅ 已记录平仓交易到数据库: ${order.symbol} ${order.side}, ` +
        `order_id=${trade.id}, PnL=${pnl.toFixed(2)} USDT (${pnlPercent.toFixed(2)}%)`);

      // 记录平仓事件（供AI决策使用）
      const closeReason = order.type === 'stop_loss' 
        ? 'stop_loss_triggered' 
        : 'take_profit_triggered';

      // 计算总手续费（开仓 + 平仓）
      // 🔧 核心修复：正确计算开仓手续费
      const contractType = this.exchangeClient.getContractType();
      const closeFee = parseFloat(trade.fee || '0');
      
      const quantoMultiplier = await getQuantoMultiplier(contract);
      const estimatedOpenFee = quantity * quantoMultiplier * entryPrice * 0.0005;
      
      const totalFee = closeFee + estimatedOpenFee;

      // 🔧 修复: order_id 统一存储实际平仓成交的订单ID，与 trades 表保持一致
      const closeOrderId = trade.id || order.order_id; // 优先使用成交ID，与trades表保持一致
      
      // 获取 entry_order_id
      const positionEntryOrderId = position.entry_order_id as string | null || null;
      
      await this.dbClient.execute({
        sql: `INSERT INTO position_close_events 
              (symbol, side, close_reason, trigger_type, trigger_price, close_price, entry_price, 
               quantity, leverage, pnl, pnl_percent, fee, trigger_order_id, close_trade_id, order_id, 
               position_order_id, created_at, processed)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          order.symbol,
          order.side,
          closeReason,
          'exchange_order',  // 触发类型：交易所条件单
          parseFloat(order.trigger_price),
          exitPrice,
          entryPrice,
          quantity,
          position.leverage || 1,
          pnl,
          pnlPercent,
          totalFee,
          order.order_id,
          trade.id,
          closeOrderId,
          positionEntryOrderId,
          new Date().toISOString(),
          0 // 未处理
        ]
      });

      logger.info(`📝 已记录平仓事件到数据库: ${order.symbol} ${closeReason}`);

      logger.info(`📝 已记录平仓事件到数据库: ${order.symbol} ${closeReason}`);
      
      // 验证记录是否成功插入
      const verifyResult = await this.dbClient.execute({
        sql: `SELECT COUNT(*) as count FROM trades WHERE order_id = ? AND type = 'close'`,
        args: [trade.id]
      });
      const recordCount = Number(verifyResult.rows[0]?.count || 0);
      if (recordCount > 0) {
        logger.info(`✅ 验证成功: 平仓交易已存入数据库 (order_id: ${trade.id})`);
      } else {
        logger.error(`❌ 验证失败: 平仓交易未找到 (order_id: ${trade.id})`);
      }
    } catch (error: any) {
      logger.error(`记录平仓交易失败:`, error);
      logger.error(`SQL插入参数:`, {
        orderId: order.order_id,
        symbol: order.symbol,
        side: order.side,
        type: 'close',
        tradeId: closeTrade?.id || closeTrade?.orderId,
        position: {
          entry_price: position?.entry_price,
          leverage: position?.leverage
        }
      });
    }
  }

  /**
   * 删除持仓记录
   */
  private async removePosition(symbol: string, side: 'long' | 'short') {
    try {
      await this.dbClient.execute({
        sql: `DELETE FROM positions WHERE symbol = ? AND side = ?`,
        args: [symbol, side]
      });

      logger.debug(`已删除持仓记录: ${symbol} ${side}`);
    } catch (error: any) {
      logger.error(`删除持仓记录失败:`, error);
    }
  }

  /**
   * 🚨 自动重建止损单（当检测到止损单失效但持仓仍存在时）
   * 
   * 触发场景：
   * 1. 交易所端止损单被标记为cancelled/expired
   * 2. 持仓仍然存在
   * 3. 价格未穿越止损触发线（说明不是触发而是失效）
   * 
   * @param order 数据库中的止损单记录
   */
  private async recreateStopLossOrder(order: DBPriceOrder): Promise<void> {
    try {
      logger.error(`🚨 [自动修复] 开始重建止损单: ${order.symbol} ${order.side}`);
      
      // 从数据库读取持仓信息，获取正确的数量和止损价
      const dbPosition = await this.dbClient.execute({
        sql: 'SELECT * FROM positions WHERE symbol = ? AND side = ?',
        args: [order.symbol, order.side]
      });
      
      if (dbPosition.rows.length === 0) {
        logger.error(`❌ [自动修复失败] 数据库中未找到持仓信息: ${order.symbol} ${order.side}`);
        await this.updateOrderStatus(order.order_id, 'cancelled');
        return;
      }
      
      const pos = dbPosition.rows[0] as any;
      const quantity = parseFloat(pos.quantity as string || order.quantity);
      const stopLossPrice = parseFloat(pos.stop_loss as string || order.trigger_price);
      
      logger.info(`📋 [自动修复] 持仓信息: 数量=${quantity}, 止损价=${stopLossPrice}`);
      
      // 调用交易所API创建新的止损条件单（使用 setPositionStopLoss 统一接口）
      const contract = this.exchangeClient.normalizeContract(order.symbol);
      const result = await this.exchangeClient.setPositionStopLoss(
        contract,
        stopLossPrice,
        undefined // 不重建止盈单
      );
      
      if (!result.success) {
        throw new Error(result.message || '创建止损单失败');
      }
      
      const newStopLossOrderId = result.stopLossOrderId;
      logger.info(`✅ [自动修复] 新止损单已在交易所创建: ID=${newStopLossOrderId}`);
      
      // 更新数据库：旧止损单标记为cancelled，新止损单插入
      await this.dbClient.execute('BEGIN TRANSACTION');
      try {
        // 1. 标记旧止损单为cancelled
        await this.updateOrderStatus(order.order_id, 'cancelled');
        
        // 2. 插入新止损单记录
        await this.dbClient.execute({
          sql: `INSERT INTO price_orders 
                (order_id, symbol, side, type, trigger_price, order_price, quantity, 
                 status, position_order_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          args: [
            newStopLossOrderId || `recreated-${Date.now()}`,
            order.symbol,
            order.side,
            'stop_loss',
            stopLossPrice.toString(),
            '0',
            quantity.toString(),
            'active',
            pos.entry_order_id,
            getChinaTimeISO()
          ]
        });
        
        await this.dbClient.execute('COMMIT');
        logger.info(`✅ [自动修复成功] 数据库已更新：旧止损单cancelled，新止损单active (${newStopLossOrderId})`);
        
      } catch (dbError: any) {
        await this.dbClient.execute('ROLLBACK');
        logger.error(`❌ [自动修复失败] 数据库更新失败: ${dbError.message}`);
        throw dbError;
      }
      
    } catch (recreateError: any) {
      logger.error(`❌ [自动修复失败] 重建止损单过程中发生错误: ${recreateError.message}`);
      logger.error(`   - 订单ID: ${order.order_id}`);
      logger.error(`   - 合约: ${order.symbol}`);
      logger.error(`   - 方向: ${order.side}`);
      logger.error(`   - 触发价: ${order.trigger_price}`);
      
      // 即使失败也要标记旧订单为cancelled，避免重复尝试
      try {
        await this.updateOrderStatus(order.order_id, 'cancelled');
      } catch (updateError: any) {
        logger.error(`❌ 标记旧订单状态失败: ${updateError.message}`);
      }
      
      throw recreateError;
    }
  }
}
