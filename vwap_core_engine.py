# ==========================================================
# FILE: vwap_core_engine.py
# ==========================================================
# 🚨 MODIFIED: [Thundering Herd 영구 소각] _retry_api 내의 await asyncio.sleep(0.06) 파편화 땜질 전면 삭제.
# 🚨 MODIFIED: [중앙 통제소 위임] 모든 API 지연을 GlobalThrottle(중앙 통제소)로 100% 위임하여 이벤트 루프 교착 상태 완벽 방어.
# 🚨 MODIFIED: [순수 슬라이싱 아키텍처 팩트 수복] 슬라이싱 엔진 내부에서 목표가 2% 이내 접근 시 강제로 스윕(Sweep)해버리는 기형적인 조건문을 영구 소각하고, 오직 정밀한 1분 단위 분할(Slicing) 타격만 집행하도록 100% 팩트 교정 완료.
# 🚨 MODIFIED: [하이재킹 1회분 절대 락온 (V93.90)] 하이재킹 발동 시 예산 전액/물량 전량을 무식하게 스윕(Sweep)하던 맹독성 뇌관을 100% 영구 소각. 금일 로컬 지시서(slice_state)에 할당된 '미체결 1회분(Portion)' 수량만을 정밀하게 추출하여 요격함으로써 퀀트 예산 통제 원칙 완벽 사수.
# 🚨 MODIFIED: [상방 매도 하이재킹 큐 장부 보존] 전량 익절망(clear_queue)을 소각하고, 1회분 타격량만큼만 큐 장부에서 부분 차감(pop_lots)하도록 팩트 결속.
# ==========================================================
import logging
import asyncio
import math
import time
import datetime
from zoneinfo import ZoneInfo
import html
import functools

from scheduler_core import get_budget_allocation
from state_io_manager import _read_json_safe_sync, _atomic_write_json_sync

def _safe_float(val):
    try:
        f_val = float(str(val or 0.0).replace(',', ''))
        if math.isnan(f_val) or math.isinf(f_val):
            return 0.0
        return f_val
    except Exception:
        return 0.0

async def _retry_api(func, *args, timeout=15.0, default=None, **kwargs):
    """ 🚨 [Case 32, 33] 중앙 집중형 TPS 캡핑 (GlobalThrottle 위임) 및 지수 백오프 래퍼 """
    for attempt in range(3):
        try:
            if asyncio.iscoroutinefunction(func):
                 return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
            else:
                p_func = functools.partial(func, *args, **kwargs)
                return await asyncio.wait_for(asyncio.to_thread(p_func), timeout=timeout)
        except Exception as e:
            if attempt == 2:
                func_name = getattr(func, '__name__', 'unknown_func')
                logging.debug(f"🚨 API 래퍼 최종 실패 ({func_name}): {e}")
                return default
            await asyncio.sleep(1.0 * (2 ** attempt))
    return default

async def _safe_send(context, chat_id, text, timeout=15.0, **kwargs):
    if not chat_id: return None
    try:
        return await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=text, **kwargs), timeout=timeout)
    except Exception as e:
        logging.error(f"🚨 텔레그램 전송 실패: {e}")
        return None

async def execute_vwap_init(tx_lock, cfg, broker, chat_id, context, vwap_cache):
    async with tx_lock:
        active_tickers = await _retry_api(cfg.get_active_tickers, default=[])
        if isinstance(active_tickers, str): active_tickers = [active_tickers]
        elif not isinstance(active_tickers, list): active_tickers = []
        
        for raw_t in active_tickers:
            t = str(raw_t).strip().upper()
            if not t: continue
            
            try:
                version = await _retry_api(cfg.get_version, t, default="V14")
                is_manual_vwap = await _retry_api(getattr(cfg, 'get_manual_vwap_mode', lambda x: False), t, default=False)
                
                if version == "V_REV" or (version == "V14" and is_manual_vwap):
                    if not vwap_cache.get(f"REV_{t}_nuked"):
                        msg = f"🌅 <b>[{html.escape(str(t))}] 자체 1분 슬라이싱 VWAP 엔진 / Gap Hijack 섀도우 관측망 기상</b>\n"
                        msg += f"▫️ KIS 예약 덫 관망 및 장 마감 34분 전 로컬 펄스 타격 엔진의 가동 대기를 확인했습니다.\n"
                        msg += f"▫️ 운용종목 갭 이탈 감지 시 즉각 개입(Gap Hijack)하는 양방향 섀도우 모드가 함께 가동됩니다. ⚔️"

                        vwap_cache[f"REV_{t}_nuked"] = True
                        
                        await _safe_send(context, chat_id, msg, parse_mode='HTML', disable_notification=True)
            except Exception as e:
                logging.error(f"🚨 [{t}] 관측 모드 샌드박스 에러 (격리 완료): {e}")
                vwap_cache[f"REV_{t}_nuked"] = False 

async def execute_vwap_trade(tx_lock, cfg, broker, strategy, queue_ledger, chat_id, context, base_map, vwap_cache):
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    today_hyphen = now_est.strftime('%Y-%m-%d')
    
    kst_zone = ZoneInfo('Asia/Seoul')
    now_kst = datetime.datetime.now(kst_zone)
    today_kst_str = now_kst.strftime('%Y%m%d')

    async with tx_lock:
        res = await _retry_api(broker.get_account_balance, timeout=15.0)
        cash = _safe_float(res[0]) if isinstance(res, (list, tuple)) and len(res) > 0 else 0.0
        holdings = res[1] if isinstance(res, (list, tuple)) and len(res) > 1 else {}
        if not isinstance(holdings, dict): holdings = {}
                
        if res is None: return
        
        active_tickers = await _retry_api(cfg.get_active_tickers, default=[])
        if isinstance(active_tickers, str): active_tickers = [active_tickers]
        elif not isinstance(active_tickers, list): active_tickers = []
        
        alloc_res = await _retry_api(get_budget_allocation, cash, active_tickers, cfg, default=({}, {}))
        allocated_cash = alloc_res[1] if isinstance(alloc_res, (list, tuple)) and len(alloc_res) > 1 else {}
        if not isinstance(allocated_cash, dict): allocated_cash = {}
        
        t_curr_p = 0.0
        nuked_count = 0
        
        for raw_t in active_tickers:
            t = str(raw_t).strip().upper()
            if not t: continue
            
            actual_qty = int(_safe_float(holdings.get(t, {}).get('qty', 0)))

            vrev_q_qty = 0
            if queue_ledger:
                q_data = await _retry_api(queue_ledger.get_queue, t, default=[])
                vrev_q_qty = sum(int(_safe_float(item.get("qty"))) for item in (q_data or []) if isinstance(item, dict))

            try:
                version = await _retry_api(cfg.get_version, t, default="V14")
                is_manual_vwap = await _retry_api(getattr(cfg, 'get_manual_vwap_mode', lambda x: False), t, default=False)

                if version == "V_REV" or (version == "V14" and is_manual_vwap):
                    slice_file = f"data/vrev_slice_state_{t}.json"
                    
                    try:
                        after_state_file = f"data/vrev_aftermarket_state_{t}.json"
                        after_state = await _retry_api(_read_json_safe_sync, after_state_file, today_hyphen, default={})
                        if after_state.get('date') == today_hyphen:
                            pending_aftermarket = any(isinstance(o, dict) and str(o.get('status')) == 'PENDING' for o in after_state.get('orders', []))
                            if pending_aftermarket:
                                logging.info(f"⏳ [{t}] 애프터장(16:01) 이관 대기 중. 정규장 마감 직전(16:00)의 Gap Hijack 및 슬라이싱 엔진을 전면 바이패스합니다.")
                                continue
                    except Exception as e:
                        logging.error(f"🚨 [{t}] 애프터장 이관 상태 교차 검증 에러: {e}")

                    # ======================================================
                    # [ 1. Gap Hijack (상/하방 갭 하이재킹 전용) 모니터링 ]
                    # ======================================================
                    is_downward_hijacked_now = vwap_cache.get(f"REV_{t}_gap_hijack_fired", False)
                    is_upward_hijacked_now = vwap_cache.get(f"REV_{t}_upward_hijack_fired", False)
                    
                    if (version == "V_REV" or (version == "V14" and is_manual_vwap)) and not (is_downward_hijacked_now or is_upward_hijacked_now):
                        t_curr_p = _safe_float(await _retry_api(broker.get_current_price, t))
                        df_1min_t = await _retry_api(broker.get_1min_candles_df, t)
                                
                        if df_1min_t is not None and not df_1min_t.empty:
                            df_t = df_1min_t.copy()
                            
                            df_t = df_t[df_t.index.date == now_est.date()]
                            
                            if 'time_est' in df_t.columns:
                                df_t = df_t[(df_t['time_est'] >= '093000') & (df_t['time_est'] <= '155900')]
                                
                            if not df_t.empty:
                                df_t['high'] = df_t['high'].ffill().bfill()
                                df_t['low'] = df_t['low'].ffill().bfill()
                                df_t['close'] = df_t['close'].ffill().bfill()
                                df_t['volume'] = df_t['volume'].ffill().bfill().fillna(0)

                                df_t['tp'] = (df_t['high'].astype(float) + df_t['low'].astype(float) + df_t['close'].astype(float)) / 3.0
                                df_t['vol'] = df_t['volume'].astype(float)
                                df_t['vol_tp'] = df_t['tp'] * df_t['vol']
                                
                                c_vol = df_t['vol'].sum()
                                t_vwap = df_t['vol_tp'].sum() / c_vol if c_vol > 0 else t_curr_p
                                
                                gap_pct = ((t_curr_p - t_vwap) / t_vwap * 100.0) if t_vwap > 0 else 0.0
                                
                                gap_thresh = _safe_float(await _retry_api(getattr(cfg, 'get_vrev_gap_threshold', lambda x: -2.0), t, default=-2.0))
                                if gap_thresh == -0.67: gap_thresh = -2.0
                                
                                # ----------------------------------------------------
                                # 🚨 [A. 하방 매수 하이재킹 격발망]
                                # ----------------------------------------------------
                                if gap_pct <= gap_thresh:
                                    slice_state_check = await _retry_api(_read_json_safe_sync, slice_file, today_hyphen, default={})
                                    has_buy_plan = any(isinstance(o, dict) and str(o.get('side')) == 'BUY' for o in slice_state_check.get('orders', []))
                                    
                                    if not has_buy_plan:
                                        if not vwap_cache.get(f"REV_{t}_gap_hijack_blocked_log", False):
                                            logging.info(f"⚡ [{t}] 하방 Gap Hijack 조건 도달({gap_pct:.2f}%) ➔ 🛑 금일 통합지시서에 매수(BUY) 플랜이 없어 스윕 매수를 전면 차단(Bypass)합니다.")
                                            vwap_cache[f"REV_{t}_gap_hijack_blocked_log"] = True
                                        
                                        vwap_cache[f"REV_{t}_gap_hijack_fired"] = True
                                        is_downward_hijacked_now = True
                                    else:
                                        logging.info(f"⚡ [{t}] Downward Gap Hijack Triggered! gap: {gap_pct:.2f}%, thresh: {gap_thresh}%")
                                        nuked_count = 0
                                        
                                        try:
                                            est_now = datetime.datetime.now(ZoneInfo('America/New_York'))
                                            d_str = est_now.strftime('%Y%m%d')
                                            
                                            resv_orders = await _retry_api(broker.get_reservation_orders, t, d_str, d_str, default=[])
                                            safe_resv_orders = resv_orders if isinstance(resv_orders, list) else []
                                            
                                            for req in safe_resv_orders:
                                                if not isinstance(req, dict): continue
                                                
                                                side_cd = str(req.get('sll_buy_dvsn_cd') or req.get('sll_buy_dvsn') or '')
                                                if side_cd == '01': continue 
                                                
                                                odno = str(req.get('ovrs_rsvn_odno') or req.get('odno') or '')
                                                ord_dt = str(req.get('rsvn_ord_rcit_dt') or req.get('ord_dt') or d_str)
                                                if odno:
                                                    c_res = await _retry_api(broker.cancel_reservation_order, ord_dt, odno)
                                                    if c_res: nuked_count += 1
                                        
                                            unfilled = await _retry_api(broker.get_unfilled_orders_detail, t, default=[])
                                            safe_unfilled = unfilled if isinstance(unfilled, list) else []
                                            
                                            for uo in safe_unfilled:
                                                if not isinstance(uo, dict): continue
                                                
                                                side_cd = str(uo.get('sll_buy_dvsn_cd') or uo.get('sll_buy_dvsn') or '')
                                                if side_cd == '01': continue
                                                
                                                dvsn = str(uo.get('ord_dvsn_cd') or uo.get('ord_dvsn') or '').strip().zfill(2)
                                                if dvsn in ['36', '00']:
                                                    u_odno = str(uo.get('odno') or '')
                                                    if u_odno:
                                                        c_res = await _retry_api(broker.cancel_order, t, u_odno)
                                                        if c_res: nuked_count += 1
                                            
                                            logging.info(f"⚡ [{t}] KIS 실원장 스캔: 예약 및 일반 매수(BUY) 덫 {nuked_count}건 팩트 파기 완료 (SELL 구출망 보존).")
                                        except Exception as e:
                                            logging.error(f"🚨 [{t}] KIS 실원장 덫 스캔 에러: {e}")

                                        try:
                                            s_state = await _retry_api(_read_json_safe_sync, slice_file, today_hyphen, default={})
                                            s_state['hijacked'] = True
                                            s_state['date'] = today_hyphen
                                            await _retry_api(_atomic_write_json_sync, slice_file, s_state)
                                            logging.info(f"⚡ [{t}] 로컬 1분 슬라이싱 엔진 무효화 (hijacked) 마킹 완료.")
                                        except Exception as e:
                                            logging.error(f"🚨 [{t}] 로컬 슬라이스 무효화 처리 에러: {e}")

                                        seed = await _retry_api(cfg.get_seed, t, default=0.0)
                                        daily_limit = _safe_float(seed) * 0.15
                                        alloc_cash = _safe_float(allocated_cash.get(t, 0.0))
                                       
                                        if version == "V_REV":
                                            safe_alloc_cash = min(alloc_cash, daily_limit) if daily_limit > 0 else alloc_cash
                                            total_spent = 0.0
                                            if hasattr(strategy, 'v_rev_plugin'):
                                                spent_dict = strategy.v_rev_plugin.executed.get("BUY_BUDGET")
                                                safe_spent_dict = spent_dict if isinstance(spent_dict, dict) else {}
                                                total_spent = _safe_float(safe_spent_dict.get(t, 0.0))
                                        else:
                                            safe_alloc_cash = alloc_cash
                                            total_spent = 0.0
                                            if hasattr(strategy, 'v14_vwap_plugin'):
                                                spent_dict = strategy.v14_vwap_plugin.executed.get("BUY_BUDGET")
                                                safe_spent_dict = spent_dict if isinstance(spent_dict, dict) else {}
                                                total_spent = _safe_float(safe_spent_dict.get(t, 0.0))
                                            
                                        rem_budget = max(0.0, safe_alloc_cash - total_spent)

                                        # 🚨 NEW: 1일분 예산 전액 무지성 덤핑 소각 및 로컬 지시서 기반 1회분(Portion) 정밀 추출
                                        rem_buy_qty_from_plan = sum(max(0, int(_safe_float(o.get('total_qty', 0))) - int(_safe_float(o.get('filled_qty', 0)))) for o in slice_state_check.get('orders', []) if str(o.get('side')) == 'BUY')

                                        ask_price = _safe_float(await _retry_api(broker.get_ask_price, t))
                                        curr_p = _safe_float(await _retry_api(broker.get_current_price, t))
                                        exec_price = ask_price if ask_price > 0 else curr_p
                                        
                                        max_affordable_qty = int(math.floor(rem_budget / exec_price)) if exec_price > 0 else 0
                                        # 🚨 지시서상 남은 1회분 물량과 잔여 예산 물량 중 작은 값을 취하여 예산 통제 100% 사수
                                        buy_qty = min(rem_buy_qty_from_plan, max_affordable_qty)
                                         
                                        if buy_qty > 0:
                                            res = await _retry_api(broker.send_order, t, "BUY", buy_qty, exec_price, "LIMIT")
                                            safe_res = res if isinstance(res, dict) else {}
                                            odno = str(safe_res.get('odno') or '')
                                            
                                            if safe_res.get('rt_cd') == '0' and odno:
                                                vwap_cache[f"REV_{t}_gap_hijack_fired"] = True
                                                is_downward_hijacked_now = True
                                                
                                                msg = f"⚡ <b>[{html.escape(str(t))}] 🤖 하방 모멘텀 자율주행 (Gap Hijack) 섀도우 오버라이드 격발!</b>\n"
                                                msg += f"▫️ 당일 누적 VWAP 이탈률(<b>{gap_pct:+.2f}%</b>)이 임계치(<b>{gap_thresh}%</b>)를 하향 돌파했습니다.\n"
                                                msg += f"▫️ 예약/미체결 덫({nuked_count}건) 파기 후, 금일 지시서상의 <b>잔여 매수 1회분</b>을 매도 1호가로 일괄 타격(Sweep)했습니다!\n"
                                                msg += f"▫️ 스윕 수량: <b>{buy_qty}주</b> (단가: ${exec_price:.2f})"
                                                
                                                await _safe_send(context, chat_id, msg, parse_mode='HTML')
                                                
                                                if version == "V_REV":
                                                    if hasattr(strategy, 'v_rev_plugin'):
                                                        await _retry_api(strategy.v_rev_plugin.record_execution, t, "BUY", buy_qty, exec_price)
                                                    if queue_ledger:
                                                        await _retry_api(queue_ledger.add_lot, t, buy_qty, exec_price, "GAP_HIJACK_BUY")
                                                else:
                                                    if hasattr(strategy, 'v14_vwap_plugin'):
                                                        await _retry_api(strategy.v14_vwap_plugin.record_execution, t, "BUY", buy_qty, exec_price)
                                            else:
                                                err_msg = html.escape(str(safe_res.get('msg1') or '응답 없음/통신 장애'))
                                                logging.error(f"🚨 [{t}] 하방 갭 하이재킹 KIS 서버 거절: {err_msg}")
                                                reject_msg = (
                                                    f"🚨 <b>[{html.escape(str(t))}] 하방 갭 하이재킹 스윕(Sweep) 서버 거절 (Reject)!</b>\n"
                                                    f"▫️ 사유: <code>{err_msg}</code>\n"
                                                    f"▫️ 조치: 다음 스캔 시 재시도합니다."
                                                )
                                                await _safe_send(context, chat_id, reject_msg, parse_mode='HTML')
                                        else:
                                            vwap_cache[f"REV_{t}_gap_hijack_fired"] = True
                                            is_downward_hijacked_now = True
                                            logging.info(f"⚡ [{t}] 하방 Gap Hijack 격발 조건을 만족했으나 1회분 잔여 물량 소진으로 스윕 매수 생략 (플래그 락온 완료).")

                                # ----------------------------------------------------
                                # 🚨 [B. 상방 매도 하이재킹 (Upward Sell Hijack) 격발망]
                                # ----------------------------------------------------
                                elif gap_pct >= 2.0:
                                    slice_state_check = await _retry_api(_read_json_safe_sync, slice_file, today_hyphen, default={})
                                    has_sell_plan = any(isinstance(o, dict) and str(o.get('side')) == 'SELL' for o in slice_state_check.get('orders', []))
                                    
                                    if not has_sell_plan:
                                        if not vwap_cache.get(f"REV_{t}_upward_hijack_blocked_log", False):
                                            logging.info(f"⚡ [{t}] 상방 Gap Hijack 조건 도달({gap_pct:+.2f}%) ➔ 🛑 매도(SELL) 플랜 없음. 스마트 보호를 소각하고 무한매수 원칙에 따라 정상 슬라이싱 매수를 지속합니다.")
                                            vwap_cache[f"REV_{t}_upward_hijack_blocked_log"] = True
                                    else:
                                        can_upward_hijack = False
                                        target_sell_qty = 0
                                        
                                        # 🚨 NEW: 장부 전량(Clear) 덤핑 맹점 소각 및 로컬 지시서 기반 1회분(Portion) 정밀 추출
                                        rem_sell_qty_from_plan = sum(max(0, int(_safe_float(o.get('total_qty', 0))) - int(_safe_float(o.get('filled_qty', 0)))) for o in slice_state_check.get('orders', []) if str(o.get('side')) == 'SELL')
                                        
                                        if rem_sell_qty_from_plan > 0:
                                            if version == "V_REV":
                                                if vrev_q_qty > 0:
                                                    can_upward_hijack = True
                                                    target_sell_qty = min(rem_sell_qty_from_plan, vrev_q_qty)
                                            else:
                                                if actual_qty > 0:
                                                    can_upward_hijack = True
                                                    target_sell_qty = min(rem_sell_qty_from_plan, actual_qty)
                                                
                                        if can_upward_hijack:
                                            logging.info(f"⚡ [{t}] Upward Sell Hijack Triggered! gap: {gap_pct:.2f}% >= +2.0%, Target 1-Portion Qty: {target_sell_qty}주")
                                            nuked_count = 0
                                            
                                            try:
                                                est_now = datetime.datetime.now(ZoneInfo('America/New_York'))
                                                d_str = est_now.strftime('%Y%m%d')
                                                
                                                resv_orders = await _retry_api(broker.get_reservation_orders, t, d_str, d_str, default=[])
                                                safe_resv_orders = resv_orders if isinstance(resv_orders, list) else []
                                                
                                                for req in safe_resv_orders:
                                                    if not isinstance(req, dict): continue
                                                    
                                                    side_cd = str(req.get('sll_buy_dvsn_cd') or req.get('sll_buy_dvsn') or '')
                                                    if side_cd == '02': continue 
                                                    
                                                    odno = str(req.get('ovrs_rsvn_odno') or req.get('odno') or '')
                                                    ord_dt = str(req.get('rsvn_ord_rcit_dt') or req.get('ord_dt') or d_str)
                                                    if odno:
                                                        c_res = await _retry_api(broker.cancel_reservation_order, ord_dt, odno)
                                                        if c_res: nuked_count += 1
                                            
                                                unfilled = await _retry_api(broker.get_unfilled_orders_detail, t, default=[])
                                                safe_unfilled = unfilled if isinstance(unfilled, list) else []
                                                
                                                for uo in safe_unfilled:
                                                    if not isinstance(uo, dict): continue
                                                    
                                                    side_cd = str(uo.get('sll_buy_dvsn_cd') or uo.get('sll_buy_dvsn') or '')
                                                    if side_cd == '02': continue 
                                                    
                                                    dvsn = str(uo.get('ord_dvsn_cd') or uo.get('ord_dvsn') or '').strip().zfill(2)
                                                    if dvsn in ['36', '00']:
                                                        u_odno = str(uo.get('odno') or '')
                                                        if u_odno:
                                                            c_res = await _retry_api(broker.cancel_order, t, u_odno)
                                                            if c_res: nuked_count += 1
                                                
                                                logging.info(f"⚡ [{t}] KIS 실원장 스캔: 예약 및 일반 매도(SELL) 덫 {nuked_count}건 팩트 파기 완료 (BUY 덫 보존).")
                                            except Exception as e:
                                                logging.error(f"🚨 [{t}] 상방 매도 하이재킹 KIS 실원장 덫 스캔 에러: {e}")

                                            try:
                                                s_state = await _retry_api(_read_json_safe_sync, slice_file, today_hyphen, default={})
                                                s_state['hijacked'] = True
                                                s_state['upward_hijacked'] = True
                                                s_state['date'] = today_hyphen
                                                await _retry_api(_atomic_write_json_sync, slice_file, s_state)
                                                logging.info(f"⚡ [{t}] 상방 하이재킹: 로컬 1분 슬라이싱 엔진 무효화 (upward_hijacked) 마킹 완료.")
                                            except Exception as e:
                                                logging.error(f"🚨 [{t}] 상방 하이재킹 로컬 슬라이스 무효화 처리 에러: {e}")

                                            bid_price = _safe_float(await _retry_api(broker.get_bid_price, t))
                                            curr_p = _safe_float(await _retry_api(broker.get_current_price, t))
                                            exec_price = bid_price if bid_price > 0 else curr_p

                                            if exec_price > 0 and target_sell_qty > 0:
                                                res = await _retry_api(broker.send_order, t, "SELL", target_sell_qty, exec_price, "LIMIT")
                                                safe_res = res if isinstance(res, dict) else {}
                                                odno = str(safe_res.get('odno') or '')

                                                if safe_res.get('rt_cd') == '0' and odno:
                                                    vwap_cache[f"REV_{t}_upward_hijack_fired"] = True
                                                    is_upward_hijacked_now = True
                                                    
                                                    if version == "V_REV":
                                                        if queue_ledger:
                                                            # 🚨 NEW: 전량 익절(clear_queue) 맹독성 소각 및 1회분 부분 차감(pop_lots) 팩트 교정
                                                            await _retry_api(queue_ledger.pop_lots, t, target_sell_qty, exec_price)
                                                    else:
                                                        if hasattr(strategy, 'v14_vwap_plugin'):
                                                            await _retry_api(strategy.v14_vwap_plugin.record_execution, t, "SELL", target_sell_qty, exec_price)
                                                    
                                                    msg = f"🚀 <b>[{html.escape(str(t))}] 🤖 상방 모멘텀 자율주행 (Sell Hijack) 격발!</b>\n"
                                                    msg += f"▫️ 당일 누적 VWAP 대비 현재가 슈팅(<b>+{gap_pct:.2f}%</b>)이 익절 임계치(<b>+2.0%</b>)를 관통했습니다.\n"
                                                    msg += f"▫️ KIS 덫({nuked_count}건) 파기 후, 금일 지시서상의 <b>잔여 익절 1회분</b>을 매수 1호가로 일괄 타격(Sweep)하여 고점 수익을 확정합니다!\n"
                                                    msg += f"▫️ 1회분 익절 타격 수량: <b>{target_sell_qty}주</b> (단가: ${exec_price:.2f})\n"
                                                    msg += f"▫️ <b>당일 매도 슬라이싱 엔진 가동을 전면 마비시킵니다 (조기 퇴근 락온).</b>"
                                                    
                                                    await _safe_send(context, chat_id, msg, parse_mode='HTML')
                                                else:
                                                    err_msg = html.escape(str(safe_res.get('msg1') or '응답 없음/통신 장애'))
                                                    logging.error(f"🚨 [{t}] 상방 갭 하이재킹 KIS 서버 거절: {err_msg}")
                                                    reject_msg = (
                                                        f"🚨 <b>[{html.escape(str(t))}] 상방 하이재킹 스윕(Sweep) 서버 거절!</b>\n"
                                                        f"▫️ 사유: <code>{err_msg}</code>\n"
                                                        f"▫️ 조치: 다음 스캔 시 재시도합니다."
                                                    )
                                                    await _safe_send(context, chat_id, reject_msg, parse_mode='HTML')

                    # ======================================================
                    # [ 2. 자체 VWAP 1분 슬라이싱 로컬 엔진 가동 ]
                    # ======================================================
                    curr_time_obj = now_est.time()
                    time_start = datetime.time(15, 27)
                    time_end = datetime.time(15, 57, 59)
                    
                    if time_start <= curr_time_obj <= time_end:
                        slice_state = await _retry_api(_read_json_safe_sync, slice_file, today_hyphen, default={})
                        
                        if slice_state.get('date') != today_hyphen:
                            continue 
                            
                        # 🚨 상방(매도) 하이재킹 플래그 동기화
                        is_state_hijacked = slice_state.get('hijacked', False) or is_downward_hijacked_now
                        is_state_upward_hijacked = slice_state.get('upward_hijacked', False) or is_upward_hijacked_now
                        
                        orders = slice_state.get('orders', [])
                        if not isinstance(orders, list): orders = []
                        if not orders: continue
                        
                        is_cleanup_phase = (curr_time_obj >= datetime.time(15, 57))
                            
                        curr_hm = now_est.strftime("%H:%M")
                        try:
                            vwap_profile = await _retry_api(cfg.get_vwap_profile, t, default={})
                            if not isinstance(vwap_profile, dict): vwap_profile = {}
                        except Exception: vwap_profile = {}
                        
                        cum_weight = _safe_float(vwap_profile.get(curr_hm, 0.0))
                        
                        if is_cleanup_phase:
                            cum_weight = 1.0
                        elif cum_weight == 0.0:
                            start_mins = 15 * 60 + 27
                            curr_mins = now_est.hour * 60 + now_est.minute
                            elapsed = max(0, curr_mins - start_mins + 1)
                            cum_weight = min(1.0, max(0.0, elapsed / 29.0))
                            
                        state_changed = False
                        
                        for o in orders:
                            if not isinstance(o, dict): continue
                            
                            total_qty = int(_safe_float(o.get('total_qty')))
                            filled_qty = int(_safe_float(o.get('filled_qty')))
                            target_price = _safe_float(o.get('target_price'))
                            side = str(o.get('side', 'BUY'))
                            last_odno = str(o.get('last_odno', ''))
                            
                            # 🚨 하방/상방 갭 하이재킹 발동 시 슬라이싱 바이패스 로직
                            if (is_state_hijacked or is_state_upward_hijacked) and side == 'BUY':
                                continue
                            if is_state_upward_hijacked and side == 'SELL':
                                continue
                            
                            if filled_qty >= total_qty and not last_odno:
                                continue
                            
                            ccld_qty_this_tick = 0
                            if last_odno:
                                cancel_successful = False
                                c_res = await _retry_api(broker.cancel_order, t, last_odno, timeout=10.0)
                                if isinstance(c_res, dict) and str(c_res.get('rt_cd', '')) == '0':
                                    cancel_successful = True
                                    
                                is_still_open = False
                                if not cancel_successful:
                                    unf = await _retry_api(broker.get_unfilled_orders_detail, t, default=[])
                                    safe_unf = unf if isinstance(unf, list) else []
                                    if any(isinstance(x, dict) and str(x.get('odno', '')) == last_odno for x in safe_unf):
                                        is_still_open = True
                                        
                                if is_still_open:
                                    logging.warning(f"🚨 [{t}] 취소 실패 및 미체결 잔존 확인 (Double Spending 방어). 다음 분으로 이연합니다.")
                                    continue
                                
                                try:
                                    _execs = await _retry_api(broker.get_execution_history, t, today_kst_str, today_kst_str, default=[])
                                    _safe_execs = _execs if isinstance(_execs, list) else []
                                    _filled_rec = next((ex for ex in _safe_execs if isinstance(ex, dict) and str(ex.get('odno', '')) == last_odno), None)
                                    
                                    if _filled_rec:
                                        ccld_qty_this_tick = int(_safe_float(_filled_rec.get('ft_ccld_qty')))
                                        real_exec_price = _safe_float(_filled_rec.get('ft_ccld_unpr3'))
                                        if real_exec_price == 0.0: real_exec_price = target_price
                                    else:
                                        ccld_qty_this_tick = 0
                                        real_exec_price = 0.0
                                except Exception as e:
                                    logging.error(f"🚨 [{t}] 자체 슬라이싱 체결 원장 교차 검증 에러: {e}")
                                    ccld_qty_this_tick = 0
                                    real_exec_price = 0.0
                                
                                if ccld_qty_this_tick > 0:
                                    processed_odnos = vwap_cache.setdefault(f"PROCESSED_ODNOS_{t}", set())
                                    if last_odno not in processed_odnos:
                                        processed_odnos.add(last_odno)

                                        def _sync_ledger_atomic(tkr, sde, c_qty, r_price, q_ledger, strat, ver):
                                            if ver == "V_REV":
                                                if q_ledger:
                                                    if sde == "BUY":
                                                        q_ledger.add_lot(tkr, c_qty, r_price, "VREV_VWAP_BUY")
                                                    else:
                                                        q_ledger.pop_lots(tkr, c_qty, r_price)
                                                if hasattr(strat, 'v_rev_plugin'):
                                                    strat.v_rev_plugin.record_execution(tkr, sde, c_qty, r_price)
                                            else:
                                                if hasattr(strat, 'v14_vwap_plugin'):
                                                    strat.v14_vwap_plugin.record_execution(tkr, sde, c_qty, r_price)

                                        try:
                                            p_sync = functools.partial(_sync_ledger_atomic, t, side, ccld_qty_this_tick, real_exec_price, queue_ledger, strategy, version)
                                            await asyncio.wait_for(asyncio.to_thread(p_sync), timeout=10.0)
                                            logging.info(f"💾 [{t}] 자체 슬라이싱 체결 장부 원자적 동기화 완료: {side} {ccld_qty_this_tick}주 @ ${real_exec_price:.2f}")
                                        except Exception as e:
                                            processed_odnos.remove(last_odno) 
                                            logging.error(f"🚨 [{t}] 자체 슬라이싱 체결 장부 동기화 실패 (캐시 롤백): {e}")
                                        
                                        msg_side = "매수" if side == "BUY" else "매도"
                                        logging.info(f"⚡ [{t}] 섀도 엔진 체결 팩트 장부 동기화 완료: {msg_side} {ccld_qty_this_tick}주 @ ${real_exec_price:.2f} (텔레그램 타전 바이패스)")

                                filled_qty += ccld_qty_this_tick
                                o['filled_qty'] = filled_qty
                                o['last_odno'] = ""
                                o['last_sent_qty'] = 0
                                state_changed = True
                            
                            if is_cleanup_phase:
                                continue 

                            if filled_qty >= total_qty: continue
                            
                            target_cum_qty = round(total_qty * cum_weight)
                            qty_to_send = target_cum_qty - filled_qty
                            
                            exec_price = 0.0
                            if side == "BUY":
                                exec_price = _safe_float(await _retry_api(broker.get_ask_price, t))
                            else:
                                exec_price = _safe_float(await _retry_api(broker.get_bid_price, t))
                                    
                            if exec_price <= 0.0:
                                exec_price = _safe_float(await _retry_api(broker.get_current_price, t))
                                         
                            # 🚨 MODIFIED: [순수 슬라이싱 아키텍처 팩트 수복] 스윕 로직 영구 소각, 무조건 1분 할당량 분할 타격
                            if target_price > 0.0:
                                is_target_hit = False
                                if side == "BUY" and exec_price <= target_price:
                                    is_target_hit = True
                                elif side == "SELL" and exec_price >= target_price:
                                    is_target_hit = True

                                if is_target_hit:
                                    qty_to_send = target_cum_qty - filled_qty
                                else:
                                    continue 
                                    
                            if qty_to_send <= 0: continue
                                      
                            if exec_price > 0:
                                if side == "SELL" and qty_to_send > 0:
                                    rt_qty = qty_to_send
                                    bal_tuple = await _retry_api(broker.get_account_balance)
                                    if isinstance(bal_tuple, (list, tuple)) and len(bal_tuple) > 1:
                                        rt_qty = int(_safe_float(bal_tuple[1].get(t, {}).get('qty', 0)))
                                    
                                    qty_to_send = min(qty_to_send, rt_qty)

                                res = None
                                if qty_to_send > 0:
                                    res = await _retry_api(broker.send_order, t, side, qty_to_send, exec_price, "LIMIT")
                                else:
                                    logging.warning(f"🚨 [{t}] VWAP 슬라이싱 매도 스킵: KIS 실잔고 0주 캡핑 (Ghost-Dumping 방어)")
                                    res = {'rt_cd': '999', 'msg1': '보유 수량 0주 캡핑으로 매도 스킵'}

                                safe_res = res if isinstance(res, dict) else {}
                                if safe_res.get('rt_cd') == '0' and safe_res.get('odno'):
                                    o['last_odno'] = safe_res.get('odno')
                                    o['last_sent_qty'] = qty_to_send
                                    o['last_price'] = exec_price
                                    state_changed = True
                                    logging.info(f"🔪 [{t}] 정밀 요격망(Slicing): {side} {qty_to_send}주 @ ${exec_price:.2f} (누적 {cum_weight*100:.1f}%)")
                                else:
                                    logging.error(f"🚨 [{t}] VWAP 슬라이싱 거절: {safe_res.get('msg1')}")
                                
                        if state_changed:
                            try:
                                await asyncio.wait_for(asyncio.to_thread(_atomic_write_json_sync, slice_file, slice_state), timeout=10.0)
                            except Exception as e:
                                logging.error(f"🚨 [{t}] 로컬 1분 슬라이싱 엔진 상태 기록 실패 (Atomic Write): {e}")

            except Exception as e:
                logging.error(f"🚨 [{t}] 섀도우 엔진 단일 종목 연산 중 치명적 오류 (Cascade 방어): {e}", exc_info=True)
                continue
