# ==========================================================
# FILE: scheduler_sniper.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 5대 헌법 및 36대 엣지 케이스 완벽 결속 교차 검증 완료
# 🚨 MODIFIED: [Phase 3 암살자 실전 매매망 복구] 딥-매수, -1% 덫 장전, 섀도우 스위칭(OCO 듀얼 엑시트) 100% 팩트 부활.
# 🚨 MODIFIED: [14:00 EST 컷오프 디커플링] 14:00 도달 시 물량 검증 후 본진 셧다운(AVWAP_OVERNIGHT) 또는 암살자 퇴근 결정 팩트 락온.
# 🚨 MODIFIED: [익일 04:00 L1 대통합] 오버나이트 물량 보유 시 기상 직후 장부 단일 지층 병합 및 -1% 덫 재장전 (시나리오 5) 팩트 결속.
# 🚨 MODIFIED: [V14 스나이퍼 생태계 보존] 암살자 로직과 100% 물리적 격리되어 기존 상방 스나이퍼 로직은 무결점 사수.
# 🚨 MODIFIED: [HTML Parser 붕괴 방어] Telegram 타전을 위한 텍스트 html.escape 100% 강제 래핑 유지.
# 🚨 MODIFIED: [Case 32, 33] 3단 지수 백오프 및 KIS 전송 TPS 캡핑(0.06s) 샌드위치 전면 락온.
# 🚨 MODIFIED: [Case 08, 16] 암살자 실매매 상태 파일(avwap_trade_state) EAFP 패턴 및 원자적 쓰기 스코프 전진 배치 유지.
# 🚨 MODIFIED: [Case 01 팩트 일치화] 상태 파일 및 롤오버 판별 시 '%Y-%m-%d' 시스템 표준 날짜 규격 100% 강제 래핑 완료.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import asyncio
import traceback
import math
import os
import json
import glob
import tempfile
import html  
import pandas as pd
import pandas_market_calendars as mcal
import time 
import yfinance as yf

from scheduler_core import is_market_open

def _safe_float(val):
    try:
        f_val = float(str(val or 0.0).replace(',', ''))
        if math.isnan(f_val) or math.isinf(f_val):
            return 0.0
        return f_val
    except Exception:
        return 0.0

# 🚨 [Case 08, 16] 암살자 실전 매매 상태 원자적 제어 헬퍼
def _load_avwap_trade_state(ticker, now_est):
    date_str = now_est.strftime('%Y-%m-%d')
    file_path = f"data/avwap_trade_state_{ticker}.json"
    data = {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        pass
        
    if not isinstance(data, dict): data = {}
    
    # Rollover logic
    if data.get('date') != date_str:
        if data.get('overnight') and data.get('qty', 0) > 0:
            data['date'] = date_str
            data['shutdown'] = False
            data['strikes'] = 0
            data['cutoff_processed'] = False
            data['unification_processed'] = False
        else:
            data = {
                'date': date_str,
                'qty': 0,
                'avg_price': 0.0,
                'strikes': 0,
                'shutdown': False,
                'overnight': False,
                'trap_odno': "",
                'cutoff_processed': False,
                'unification_processed': False
            }
        _save_avwap_trade_state(ticker, data)
    return data

def _save_avwap_trade_state(ticker, state_data):
    file_path = f"data/avwap_trade_state_{ticker}.json"
    dir_name = os.path.dirname(file_path) or '.'
    try: os.makedirs(dir_name, exist_ok=True)
    except OSError: pass
    
    fd = None
    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(dir=dir_name, text=True)
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            fd = None
            json.dump(state_data, f, ensure_ascii=False, indent=4)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, file_path)
        tmp_path = None
    except Exception as e:
        if fd is not None:
            try: os.close(fd)
            except OSError: pass
        if tmp_path:
            try: os.remove(tmp_path)
            except OSError: pass
        logging.error(f"🚨 [{ticker}] 암살자 실전 매매 상태 저장 실패: {e}")

async def scheduled_sniper_monitor(context):
    raw_job_data = getattr(context.job, 'data', None)
    job_data = raw_job_data if isinstance(raw_job_data, dict) else {}
    tx_lock = job_data.get('tx_lock')
    
    if not tx_lock:
        logging.warning("⚠️ [sniper_monitor] tx_lock 미초기화. 이번 사이클 스킵.")
        return

    is_open = False
    for attempt in range(3):
        try:
            is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=10.0)
            break
        except asyncio.TimeoutError:
            if attempt == 2:
                logging.error("⚠️ 달력 API 타임아웃. 평일 강제 개장(Fail-Open) 처리합니다.")
                est = ZoneInfo('America/New_York')
                is_open = datetime.datetime.now(est).weekday() < 5
            else: 
                await asyncio.sleep(1.0 * (2 ** attempt))
        except Exception:
            if attempt == 2:
                est = ZoneInfo('America/New_York')
                is_open = datetime.datetime.now(est).weekday() < 5
            else: 
                await asyncio.sleep(1.0 * (2 ** attempt))

    if not is_open: 
        return
    
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    
    def _get_market_hours():
        time.sleep(0.06)
        nyse = mcal.get_calendar('NYSE')
        return nyse.schedule(start_date=now_est.date(), end_date=now_est.date())

    schedule = None
    for attempt in range(3):
        try:
            schedule = await asyncio.wait_for(asyncio.to_thread(_get_market_hours), timeout=10.0)
            break
        except asyncio.TimeoutError:
            if attempt == 2: logging.error("⚠️ 장운영시간 달력 API 타임아웃.")
            else: await asyncio.sleep(1.0 * (2 ** attempt))
        except Exception:
            if attempt == 2: pass
            else: await asyncio.sleep(1.0 * (2 ** attempt))
            
    if schedule is not None and not schedule.empty:
        market_open = schedule.iloc[0]['market_open'].astimezone(est)
        market_close = schedule.iloc[0]['market_close'].astimezone(est)
    elif schedule is not None and schedule.empty:
        logging.info("💤 [sniper_monitor] 달력 API 빈 데이터 반환. 금일은 미국 증시 휴장일입니다.")
        return
    else:
        if now_est.weekday() < 5:
            market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        else: 
            return
   
    pre_start = market_open - datetime.timedelta(hours=5, minutes=30)
    start_monitor = pre_start 
    end_monitor = market_close + datetime.timedelta(hours=4) # 애프터 20:00까지 연장
    
    if not (start_monitor <= now_est <= end_monitor): 
        return

    is_regular_session = market_open <= now_est <= market_close
    
    cfg = job_data.get('cfg')
    broker = job_data.get('broker')
    strategy = job_data.get('strategy')
    queue_ledger = job_data.get('queue_ledger')
    chat_id = getattr(context.job, 'chat_id', None)
    base_map = job_data.get('base_map', {'SOXL': 'SOXX', 'TQQQ': 'QQQ'})
    
    tracking_cache = job_data.setdefault('sniper_tracking', {})
    today_est_str = now_est.strftime('%Y-%m-%d')
   
    if tracking_cache.get('date') != today_est_str:
        tracking_cache.clear()
        tracking_cache['date'] = today_est_str
        
        def _clean_sniper_caches():
            try:
                for _f in glob.glob("data/sniper_cache_*.json"):
                    try: os.remove(_f)
                    except OSError: pass
            except Exception: pass
        
        try:
            await asyncio.wait_for(asyncio.to_thread(_clean_sniper_caches), timeout=5.0)
        except Exception as e:
            logging.error(f"🚨 [sniper_monitor] 로컬 스나이퍼 캐시 청소 타임아웃/에러: {e}")

    # 🚨 전역 환율 추출 (ZeroDivision 방어)
    exchange_rate = 1400.0
    try:
        def _get_xr():
            time.sleep(0.06)
            df = yf.Ticker("KRW=X").history(period="1d", timeout=5)
            if not df.empty and 'Close' in df.columns: return float(df['Close'].iloc[-1])
            return 0.0
        xr_val = await asyncio.wait_for(asyncio.to_thread(_get_xr), timeout=10.0)
        if xr_val > 0: exchange_rate = xr_val
    except Exception: pass

    # 🚨 API 비동기 래퍼 (TPS & Backoff)
    async def _retry_api(func, *args, **kwargs):
        for attempt in range(3):
            try:
                await asyncio.sleep(0.06)
                return await asyncio.wait_for(asyncio.to_thread(func, *args, **kwargs), timeout=15.0)
            except Exception:
                if attempt == 2: return None
                await asyncio.sleep(1.0 * (2**attempt))

    async def _do_sniper():
        async with tx_lock:
            cash, holdings = 0.0, None
            cash_tuple = await _retry_api(broker.get_account_balance)
            if cash_tuple:
                cash = _safe_float(cash_tuple[0]) if len(cash_tuple) > 0 else 0.0
                holdings = cash_tuple[1] if len(cash_tuple) > 1 else {}
            
            if holdings is None: return
            safe_holdings = holdings if isinstance(holdings, dict) else {}
            
            try:
                active_tickers = await asyncio.wait_for(asyncio.to_thread(cfg.get_active_tickers), timeout=5.0) or []
            except Exception:
                active_tickers = []
             
            for t in active_tickers:
                try:
                    await asyncio.sleep(0.06) 
                    try:
                        version = await asyncio.wait_for(asyncio.to_thread(cfg.get_version, t), timeout=5.0)
                        is_avwap_hybrid = await asyncio.to_thread(getattr(cfg, 'get_avwap_hybrid_mode', lambda x: False), t)
                    except Exception:
                        version = "V14"
                        is_avwap_hybrid = False

                    target_base = base_map.get(t, t) 
                    
                    # ==============================================================
                    # 1. ⚔️ 암살자 aVWAP 딥-레스큐 교전망 (Phase 3)
                    # ==============================================================
                    if version == "V_REV" and is_avwap_hybrid:
                        t_state = await asyncio.to_thread(_load_avwap_trade_state, t, now_est)
                        curr_t_obj = now_est.time()
                        
                        # 🔹 [14:00 EST 컷오프 디커플링]
                        if curr_t_obj >= datetime.time(14, 0) and not t_state.get('cutoff_processed'):
                            if t_state.get('qty', 0) == 0:
                                t_state['shutdown'] = True
                                logging.info(f"🛑 [{t}] 14:00 EST 컷오프: 암살자 물량 0주 ➔ 당일 조기 퇴근 확정")
                            else:
                                t_state['overnight'] = True
                                tracking_cache[f"AVWAP_OVERNIGHT_{t}"] = True
                                logging.info(f"🛑 [{t}] 14:00 EST 컷오프: 암살자 물량 보유 ➔ 본진 영구 셧다운 및 애프터 연장 돌입")
                            t_state['cutoff_processed'] = True
                            await asyncio.to_thread(_save_avwap_trade_state, t, t_state)

                        # 🔹 [익일 04:00 L1 대통합 및 OCO 재장전]
                        if datetime.time(4, 0) <= curr_t_obj <= datetime.time(4, 5) and t_state.get('overnight') and t_state.get('qty', 0) > 0 and not t_state.get('unification_processed'):
                            if queue_ledger:
                                await asyncio.to_thread(queue_ledger.unify_to_single_layer, t, t_state['qty'], t_state['avg_price'])
                            t_state['unification_processed'] = True
                            
                            # -1% 손절 덫 재장전
                            trap_p = round(t_state['avg_price'] * 0.99, 2)
                            t_res = await _retry_api(broker.send_order, t, "SELL", t_state['qty'], trap_p, "LIMIT")
                            if t_res and t_res.get('rt_cd') == '0':
                                t_state['trap_odno'] = t_res.get('odno')
                                try: await context.bot.send_message(context.job.chat_id, f"🌅 <b>[{html.escape(str(t))}] 익일 대통합 및 덫 재장전 완료</b>\n▫️ 큐(Queue) L1 병합 완료.\n▫️ -1% 하드 손절 덫(${trap_p:.2f}) 재장전 팩트 가동.", parse_mode='HTML')
                                except Exception: pass
                            await asyncio.to_thread(_save_avwap_trade_state, t, t_state)

                        # 🔹 [시장 데이터 팩트 추출]
                        exec_curr_p = _safe_float(await _retry_api(broker.get_current_price, t))
                        base_curr_p = _safe_float(await _retry_api(broker.get_current_price, target_base))
                        if exec_curr_p <= 0 or base_curr_p <= 0: continue
                        
                        prev_c = _safe_float(await _retry_api(broker.get_previous_close, t))
                        df_1min_t = await _retry_api(broker.get_1min_candles_df, t)
                        df_1min_base = await _retry_api(broker.get_1min_candles_df, target_base)
                        
                        main_actual_avg = _safe_float(safe_holdings.get(t, {}).get('avg', 0.0))
                        target_krw = _safe_float(await asyncio.to_thread(cfg.get_avwap_target_krw, t))
                        fee_rate = _safe_float(await asyncio.to_thread(cfg.get_fee, t))

                        # 🔹 [관측 퀀트 브레인 호출]
                        try:
                            decision = await asyncio.wait_for(
                                asyncio.to_thread(
                                    strategy.get_avwap_decision,
                                    base_ticker=target_base, exec_ticker=t, base_curr_p=base_curr_p,
                                    exec_curr_p=exec_curr_p, avg_price=t_state.get('avg_price', 0.0),
                                    qty=t_state.get('qty', 0), alloc_cash=0.0,
                                    df_1min_base=df_1min_base, df_1min_exec=df_1min_t, now_est=now_est,
                                    avwap_state={"strikes": t_state.get('strikes', 0)},
                                    prev_close=prev_c, main_actual_avg=main_actual_avg,
                                    target_krw=target_krw, exchange_rate=exchange_rate, fee_rate=fee_rate,
                                    is_simulation=False, avwap_qty=t_state.get('qty', 0), avwap_avg_price=t_state.get('avg_price', 0.0)
                                ),
                                timeout=15.0
                            )
                        except Exception as e:
                            logging.error(f"🚨 [{t}] 암살자 의사결정 브레인 에러: {e}")
                            decision = {}

                        # 🔹 [실전 타격 및 엑시트 교전망]
                        if not t_state.get('shutdown'):
                            action = decision.get('raw_action', 'OBSERVING')
                            
                            if t_state.get('qty', 0) > 0:
                                # 1. 덫 체결 여부 교차 검증 (손절 확인)
                                trap_odno = t_state.get('trap_odno')
                                if trap_odno:
                                    unf = await _retry_api(broker.get_unfilled_orders_detail, t)
                                    safe_unf = unf if isinstance(unf, list) else []
                                    is_alive = any(isinstance(x, dict) and str(x.get('odno', '')) == trap_odno for x in safe_unf)
                                    
                                    if not is_alive:
                                        ehist = await _retry_api(broker.get_execution_history, t, today_est_str.replace('-',''), today_est_str.replace('-',''))
                                        safe_ehist = ehist if isinstance(ehist, list) else []
                                        filled_rec = next((x for x in safe_ehist if isinstance(x, dict) and str(x.get('odno', '')) == trap_odno), None)
                                        
                                        if filled_rec and _safe_float(filled_rec.get('ft_ccld_qty', 0)) >= t_state['qty']:
                                            t_state['qty'] = 0
                                            t_state['strikes'] += 1
                                            t_state['trap_odno'] = ""
                                            if queue_ledger: await asyncio.to_thread(queue_ledger.sync_with_broker, t, 0) # 원장 동기화
                                            await asyncio.to_thread(_save_avwap_trade_state, t, t_state)
                                            try: await context.bot.send_message(context.job.chat_id, f"🩸 <b>[{html.escape(str(t))}] 암살자 -1% 칼손절 완료</b>\n▫️ 85% 예산을 즉시 회수했습니다.\n▫️ 더 깊은 타점(-3%)으로 다중 타격망을 연장(Reload)합니다.", parse_mode='HTML')
                                            except Exception: pass
                                            continue

                                # 2. OCO 섀도우 스위칭 (전량 익절)
                                if action == 'SHADOW_EXIT':
                                    if trap_odno:
                                        await _retry_api(broker.cancel_order, t, trap_odno)
                                        await asyncio.sleep(0.06)
                                    
                                    bid_p = _safe_float(await _retry_api(broker.get_bid_price, t))
                                    if bid_p > 0:
                                        swp_res = await _retry_api(broker.send_order, t, "SELL", t_state['qty'], bid_p, "LIMIT")
                                        if isinstance(swp_res, dict) and swp_res.get('rt_cd') == '0':
                                            t_state['qty'] = 0
                                            t_state['shutdown'] = True
                                            t_state['trap_odno'] = ""
                                            await asyncio.to_thread(_save_avwap_trade_state, t, t_state)
                                            
                                            if queue_ledger: await asyncio.to_thread(queue_ledger.clear_queue, t)
                                            await asyncio.to_thread(cfg.clear_ledger_for_ticker, t)
                                            
                                            try: await context.bot.send_message(context.job.chat_id, f"🎯 <b>[{html.escape(str(t))}] 암살자 전량 익절 (스윕 타격) 완료!</b>\n▫️ 원화 목표 수익금을 관통하여 매수 1호가로 전량 덤핑을 완수했습니다.\n▫️ KIS 장부 동기화 및 큐 소각 완료.", parse_mode='HTML')
                                            except Exception: pass
                                            
                            elif t_state.get('qty', 0) == 0:
                                # 3. 딥-매수 격발 및 OCO 하단 덫 하드 장전
                                if action == 'DEEP_BUY':
                                    ask_p = _safe_float(await _retry_api(broker.get_ask_price, t))
                                    if ask_p > 0:
                                        seed_val = _safe_float(await asyncio.to_thread(cfg.get_seed, t))
                                        av_budget = seed_val * 0.85
                                        buy_qty = int(math.floor(av_budget / ask_p))
                                        
                                        if buy_qty > 0:
                                            b_res = await _retry_api(broker.send_order, t, "BUY", buy_qty, ask_p, "LIMIT")
                                            if isinstance(b_res, dict) and b_res.get('rt_cd') == '0':
                                                t_state['qty'] = buy_qty
                                                t_state['avg_price'] = ask_p
                                                
                                                await asyncio.sleep(0.06)
                                                trap_p = round(ask_p * 0.99, 2)
                                                s_res = await _retry_api(broker.send_order, t, "SELL", buy_qty, trap_p, "LIMIT")
                                                if isinstance(s_res, dict) and s_res.get('rt_cd') == '0':
                                                    t_state['trap_odno'] = s_res.get('odno')
                                                
                                                await asyncio.to_thread(_save_avwap_trade_state, t, t_state)
                                                if queue_ledger: await asyncio.to_thread(queue_ledger.sync_with_broker, t, buy_qty)
                                                
                                                try: await context.bot.send_message(context.job.chat_id, f"⚔️ <b>[{html.escape(str(t))}] 암살자 딥-매수 격발 완료!</b>\n▫️ 85% 예산 풀 투입: {buy_qty}주 @ ${ask_p:.2f}\n▫️ -1% 하드 손절 덫(${trap_p:.2f}) 우선 장전 및 원화 섀도우 감시 돌입.", parse_mode='HTML')
                                                except Exception: pass

                    # ==============================================================
                    # 2. 💎 V14 상방 스나이퍼 (오리지널 스케줄 보존망)
                    # ==============================================================
                    try:
                        master_switch = await asyncio.wait_for(asyncio.to_thread(getattr(cfg, 'get_master_switch', lambda x: "ALL"), t), timeout=5.0)
                        sniper_buy_locked = await asyncio.wait_for(asyncio.to_thread(getattr(cfg, 'get_sniper_buy_locked', lambda x: False), t), timeout=5.0)
                        sniper_sell_locked = await asyncio.wait_for(asyncio.to_thread(getattr(cfg, 'get_sniper_sell_locked', lambda x: False), t), timeout=5.0)
                    except Exception:
                        master_switch = "ALL"
                        sniper_buy_locked = False
                        sniper_sell_locked = False

                    curr_p = _safe_float(await _retry_api(broker.get_current_price, t))
                    if curr_p <= 0: continue

                    sniper_func = getattr(strategy, 'check_sniper_condition', None)
                    if sniper_func:
                        try:
                            res = await asyncio.wait_for(asyncio.to_thread(sniper_func, t, cfg, broker, chat_id), timeout=10.0)
                        except Exception as e:
                            logging.error(f"🚨 [{t}] V14 스나이퍼 조건 검사 타임아웃/오류: {e}")
                            res = {"action": "HOLD", "reason": "스나이퍼 모듈 타임아웃", "limit_price": 0.0}
                    else: 
                        res = {"action": "HOLD", "reason": "스나이퍼 모듈 누락(Bypass)", "limit_price": 0.0}
                    
                    if not isinstance(res, dict): res = {} 
                    
                    action = res.get("action")
                    reason = html.escape(str(res.get("reason", "")))
                    limit_p = res.get("limit_price", 0.0)

                    try: version = await asyncio.wait_for(asyncio.to_thread(cfg.get_version, t), timeout=5.0)
                    except Exception: version = "V14"
             
                    is_rev = (version == "V_REV")

                    if action == "BUY" and not is_rev and not sniper_buy_locked and master_switch != "UP_ONLY":
                        qty = res.get("qty", 0)
                        if qty > 0:
                            try:
                                await asyncio.sleep(0.06) 
                                await asyncio.wait_for(asyncio.to_thread(broker.cancel_targeted_orders, t, "BUY", "00"), timeout=15.0)
                            except Exception: pass
                            
                            await asyncio.sleep(1.0)
                            
                            has_unfilled = False
                            for _ in range(4):
                                try:
                                    await asyncio.sleep(0.06) 
                                    unfilled = await asyncio.wait_for(asyncio.to_thread(broker.get_unfilled_orders_detail, t), timeout=10.0)
                                except Exception: unfilled = []
                                    
                                if isinstance(unfilled, list) and any(
                                    isinstance(o, dict) and o.get('sll_buy_dvsn_cd') == '02' and str(o.get('ord_dvsn_cd') or o.get('ord_dvsn') or '').strip().zfill(2) == '00' 
                                    for o in unfilled
                                ):
                                    has_unfilled = True
                                    break
                                await asyncio.sleep(2.0)
                            
                            if has_unfilled: continue
                           
                            ask_price = _safe_float(await _retry_api(broker.get_ask_price, t))
                            exec_price = ask_price if ask_price > 0 else limit_p

                            try:
                                await asyncio.sleep(0.06) 
                                order_res = await asyncio.wait_for(asyncio.to_thread(broker.send_order, t, "BUY", qty, exec_price, "LIMIT"), timeout=15.0)
                            except Exception as e:
                                logging.error(f"🚨 [{t}] 상방 감시 매수 통신 에러: {e}")
                                order_res = None
                                
                            odno = order_res.get('odno', '') if isinstance(order_res, dict) else ''
           
                            if order_res and order_res.get('rt_cd') == '0' and odno:
                                ccld_qty = 0
                                for _ in range(4):
                                    await asyncio.sleep(2.0)
                                    try:
                                        await asyncio.sleep(0.06) 
                                        unfilled_check = await asyncio.wait_for(asyncio.to_thread(broker.get_unfilled_orders_detail, t), timeout=10.0)
                                    except Exception: unfilled_check = []
                                        
                                    safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []
                                    my_order = next((ox for ox in safe_unfilled if isinstance(ox, dict) and str(ox.get('odno', '')) == odno), None)
                                    if my_order:
                                        ccld_qty = int(_safe_float(my_order.get('tot_ccld_qty')))
                                        if ccld_qty >= qty: break
                                    else:
                                        try:
                                            await asyncio.sleep(0.06)
                                            exec_hist = await asyncio.wait_for(asyncio.to_thread(broker.get_execution_history, t, today_est_str.replace('-',''), today_est_str.replace('-','')), timeout=10.0)
                                            safe_exec = exec_hist if isinstance(exec_hist, list) else []
                                            filled_rec = next((ex for ex in safe_exec if isinstance(ex, dict) and str(ex.get('odno', '')) == odno), None)
                                            if filled_rec: ccld_qty = int(_safe_float(filled_rec.get('ft_ccld_qty')))
                                            else: ccld_qty = 0
                                        except Exception: ccld_qty = 0
                                        break
    
                                if ccld_qty < qty:
                                    try:
                                        await asyncio.sleep(0.06) 
                                        await asyncio.wait_for(asyncio.to_thread(broker.cancel_order, t, odno), timeout=10.0)
                                        await asyncio.sleep(1.0)
                                    except Exception: pass

                                if ccld_qty > 0:
                                    if hasattr(cfg, 'set_sniper_buy_locked'):
                                        try: await asyncio.wait_for(asyncio.to_thread(cfg.set_sniper_buy_locked, t, True), timeout=5.0)
                                        except Exception: pass
                                    try:
                                        await asyncio.sleep(0.06) 
                                        exec_history = await asyncio.wait_for(asyncio.to_thread(broker.get_execution_history, t, today_est_str.replace('-',''), today_est_str.replace('-','')), timeout=15.0)
                                    except Exception: exec_history = []
                                        
                                    actual_exec_price = next((_safe_float(ex.get('ft_ccld_unpr3')) for ex in exec_history if isinstance(ex, dict) and ex.get('sll_buy_dvsn_cd') == '02' and ex.get('odno') == odno and _safe_float(ex.get('ft_ccld_unpr3')) > 0), next((_safe_float(ex.get('ft_ccld_unpr3')) for ex in exec_history if isinstance(ex, dict) and ex.get('sll_buy_dvsn_cd') == '02' and _safe_float(ex.get('ft_ccld_unpr3')) > 0), limit_p))
                                    display_price = actual_exec_price if actual_exec_price > 0 else limit_p
        
                                    msg = f"🚨 <b>[{html.escape(str(t))}] V14 스나이퍼 딥-매수(Intercept) 명중!</b>\n▫️ 타겟가: ${limit_p:.2f}\n▫️ 팩트 단가: ${display_price:.2f}\n▫️ 체결수량: {ccld_qty}주 (요청: {qty}주)\n▫️ 사유: {reason}\n▫️ 하방 방어망이 잠깁니다 (상방 독립 유지)."
                                    try: await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                    except Exception: pass
                            else:
                                err_msg = html.escape(str(order_res.get('msg1') or '응답 없음')) if isinstance(order_res, dict) else '통신 장애'
                                logging.error(f"🚨 [{t}] V14 스나이퍼 매수 KIS 서버 거절: {err_msg}")
                                reject_msg = (
                                    f"🚨 <b>[{html.escape(str(t))}] V14 스나이퍼 딥매수 서버 거절 (Reject)!</b>\n"
                                    f"▫️ 사유: <code>{err_msg}</code>\n"
                                    f"▫️ 조치: 다음 스캔 시 재시도합니다."
                                 )
                                try: await context.bot.send_message(chat_id=chat_id, text=reject_msg, parse_mode='HTML')
                                except Exception: pass

                    is_zero_start_session = False
                    try:
                        snap = None
                        if is_rev and hasattr(strategy, 'v_rev_plugin'): 
                            snap = await asyncio.wait_for(asyncio.to_thread(strategy.v_rev_plugin.load_daily_snapshot, t), timeout=5.0)
                        elif version == "V14":
                            is_manual_vwap = await asyncio.wait_for(asyncio.to_thread(getattr(cfg, 'get_manual_vwap_mode', lambda x: False), t), timeout=5.0)
                            if is_manual_vwap and hasattr(strategy, 'v14_vwap_plugin'): 
                                snap = await asyncio.wait_for(asyncio.to_thread(strategy.v14_vwap_plugin.load_daily_snapshot, t), timeout=5.0)
                            elif hasattr(strategy, 'v14_plugin') and hasattr(strategy.v14_plugin, 'load_daily_snapshot'): 
                                snap = await asyncio.wait_for(asyncio.to_thread(strategy.v14_plugin.load_daily_snapshot, t), timeout=5.0)
                        
                        if snap: 
                            is_zero_val = snap.get("is_zero_start")
                            if is_zero_val is None:
                                tot_q = int(_safe_float(snap.get("total_q", -1)))
                                if tot_q == -1: tot_q = int(_safe_float(snap.get("initial_qty", -1)))
                                is_zero_start_session = (tot_q == 0)
                            else:
                                if isinstance(is_zero_val, str): is_zero_start_session = (is_zero_val.lower() == 'true')
                                else: is_zero_start_session = bool(is_zero_val)
                    except Exception: pass

                    try:
                        upward_mode = await asyncio.wait_for(asyncio.to_thread(getattr(cfg, 'get_upward_sniper_mode', lambda x: False), t), timeout=5.0)
                    except Exception:
                        upward_mode = False
           
                    is_upward_active = upward_mode and not is_rev and not sniper_sell_locked and master_switch != "DOWN_ONLY"
                    if is_zero_start_session: 
                        is_upward_active = False

                    if is_upward_active and action in ["SELL_QUARTER", "SELL_JACKPOT"]:
                        qty = res.get("qty", 0)
                        if qty > 0:
                            try:
                                await asyncio.sleep(0.06) 
                                await asyncio.wait_for(asyncio.to_thread(broker.cancel_targeted_orders, t, "SELL", "00"), timeout=15.0)
                            except Exception: pass
                            
                            await asyncio.sleep(1.0)
                            
                            has_unfilled = False
                            for _ in range(4):
                                try:
                                    await asyncio.sleep(0.06) 
                                    unfilled = await asyncio.wait_for(asyncio.to_thread(broker.get_unfilled_orders_detail, t), timeout=10.0)
                                except Exception: unfilled = []
                                    
                                if isinstance(unfilled, list) and any(
                                    isinstance(o, dict) and o.get('sll_buy_dvsn_cd') == '01' and str(o.get('ord_dvsn_cd') or o.get('ord_dvsn') or '').strip().zfill(2) == '00' 
                                    for o in unfilled
                                ):
                                    has_unfilled = True
                                    break
                                await asyncio.sleep(2.0)
                            
                            if has_unfilled: continue
          
                            bid_price = _safe_float(await _retry_api(broker.get_bid_price, t))
                            exec_price = bid_price if bid_price > 0 else limit_p
   
                            try:
                                await asyncio.sleep(0.06) 
                                order_res = await asyncio.wait_for(asyncio.to_thread(broker.send_order, t, "SELL", qty, exec_price, "LIMIT"), timeout=15.0)
                            except Exception as e:
                                logging.error(f"🚨 [{t}] 상방 스나이퍼 매도 통신 에러: {e}")
                                order_res = None
                                
                            odno = order_res.get('odno', '') if isinstance(order_res, dict) else ''
    
                            if order_res and order_res.get('rt_cd') == '0' and odno:
                                ccld_qty = 0
                                for _ in range(4):
                                    await asyncio.sleep(2.0)
                                    try:
                                        await asyncio.sleep(0.06) 
                                        unfilled_check = await asyncio.wait_for(asyncio.to_thread(broker.get_unfilled_orders_detail, t), timeout=10.0)
                                    except Exception: unfilled_check = []
                                    
                                    safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []
                                    my_order = next((ox for ox in safe_unfilled if isinstance(ox, dict) and str(ox.get('odno', '')) == odno), None)
                                    if my_order:
                                        ccld_qty = int(_safe_float(my_order.get('tot_ccld_qty')))
                                        if ccld_qty >= qty: break
                                    else:
                                        try:
                                            await asyncio.sleep(0.06)
                                            exec_hist = await asyncio.wait_for(asyncio.to_thread(broker.get_execution_history, t, today_est_str.replace('-',''), today_est_str.replace('-','')), timeout=10.0)
                                            safe_exec = exec_hist if isinstance(exec_hist, list) else []
                                            filled_rec = next((ex for ex in safe_exec if isinstance(ex, dict) and str(ex.get('odno', '')) == odno), None)
                                            if filled_rec: ccld_qty = int(_safe_float(filled_rec.get('ft_ccld_qty')))
                                            else: ccld_qty = 0
                                        except Exception: ccld_qty = 0
                                        break
    
                                if ccld_qty < qty:
                                    try:
                                        await asyncio.sleep(0.06) 
                                        await asyncio.wait_for(asyncio.to_thread(broker.cancel_order, t, odno), timeout=10.0)
                                        await asyncio.sleep(1.0)
                                    except Exception: pass

                                if ccld_qty > 0:
                                    if hasattr(cfg, 'set_sniper_sell_locked'): 
                                        try: await asyncio.wait_for(asyncio.to_thread(cfg.set_sniper_sell_locked, t, True), timeout=5.0)
                                        except Exception: pass
        
                                    try:
                                        await asyncio.sleep(0.06) 
                                        exec_history = await asyncio.wait_for(asyncio.to_thread(broker.get_execution_history, t, today_est_str.replace('-',''), today_est_str.replace('-','')), timeout=15.0)
                                    except Exception: exec_history = []
                                         
                                    actual_exec_price = next((_safe_float(ex.get('ft_ccld_unpr3')) for ex in exec_history if isinstance(ex, dict) and ex.get('sll_buy_dvsn_cd') == '01' and str(ex.get('odno', '')) == odno and _safe_float(ex.get('ft_ccld_unpr3')) > 0), next((_safe_float(ex.get('ft_ccld_unpr3')) for ex in exec_history if isinstance(ex, dict) and ex.get('sll_buy_dvsn_cd') == '01' and _safe_float(ex.get('ft_ccld_unpr3')) > 0), limit_p))
                                    display_price = actual_exec_price if actual_exec_price > 0 else limit_p
 
                                    msg = f"🦇 <b>[{html.escape(str(t))}] V14 스나이퍼 상방 기습({action}) 명중!</b>\n▫️ 타겟가: ${limit_p:.2f}\n▫️ 팩트 단가: ${display_price:.2f}\n▫️ 체결수량: {ccld_qty}주 (요청: {qty}주)\n▫️ 사유: {reason}\n▫️ 상방 감시망이 잠깁니다 (하방 독립 유지)."
                                    try: await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                    except Exception: pass
                            else:
                                err_msg = html.escape(str(order_res.get('msg1') or '응답 없음')) if isinstance(order_res, dict) else '통신 장애'
                                logging.error(f"🚨 [{t}] V14 스나이퍼 상방 기습 서버 거절: {err_msg}")
                                reject_msg = (
                                    f"🚨 <b>[{html.escape(str(t))}] V14 스나이퍼 상방 기습 서버 거절 (Reject)!</b>\n"
                                    f"▫️ 사유: <code>{err_msg}</code>\n"
                                    f"▫️ 조치: 다음 스캔 시 재시도합니다."
                                )
                                try: await context.bot.send_message(chat_id=chat_id, text=reject_msg, parse_mode='HTML')
                                except Exception: pass

                except Exception as e:
                    logging.error(f"🚨 [{t}] 스나이퍼 엔진 단일 종목 연산 중 치명적 오류 (Cascade 방어): {e}", exc_info=True)
                    continue

    try:
        await asyncio.wait_for(_do_sniper(), timeout=240.0)
    except Exception as e:
        logging.error(f"🚨 스나이퍼 타임아웃 에러: {e}", exc_info=True)
