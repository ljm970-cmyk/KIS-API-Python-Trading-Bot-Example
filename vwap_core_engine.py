# ==========================================================
# FILE: vwap_core_engine.py
# ==========================================================
# 🚨 VERIFIED: [도메인 주도 설계 (DDD) 신규 파일] 정규장 VWAP(슬라이싱/하이재킹) 매매망 전담 엔진
# 🚨 NEW: [V14 자율주행 엔진 대통합] V14 VWAP 모드 또한 본 파일의 Gap Hijack 및 Slicing 로직을 100% 공유하도록 병합 및 예산 분리 팩트 락온 완료
# 🚨 MODIFIED: [Case 41 절대 방어망 팩트 수술] 상방 갭 하이재킹(Upward Sell Hijack) 발동 시, V-REV 모드는 KIS 실잔고 조회를 전면 소각하고 오직 '로컬 큐(Queue) 장부'의 합산 수량만을 100% 락온하여 개인 장기 물량 훼손을 원천 차단.
# 🚨 NEW: [양방향 Gap Hijack 팩트 결속] 하방(-2%) 전용이던 섀도우 엔진에 상방(+2.0% 하드코딩) 매도 하이재킹 엔진 100% 팩트 이식.
# 🚨 MODIFIED: [자전거래 간섭 방어] 상/하방 하이재킹 성공 시 hijacked 플래그를 원자적으로 락온하여 잔여 1분 슬라이싱 매수/매도 행위를 완벽히 마비.
# 🚨 MODIFIED: [제2헌법 준수] 스케줄러에서 분리되어 단일 책임 원칙(SRP)을 100% 준수하는 순수 매매 집행 코어 모듈.
# 🚨 MODIFIED: [제1헌법 절대 준수] 로컬 상태 파일 I/O, Config 조회, 큐 장부 연산 전역에 `asyncio.wait_for` 타임아웃 족쇄 강제.
# 🚨 MODIFIED: [Case 32 & 33 절대 규칙] Gap Hijack 및 1분 슬라이싱 전역에 3단 지수 백오프와 TPS 캡핑(0.06s) 샌드위치 락온.
# 🚨 MODIFIED: [Case 39 & 40 타임라인 역전 방어] 16:01 애프터장으로 지연 이관이 확정된 본진 플랜이 존재한다면 정규장 마감 직전의 Gap Hijack 및 슬라이싱 루프를 전면 바이패스(Bypass)하여 Double Spending 원천 차단.
# 🚨 MODIFIED: [Case 35 결측치 맹독성 방어] 갭 하이재킹 판별을 위한 기초지수 VWAP 연산 시, `ffill().bfill()` 래핑을 강제하여 NaN 전이(Math Collapse) 원천 차단.
# 🚨 MODIFIED: [무덤핑 정밀 요격 사수] 목표가 미충족 시 1주도 사지 않는 관망세 유지 및 15:57 클린업 페이즈 시 강제 덤핑(Dumping) 로직 전면 소각 유지.
# 🚨 MODIFIED: [V-REV 일시불 요격 패러독스 방어] 목표가가 현재가 대비 터무니없이 높을 경우, 전량 스윕(Sweep)을 강제 해제하고 정상적인 1분 슬라이싱 궤도로 복구 락온.
# 🚨 MODIFIED: [Ghost-Dumping 붕괴 방어] 1분 슬라이싱 매도 타격 직전에 KIS 실잔고를 스캔하여 수량을 정밀 캡핑(`min(qty, rt_qty)`).
# 🚨 MODIFIED: [Scope Mismatch 궁극 방어] 파일 I/O 스레드로 위임되는 `_sync_ledger_atomic` 함수에 명시적 파라미터를 주입하여 클로저 오염으로 인한 `UnboundLocalError` 원천 봉쇄.
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
    """ 🚨 [수학 연산 붕괴 방어] NaN, Infinity 및 String-Comma 맹독성 데이터 정밀 필터링 """
    try:
        f_val = float(str(val or 0.0).replace(',', ''))
        if math.isnan(f_val) or math.isinf(f_val):
            return 0.0
        return f_val
    except Exception:
        return 0.0

async def _retry_api(func, *args, timeout=15.0, default=None, **kwargs):
    """ 🚨 [Case 32, 33] 중앙 집중형 TPS 캡핑 및 지수 백오프 비동기 래퍼 """
    for attempt in range(3):
        try:
            await asyncio.sleep(0.06)
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
    """ 🚨 [Event Loop Deadlock 방어] 텔레그램 통신 샌드박스 래핑 """
    if not chat_id: return None
    try:
        return await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=text, **kwargs), timeout=timeout)
    except Exception as e:
        logging.error(f"🚨 텔레그램 전송 실패: {e}")
        return None

async def execute_vwap_init(tx_lock, cfg, broker, chat_id, context, vwap_cache):
    """ 🚨 [관측망 기상] V-REV 및 V14 슬라이싱 / 갭 하이재킹 모니터링 시작 브리핑 """
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
                
                # 🚨 V14 VWAP 모드 통합 개방 락온
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
    """ 🚨 [코어 엔진] 양방향 Gap Hijack 탐지, 자체 1분 Slicing, 무덤핑 정밀 요격 타격망 메인 루프 """
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    today_hyphen = now_est.strftime('%Y-%m-%d')
    
    # 🚨 NEW: [Time Paradox 팩트 교정] 자체 슬라이싱 KIS 원장 조회를 위한 KST 팩트 주입
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
            
            # 🚨 [보유량 팩트 선취] 상방 하이재킹 판단을 위한 실잔고 파악
            actual_qty = int(_safe_float(holdings.get(t, {}).get('qty', 0)))

            # 🚨 NEW: [큐 장부 절대주의 수복] KIS 잔고 오인 덤핑 방어를 위한 큐 장부 수량 팩트 추출
            vrev_q_qty = 0
            if queue_ledger:
                q_data = await _retry_api(queue_ledger.get_queue, t, default=[])
                vrev_q_qty = sum(int(_safe_float(item.get("qty"))) for item in (q_data or []) if isinstance(item, dict))

            try:
                version = await _retry_api(cfg.get_version, t, default="V14")
                is_manual_vwap = await _retry_api(getattr(cfg, 'get_manual_vwap_mode', lambda x: False), t, default=False)

                # 🚨 V14 VWAP 모드 통합 개방
                if version == "V_REV" or (version == "V14" and is_manual_vwap):
                    slice_file = f"data/vrev_slice_state_{t}.json"
                    
                    # 🚨 NEW: [Case 39 & 40 타임라인 역전 패러독스 차단] 애프터장 지연 이관 상태 교차 검증 (Timeline Inversion 방어)
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
                    # [ 1. Gap Hijack (양방향 갭 하이재킹) 모니터링 ]
                    # ======================================================
                    is_downward_hijacked_now = vwap_cache.get(f"REV_{t}_gap_hijack_fired", False)
                    is_upward_hijacked_now = vwap_cache.get(f"REV_{t}_upward_hijack_fired", False)
                    
                    # 🚨 V14 VWAP 모드도 Gap Hijack 엔진에 100% 동기화 팩트 허용
                    if (version == "V_REV" or (version == "V14" and is_manual_vwap)) and not (is_downward_hijacked_now or is_upward_hijacked_now):
                        # 🚨 MODIFIED: 기초자산(base_tkr) 대신 본종목(t) 100% 팩트 타격 롤오버
                        t_curr_p = _safe_float(await _retry_api(broker.get_current_price, t))
                        df_1min_t = await _retry_api(broker.get_1min_candles_df, t)
                                
                        if df_1min_t is not None and not df_1min_t.empty:
                            df_t = df_1min_t.copy()
                            
                            # 🚨 [Time Paradox 붕괴 궁극 수술] 5일치(5d) 캔들 합산으로 인한 VWAP 오염 패러독스 원천 차단
                            df_t = df_t[df_t.index.date == now_est.date()]
                            
                            if 'time_est' in df_t.columns:
                                df_t = df_t[(df_t['time_est'] >= '093000') & (df_t['time_est'] <= '155900')]
                                
                            if not df_t.empty:
                                # 🚨 MODIFIED: [Case 35 결측치 맹독성 방어] ffill 강제 락온
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
                                    logging.info(f"⚡ [{t}] Downward Gap Hijack Triggered! gap: {gap_pct:.2f}%, thresh: {gap_thresh}%")
                                    nuked_count = 0
                                    
                                    try:
                                        est_now = datetime.datetime.now(ZoneInfo('America/New_York'))
                                        d_str = est_now.strftime('%Y%m%d')
                                        
                                        resv_orders = await _retry_api(broker.get_reservation_orders, t, d_str, d_str, default=[])
                                        safe_resv_orders = resv_orders if isinstance(resv_orders, list) else []
                                        
                                        for req in safe_resv_orders:
                                            if not isinstance(req, dict): continue
                                            
                                            # 🚨 SELL 구출망 생존 락온 (예약 주문)
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
                                            
                                            # 🚨 SELL 구출망 생존 락온 (일반 미체결 주문)
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

                                    await asyncio.sleep(2.0)

                                    seed = await _retry_api(cfg.get_seed, t, default=0.0)
                                    daily_limit = _safe_float(seed) * 0.15
                                    alloc_cash = _safe_float(allocated_cash.get(t, 0.0))
                                    
                                    # 🚨 NEW: V14 vs V-REV 모드별 잔여 예산 동적 추출 디커플링
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

                                    ask_price = _safe_float(await _retry_api(broker.get_ask_price, t))
                                    curr_p = _safe_float(await _retry_api(broker.get_current_price, t))
                                            
                                    exec_price = ask_price if ask_price > 0 else curr_p
                                    buy_qty = int(math.floor(rem_budget / exec_price)) if exec_price > 0 else 0
                                    
                                    if buy_qty > 0:
                                        res = await _retry_api(broker.send_order, t, "BUY", buy_qty, exec_price, "LIMIT")
                                        safe_res = res if isinstance(res, dict) else {}
                                        odno = str(safe_res.get('odno') or '')
                                        
                                        if safe_res.get('rt_cd') == '0' and odno:
                                            vwap_cache[f"REV_{t}_gap_hijack_fired"] = True
                                            is_downward_hijacked_now = True
                                            
                                            msg = f"⚡ <b>[{html.escape(str(t))}] 🤖 하방 모멘텀 자율주행 (Gap Hijack) 섀도우 오버라이드 격발!</b>\n"
                                            msg += f"▫️ 운용종목 당일 누적 VWAP 이탈률(<b>{gap_pct:+.2f}%</b>)이 임계치(<b>{gap_thresh}%</b>)를 하향 돌파했습니다.\n"
                                            msg += f"▫️ KIS 예약/미체결 매수 덫({nuked_count}건) 파기 및 로컬 엔진 스톱 후, 잔여 예산 100%를 매도 1호가로 일괄 스윕(Sweep) 타격했습니다!\n"
                                            msg += f"▫️ 스윕 수량: <b>{buy_qty}주</b> (단가: ${exec_price:.2f})"
                                            
                                            await _safe_send(context, chat_id, msg, parse_mode='HTML')
                                            
                                            # 🚨 NEW: V14 vs V-REV 모드별 매수 장부 동기화 디커플링
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
                                        logging.info(f"⚡ [{t}] 하방 Gap Hijack 격발 조건을 만족했으나 잉여 예산 소진으로 스윕 매수 생략 (플래그 락온 완료).")

                                # ----------------------------------------------------
                                # 🚨 [B. 상방 매도 하이재킹 (Upward Sell Hijack) 격발망]
                                # ----------------------------------------------------
                                # 🚨 MODIFIED: [Case 41 절대 방어망] V-REV 모드는 KIS 실잔고를 무시하고 오직 큐 장부 물량만으로 100% 팩트 락온 (V14는 KIS 실잔고 팩트 반영)
                                can_upward_hijack = False
                                target_sell_qty = 0
                                
                                if gap_pct >= 2.0:
                                    if version == "V_REV":
                                        if vrev_q_qty > 0:
                                            can_upward_hijack = True
                                            target_sell_qty = vrev_q_qty
                                    else:
                                        if actual_qty > 0:
                                            can_upward_hijack = True
                                            target_sell_qty = actual_qty
                                            
                                if can_upward_hijack:
                                    logging.info(f"⚡ [{t}] Upward Sell Hijack Triggered! gap: {gap_pct:.2f}% >= +2.0%, Target Qty: {target_sell_qty}주")
                                    nuked_count = 0
                                    
                                    try:
                                        est_now = datetime.datetime.now(ZoneInfo('America/New_York'))
                                        d_str = est_now.strftime('%Y%m%d')
                                        
                                        resv_orders = await _retry_api(broker.get_reservation_orders, t, d_str, d_str, default=[])
                                        safe_resv_orders = resv_orders if isinstance(resv_orders, list) else []
                                        
                                        for req in safe_resv_orders:
                                            if not isinstance(req, dict): continue
                                            
                                            # 🚨 BUY는 보존하고 SELL만 핀셋 소각 (하방과 정반대)
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
    
                                    await asyncio.sleep(2.0)
    
                                    bid_price = _safe_float(await _retry_api(broker.get_bid_price, t))
                                    curr_p = _safe_float(await _retry_api(broker.get_current_price, t))
                                    
                                    exec_price = bid_price if bid_price > 0 else curr_p
                                    
                                    if exec_price > 0:
                                        res = await _retry_api(broker.send_order, t, "SELL", target_sell_qty, exec_price, "LIMIT")
                                        safe_res = res if isinstance(res, dict) else {}
                                        odno = str(safe_res.get('odno') or '')
                                        
                                        if safe_res.get('rt_cd') == '0' and odno:
                                            vwap_cache[f"REV_{t}_upward_hijack_fired"] = True
                                            is_upward_hijacked_now = True
                                            
                                            # 🚨 NEW: V14 vs V-REV 모드별 매도 장부 동기화 디커플링
                                            if version == "V_REV":
                                                if queue_ledger:
                                                    await _retry_api(queue_ledger.clear_queue, t)
                                            else:
                                                if hasattr(strategy, 'v14_vwap_plugin'):
                                                    await _retry_api(strategy.v14_vwap_plugin.record_execution, t, "SELL", target_sell_qty, exec_price)
                                            
                                            msg = f"🚀 <b>[{html.escape(str(t))}] 🤖 상방 모멘텀 자율주행 (Sell Hijack) 격발!</b>\n"
                                            msg += f"▫️ 당일 누적 VWAP 대비 현재가 슈팅(<b>+{gap_pct:.2f}%</b>)이 익절 임계치(<b>+2.0%</b>)를 관통했습니다.\n"
                                            msg += f"▫️ KIS 예약/미체결 매도 덫({nuked_count}건) 파기 후, 보유 물량을 매수 1호가로 일괄 스윕(Sweep) 덤핑하여 종가 거품 붕괴를 회피합니다!\n"
                                            msg += f"▫️ 전량 익절 수량: <b>{target_sell_qty}주</b> (단가: ${exec_price:.2f})\n"
                                            msg += f"▫️ <b>당일 슬라이싱 엔진 가동을 전면 마비시킵니다 (조기 퇴근 락온).</b>"
                                            
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
                        
                        # 🚨 MODIFIED: [Date Schema Mismatch 원천 차단] today_hyphen 강제 결속
                        if slice_state.get('date') != today_hyphen:
                            continue 
                            
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
                            
                            # 🚨 [자전거래 간섭 차단] 상/하방 하이재킹 시 슬라이싱 마비 락온
                            if (is_state_hijacked or is_state_upward_hijacked) and side == 'BUY':
                                continue
                            if is_state_upward_hijacked and side == 'SELL':
                                continue # 상방 하이재킹으로 이미 전량 던졌으므로 SELL 슬라이싱 전면 마비
                            
                            if filled_qty >= total_qty and not last_odno:
                                continue
                            
                            ccld_qty_this_tick = 0
                            if last_odno:
                                cancel_successful = False
                                c_res = await _retry_api(broker.cancel_order, t, last_odno, timeout=10.0)
                                if isinstance(c_res, dict) and str(c_res.get('rt_cd', '')) == '0':
                                    cancel_successful = True
                                    await asyncio.sleep(0.5) 
                                    
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
                                    # 🚨 NEW: [Time Paradox 팩트 교정] KIS 원장 100% 동기화를 위해 today_kst_str 주입
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

                                        # 🚨 NEW: [Scope Mismatch 궁극 방어] 클로저(Closure) 오염을 방지하기 위한 명시적 파라미터 패싱 락온
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
                                            # 🚨 MODIFIED: [제1헌법] 파라미터 명시적 전달 및 wait_for 타임아웃 래핑
                                            p_sync = functools.partial(_sync_ledger_atomic, t, side, ccld_qty_this_tick, real_exec_price, queue_ledger, strategy, version)
                                            await asyncio.wait_for(asyncio.to_thread(p_sync), timeout=10.0)
                                            logging.info(f"💾 [{t}] 자체 슬라이싱 체결 장부 원자적 동기화 완료: {side} {ccld_qty_this_tick}주 @ ${real_exec_price:.2f}")
                                        except Exception as e:
                                            processed_odnos.remove(last_odno) # 롤백 처리
                                            logging.error(f"🚨 [{t}] 자체 슬라이싱 체결 장부 동기화 실패 (캐시 롤백): {e}")
                                        
                                        # 🚨 MODIFIED: [메시지 폭탄 소각] 1분 단위 Slicing 체결마다 텔레그램을 타전하는 Spaming 뇌관을 전면 소각하고, 16:05 EST 일괄 정산망으로 100% 위임합니다.
                                        msg_side = "매수" if side == "BUY" else "매도"
                                        logging.info(f"⚡ [{t}] 섀도 엔진 체결 팩트 장부 동기화 완료: {msg_side} {ccld_qty_this_tick}주 @ ${real_exec_price:.2f} (텔레그램 타전 바이패스)")

                                filled_qty += ccld_qty_this_tick
                                o['filled_qty'] = filled_qty
                                o['last_odno'] = ""
                                o['last_sent_qty'] = 0
                                state_changed = True
                            
                            # 🚨 NEW: [무덤핑 절대 헌법 팩트 결속]
                            if is_cleanup_phase:
                                continue # 15:57 이후로는 미체결 취소만 집행하고 신규 타격을 전면 바이패스(Bypass)

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
                                         
                            # 🚨 NEW: [무덤핑 정밀 요격 사수] 목표 타점 충족 시에만 100% 즉시 타격 (미충족 시 철저한 관망세)
                            if target_price > 0.0:
                                is_target_hit = False
                                if side == "BUY" and exec_price <= target_price:
                                    is_target_hit = True
                                elif side == "SELL" and exec_price >= target_price:
                                    is_target_hit = True

                                if is_target_hit:
                                    # 🚨 NEW: [V-REV 일시불 요격 패러독스 방어] 목표가가 터무니없이 멀리 있는 경우 스윕(Sweep)을 강제 해제하고 1분 슬라이싱 궤도 락온
                                    if side == "BUY" and target_price > exec_price * 1.02:
                                        qty_to_send = target_cum_qty - filled_qty
                                    elif side == "SELL" and target_price < exec_price * 0.98:
                                        qty_to_send = target_cum_qty - filled_qty
                                    else:
                                        qty_to_send = total_qty - filled_qty # 정상 요격 팩트 스윕
                                else:
                                    continue # 목표가 미충족 시 1주도 사지 않고 철저한 관망세 (Bypass)
                                    
                            if qty_to_send <= 0: continue
                                      
                            if exec_price > 0:
                                # 🚨 NEW: [Ghost-Dumping 붕괴 방어] 1분 슬라이싱 매도 타격 직전에 KIS 실잔고를 스캔하여 매도 수량을 캡핑.
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
