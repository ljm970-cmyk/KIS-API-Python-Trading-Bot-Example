# ==========================================================
# FILE: vwap_aftermarket_engine.py
# ==========================================================
# 🚨 VERIFIED: [도메인 주도 설계 (DDD) 신규 파일] 자본 잠김(Capital Lock-up) 애프터장 이연 타격 전담 엔진
# 🚨 MODIFIED: [제2헌법 준수] 스케줄러에서 분리되어 애프터장(16:01 EST) 일괄 타격만 100% 전담하는 단일 책임 원칙(SRP) 수호.
# 🚨 MODIFIED: [Case 39 & 40 절대 방어망 결속] 암살자 올인으로 인해 정규장에 집행하지 못한 본진 플랜을 유동성 고갈을 감안해 1분 슬라이싱 없이 100% 지정가(LIMIT) 일괄 타격.
# 🚨 MODIFIED: [예수금 정산 지연 방어] 15:59 MOC 덤핑 후 KIS 서버의 예수금(Balance) 갱신 지연(Lag)을 감안하여 cash <= 0.0 일 경우 10초 대기 후 재스캔하는 3단 폴백 팩트 유지.
# 🚨 MODIFIED: [Ghost-Dumping 및 자본 초과 타격 붕괴 방어] 매도 시 KIS 실잔고를 스캔하여 `min(total_qty, rt_qty)`로 캡핑하고, 매수 시 가용 현금 내에서 `math.floor(cash / exec_price)`로 수량을 정밀 캡핑.
# 🚨 MODIFIED: [Scope Mismatch 궁극 방어] 장부 동기화 함수(`_sync_aftermarket_ledger_atomic`)를 모듈 전역(Global) 레벨로 분리 및 명시적 파라미터 주입을 강제하여 클로저 오염으로 인한 `UnboundLocalError` 원천 봉쇄.
# 🚨 MODIFIED: [제1헌법 절대 준수] 로컬 상태 파일 I/O, Config 조회, 통신 전역에 `asyncio.wait_for` 타임아웃 족쇄 강제 결속.
# 🚨 MODIFIED: [Event Loop Deadlock 방어] 텔레그램 통신(send_message)에 `asyncio.wait_for(timeout=15.0)` 족쇄 100% 래핑.
# 🚨 NEW: [런타임 즉사 방어] 구버전 파이썬 호환성을 위해 `asyncio.TimeoutError`와 `Exception` 분리 캡처 폴백을 I/O 헬퍼에 전면 하드코딩 완료.
# 🚨 NEW: [V14 자율주행 엔진 대통합] V14 VWAP 모드 또한 본 파일의 애프터장 이관 로직을 100% 공유하도록 병합 및 팩트 락온 완료.
# ==========================================================
import logging
import asyncio
import math
import time
import datetime
from zoneinfo import ZoneInfo
import html
import functools

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
        except asyncio.TimeoutError:
            if attempt == 2:
                func_name = getattr(func, '__name__', 'unknown_func')
                logging.debug(f"🚨 API 래퍼 타임아웃 ({func_name})")
                return default
            await asyncio.sleep(1.0 * (2 ** attempt))
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
    except asyncio.TimeoutError:
        logging.error("🚨 텔레그램 전송 타임아웃")
        return None
    except Exception as e:
        logging.error(f"🚨 텔레그램 전송 실패: {e}")
        return None

def _sync_aftermarket_ledger_atomic(tkr, sde, c_qty, r_price, q_ledger, strat, ver):
    """ 🚨 [Scope Mismatch 궁극 방어] 전역 레벨로 완전히 적출되어 클로저 오염 및 UnboundLocalError 제로(0) 달성 """
    if ver == "V_REV":
        if q_ledger:
            if sde == "BUY":
                q_ledger.add_lot(tkr, c_qty, r_price, "VREV_AFTERMARKET_BUY")
            else:
                q_ledger.pop_lots(tkr, c_qty, r_price)
        if hasattr(strat, 'v_rev_plugin'):
            strat.v_rev_plugin.record_execution(tkr, sde, c_qty, r_price)
    else:
        if hasattr(strat, 'v14_vwap_plugin'):
            strat.v14_vwap_plugin.record_execution(tkr, sde, c_qty, r_price)

async def execute_aftermarket_trade(tx_lock, cfg, broker, strategy, queue_ledger, chat_id, context):
    """ 🚨 [애프터장 전담 엔진] 자본 잠김(Capital Lock-up) 구출 및 지연 이관 플랜 100% 일괄 타격 파이프라인 """
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    today_hyphen = now_est.strftime('%Y-%m-%d')

    async with tx_lock:
        try:
            active_tickers = await _retry_api(cfg.get_active_tickers, default=[])
        except Exception:
            active_tickers = []
        
        if isinstance(active_tickers, str): active_tickers = [active_tickers]
        elif not isinstance(active_tickers, list): active_tickers = []
        
        for raw_t in active_tickers:
            t = str(raw_t).strip().upper()
            if not t: continue
            
            try:
                await asyncio.sleep(0.06)
                try:
                    version = await _retry_api(cfg.get_version, t, default="V14")
                    is_manual_vwap = await _retry_api(getattr(cfg, 'get_manual_vwap_mode', lambda x: False), t, default=False)
                except Exception:
                    version = "V14"
                    is_manual_vwap = False
                
                # 🚨 NEW: V14 VWAP 모드 통합 개방 락온
                if version != "V_REV" and not (version == "V14" and is_manual_vwap): continue
                
                state_file = f"data/vrev_aftermarket_state_{t}.json"
                try:
                    after_state = await _retry_api(_read_json_safe_sync, state_file, today_hyphen, default={})
                except Exception:
                    after_state = {}
                
                # 🚨 MODIFIED: [Date Schema Mismatch 원천 차단] today_hyphen 강제 결속
                if after_state.get('date') != today_hyphen: continue
                orders = after_state.get('orders', [])
                if not isinstance(orders, list) or not orders: continue
                
                # 2️⃣ [Edge Case 1 방어] 15:59 MOC 정산 지연(Balance Lag) 대비 예수금 복구 스캔
                cash = 0.0
                for attempt in range(3):
                    await asyncio.sleep(0.06)
                    try:
                        cash_tuple = await _retry_api(broker.get_account_balance, timeout=15.0)
                        cash = _safe_float(cash_tuple[0]) if isinstance(cash_tuple, (list, tuple)) and len(cash_tuple) > 0 else 0.0
                        if cash > 0.0: break
                    except Exception:
                        pass
                    
                    if attempt < 2:
                        logging.warning(f"⏳ [{t}] 애프터장 타격 대기: KIS 예수금 정산 지연(0.0). 10초 후 재스캔합니다.")
                        await asyncio.sleep(10.0)
                        
                if cash <= 0.0:
                    logging.error(f"🚨 [{t}] 애프터장 예수금 확보 실패 (자본 잠김 미해소). 타격 중단.")
                    continue

                # 3️⃣ [Edge Case 2 & 39 방어] 애프터마켓 유동성 증발 대비 100% 일괄 타격 및 자본초과 캡핑
                msgs = ""
                state_changed = False
                
                for o in orders:
                    if not isinstance(o, dict) or str(o.get('status')) == 'COMPLETED': continue
                    
                    side = str(o.get('side', 'BUY'))
                    total_qty = int(_safe_float(o.get('total_qty')))
                    target_price = _safe_float(o.get('target_price'))
                    desc = html.escape(str(o.get('desc', '')))
                    
                    exec_price = 0.0
                    for p_attempt in range(3):
                        try:
                            await asyncio.sleep(0.06)
                            if side == "BUY": 
                                p_val = await _retry_api(broker.get_ask_price, t, timeout=10.0)
                            else: 
                                p_val = await _retry_api(broker.get_bid_price, t, timeout=10.0)
                            exec_price = _safe_float(p_val)
                            break
                        except Exception:
                            if p_attempt == 2: exec_price = 0.0
                            else: await asyncio.sleep(1.0 * (2 ** p_attempt))
                    
                    if exec_price <= 0.0:
                        for p_attempt in range(3):
                            try:
                                await asyncio.sleep(0.06)
                                p_val = await _retry_api(broker.get_current_price, t, timeout=10.0)
                                exec_price = _safe_float(p_val)
                                break
                            except Exception:
                                if p_attempt == 2: exec_price = 0.0
                                else: await asyncio.sleep(1.0 * (2 ** p_attempt))
                                 
                    is_target_hit = False
                    if target_price > 0.0 and exec_price > 0.0:
                        if side == "BUY" and exec_price <= target_price: is_target_hit = True
                        elif side == "SELL" and exec_price >= target_price: is_target_hit = True
                        
                    if not is_target_hit:
                        msgs += f"⏸️ {desc}: 목표가(${target_price:.2f}) 미도달 (현재가 ${exec_price:.2f}) ➔ 관망 유지\n"
                        continue

                    # 🚨 NEW: 애프터장 자본 잠김 후폭풍 팩트 방어 (Ghost Dumping / Budget Exceed)
                    if side == "BUY":
                        max_buy = int(math.floor(cash / exec_price)) if exec_price > 0 else 0
                        if total_qty > max_buy:
                            logging.warning(f"🚨 [{t}] 애프터장 매수 수량 캡핑 가동 (가용 현금 한도 초과): {total_qty} -> {max_buy}")
                            total_qty = max_buy
                        if total_qty <= 0:
                            msgs += f"⚠️ {desc}: KIS 예수금 부족으로 애프터장 타격 스킵\n"
                            continue
                    else:
                        rt_qty = total_qty
                        for attempt_bal in range(3):
                            try:
                                await asyncio.sleep(0.06)
                                bal_tuple = await _retry_api(broker.get_account_balance, timeout=10.0)
                                if isinstance(bal_tuple, (list, tuple)) and len(bal_tuple) > 1:
                                    rt_qty = int(_safe_float(bal_tuple[1].get(t, {}).get('qty', 0)))
                                break
                            except Exception:
                                if attempt_bal == 2: pass
                                else: await asyncio.sleep(1.0 * (2 ** attempt_bal))
                        
                        total_qty = min(total_qty, rt_qty)
                        if total_qty <= 0:
                            msgs += f"⚠️ {desc}: KIS 보유 잔고 0주 (Ghost-Dumping 방어)\n"
                            o['status'] = 'COMPLETED'
                            state_changed = True
                            continue
                        
                    res = None
                    for s_attempt in range(3):
                        try:
                            await asyncio.sleep(0.06)
                            res = await _retry_api(broker.send_order, t, side, total_qty, target_price, "LIMIT", timeout=15.0)
                            break
                        except Exception as e:
                            if s_attempt == 2: logging.error(f"🚨 [{t}] 애프터장 타격 에러: {e}")
                            else: await asyncio.sleep(1.0 * (2 ** s_attempt))
                            
                    safe_res = res if isinstance(res, dict) else {}
                    is_success = safe_res.get('rt_cd') == '0'
                    odno = str(safe_res.get('odno') or '')
                    
                    if is_success and odno:
                        o['status'] = 'COMPLETED'
                        o['odno'] = odno
                        if side == "BUY": cash -= (total_qty * target_price) 
                        state_changed = True
                        msgs += f"🎯 {desc}: {total_qty}주 @ ${target_price:.2f} ➔ 일괄 타격 완료 (LIMIT)\n"
                        
                        # 🚨 MODIFIED: [Scope Mismatch 궁극 방어] 모듈 전역으로 분리된 헬퍼 함수를 통해 완벽한 클로저 차단 및 I/O 락온
                        try:
                            p_sync = functools.partial(_sync_aftermarket_ledger_atomic, t, side, total_qty, target_price, queue_ledger, strategy, version)
                            await asyncio.wait_for(asyncio.to_thread(p_sync), timeout=10.0)
                            logging.info(f"💾 [{t}] 애프터장 체결 장부 원자적 동기화 완료: {side} {total_qty}주 @ ${target_price:.2f}")
                        except Exception as e:
                            logging.error(f"🚨 [{t}] 애프터장 타격 장부 동기화 실패: {e}")
                    else:
                        err_msg = html.escape(str(safe_res.get('msg1') or '거절'))
                        msgs += f"❌ {desc}: 16:01 타격 실패 ({err_msg})\n"
                        
                if msgs.strip() and chat_id:
                    await _safe_send(context, chat_id, f"🌃 <b>[{html.escape(t)}] 16:01 EST 애프터장 일괄 타격 보고</b>\n\n{msgs.strip()}", parse_mode='HTML')
                    
                # 4️⃣ [Edge Case 3] 원자적 쓰기 (상태 저장)
                if state_changed:
                    try:
                        await asyncio.wait_for(asyncio.to_thread(_atomic_write_json_sync, state_file, after_state), timeout=10.0)
                    except Exception as e:
                        logging.error(f"🚨 [{t}] 애프터장 상태 파일 원자적 갱신 실패: {e}")
                    
            except Exception as e:
                logging.error(f"🚨 [{t}] 애프터장 루프 치명적 오류: {e}", exc_info=True)
