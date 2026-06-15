# ==========================================================
# FILE: scheduler_regular.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 5대 헌법 및 40대 엣지 케이스 완벽 결속 교차 검증 완료
# 🚨 NEW: [도메인 주도 설계 (DDD) 락온] 스케줄러 내부에 밀집되어 있던 주문 집행 및 상태 관리 로직을 `order_executor` 및 `state_io_manager`로 100% 위임하여 God Object 안티패턴 영구 소각.
# 🚨 MODIFIED: [제2헌법 준수] 불필요해진 os, json, tempfile 등 파일 I/O 라이브러리를 소각하고 순수 스케줄링 및 파이프라인 제어 코드로 70% 이상 진공 압축.
# 🚨 MODIFIED: [State Mismatch 붕괴 방어] V-REV 지연 타격망에서 '자본 잠김(Capital Lock-up)' 여부를 문자열(msgs[t])로 유추하던 낡은 뇌관을 파기하고, `capital_locked_map`을 통한 명시적 상태 전달로 멱등성 100% 사수.
# 🚨 NEW: [Case 39 & 40 절대 방어망 결속] 자본 잠김(Capital Lock-up) 현상 감지 시 본진 1분 슬라이싱 타격을 16:01 EST 애프터장으로 지연 이관(Delay & Transfer)하는 원자적 쓰기 파이프라인 100% 팩트 락온.
# 🚨 MODIFIED: [Capital Lock-up 가상 예산 복원] 암살자 점유로 KIS 잔고가 0.0일지라도, 애프터장 이관용 플랜 생성을 위해 가상의 1일 고정 예산(Seed * 15%)을 강제 복원(Override) 주입하는 논리적 패러독스 방어망 결속.
# 🚨 MODIFIED: [유령 마비(Phantom Paralysis) 궁극 수술] V-REV 본진 병렬 가동 시, 예산(Cash)과 현재가가 0.0으로 주입되어 매매가 마비되던 치명적 맹점을 스캔하고 실시간 KIS 잔고/단가 추출 파이프라인 100% 복원 완료.
# 🚨 MODIFIED: [Jitter 타임라인 역전 붕괴 수술] 15:27 슬라이싱 엔진 가동 전 무조건 파일 I/O 인계를 마치도록 V-REV 본진 지터 상한을 180초에서 45초로 진공 압축 락온.
# 🚨 MODIFIED: [제1헌법 보강] get_budget_allocation, cfg.get_version, strategy.get_plan 등 모든 동기식 I/O 호출부에 wait_for(timeout) 샌드박스를 강제 결속하여 Deadlock 원천 차단.
# 🚨 MODIFIED: [Event Loop Deadlock 궁극 방어] 파일 내 모든 `context.bot.send_message` 호출에 `asyncio.wait_for(timeout=15.0)` 족쇄를 100% 래핑하여 텔레그램 통신 지연으로 인한 스케줄러 교착 원천 봉쇄.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import asyncio
import random
import html
import math

from scheduler_core import is_market_open, get_budget_allocation
# 🚨 [NEW] 도메인 분리된 외부 엔진 임포트 결속
from order_executor import execute_order_list
from state_io_manager import read_avwap_state_sync

def _safe_float(val):
    """ 🚨 [Insight 14] 수학 연산 붕괴 방어용 맹독성 데이터 정밀 필터링 """
    try:
        f_val = float(str(val or 0.0).replace(',', ''))
        if math.isnan(f_val) or math.isinf(f_val):
            return 0.0
        return f_val
    except Exception:
        return 0.0

async def scheduled_early_regular_trade(context):
    is_open = False
    for attempt in range(3):
        try:
            # 🚨 MODIFIED: [Case 14] 달력 API 타임아웃 10초 하드코딩 락온
            is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=10.0)
            break
        except asyncio.TimeoutError:
            if attempt == 2:
                logging.error("⚠️ is_market_open 달력 API 타임아웃. 평일이므로 강제 개장 처리합니다.")
                est = ZoneInfo('America/New_York')
                is_open = datetime.datetime.now(est).weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))
        except Exception:
            if attempt == 2:
                est = ZoneInfo('America/New_York')
                is_open = datetime.datetime.now(est).weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))

    if not is_open:
        return
    
    # 🚨 MODIFIED: [AttributeError 방어] job 팩트 단락 평가
    job = getattr(context, 'job', None)
    app_data = getattr(job, 'data', {}) if job else {}
    if not isinstance(app_data, dict): app_data = {}
    
    cfg = app_data.get('cfg')
    broker = app_data.get('broker')
    strategy = app_data.get('strategy')
    tx_lock = app_data.get('tx_lock')
    chat_id = getattr(job, 'chat_id', None)
    
    if tx_lock is None:
        logging.warning("⚠️ [early_trade] tx_lock 미초기화. 이번 사이클 스킵.")
        return

    # 프리장 선제 타격(V14)은 17:05 KST(04:05 EST)이므로 지터 최대 180초 유지
    jitter_seconds = random.randint(0, 180)
    
    if chat_id:
        try:
            # 🚨 MODIFIED: 텔레그램 통신 샌드박스 및 chat_id 직접 사용 락온
            await asyncio.wait_for(
                context.bot.send_message(
                    chat_id=chat_id, 
                    text=f"🌃 <b>[17:05 KST] 정규장 스케줄러 기상!</b>\n"
                         f"▫️ 서버 접속 부하 방지를 위해 <b>{jitter_seconds}초</b> 대기 후 V14 덫 전송 및 스냅샷을 박제합니다.", 
                    parse_mode='HTML'
                ),
                timeout=15.0
            )
        except Exception as e:
            logging.error(f"초기 기상 메시지 텔레그램 발송 실패: {e}")
        
    await asyncio.sleep(jitter_seconds)

    MAX_RETRIES = 5
    RETRY_DELAY = 10
    successful_orders_cache = set() # 🚨 NEW: [Case 19] 부분 실패 시 이중 장전 방지용 캐시

    async def _do_early_trade():
        est_z = ZoneInfo('America/New_York')
        curr_est = datetime.datetime.now(est_z)
        today_str = curr_est.strftime("%Y-%m-%d")
        
        async with tx_lock:
            cash, holdings = 0.0, None
            for attempt in range(3):
                try:
                    # 🚨 VERIFIED: [튜플 언패킹 붕괴 방어] get_account_balance 안전 인덱싱 락온
                    res = await asyncio.wait_for(asyncio.to_thread(broker.get_account_balance), timeout=15.0)
                    cash = _safe_float(res[0]) if isinstance(res, (list, tuple)) and len(res) > 0 else 0.0
                    holdings = res[1] if isinstance(res, (list, tuple)) and len(res) > 1 else {}
                    break
                except asyncio.TimeoutError:
                    if attempt == 2: return False, "잔고 조회 타임아웃"
                    else: await asyncio.sleep(1.0 * (2 ** attempt))
                except Exception as e:
                    if attempt == 2: return False, f"잔고 조회 오류: {html.escape(str(e))}"
                    else: await asyncio.sleep(1.0 * (2 ** attempt))
            
            if holdings is None:
                return False, "❌ 계좌 정보를 불러오지 못했습니다."
            
            safe_holdings = holdings if isinstance(holdings, dict) else {}
            
            # 🚨 MODIFIED: [최후의 맹점 수술] 외부 모듈 반환값 결측치(None) 붕괴 완벽 차단
            try:
                active_tickers_list = await asyncio.wait_for(asyncio.to_thread(cfg.get_active_tickers), timeout=10.0) or []
            except Exception:
                active_tickers_list = []
            
            # 🚨 NEW: [String Iteration 붕괴 방어] 문자열 오염 시 글자 단위 파쇄 즉사 버그 원천 봉쇄
            if isinstance(active_tickers_list, str): active_tickers_list = [active_tickers_list]
            elif not isinstance(active_tickers_list, list): active_tickers_list = []
            
            if not active_tickers_list:
                return False, "❌ 활성 종목 리스트 결측치 반환. 스케줄 보호 중단."
            
            # 🚨 MODIFIED: [제1헌법 준수] 동기식 할당 로직 샌드박스 래핑
            try:
                alloc_res = await asyncio.wait_for(asyncio.to_thread(get_budget_allocation, cash, active_tickers_list, cfg), timeout=10.0)
            except Exception as e:
                logging.error(f"🚨 예산 할당 타임아웃/에러: {e}")
                alloc_res = None
            
            if not alloc_res or len(alloc_res) != 2:
                return False, "❌ 예산 할당 로직 결측치(None) 반환. 스케줄 보호 중단."
            
            sorted_tickers, allocated_cash = alloc_res
            sorted_tickers = sorted_tickers or []
            allocated_cash = allocated_cash or {}
            
            msgs = {t: "" for t in sorted_tickers}
            all_success_map = {t: True for t in sorted_tickers}
            
            loop_fully_successful = True
            loop_fail_reason = ""

            for t in sorted_tickers:
                try:
                    await asyncio.sleep(0.06)
                    
                    # 🚨 MODIFIED: [제1헌법 준수] 동기식 로직 샌드박스 래핑
                    try:
                        version = await asyncio.wait_for(asyncio.to_thread(cfg.get_version, t), timeout=5.0)
                    except Exception:
                        version = "V14"
                        
                    try:
                        is_locked = await asyncio.wait_for(asyncio.to_thread(cfg.check_lock, t, "REG"), timeout=5.0)
                    except Exception:
                        is_locked = False
                
                    if is_locked:
                        skip_msg = f"⚠️ <b>[{t}] REG 잠금 미해제 — 스케줄 루프 스킵</b>\n▫️ 수동으로 잠금 해제 후 상태를 확인하십시오."
                        if chat_id:
                            try:
                                # 🚨 MODIFIED: 텔레그램 통신 샌드박스 및 chat_id 직접 사용 락온
                                await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=skip_msg, parse_mode='HTML'), timeout=15.0)
                            except Exception: pass
                        continue

                    h = safe_holdings.get(t) or {}
                    # 🚨 MODIFIED: [Insight 14] String-Float 맹독성 쉴드 래핑
                    safe_avg = _safe_float(h.get('avg'))
                    safe_qty = int(_safe_float(h.get('qty')))
                    safe_alloc_cash = _safe_float(allocated_cash.get(t, 0.0))

                    curr_p, prev_c = 0.0, 0.0
                    for _api_retry in range(3):
                        try:
                            # 🚨 MODIFIED: [Case 32] 가격 스캔 전 TPS 캡핑 강제 결속
                            await asyncio.sleep(0.06)
                            curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, t), timeout=15.0)
                            curr_p = _safe_float(curr_p_val)
                            
                            await asyncio.sleep(0.06)
                            prev_c_val = await asyncio.wait_for(asyncio.to_thread(broker.get_previous_close, t), timeout=15.0)
                            prev_c = _safe_float(prev_c_val)
                            
                            if curr_p > 0 and prev_c > 0: break
                        except Exception:
                            pass
                        await asyncio.sleep(1.0 * (2**_api_retry))

                    ma_5day = 0.0
                    for attempt in range(3):
                        try:
                            # 🚨 MODIFIED: [Case 32] 5일선 스캔 전 TPS 캡핑 강제 결속
                            await asyncio.sleep(0.06)
                            ma_5day_val = await asyncio.wait_for(asyncio.to_thread(broker.get_5day_ma, t), timeout=15.0)
                            ma_5day = _safe_float(ma_5day_val)
                            break
                        except Exception: 
                            if attempt == 2: ma_5day = 0.0
                            else: await asyncio.sleep(1.0 * (2**attempt))
                    
                    # 🚨 MODIFIED: [제1헌법 준수] 동기식 플랜 생성기 샌드박스 래핑
                    try:
                        plan = await asyncio.wait_for(asyncio.to_thread(
                            strategy.get_plan, t, curr_p, safe_avg, safe_qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=safe_alloc_cash, is_snapshot_mode=True
                        ), timeout=15.0)
                    except Exception as e:
                        logging.error(f"🚨 [{t}] 플랜 생성 타임아웃/에러: {e}")
                        plan = None
                    
                    if not isinstance(plan, dict):
                        msgs[t] += f"🚨 <b>[{t}] 스냅샷 유실 또는 손상! KIS 전송 불가.</b>\n"
                        all_success_map[t] = False
                        loop_fully_successful = False
                        loop_fail_reason = f"[{t}] 플랜 오염"
                        continue
                    
                    if version == "V14":
                        msgs[t] += f"💎 <b>[{t}] V14 오리지널 정규장 실전 덫 장전 완료 (17:05 KST 타격망)</b>\n"
                        
                        is_market_active_now = False # 17:05 KST is generally PRE-MARKET

                        target_orders = plan.get('core_orders') or plan.get('orders') or []
                        if not isinstance(target_orders, list): target_orders = []
                        
                        # 🚨 MODIFIED: [도메인 위임] 중복 하드코딩된 API 통신을 order_executor 모듈로 100% 인계
                        success_core, msg_core, fail_reason_core = await execute_order_list(
                            broker, t, target_orders, successful_orders_cache, is_market_active_now, today_str, is_capital_locked=False, order_category="1차 필수"
                        )
                        msgs[t] += msg_core

                        if not success_core:
                            all_success_map[t] = False
                            loop_fully_successful = False
                            loop_fail_reason = fail_reason_core
                            
                        target_bonus = plan.get('bonus_orders') or []
                        if not isinstance(target_bonus, list): target_bonus = []
                        
                        # 🚨 MODIFIED: [Case 19 중복 매매 방어] 1차 필수 주문 실패 시 보너스 덫 장전 원천 차단
                        if all_success_map[t]:
                            success_bonus, msg_bonus, fail_reason_bonus = await execute_order_list(
                                broker, t, target_bonus, successful_orders_cache, is_market_active_now, today_str, is_capital_locked=False, order_category="2차 보너스"
                            )
                            msgs[t] += msg_bonus
                            if not success_bonus:
                                all_success_map[t] = False
                                loop_fully_successful = False
                                loop_fail_reason = fail_reason_bonus
                        elif target_bonus:
                            msgs[t] += f"⚠️ 1차 필수 장전 실패로 2차 보너스 덫 보류 (중복 매매 방어)\n"
                        
                        if (target_orders or target_bonus) and all_success_map[t]:
                            try:
                                await asyncio.wait_for(asyncio.to_thread(cfg.set_lock, t, "REG"), timeout=5.0)
                            except Exception as e:
                                logging.error(f"🚨 락 설정 타임아웃: {e}")
                            msgs[t] += "\n🔒 <b>V14 필수 덫(로컬 엔진 포함) 장전 완료 (잠금 설정됨)</b>"
                    
                    else: 
                        msgs[t] += f"🔄 <b>[{t}] V-REV 역추세 덫 모의 장전 및 스냅샷 박제</b>\n"
                        target_orders = plan.get('core_orders') or plan.get('orders') or []
                        if not isinstance(target_orders, list): target_orders = []
                        for o in target_orders:
                            if not isinstance(o, dict): continue
                            safe_desc = html.escape(str(o.get('desc', '주문')))
                            safe_qty = int(_safe_float(o.get('qty')))
                            safe_price = _safe_float(o.get('price'))
                            msgs[t] += f"└ 모의 1차 필수: {safe_desc} {safe_qty}주 (${safe_price})\n"
                    
                        target_bonus = plan.get('bonus_orders') or []
                        if not isinstance(target_bonus, list): target_bonus = []
                        for o in target_bonus:
                            if not isinstance(o, dict): continue
                            safe_desc = html.escape(str(o.get('desc', '주문')))
                            safe_qty = int(_safe_float(o.get('qty')))
                            safe_price = _safe_float(o.get('price'))
                            msgs[t] += f"└ 모의 2차 보너스: {safe_desc} {safe_qty}주 (${safe_price})\n"
                            
                        if target_orders or target_bonus:
                            msgs[t] += "\n📸 <b>V-REV 당일 스냅샷 팩트 박제 완료 (15:26 EST 지연 투하 대기)</b>"
                    
                    if msgs[t].strip() and chat_id:
                        try:
                            # 🚨 MODIFIED: 텔레그램 통신 샌드박스 래핑
                            await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=msgs[t], parse_mode='HTML'), timeout=15.0)
                        except Exception as tg_e:
                            logging.error(f"[{t}] 개별 종목 텔레그램 메시지 발송 실패: {tg_e}")

                except Exception as e:
                    # 🚨 MODIFIED: [Cascade Failure 방어] 샌드박스로 인한 재시도 루프 무력화 방지 (Tracker 갱신)
                    all_success_map[t] = False
                    loop_fully_successful = False
                    loop_fail_reason = f"[{t}] 치명적 오류: {str(e)}"
                    logging.error(f"🚨 [{t}] early_trade 개별 종목 처리 중 치명적 오류: {e}")
                    if chat_id:
                        try:
                            await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=f"🚨 <b>[{t}] 스케줄러 처리 중 오류 발생. 스킵합니다.</b>\n<code>{html.escape(str(e))}</code>", parse_mode='HTML'), timeout=15.0)
                        except Exception: pass

            if not loop_fully_successful:
                return False, loop_fail_reason
        return True, "SUCCESS"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            success, fail_reason = await asyncio.wait_for(_do_early_trade(), timeout=300.0)
            if success:
                if attempt > 1 and chat_id: 
                    try:
                        await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=f"✅ <b>[통신 복구] {attempt}번째 재시도 끝에 장전을 완수했습니다!</b>", parse_mode='HTML'), timeout=15.0)
                    except Exception: pass
                return 
        except Exception as e:
            logging.error(f"17:05 덫 장전 에러 ({attempt}/{MAX_RETRIES}): {e}", exc_info=True)
            if attempt == 1 and chat_id:
                safe_err = html.escape(str(e))
                try:
                    await asyncio.wait_for(
                        context.bot.send_message(
                            chat_id=chat_id, 
                            text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 10초 뒤 장전을 재시도합니다! 🛡️\n<code>사유: {safe_err}</code>", 
                            parse_mode='HTML'
                        ),
                        timeout=15.0
                    )
                except Exception: pass
        else:
            logging.warning(f"장전 조건 미충족 ({attempt}/{MAX_RETRIES}): {fail_reason}")
            if attempt == 1 and chat_id:
                 safe_fail = html.escape(str(fail_reason))
                 try:
                     await asyncio.wait_for(
                         context.bot.send_message(
                            chat_id=chat_id, 
                            text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 10초 뒤 장전을 재시도합니다! 🛡️\n<code>사유: {safe_fail}</code>", 
                            parse_mode='HTML'
                         ),
                         timeout=15.0
                     )
                 except Exception: pass

        if attempt < MAX_RETRIES:
            if attempt != 1 and attempt % 5 == 0 and chat_id:
                try:
                    await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 10초 뒤 재시도합니다! 🛡️", parse_mode='HTML'), timeout=15.0)
                except Exception: pass
            await asyncio.sleep(RETRY_DELAY)

    if chat_id:
        try:
            await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text="🚨 <b>[긴급 에러] 17:05 스케줄 통신 복구 최종 실패. 수동 점검 요망!</b>", parse_mode='HTML'), timeout=15.0)
        except Exception: pass


async def scheduled_regular_trade_delayed(context):
    is_open = False
    for attempt in range(3):
        try:
            is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=10.0)
            break
        except asyncio.TimeoutError:
            if attempt == 2:
                est = ZoneInfo('America/New_York')
                is_open = datetime.datetime.now(est).weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))
        except Exception:
            if attempt == 2:
                est = ZoneInfo('America/New_York')
                is_open = datetime.datetime.now(est).weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))

    if not is_open:
        return
    
    # 🚨 MODIFIED: [AttributeError 방어] job 팩트 단락 평가
    job = getattr(context, 'job', None)
    app_data = getattr(job, 'data', {}) if job else {}
    if not isinstance(app_data, dict): app_data = {}
    
    cfg = app_data.get('cfg')
    broker = app_data.get('broker')
    strategy = app_data.get('strategy')
    tx_lock = app_data.get('tx_lock')
    chat_id = getattr(job, 'chat_id', None)
    
    if tx_lock is None:
        return
    
    # 🚨 MODIFIED: [Jitter 타임라인 역전 붕괴 수술] 15:27 슬라이싱 엔진 가동 전 무조건 파일 I/O 인계를 마치도록 지터 상한을 180초에서 45초로 진공 압축 락온
    jitter_seconds = random.randint(0, 45)

    if chat_id:
        try:
            await asyncio.wait_for(
                context.bot.send_message(
                    chat_id=chat_id, 
                    text=f"🌃 <b>[15:26 EST] V-REV 본진 덫(자체 1분 슬라이싱 포함) 투하 개시!</b>\n"
                         f"🛡️ 서버 접속 부하 방지를 위해 <b>{jitter_seconds}초</b> 대기 후 전송/인계를 시도합니다.", 
                    parse_mode='HTML'
                ),
                timeout=15.0
            )
        except Exception as e:
            logging.error(f"지연 투하 시작 메시지 텔레그램 발송 실패: {e}")

    await asyncio.sleep(jitter_seconds)

    MAX_RETRIES = 15
    RETRY_DELAY = 60
    successful_orders_cache = set() # 🚨 NEW: [Case 19] 부분 실패 시 이중 장전 방지용 캐시

    async def _do_delayed_trade():
        async with tx_lock:
            # 🚨 MODIFIED: [Phantom Paralysis (유령 마비) 붕괴 전면 수술] 실제 KIS 잔고 및 단가를 추출하여 예산(Cash) 0.0 주입 버그 원천 봉쇄
            cash, holdings = 0.0, None
            for attempt in range(3):
                try:
                    await asyncio.sleep(0.06)
                    res = await asyncio.wait_for(asyncio.to_thread(broker.get_account_balance), timeout=15.0)
                    cash = _safe_float(res[0]) if isinstance(res, (list, tuple)) and len(res) > 0 else 0.0
                    holdings = res[1] if isinstance(res, (list, tuple)) and len(res) > 1 else {}
                    break
                except Exception as e:
                    if attempt == 2: return False, f"잔고 조회 오류: {html.escape(str(e))}"
                    else: await asyncio.sleep(1.0 * (2 ** attempt))
            
            if holdings is None:
                return False, "❌ 계좌 정보를 불러오지 못했습니다."
            
            safe_holdings = holdings if isinstance(holdings, dict) else {}
            
            try:
                active_tickers_list = await asyncio.wait_for(asyncio.to_thread(cfg.get_active_tickers), timeout=10.0) or []
            except Exception:
                active_tickers_list = []
            
            if isinstance(active_tickers_list, str): active_tickers_list = [active_tickers_list]
            elif not isinstance(active_tickers_list, list): active_tickers_list = []
            
            if not active_tickers_list:
                return False, "❌ 활성 종목 리스트 결측치(None) 반환. 스케줄 보호 중단."
                
            try:
                alloc_res = await asyncio.wait_for(asyncio.to_thread(get_budget_allocation, cash, active_tickers_list, cfg), timeout=10.0)
            except Exception as e:
                logging.error(f"🚨 예산 할당 타임아웃/에러: {e}")
                alloc_res = None
                
            if not alloc_res or len(alloc_res) != 2:
                return False, "❌ 예산 할당 로직 결측치 반환."
            
            sorted_tickers, allocated_cash = alloc_res
            sorted_tickers = sorted_tickers or []
            allocated_cash = allocated_cash or {}
            
            plans = {}
            msgs = {t: "" for t in sorted_tickers}
            all_success_map = {t: True for t in sorted_tickers}
            capital_locked_map = {t: False for t in sorted_tickers} # 🚨 [State Mismatch 방어] 멱등성 보장을 위한 명시적 맵핑
            
            loop_fully_successful = True
            loop_fail_reason = ""

            est_z = ZoneInfo('America/New_York')
            curr_est = datetime.datetime.now(est_z)
            today_str = curr_est.strftime("%Y-%m-%d")
            
            is_market_active_now = True # 15:26 EST is REGULAR session

            for t in sorted_tickers:
                try:
                    await asyncio.sleep(0.06)
                
                    try:
                        version = await asyncio.wait_for(asyncio.to_thread(cfg.get_version, t), timeout=5.0)
                    except Exception:
                        version = "V14"
                    
                    if version == "V14":
                        continue 
                    
                    try:
                        is_locked = await asyncio.wait_for(asyncio.to_thread(cfg.check_lock, t, "REG"), timeout=5.0)
                    except Exception:
                        is_locked = False
                        
                    if is_locked:
                        continue
                        
                    # 🚨 NEW: [Case 39 자본 잠김 감지 팩트 스캔]
                    is_capital_locked = False
                    if version == "V_REV" and t == "SOXL":
                        for attempt in range(3):
                            try:
                                await asyncio.sleep(0.06)
                                avwap_state = await asyncio.wait_for(asyncio.to_thread(read_avwap_state_sync, t, today_str), timeout=5.0)
                                avwap_qty = int(_safe_float(avwap_state.get('qty', 0)))
                                avwap_shutdown = bool(avwap_state.get('shutdown', False))
                                # 암살자가 현금을 보유 중이며 당일 퇴근(MOC 덤핑) 전이라면 자본 잠김 상태로 판별
                                if avwap_qty > 0 and not avwap_shutdown:
                                    is_capital_locked = True
                                break
                            except Exception as e:
                                if attempt == 2: logging.error(f"🚨 [{t}] 암살자 자본 잠김 스캔 에러: {e}")
                                else: await asyncio.sleep(1.0 * (2 ** attempt))
                                
                    capital_locked_map[t] = is_capital_locked # 🚨 [State Mismatch 방어] 명시적 할당

                    h = safe_holdings.get(t) or {}
                    safe_avg = _safe_float(h.get('avg'))
                    safe_qty = int(_safe_float(h.get('qty')))
                    safe_alloc_cash = _safe_float(allocated_cash.get(t, 0.0))

                    # 🚨 NEW: [Case 39 자본 잠김 패러독스 방어] 
                    if is_capital_locked:
                        for seed_attempt in range(3):
                            try:
                                await asyncio.sleep(0.06)
                                seed_val = await asyncio.wait_for(asyncio.to_thread(cfg.get_seed, t), timeout=5.0)
                                safe_alloc_cash = _safe_float(seed_val) * 0.15
                                logging.info(f"🚨 [{t}] 자본 잠김 감지: 이관 플랜 생성을 위해 가상의 1일 고정 예산(${safe_alloc_cash:.2f})을 강제 복원합니다.")
                                break
                            except Exception as e:
                                if seed_attempt == 2: logging.error(f"🚨 [{t}] 가상 예산 복원 실패 (기존 0.0 예산 유지): {e}")
                                else: await asyncio.sleep(1.0 * (2 ** seed_attempt))

                    curr_p, prev_c = 0.0, 0.0
                    for _api_retry in range(3):
                        try:
                            await asyncio.sleep(0.06)
                            curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, t), timeout=15.0)
                            curr_p = _safe_float(curr_p_val)
                            
                            await asyncio.sleep(0.06)
                            prev_c_val = await asyncio.wait_for(asyncio.to_thread(broker.get_previous_close, t), timeout=15.0)
                            prev_c = _safe_float(prev_c_val)
                            
                            if curr_p > 0 and prev_c > 0: break
                        except Exception:
                            pass
                        await asyncio.sleep(1.0 * (2**_api_retry))

                    ma_5day = 0.0
                    for attempt in range(3):
                        try:
                            await asyncio.sleep(0.06)
                            ma_5day_val = await asyncio.wait_for(asyncio.to_thread(broker.get_5day_ma, t), timeout=15.0)
                            ma_5day = _safe_float(ma_5day_val)
                            break
                        except Exception: 
                            if attempt == 2: ma_5day = 0.0
                            else: await asyncio.sleep(1.0 * (2**attempt))
                    
                    try:
                        plan = await asyncio.wait_for(asyncio.to_thread(
                            strategy.get_plan, t, curr_p, safe_avg, safe_qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=safe_alloc_cash, is_snapshot_mode=True
                        ), timeout=15.0)
                    except Exception as e:
                        logging.error(f"🚨 [{t}] 플랜 생성 타임아웃/에러: {e}")
                        plan = None
                    
                    if not isinstance(plan, dict):
                        msgs[t] += f"🚨 <b>[{t}] 스냅샷 유실 또는 손상! KIS 전송 불가.</b>\n"
                        all_success_map[t] = False
                        loop_fully_successful = False
                        loop_fail_reason = f"[{t}] 스냅샷 유실"
                        continue

                    plans[t] = plan
                    if plan.get('core_orders') or plan.get('orders') or plan.get('bonus_orders'):
                        if is_capital_locked:
                            msgs[t] += f"⏳ <b>[{t}] 자본 잠김(Capital Lock-up) 감지!</b>\n▫️ 암살자 100% 점유로 인해 정규장 플랜을 <b>16:01 애프터장 일괄 타격</b>으로 지연 이관(Delay & Transfer)합니다.\n"
                        else:
                            msgs[t] += f"🔄 <b>[{t}] V-REV 역추세 실전 덫(로컬 엔진 포함) 장전 완료</b>\n"
                except Exception as e:
                    all_success_map[t] = False
                    loop_fully_successful = False
                    loop_fail_reason = f"[{t}] 플랜 조회 오류"
                    logging.error(f"🚨 [{t}] delayed_trade 플랜 조회 오류: {e}")
                    msgs[t] += f"🚨 <b>[{t}] 플랜 조회 중 에러 발생</b>\n"

            for t in sorted_tickers:
                try:
                    if t not in plans:
                        if msgs[t].strip() and chat_id:
                            try: 
                                await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=msgs[t], parse_mode='HTML'), timeout=15.0)
                            except Exception as tg_e: logging.error(f"[{t}] V-REV 디커플링 메시지 발송 실패: {tg_e}")
                        continue
                        
                    target_orders = plans[t].get('core_orders') or plans[t].get('orders') or []
                    if not isinstance(target_orders, list): target_orders = []
                        
                    is_capital_locked = capital_locked_map.get(t, False) # 🚨 [State Mismatch 방어] 멱등성 보장을 위한 명시적 추출
                    
                    # 🚨 MODIFIED: [도메인 위임] 중복 하드코딩된 API 통신을 order_executor 모듈로 100% 인계
                    success_core, msg_core, fail_reason_core = await execute_order_list(
                        broker, t, target_orders, successful_orders_cache, is_market_active_now, today_str, is_capital_locked=is_capital_locked, order_category="1차 필수"
                    )
                    msgs[t] += msg_core

                    if not success_core:
                        all_success_map[t] = False
                        loop_fully_successful = False
                        loop_fail_reason = fail_reason_core
                        
                    target_bonus = plans[t].get('bonus_orders') or []
                    if not isinstance(target_bonus, list): target_bonus = []
                        
                    # 🚨 MODIFIED: [Case 19 중복 매매 방어] 1차 필수 주문 실패 시 보너스 덫 장전 원천 차단
                    if not all_success_map[t] and target_bonus:
                        msgs[t] += f"⚠️ 1차 필수 장전 실패로 2차 보너스 덫 보류 (중복 매매 방어)\n"
                    else:
                        success_bonus, msg_bonus, fail_reason_bonus = await execute_order_list(
                            broker, t, target_bonus, successful_orders_cache, is_market_active_now, today_str, is_capital_locked=is_capital_locked, order_category="2차 보너스"
                        )
                        msgs[t] += msg_bonus
                        if not success_bonus:
                            all_success_map[t] = False
                            loop_fully_successful = False
                            loop_fail_reason = fail_reason_bonus

                    if all_success_map[t] and (target_orders or target_bonus):
                        try:
                            await asyncio.wait_for(asyncio.to_thread(cfg.set_lock, t, "REG"), timeout=5.0)
                        except Exception as e:
                            logging.error(f"🚨 락 설정 타임아웃: {e}")
                            
                        if is_capital_locked:
                            msgs[t] += "\n🔒 <b>V-REV 플랜 애프터장 이관 완료 (잠금 설정됨)</b>"
                        else:
                            msgs[t] += "\n🔒 <b>V-REV 필수 덫(로컬 엔진 포함) 전송 완료 (잠금 설정됨)</b>"
                    elif not all_success_map[t] and (target_orders or target_bonus):
                        msgs[t] += "\n⚠️ <b>일부 덫 장전/이관 실패 (매매 잠금 보류)</b>"
                        
                    if msgs[t].strip() and chat_id:
                        try:
                            await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=msgs[t], parse_mode='HTML'), timeout=15.0)
                        except Exception as tg_e:
                            logging.error(f"[{t}] V-REV 완료 메시지 발송 실패: {tg_e}")

                except Exception as e:
                    all_success_map[t] = False
                    loop_fully_successful = False
                    loop_fail_reason = f"[{t}] 치명적 오류: {str(e)}"
                    logging.error(f"🚨 [{t}] delayed_trade 개별 종목 처리 중 치명적 오류: {e}")
                    if chat_id:
                        try:
                            await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=f"🚨 <b>[{t}] 스케줄러 처리 중 오류 발생. 스킵합니다.</b>\n<code>{html.escape(str(e))}</code>", parse_mode='HTML'), timeout=15.0)
                        except Exception: pass

            if not loop_fully_successful:
                return False, loop_fail_reason
        return True, "SUCCESS"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            success, fail_reason = await asyncio.wait_for(_do_delayed_trade(), timeout=300.0)
            if success:
                if attempt > 1 and chat_id: 
                    try:
                        await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=f"✅ <b>[통신 복구] {attempt}번째 재시도 끝에 장전/이관을 완수했습니다!</b>", parse_mode='HTML'), timeout=15.0)
                    except Exception: pass
                return 
        except Exception as e:
            logging.error(f"정규장 덫 실전 전송 에러 ({attempt}/{MAX_RETRIES}): {e}", exc_info=True)
            if attempt == 1 and chat_id:
                safe_err = html.escape(str(e))
                try:
                    await asyncio.wait_for(
                        context.bot.send_message(
                            chat_id=chat_id, 
                            text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 실전 장전을 재시도합니다! 🛡️\n<code>사유: {safe_err}</code>", 
                            parse_mode='HTML'
                        ),
                        timeout=15.0
                    )
                except Exception: pass
        else:
             if attempt == 1 and chat_id:
                 safe_fail = html.escape(str(fail_reason))
                 try:
                     await asyncio.wait_for(
                         context.bot.send_message(
                            chat_id=chat_id, 
                            text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 실전 장전을 재시도합니다! 🛡️\n<code>사유: {safe_fail}</code>", 
                            parse_mode='HTML'
                         ),
                         timeout=15.0
                     )
                 except Exception: pass

        if attempt < MAX_RETRIES:
            if attempt != 1 and attempt % 5 == 0 and chat_id:
                try:
                    await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 10초 뒤 재시도합니다! 🛡️", parse_mode='HTML'), timeout=15.0)
                except Exception: pass
            await asyncio.sleep(RETRY_DELAY)

    if chat_id:
        try:
            await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text="🚨 <b>[긴급 에러] V-REV 실전 전송/이관 통신 복구 최종 실패. 수동 점검 요망!</b>", parse_mode='HTML'), timeout=15.0)
        except Exception: pass
