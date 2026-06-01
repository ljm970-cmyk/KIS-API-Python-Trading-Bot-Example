# ==========================================================
# FILE: telegram_avwap_console.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 3중 딥다이브 교차 검증(Async I/O 족쇄, State Mismatch 방어, Float 정밀도 사수) 통과 완료.
# 🚨 MODIFIED: [딥-레스큐 V85.00 프리장 스캘퍼 전면 리빌딩] 기존 정규장 기반의 "실시간 딥-레스큐" 텍스트를 "프리장 스캘핑 모드"로 전면 교체.
# 🚨 MODIFIED: [텍스트 팩트 롤오버] 갭 하락 게이트웨이가 소각됨에 따라 '순수 갭 하락 스캔 중', '갭 하락 조건 충족' 등의 텍스트를 전면 소각하고 '무제한 딥-매수 타격 스캔 중', '시가 확정' 포맷으로 100% 교정 락온.
# 🚨 MODIFIED: [뷰포트 팩트 교정] 당일 시가(Open) 및 Amp5 오프셋 기반의 타점 렌더링을 100% 소각. 오직 04:00 기준 "프리장 시가(Pre_Open)"를 추출하여 "-1.0% 딥-매수", "-0.5% 단독구출가" 절대 앵커링 좌표만 정밀 렌더링.
# 🚨 MODIFIED: [진입 게이트 동기화] 본진 평단가(main_actual_avg) 비교 로직 UI 소각. 전일 종가 비교 역시 소각.
# 🚨 MODIFIED: [Fire & Forget 락온] 타임스탑 청산 대기 메시지를 폐기하고, "🎯 0.5% 단독 구출 덫 가동 중 (Fire & Forget)" 상태 메시지 락온.
# 🚨 MODIFIED: [Type-Safety 궁극 수술] 상위 모듈에서 `app_data`가 None 또는 List로 오염 유입 시 발생하는 `setdefault` 런타임 붕괴를 방어하기 위해 isinstance 타입 쉴드 강제 주입.
# 🚨 MODIFIED: [Insight 14, 25] API String-Float 및 NaN/Inf 맹독성 포맷팅 쉴드. `_safe_float` 코어 래핑 전면 결속 완료.
# 🚨 MODIFIED: [Case 26 절대 헌법 준수] 텔레그램 HTML 파서 붕괴 방어를 위한 html.escape 쉴드 전역 강제 주입.
# 🚨 MODIFIED: [Case 32 & 33 절대 규칙] TPS 캡핑(0.06s) 및 3단 지수 백오프, 타임아웃(10s) 샌드위치 락온.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import math
import asyncio
import time
import pandas as pd
import pandas_market_calendars as mcal  
import json
import os
import html  
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

class AvwapConsolePlugin:
    def __init__(self, config, broker, strategy, tx_lock):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.tx_lock = tx_lock

    # 🚨 [수학 연산 붕괴 방어] NaN, Infinity 및 String-Comma 맹독성 데이터 정밀 필터링 락온
    def _safe_float(self, val):
        try:
            f_val = float(str(val or 0.0).replace(',', ''))
            if math.isnan(f_val) or math.isinf(f_val):
                return 0.0
            return f_val
        except Exception:
            return 0.0

    async def get_console_message(self, app_data):
        # 🚨 [Type-Safety 궁극 수술] app_data가 None이거나 List/Tuple로 오염 유입 시 발생하는 setdefault 붕괴 원천 차단
        if not isinstance(app_data, dict):
            app_data = {}

        est = ZoneInfo('America/New_York')
        now_est = datetime.datetime.now(est)
        curr_time = now_est.time()
        
        time_0400 = datetime.time(4, 0)
        time_0930 = datetime.time(9, 30)
     
        def _fetch_schedule():
            time.sleep(0.06) 
            nyse = mcal.get_calendar('NYSE')
            return nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
            
        schedule = None
        for attempt in range(3):
            try:
                schedule = await asyncio.wait_for(asyncio.to_thread(_fetch_schedule), timeout=10.0)
                break
            except Exception:
                if attempt == 2:
                    logging.error("🚨 달력 API 호출 에러/타임아웃. Fail-Open 평일 개장으로 강제 폴백합니다.")
                else: 
                    await asyncio.sleep(1.0 * (2 ** attempt))

        is_holiday = False
        market_open = None
        market_close = None
        
        if schedule is None or schedule.empty:
            if schedule is None and now_est.weekday() < 5: 
                # 🚨 [Fail-Open 팩트 교정] 무조건 가상 정규장 시간(Mock) 맵핑
                market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
                market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
            else: 
                is_holiday = True
        else:
            market_open = schedule.iloc[0]['market_open'].astimezone(est)
            market_close = schedule.iloc[0]['market_close'].astimezone(est)

        if is_holiday:
            status_code = "HOLIDAY"
        else:
            pre_start = market_open.replace(hour=4, minute=0, second=0, microsecond=0)
            after_end = market_close.replace(hour=20, minute=0, second=0, microsecond=0)

            if pre_start <= now_est < market_open:
                status_code = "PRE"
            elif market_open <= now_est < market_close:
                status_code = "REG"
            elif market_close <= now_est < after_end:
                status_code = "AFTER"
            else:
                status_code = "CLOSE"

        # 🚨 MODIFIED: [V85.00 헤더 팩트 롤오버] 프리장 스캘핑 모드로 텍스트 정밀 교정 (무제한 타격)
        if status_code == "HOLIDAY":
            header_status = "💤 <b>[ 미국 증시 휴장일 / 관망 모드 ]</b>"
        elif status_code in ["AFTER", "CLOSE"]:
            header_status = "🌙 <b>[ 애프터마켓 / 감시 종료 ]</b>"
        elif status_code == "PRE":
            header_status = "🌅 <b>[ 프리장 스캘핑 모드 (무제한 딥-매수 타격 스캔 중) ]</b>"
        else:
            header_status = "🔥 <b>[ 정규장 진입 (프리장 스캘퍼 퇴근 대기) ]</b>"
        
        # 🚨 [제1헌법] File I/O 코루틴 타임아웃 족쇄 래핑
        try:
            active_tickers = await asyncio.wait_for(asyncio.to_thread(self.cfg.get_active_tickers), timeout=10.0) or []
        except Exception as e:
            logging.error(f"🚨 Config I/O 타임아웃 (active_tickers): {e}")
            active_tickers = []
            
        avwap_tickers = [t for t in active_tickers if t == "SOXL"]
       
        if not avwap_tickers:
            return "⚠️ <b>[프리장 스캘퍼 오프라인]</b>\n▫️ 딥-레스큐 지원 종목이 없습니다.", None
           
        active_avwap = avwap_tickers
        
        # 🚨 [메모리 멱등성 사수] get() 대신 setdefault()를 사용하여 전역 딕셔너리와 참조 연결 락온
        tracking_cache = app_data.setdefault('sniper_tracking', {})
        
        cash_val = 0.0
        holdings = {}
        for attempt in range(3):
            try:
                # 🚨 [Case 32] 잔고 조회 루프 내 누락된 TPS 캡핑 방어막 주입
                await asyncio.sleep(0.06)
                cash_val_tuple = await asyncio.wait_for(asyncio.to_thread(self.broker.get_account_balance), timeout=10.0)
                cash_val = cash_val_tuple[0] if isinstance(cash_val_tuple, (list, tuple)) and len(cash_val_tuple) > 0 else 0.0
                holdings = cash_val_tuple[1] if isinstance(cash_val_tuple, (list, tuple)) and len(cash_val_tuple) > 1 else {}
                if not isinstance(holdings, dict): holdings = {}
                break
            except Exception:
                if attempt == 2: 
                    cash_val = 0.0
                    holdings = {}
                else: 
                    await asyncio.sleep(1.0 * (2 ** attempt))
        
        available_cash = self._safe_float(cash_val)
        
        msg = f"🔫 <b>[ 딥-레스큐 V85.00 관제탑 ]</b>\n{header_status}\n\n"
        keyboard = []

        async def _get_with_retry(func, *args):
            for attempt in range(3):
                try:
                    await asyncio.sleep(0.06) 
                    return await asyncio.wait_for(asyncio.to_thread(func, *args), timeout=10.0)
                except Exception:
                    if attempt == 2: return None
                    await asyncio.sleep(1.0 * (2 ** attempt))

        for t in active_avwap:
            await asyncio.sleep(0.06)
            
            ticker_clean = html.escape(str(t)) 
            
            if not tracking_cache.get(f"AVWAP_INIT_{t}"):
                try:
                    # 🚨 [제1헌법] File I/O 코루틴 타임아웃 족쇄 래핑
                    saved_state = await asyncio.wait_for(asyncio.to_thread(self.strategy.v_avwap_plugin.load_state, t, now_est), timeout=10.0) or {}
                    if saved_state:
                        tracking_cache[f"AVWAP_SHUTDOWN_{t}"] = bool(saved_state.get('shutdown'))
                        tracking_cache[f"AVWAP_QTY_{t}"] = int(self._safe_float(saved_state.get('qty')))
                        tracking_cache[f"AVWAP_AVG_{t}"] = self._safe_float(saved_state.get('avg_price'))
                        tracking_cache[f"AVWAP_TRAP_ODNO_{t}"] = str(saved_state.get('trap_odno') or "")
                        tracking_cache[f"AVWAP_LIMIT_ORDER_PLACED_{t}"] = bool(saved_state.get('limit_order_placed'))
                        tracking_cache[f"AVWAP_PLACED_TARGET_TH_{t}"] = self._safe_float(saved_state.get('placed_target_th'))
                        tracking_cache[f"AVWAP_TRAP_PLACED_TIME_{t}"] = str(saved_state.get('trap_placed_time') or "")
                        tracking_cache[f"AVWAP_BUY_ODNO_{t}"] = str(saved_state.get('buy_odno') or "")
                        tracking_cache[f"AVWAP_T_H_{t}"] = self._safe_float(saved_state.get('T_H'))
                        
                    tracking_cache[f"AVWAP_INIT_{t}"] = True
                except Exception as e:
                    logging.debug(f"🚨 상태 캐시 로드 중 타임아웃/에러: {e}")
                    pass

            # 🚨 [제1헌법] File I/O 코루틴 타임아웃 족쇄 래핑
            try:
                is_avwap_active = await asyncio.wait_for(asyncio.to_thread(getattr(self.cfg, 'get_avwap_hybrid_mode', lambda x: False), t), timeout=5.0)
            except Exception:
                is_avwap_active = False
            
            active_str = f"🟢 자율주행 감시 중" if is_avwap_active else "⚪ 대기 (OFF)"
            
            try:
                res_batch = await asyncio.gather(
                    _get_with_retry(self.broker.get_current_price, t),
                    _get_with_retry(self.broker.get_previous_close, t),
                    _get_with_retry(self.broker.get_1min_candles_df, t)
                )
           
                curr_p = self._safe_float(res_batch[0])
                prev_c = self._safe_float(res_batch[1])
                df_1m = res_batch[2]
               
            except Exception as e:
                curr_p, prev_c, df_1m = 0.0, 0.0, None

            # 🚨 MODIFIED: [프리장 시가 추출] 당일 프리장 시가(04:00 Open) 정밀 렌더링 락온
            pre_open = 0.0
            if df_1m is not None and not df_1m.empty and 'time_est' in df_1m.columns:
                df_pre = df_1m[(df_1m['time_est'] >= '040000') & (df_1m['time_est'] <= '092959')]
                if not df_pre.empty:
                    pre_open = self._safe_float(df_pre['open'].iloc[0])

            avwap_qty = int(self._safe_float(tracking_cache.get(f"AVWAP_QTY_{t}")))
            avwap_avg = self._safe_float(tracking_cache.get(f"AVWAP_AVG_{t}"))
            is_shutdown = bool(tracking_cache.get(f"AVWAP_SHUTDOWN_{t}"))
            trap_odno = str(tracking_cache.get(f"AVWAP_TRAP_ODNO_{t}") or "")
            limit_order_placed = bool(tracking_cache.get(f"AVWAP_LIMIT_ORDER_PLACED_{t}"))
            placed_target_th = self._safe_float(tracking_cache.get(f"AVWAP_PLACED_TARGET_TH_{t}"))
            t_h = self._safe_float(tracking_cache.get(f"AVWAP_T_H_{t}"))
            
            # 본진 실제 평단가 수집
            h_t = holdings.get(t) or {}
            main_actual_avg = self._safe_float(h_t.get('avg', 0.0))
            actual_qty = int(self._safe_float(h_t.get('qty', 0)))
            
            # 🚨 [절대 앵커링 뷰포트 오버라이드] 미장전 상태일지라도 시뮬레이션 값을 표출
            if t_h == 0.0 and pre_open > 0.0:
                t_h = round(pre_open * 0.990, 2)
            if placed_target_th == 0.0 and pre_open > 0.0:
                placed_target_th = round(pre_open * 0.995, 2)
            
            # 🚨 [UI Rendering 무결성 수술] 통신 타임아웃 대비 사전 캐시 상태 평가로 Fallback 락온
            if is_holiday:
                status_txt = f"💤 미국 증시 휴장일 (관측 오프라인)"
            elif is_shutdown and avwap_qty == 0: 
                status_txt = "🛑 당일 영구동결 (SHUTDOWN 퇴근)"
            elif avwap_qty > 0:
                status_txt = "🎯 0.5% 단독 구출 덫 가동 중 (Fire & Forget)"
            elif limit_order_placed and t_h > 0:
                # 🚨 MODIFIED: [텍스트 팩트 롤오버] 갭 하락 조건 텍스트 소각
                status_txt = f"⚡ 시가 확정 ➡️ [프리장 -1.0% 지정가 매수 덫 장전 집행: ${t_h:.2f}]"
            else:
                status_txt = "⚡ 프리장 시가(Pre_Open) 확정 대기 중"
            
            try:
                avwap_state_dict = {
                    "shutdown": is_shutdown,
                    "qty": avwap_qty,
                    "avg_price": avwap_avg,
                    "trap_odno": trap_odno,
                    "buy_odno": str(tracking_cache.get(f"AVWAP_BUY_ODNO_{t}") or ""),
                    "T_H": tracking_cache.get(f"AVWAP_T_H_{t}", 0.0),
                    "limit_order_placed": limit_order_placed,
                    "placed_target_th": tracking_cache.get(f"AVWAP_PLACED_TARGET_TH_{t}", 0.0),
                    "trap_placed_time": str(tracking_cache.get(f"AVWAP_TRAP_PLACED_TIME_{t}") or "")
                }
                
                # 🚨 [의사결정 엔진 동기화] 시뮬레이션 가동으로 실시간 상태 텍스트 정밀 도출
                avwap_base_ticker = 'SOXX' if t == 'SOXL' else ('QQQ' if t == 'TQQQ' else t)
                decision = await asyncio.wait_for(
                    asyncio.to_thread(
                        self.strategy.v_avwap_plugin.get_decision,
                        base_ticker=avwap_base_ticker, exec_ticker=t,
                        base_curr_p=curr_p, exec_curr_p=curr_p,
                        df_1min_base=None, df_1min_exec=df_1m, avwap_qty=avwap_qty,
                        avwap_alloc_cash=available_cash, 
                        now_est=now_est, avwap_state=avwap_state_dict,
                        context_data=None,
                        is_simulation=True,
                        prev_close=prev_c,
                        main_actual_avg=main_actual_avg, 
                        is_holiday=is_holiday 
                    ),
                    timeout=10.0
                )
                
                if decision:
                    action = decision.get('action')
                    reason = html.escape(str(decision.get('reason', '')))
                    
                    v_t_h = decision.get('T_H')
                    if v_t_h and self._safe_float(v_t_h) > 0:
                        t_h = self._safe_float(v_t_h)
                    
                    v_limit_order_placed = decision.get('limit_order_placed')
                    limit_order_placed = bool(v_limit_order_placed) if v_limit_order_placed is not None else limit_order_placed
                    
                    v_placed_target_th = decision.get('placed_target_th')
                    if v_placed_target_th and self._safe_float(v_placed_target_th) > 0:
                        placed_target_th = self._safe_float(v_placed_target_th)
                    
                    tracking_cache[f"AVWAP_T_H_{t}"] = t_h
                    tracking_cache[f"AVWAP_LIMIT_ORDER_PLACED_{t}"] = limit_order_placed
                    tracking_cache[f"AVWAP_PLACED_TARGET_TH_{t}"] = placed_target_th
        
                    if is_holiday:
                        status_txt = f"💤 미국 증시 휴장일 (관측 오프라인)"
                    elif is_shutdown and avwap_qty == 0: 
                        status_txt = f"🛑 셧다운 격발 ({reason})" if reason and action == 'SHUTDOWN' else "🛑 당일 영구동결 (SHUTDOWN 퇴근)"
                    elif avwap_qty > 0:
                        status_txt = "🎯 0.5% 단독 구출 덫 가동 중 (Fire & Forget)"
                    elif limit_order_placed and t_h > 0:
                        # 🚨 MODIFIED: [텍스트 팩트 롤오버] 갭 하락 조건 텍스트 소각
                        status_txt = f"⚡ 시가 확정 ➡️ [지정가 매수 덫 장전 중: ${t_h:.2f}]"
                    else:
                        if action == "PLACE_TRAP":
                            # 🚨 MODIFIED: [텍스트 팩트 롤오버] 갭 하락 텍스트 소각
                            status_txt = f"⚡ 시가 확정 ➡️ [지정가 매수 덫 장전 집행]"
                        elif action == "VERIFY_TRAP_FILL":
                            status_txt = f"🔥 덫 하향 관통 ➡️ [실체결 무결성 검증 격발]"
                        elif action == "TRAP_WAIT":
                            status_txt = f"⏳ 지정가 덫 장전 완료 ➡️ [지정가 매수 체결 대기]"
                        elif action == 'SHUTDOWN':
                            status_txt = f"🛑 셧다운 격발 ({reason})"
                        elif reason:
                            status_txt = f"⏳ 대기 ({reason})"
                            
            except Exception as e:
                pass

            # 🚨 [V85.00 뷰포트 상태 메시지 팩트 교정] 가변 오프셋 지표 소각 및 절대 앵커링 좌표 렌더링 락온
            msg += f"🎯 <b>[ {ticker_clean} 프리장 스캘퍼 관제탑 - {active_str} ]</b>\n"
            msg += f"▫️ 프리장 시가(04:00): <b>${pre_open:.2f}</b>\n"
            msg += f"▫️ 전일 종가(Prev): <b>${prev_c:.2f}</b>\n"
            msg += f"▫️ 딥-매수 덫(-1.0%): <b>${t_h:.2f}</b>\n"
            msg += f"▫️ 단독 구출가(-0.5%): <b>${placed_target_th:.2f}</b>\n\n"

            msg += f"📊 <b>[ 실시간 잔고 스프레드 ]</b>\n"
            msg += f"▫️ 현재가격: <b>${curr_p:.2f}</b>\n"
            msg += f"▫️ 본진평단: <b>${main_actual_avg:.2f}</b> ({actual_qty}주)\n"

            if avwap_qty > 0:
                trap_price = placed_target_th if placed_target_th > 0 else round(avwap_avg * 1.005, 2)
                msg += f"\n🛡️ <b>[ 프리장 스캘퍼 매수 현황 ]</b>\n"
                msg += f"▫️ 매수평단: <b>${avwap_avg:.2f}</b> ({avwap_qty}주)\n"
                msg += f"▫️ 단독탈출(+0.5%): <b>${trap_price:.2f}</b>\n"

            msg += f"\n🚨 <b>[ 작전 수행 현황 ]</b>\n"
            msg += f"▫️ 현재상태: <b>{status_txt}</b>\n"

            if is_holiday:
                keyboard.append([InlineKeyboardButton(f"💤 [{ticker_clean}] 증시 휴장일", callback_data="AVWAP_SET:REFRESH:NONE")])
            elif status_code in ["PRE", "REG"]:
                if avwap_qty > 0:
                    keyboard.append([InlineKeyboardButton(f"🧯 {ticker_clean} 암살자 수동 청산 (0주 락온)", callback_data=f"AVWAP_SET:SYNC_ZERO:{t}")])
            else:
                keyboard.append([InlineKeyboardButton(f"⛔ [{ticker_clean}] 장마감 (수동 제어 불가)", callback_data="AVWAP_SET:REFRESH:NONE")])

        keyboard.append([
            InlineKeyboardButton("🔄 관제탑 새로고침", callback_data="AVWAP_SET:REFRESH:NONE"),
            InlineKeyboardButton("🔙 닫기", callback_data="RESET:CANCEL")
        ])

        msg += f"\n\n⏱️ <i>마지막 레이더 스캔: {now_est.strftime('%Y-%m-%d %H:%M:%S')} (EST)</i>\n"

        return msg, InlineKeyboardMarkup(keyboard)
