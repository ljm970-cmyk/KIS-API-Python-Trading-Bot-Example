# ==========================================================
# FILE: telegram_avwap_console.py
# ==========================================================
# 🚨 MODIFIED: [V53.11 시계열 체력 듀얼 대칭 락온] 
# 🚨 MODIFIED: [V53.09 관제탑 UI 횡보장 킬 스위치 시각적 렌더링 강제 바이패스]
# 🚨 MODIFIED: [V61.00 숏(SOXS) 전면 소각 작전 지시서 적용]
# 🚨 NEW: [상대적 체력 연산 30.0% 셧다운 락온 및 UI 디커플링 수술]
# 🚨 NEW: [V65.00 AVWAP 동적 하드스탑 락온]
# 🚨 NEW: [V66.00 AVWAP 암살자 덤핑 지터(Jitter) 분산 락온]
# 🚨 MODIFIED: [V66.05 Split-Brain 시각적 디커플링 해결]
# 🚨 NEW: [V72.16 AVWAP 정점요격 스위치 UI 연동]
# 🚨 MODIFIED: [V75.02 관제탑 런타임 붕괴 및 시각적 환각 완벽 수술]
# 🚨 MODIFIED: [V75.05 텍스트 다이어트 팩트 교정] 프리장/정규장 텍스트 전면 소각
# 🚨 NEW: [V7.4 Assassin Lock-on] 관제탑 UI 렌더링 디커플링 해체
# 🚨 MODIFIED: [V76.01 ATR5 동적 하드스탑 렌더링 영구 소각 및 투트랙 엑시트 UI 동기화]
# 🚨 MODIFIED: [순수익 2.0% 절대 보장 타점 공식]
# 🚨 MODIFIED: [V77.01 데이터 기아 방어 및 런타임 무결성 팩트 수술]
# 🚨 NEW: [V77.02 프리마켓 관제탑 데이터 기아 및 런타임 붕괴 완벽 수술]
# 🚨 MODIFIED: [V77.04 Operation Dawn Sniper - 프리장 선제 타격 및 50% 팩트 오프셋 이식 완비]
# - 09:25 타겟 락온(pm_locked) 안내문 및 고정 관념 UI 텍스트 전면 영구 소각.
# - 오프셋 진폭 연산 안내를 40%에서 '50% (Amp_5d * 0.50)'로 텍스트 및 수식 100% 팩트 교정.
# - 프리장(04:00 EST)부터 매분 실시간 고가/저가(Close 기준) 동적 트레일링 궤적을 뷰포트에 실시간 연동.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import math
import asyncio
import pandas as pd
import json
import os
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

class AvwapConsolePlugin:
    def __init__(self, config, broker, strategy, tx_lock):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.tx_lock = tx_lock

    async def get_console_message(self, app_data):
        est = ZoneInfo('America/New_York')
        now_est = datetime.datetime.now(est)
        curr_time = now_est.time()
        
        time_0400 = datetime.time(4, 0)
        time_0930 = datetime.time(9, 30)
        
        # [V77.04] 04:00 시점부터 프리장 선제 격발 스코프 개방에 따른 헤더 다이어트
        if curr_time < time_0930:
            header_status = "🌅 <b>[ 프리장 선제 타격 모드 (04:00~09:29 스캔 중) ]</b>"
        else:
            header_status = "🔥 <b>[ 정규장 실시간 추격 모드 (V7.4 암살자 격발) ]</b>"
        
        active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
        avwap_tickers = [t for t in active_tickers if t == "SOXL"]
        
        if not avwap_tickers:
            return "⚠️ <b>[AVWAP 암살자 오프라인]</b>
▫️ AVWAP 지원 종목이 없습니다.", None
           
        active_avwap = avwap_tickers
        tracking_cache = app_data.get('sniper_tracking', {})
        
        msg = f"🔫 <b>[ 차세대 AVWAP V7.4 암살자 관제탑 ]</b>
{header_status}

"
        keyboard = []

        for t in active_avwap:
            # 1. State Load & Self-Healing
            if not tracking_cache.get(f"AVWAP_INIT_{t}"):
                try:
                    saved_state = await asyncio.to_thread(self.strategy.v_avwap_plugin.load_state, t, now_est)
                    if saved_state:
                        tracking_cache[f"AVWAP_BOUGHT_{t}"] = saved_state.get('bought', False)
                        tracking_cache[f"AVWAP_SHUTDOWN_{t}"] = saved_state.get('shutdown', False)
                        tracking_cache[f"AVWAP_QTY_{t}"] = saved_state.get('qty', 0)
                        tracking_cache[f"AVWAP_AVG_{t}"] = saved_state.get('avg_price', 0.0)
                        tracking_cache[f"AVWAP_STRIKES_{t}"] = saved_state.get('strikes', 0)
                        tracking_cache[f"AVWAP_DUMP_JITTER_{t}"] = saved_state.get('dump_jitter_sec', 0)
                        tracking_cache[f"AVWAP_TRAP_ODNO_{t}"] = saved_state.get('trap_odno', "")
                        
                        # V7.4 Target Fact Injection
                        tracking_cache[f"AVWAP_PM_H_{t}"] = saved_state.get('PM_H', 0.0)
                        tracking_cache[f"AVWAP_PM_L_{t}"] = saved_state.get('PM_L', 0.0)
                        tracking_cache[f"AVWAP_T_H_{t}"] = saved_state.get('T_H', 0.0)
                        tracking_cache[f"AVWAP_T_L_{t}"] = saved_state.get('T_L', 0.0)
                        tracking_cache[f"AVWAP_OFFSET_{t}"] = saved_state.get('offset', 0.0)
                        
                        tracking_cache[f"AVWAP_INIT_{t}"] = True
                except Exception as e:
                    logging.error(f"🚨 AVWAP 관제탑 상태 로드 에러 ({t}): {e}")

            is_avwap_active = await asyncio.to_thread(getattr(self.cfg, 'get_avwap_hybrid_mode', lambda x: False), t)
            active_str = "🟢 암살 가동" if is_avwap_active else "⚪ 대기 (OFF)"
            
            # 2. Fetch Current Data & Missing Params (Timeout Fallback Shield & Fact Pipelining)
            amp5 = 0.0
            df_1m = None
            try:
                # [V77.02 팩트 수혈 파이프라인] 비동기 병렬 스캔 격리
                curr_p_task = asyncio.to_thread(self.broker.get_current_price, t)
                prev_c_task = asyncio.to_thread(self.broker.get_previous_close, t)
                amp5_task = asyncio.to_thread(self.broker.get_amp_5d_data, t)
                df_task = asyncio.to_thread(self.broker.get_1min_candles_df, t)

                res_batch = await asyncio.wait_for(
                    asyncio.gather(curr_p_task, prev_c_task, amp5_task, df_task, return_exceptions=True),
                    timeout=5.0
                )
                
                curr_p = float(res_batch[0]) if not isinstance(res_batch[0], Exception) and res_batch[0] else 0.0
                prev_c = float(res_batch[1]) if not isinstance(res_batch[1], Exception) and res_batch[1] else 0.0
                amp5 = float(res_batch[2]) if not isinstance(res_batch[2], Exception) and res_batch[2] else 0.0
                df_1m = res_batch[3] if not isinstance(res_batch[3], Exception) else None
                
            except Exception as e:
                logging.debug(f"🚨 데이터 팩트 수혈 에러: {e}")
                curr_p, prev_c, amp5, df_1m = 0.0, 0.0, 0.0, None

            avwap_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
            avwap_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)
            is_shutdown = tracking_cache.get(f"AVWAP_SHUTDOWN_{t}", False)
            trap_odno = tracking_cache.get(f"AVWAP_TRAP_ODNO_{t}", "")
            
            # V7.4 Target Dynamic Trailing Default Load
            pm_h = tracking_cache.get(f"AVWAP_PM_H_{t}", 0.0)
            pm_l = tracking_cache.get(f"AVWAP_PM_L_{t}", 0.0)
            t_h = tracking_cache.get(f"AVWAP_T_H_{t}", 0.0)
            t_l = tracking_cache.get(f"AVWAP_T_L_{t}", 0.0)
            offset = tracking_cache.get(f"AVWAP_OFFSET_{t}", 0.0)
            
            # 3. Action Scan via get_decision simulation (스코프 전진 배치 락온)
            status_txt = "👀 실시간 동적 타점 스캔 중"
            if not is_avwap_active:
                status_txt = "⚪ 모드 비활성 (레이더 관측 중)"
            elif is_shutdown: 
                status_txt = "🛑 당일 영구동결 (SHUTDOWN 퇴근)"
            elif avwap_qty > 0:
                if trap_odno:
                    status_txt = "🎯 딥매수 체결 완료 (+2.0% 익절 덫 작동 중)"
                else:
                    status_txt = "🎯 딥매수 체결 완료 (15:20 청산 지터 대기)"
            else:
                try:
                    avwap_state_dict = {
                        "strikes": tracking_cache.get(f"AVWAP_STRIKES_{t}", 0),
                        "shutdown": is_shutdown,
                        "qty": avwap_qty,
                        "avg_price": avwap_avg,
                        "bought": tracking_cache.get(f"AVWAP_BOUGHT_{t}", False),
                        "daily_bought_qty": tracking_cache.get(f"AVWAP_DAILY_BOUGHT_{t}", 0),
                        "daily_sold_qty": tracking_cache.get(f"AVWAP_DAILY_SOLD_{t}", 0),
                        "trap_odno": trap_odno,
                        "PM_H": pm_h,
                        "PM_L": pm_l,
                        "T_H": t_h,
                        "T_L": t_l,
                        "offset": offset,
                        "pm_locked": False, # [V77.04] pm_locked 로직 영구 소각 처리 무결성 연동
                        "dump_jitter_sec": tracking_cache.get(f"AVWAP_DUMP_JITTER_{t}", 0)
                    }
                    
                    # [V77.02/V77.04] 런타임 수술 완료: 다이렉트 팩트 직접 주입
                    decision = await asyncio.wait_for(
                        asyncio.to_thread(
                            self.strategy.v_avwap_plugin.get_decision,
                            base_ticker=t, exec_ticker=t,
                            base_curr_p=curr_p, exec_curr_p=curr_p,
                            df_1min_base=None, df_1min_exec=df_1m, avwap_qty=avwap_qty,
                            now_est=now_est, avwap_state=avwap_state_dict,
                            context_data=None,
                            is_simulation=True,
                            amp5=amp5,
                            prev_close=prev_c
                        ),
                        timeout=5.0
                    )
                    
                    if decision:
                        action = decision.get('action')
                        reason = decision.get('reason', '')
                        
                        # [V77.04] 오프셋 50% 및 04:00 이후 실시간 고가/저점 동적 궤적 덮어쓰기
                        pm_h = decision.get('PM_H', pm_h)
                        pm_l = decision.get('PM_L', pm_l)
                        t_h = decision.get('T_H', t_h)
                        t_l = decision.get('T_L', t_l)
                        offset = decision.get('offset', offset)
                        
                        tracking_cache[f"AVWAP_PM_H_{t}"] = pm_h
                        tracking_cache[f"AVWAP_PM_L_{t}"] = pm_l
                        tracking_cache[f"AVWAP_T_H_{t}"] = t_h
                        tracking_cache[f"AVWAP_T_L_{t}"] = t_l
                        tracking_cache[f"AVWAP_OFFSET_{t}"] = offset
                        
                        # 상태 렌더링 팩트 동기화 (pm_locked 낡은 텍스트 완전 소각)
                        if action in ['BUY', 'SELL']:
                            status_txt = f"🔥 타격 조건 즉시 충족 ({reason})"
                        elif action == 'SHUTDOWN':
                            status_txt = f"🛑 셧다운 격발 ({reason})"
                        elif reason:
                            status_txt = f"⏳ 대기 ({reason})"
                        else:
                            status_txt = "👀 프리장/정규장 실시간 동적 트레일링 중"
                                
                except Exception as e:
                    logging.debug(f"AVWAP 상태 텍스트 추출 에러: {e}")

            # 4. Message Assembly (50% 오프셋 및 동적 궤적 팩트 구성)
            msg += f"🎯 <b>[ {t} (롱) 작전반 - {active_str} ]</b>
"
            msg += f"▫️ 프리장 최고 (PM_H): <b>${pm_h:.2f}</b> (종가 트레일링)
"
            msg += f"▫️ 프리장 최저 (PM_L): <b>${pm_l:.2f}</b> (종가 트레일링)
"
            msg += f"▫️ Amp5 오프셋 (50%): <b>${offset:.2f}</b>
"
            msg += f"▫️ 상승 돌파 목표 (T_H): <b>${t_h:.2f}</b> (선제타격 타점)
"
            msg += f"▫️ 하락 셧다운 기준 (T_L): <b>${t_l:.2f}</b> (09:30 이후 활성)

"

            msg += f"📊 <b>[ 실시간 현재가 스프레드 ]</b>
"
            msg += f"▫️ 전일종가: <b>${prev_c:.2f}</b> (Amp5 진폭: {amp5*100:.2f}%)
"
            msg += f"▫️ 현재가격: <b>${curr_p:.2f}</b>
"

            # [V77.01] 순수 복리 1.02 곱연산 무결성 쉴드
            if avwap_qty > 0:
                trap_price = round(avwap_avg * 1.02, 2)
                msg += f"▫️ 매수평단: <b>${avwap_avg:.2f}</b> ({avwap_qty}주)
"
                msg += f"▫️ 익절목표(+2.0%): <b>${trap_price:.2f}</b>
"

            msg += f"
🚨 <b>[ 작전 수행 현황 ]</b>
"
            msg += f"▫️ 현재상태: <b>{status_txt}</b>
"

            # 5. 0주 강제 동기화 락온 뷰포트 보존 (방탄 헌법 사수)
            if avwap_qty > 0:
                keyboard.append([InlineKeyboardButton(f"🧯 {t} 암살자 수동 청산 (0주 락온)", callback_data=f"AVWAP_SET:SYNC_ZERO:{t}")])

        keyboard.append([
            InlineKeyboardButton("🔄 관제탑 새로고침", callback_data="AVWAP_SET:REFRESH:NONE"),
            InlineKeyboardButton("🔙 닫기", callback_data="RESET:CANCEL")
        ])

        msg += f"

⏱️ <i>마지막 레이더 스캔: {now_est.strftime('%Y-%m-%d %H:%M:%S')} (EST)</i>
"

        return msg, InlineKeyboardMarkup(keyboard)
