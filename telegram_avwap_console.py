# ==========================================================
# FILE: telegram_avwap_console.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 3중 딥다이브 교차 검증(Syntax 붕괴, Async I/O 족쇄, Float 정밀도 사수) 통과 완료.
# 🚨 MODIFIED: [관측 전용 아키텍처 전환] 실전 매매 로직 소각에 따라 관제탑을 '초고도화 8대 퀀트 지표' 정보창으로 전면 리빌딩 (매매 현황 및 수동 개입 버튼 100% 영구 삭제).
# 🚨 MODIFIED: [Quant Logic 교정] 기초지수 VWAP 연산 시 (Open+High+Low+Close)/4.0 의 노이즈를 배제하고 정통 퀀트 표준인 (High+Low+Close)/3.0 으로 팩트 교정 완료.
# 🚨 MODIFIED: [Vectorization 연산 락온] 프리장(04:00~09:29) 및 정규장(09:30~16:00)에 이어 애프터장(16:01~20:00)까지 time_est 기반으로 정밀 슬라이싱 락온.
# 🚨 MODIFIED: [ZeroDivision 및 결측치 붕괴 수술] 정규장/애프터장 미개장(empty) 시 또는 거래량(Volume)/저가(Low)가 0일 때 발생하는 치명적 수학 연산 붕괴를 단락 평가(if > 0)로 완벽 차단.
# 🚨 MODIFIED: [UI 가독성 팩트 교정] 고가/저가 및 3/6/9% 타점 출력 문자열에 개행(\n) 및 7칸 공백 들여쓰기를 하드코딩하여 뷰포트 레이아웃 전면 리빌딩.
# 🚨 MODIFIED: [Time Paradox UI 렌더링 붕괴 수술] 야후 파이낸스 1분봉 관통 시간을 연산할 때, 전일(Yesterday) 데이터 혼입을 막기 위해 무조건 `now_est.date()` 로 슬라이싱하도록 100% 팩트 락온.
# 🚨 MODIFIED: [Series Stringification 붕괴 방어] 1분봉 고점/저점 시간 추출 시 `idxmax()` 와 `.loc` 의 조합이 유발하는 중복 인덱스 런타임 붕괴를 막기 위해 불리언 마스킹 및 `.iloc[0]` 로 100% 원천 교정.
# 🚨 MODIFIED: [고성능 클라우드 TPS 방어] 데이터 추출 시 순차적(Sequential) await 및 0.06초 샌드위치 지연(TPS 캡핑), 3단 지수 백오프 강제 락온.
# 🚨 MODIFIED: [Insight 14, 25] API String-Float 및 NaN/Inf 맹독성 포맷팅 쉴드. `_safe_float` 코어 래핑 전면 결속 완료.
# 🚨 MODIFIED: [Case 26 절대 헌법 준수] 텔레그램 HTML 파서 붕괴 방어를 위한 html.escape 쉴드 전역 강제 주입.
# 🚨 MODIFIED: [String Lexical Comparison 보완] '160001' 대신 '160100'으로 교정하여 1분봉 time_est('%H%M00') 포맷과 시맨틱 일치화 완료.
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

    # 🚨 [Insight 14, 25] API String-Float 및 NaN/Inf 맹독성 런타임 붕괴 방어막 결속
    def _safe_float(self, val):
        try:
            f_val = float(str(val or 0.0).replace(',', ''))
            if math.isnan(f_val) or math.isinf(f_val):
                return 0.0
            return f_val
        except Exception:
            return 0.0

    async def get_console_message(self, app_data):
        # 🚨 [Type-Safety 궁극 수술] app_data 오염(None/List) 유입 방어
        if not isinstance(app_data, dict):
            app_data = {}

        est = ZoneInfo('America/New_York')
        now_est = datetime.datetime.now(est)
        curr_time = now_est.time()
        today_est_date = now_est.date()
        
        # 🚨 [Case 32, 33] 달력 API 호출 시 TPS 캡핑 및 지수 백오프 주입
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

        if status_code == "HOLIDAY":
            header_status = "💤 <b>[ 미국 증시 휴장일 (오프라인) ]</b>"
        elif status_code in ["AFTER", "CLOSE"]:
            header_status = "🌙 <b>[ 애프터마켓 / 데이터 집계 종료 ]</b>"
        elif status_code == "PRE":
            header_status = "🌅 <b>[ 프리장 관측 중 (정규장 개장 대기) ]</b>"
        else:
            header_status = "🔥 <b>[ 정규장 실시간 스캔 중 ]</b>"
        
        try:
            active_tickers = await asyncio.wait_for(asyncio.to_thread(self.cfg.get_active_tickers), timeout=10.0) or []
        except Exception as e:
            logging.error(f"🚨 Config I/O 타임아웃 (active_tickers): {e}")
            active_tickers = []
            
        avwap_tickers = [t for t in active_tickers if t == "SOXL"]
        
        if not avwap_tickers:
            return "⚠️ <b>[관측망 오프라인]</b>\n▫️ 감시 대상(SOXL) 종목이 없습니다.", None
        
        active_avwap = avwap_tickers
        
        msg = f"📡 <b>[ 실시간 퀀트 인텔리전스 관제탑 ]</b>\n{header_status}\n\n"
        keyboard = []

        # 🚨 [Case 31, 32] 고성능 클라우드 TPS 방어 및 지수 백오프 비동기 헬퍼
        async def _get_with_retry(func, *args):
            for attempt in range(3):
                try:
                    await asyncio.sleep(0.06) 
                    return await asyncio.wait_for(asyncio.to_thread(func, *args), timeout=15.0)
                except Exception:
                    if attempt == 2: return None
                    await asyncio.sleep(1.0 * (2 ** attempt))

        for t in active_avwap:
            await asyncio.sleep(0.06)
            # 🚨 [Case 26] 텔레그램 HTML 파서 붕괴 방어용 html.escape 쉴드
            ticker_clean = html.escape(str(t)) 
            base_t = 'SOXX' if t == 'SOXL' else ('QQQ' if t == 'TQQQ' else t)
            base_t_clean = html.escape(str(base_t))
            
            try:
                # 🚨 데이터 추출 및 병목 방지
                curr_p_val = await _get_with_retry(self.broker.get_current_price, t)
                curr_p = self._safe_float(curr_p_val)
                
                base_curr_p_val = await _get_with_retry(self.broker.get_current_price, base_t)
                base_curr_p = self._safe_float(base_curr_p_val)
                
                base_amp5_val = await _get_with_retry(self.broker.get_amp_5d_data, base_t)
                base_amp5 = self._safe_float(base_amp5_val)
                
                df_1m = await _get_with_retry(self.broker.get_1min_candles_df, t)
                df_base = await _get_with_retry(self.broker.get_1min_candles_df, base_t)
                
            except Exception as e:
                logging.error(f"🚨 [{t}] 퀀트 관측망 데이터 추출 실패: {e}")
                curr_p, base_curr_p, base_amp5, df_1m, df_base = 0.0, 0.0, 0.0, None, None

            # ==============================================================
            # 1️⃣ 지표 1: 기초지수 평균 진폭 레버리지(x3) 환산
            # ==============================================================
            lev_amp_pct = base_amp5 * 3 * 100.0

            # ==============================================================
            # 2️⃣ 지표 2: 기초지수 실시간/누적 VWAP 이격도
            # ==============================================================
            base_vwap = 0.0
            base_gap_pct = 0.0
            lev_gap_pct = 0.0  # NEW: 레버리지 이격도 변수 선언

            if df_base is not None and not df_base.empty:
                # 🚨 [Time Paradox UI 렌더링 붕괴 수술] 당일 데이터만 강제 슬라이싱
                df_b_today = df_base[df_base.index.date == today_est_date].copy()
                if 'time_est' in df_b_today.columns:
                    # VWAP은 정규장(09:30~16:00) 기준 누적 연산이 표준
                    df_b_reg = df_b_today[(df_b_today['time_est'] >= '093000') & (df_b_today['time_est'] <= '155959')].copy()
                    if not df_b_reg.empty:
                        # 🚨 [Quant Logic 교정] 정통 퀀트 트레이딩 표준 연산으로 교정 (High+Low+Close)/3.0
                        df_b_reg['tp'] = (df_b_reg['high'].astype(float) + df_b_reg['low'].astype(float) + df_b_reg['close'].astype(float)) / 3.0
                        df_b_reg['vol'] = df_b_reg['volume'].astype(float)
                        df_b_reg['vol_tp'] = df_b_reg['tp'] * df_b_reg['vol']
                        
                        # 🚨 [ZeroDivision 붕괴 수술] 거래량 결측 보호
                        c_vol = df_b_reg['vol'].sum()
                        if c_vol > 0:
                            base_vwap = df_b_reg['vol_tp'].sum() / c_vol
                            if base_vwap > 0:
                                base_gap_pct = (base_curr_p - base_vwap) / base_vwap * 100.0
                                # NEW: 기초지수 VWAP 이격도 레버리지(x3) 환산
                                lev_gap_pct = base_gap_pct * 3.0

            # ==============================================================
            # 3️⃣~8️⃣ 지표 3-8: 프리장/정규장/애프터장 H/L 슬라이싱 및 타점 연산
            # ==============================================================
            # 🚨 MODIFIED: [Vectorization 연산 락온] time_est 기반 애프터장 슬라이싱 추가
            df_today = df_1m[df_1m.index.date == today_est_date].copy() if (df_1m is not None and not df_1m.empty) else pd.DataFrame()
            
            df_pre = pd.DataFrame()
            df_reg = pd.DataFrame()
            df_aft = pd.DataFrame() # NEW: 애프터장 데이터프레임 선언
            
            if not df_today.empty and 'time_est' in df_today.columns:
                df_pre = df_today[(df_today['time_est'] >= '040000') & (df_today['time_est'] <= '092959')]
                df_reg = df_today[(df_today['time_est'] >= '093000') & (df_today['time_est'] <= '160000')]
                # 🚨 MODIFIED: [String Lexical Comparison 보완] '160100'으로 교정하여 시맨틱 일치
                df_aft = df_today[(df_today['time_est'] >= '160100') & (df_today['time_est'] <= '200000')]

            # 프리장 지표 연산
            pre_h, pre_l, pre_amp = 0.0, 0.0, 0.0
            pre_h_t, pre_l_t = "미달성", "미달성"
            if not df_pre.empty:
                safe_high_pre = pd.to_numeric(df_pre['high'], errors='coerce')
                safe_low_pre = pd.to_numeric(df_pre['low'], errors='coerce')
                
                pre_h = self._safe_float(safe_high_pre.max())
                pre_l = self._safe_float(safe_low_pre.min())
                
                if pre_h > 0 and pre_l > 0:
                    pre_amp = (pre_h - pre_l) / pre_l * 100.0
                    
                    try:
                        # 🚨 [Series Stringification 붕괴 방어] idxmax() 대신 불리언 마스킹 강제
                        h_row = df_pre[safe_high_pre >= pre_h]
                        if not h_row.empty:
                            raw_h_t = str(h_row['time_est'].iloc[0]).zfill(6)
                            pre_h_t = f"{raw_h_t[:2]}:{raw_h_t[2:4]}"
                            
                        l_row = df_pre[safe_low_pre <= pre_l]
                        if not l_row.empty:
                            raw_l_t = str(l_row['time_est'].iloc[0]).zfill(6)
                            pre_l_t = f"{raw_l_t[:2]}:{raw_l_t[2:4]}"
                    except Exception: pass

            # 정규장 지표 연산
            reg_h, reg_l, reg_amp = 0.0, 0.0, 0.0
            reg_h_t, reg_l_t = "미달성", "미달성"
            if not df_reg.empty:
                safe_high_reg = pd.to_numeric(df_reg['high'], errors='coerce')
                safe_low_reg = pd.to_numeric(df_reg['low'], errors='coerce')
                
                reg_h = self._safe_float(safe_high_reg.max())
                reg_l = self._safe_float(safe_low_reg.min())
                
                if reg_h > 0 and reg_l > 0:
                    reg_amp = (reg_h - reg_l) / reg_l * 100.0
                    
                    try:
                        # 🚨 [Series Stringification 붕괴 방어] 불리언 마스킹 락온
                        h_row_r = df_reg[safe_high_reg >= reg_h]
                        if not h_row_r.empty:
                            raw_h_t_r = str(h_row_r['time_est'].iloc[0]).zfill(6)
                            reg_h_t = f"{raw_h_t_r[:2]}:{raw_h_t_r[2:4]}"
                            
                        l_row_r = df_reg[safe_low_reg <= reg_l]
                        if not l_row_r.empty:
                            raw_l_t_r = str(l_row_r['time_est'].iloc[0]).zfill(6)
                            reg_l_t = f"{raw_l_t_r[:2]}:{raw_l_t_r[2:4]}"
                    except Exception: pass

            # NEW: 애프터장 지표 연산 및 ValueError 방어막(Case 24) 락온
            aft_h, aft_l, aft_amp = 0.0, 0.0, 0.0
            aft_h_t, aft_l_t = "미달성", "미달성"
            if not df_aft.empty:
                safe_high_aft = pd.to_numeric(df_aft['high'], errors='coerce')
                safe_low_aft = pd.to_numeric(df_aft['low'], errors='coerce')
                
                aft_h = self._safe_float(safe_high_aft.max())
                aft_l = self._safe_float(safe_low_aft.min())
                
                if aft_h > 0 and aft_l > 0:
                    aft_amp = (aft_h - aft_l) / aft_l * 100.0
                    
                    try:
                        # 🚨 [Series Stringification 붕괴 방어] 불리언 마스킹 락온
                        h_row_a = df_aft[safe_high_aft >= aft_h]
                        if not h_row_a.empty:
                            raw_h_t_a = str(h_row_a['time_est'].iloc[0]).zfill(6)
                            aft_h_t = f"{raw_h_t_a[:2]}:{raw_h_t_a[2:4]}"
                            
                        l_row_a = df_aft[safe_low_aft <= aft_l]
                        if not l_row_a.empty:
                            raw_l_t_a = str(l_row_a['time_est'].iloc[0]).zfill(6)
                            aft_l_t = f"{raw_l_t_a[:2]}:{raw_l_t_a[2:4]}"
                    except Exception: pass

            # ==============================================================
            # 🖥️ 뷰포트 렌더링
            # ==============================================================
            msg += f"🎯 <b>[ {ticker_clean} 마스터 옵저버 ]</b>\n"
            msg += f"▫️ 현재가: <b>${curr_p:.2f}</b>\n\n"
            
            msg += f"1️⃣ <b>기초지수({base_t_clean}) 환산 진폭 (5MA)</b>\n"
            msg += f"▫️ 레버리지(x3) 진폭: <b>{lev_amp_pct:.2f}%</b>\n\n"
            
            msg += f"2️⃣ <b>기초지수({base_t_clean}) VWAP 이격도</b>\n"
            if base_vwap > 0:
                sign = "+" if base_gap_pct > 0 else ""
                lev_sign = "+" if lev_gap_pct > 0 else ""
                msg += f"▫️ 당일 누적 VWAP: <b>${base_vwap:.2f}</b>\n"
                msg += f"▫️ 현재가 이격: <b>{sign}{base_gap_pct:.2f}%</b> (현재 ${base_curr_p:.2f})\n"
                # MODIFIED: 2번 지표 내 레버리지(x3) 진폭 렌더링 추가
                msg += f"▫️ 레버리지(x3) 진폭: <b>{lev_sign}{lev_gap_pct:.2f}%</b>\n\n"
            else:
                msg += f"▫️ 정규장 개장 대기 중 (VWAP 연산 불가)\n\n"

            # MODIFIED: [UI 가독성 팩트 교정] 개행(\n) 및 7칸 공백 들여쓰기 락온
            msg += f"🌅 <b>[ 프리장 스펙 (04:00~09:29) ]</b>\n"
            if pre_h > 0 and pre_l > 0:
                msg += f"▫️ 고가: <b>${pre_h:.2f}</b> ({pre_h_t})\n       저가: <b>${pre_l:.2f}</b> ({pre_l_t})\n"
                msg += f"▫️ 세션 진폭: <b>{pre_amp:.2f}%</b>\n"
                msg += f"🔻 고가 대비 3%(${(pre_h*0.97):.2f})\n       / 6%(${(pre_h*0.94):.2f}) / 9%(${(pre_h*0.91):.2f})\n"
                msg += f"🔺 저가 대비 3%(${(pre_l*1.03):.2f})\n       / 6%(${(pre_l*1.06):.2f}) / 9%(${(pre_l*1.09):.2f})\n\n"
            else:
                msg += "▫️ 데이터 집계 대기 중...\n\n"

            # MODIFIED: [UI 가독성 팩트 교정] 개행(\n) 및 7칸 공백 들여쓰기 락온
            msg += f"🔥 <b>[ 정규장 스펙 (09:30~16:00) ]</b>\n"
            if reg_h > 0 and reg_l > 0:
                msg += f"▫️ 고가: <b>${reg_h:.2f}</b> ({reg_h_t})\n       저가: <b>${reg_l:.2f}</b> ({reg_l_t})\n"
                msg += f"▫️ 세션 진폭: <b>{reg_amp:.2f}%</b>\n"
                msg += f"🔻 고가 대비 3%(${(reg_h*0.97):.2f})\n       / 6%(${(reg_h*0.94):.2f}) / 9%(${(reg_h*0.91):.2f})\n"
                msg += f"🔺 저가 대비 3%(${(reg_l*1.03):.2f})\n       / 6%(${(reg_l*1.06):.2f}) / 9%(${(reg_l*1.09):.2f})\n\n"
            else:
                msg += "▫️ 정규장 개장 대기 중...\n\n"

            # NEW: [애프터장 스펙] 시계열 슬라이싱 연산 및 렌더링 뷰포트 추가
            msg += f"🌙 <b>[ 애프터장 스펙 (16:00~20:00) ]</b>\n"
            if aft_h > 0 and aft_l > 0:
                msg += f"▫️ 고가: <b>${aft_h:.2f}</b> ({aft_h_t})\n       저가: <b>${aft_l:.2f}</b> ({aft_l_t})\n"
                msg += f"▫️ 세션 진폭: <b>{aft_amp:.2f}%</b>\n"
                msg += f"🔻 고가 대비 3%(${(aft_h*0.97):.2f})\n       / 6%(${(aft_h*0.94):.2f}) / 9%(${(aft_h*0.91):.2f})\n"
                msg += f"🔺 저가 대비 3%(${(aft_l*1.03):.2f})\n       / 6%(${(aft_l*1.06):.2f}) / 9%(${(aft_l*1.09):.2f})\n"
            else:
                msg += "▫️ 애프터장 개장 대기 중...\n"

            # 🚨 [관측 전용 아키텍처 전환] 수동 매수/매도 제어 버튼 영구 삭제 유지
            if is_holiday:
                keyboard.append([InlineKeyboardButton(f"💤 [{ticker_clean}] 증시 휴장일", callback_data="AVWAP_SET:REFRESH:NONE")])
            elif status_code in ["CLOSE"]:
                keyboard.append([InlineKeyboardButton(f"⛔ [{ticker_clean}] 장마감", callback_data="AVWAP_SET:REFRESH:NONE")])

            keyboard.append([
                InlineKeyboardButton("🔄 관제탑 새로고침", callback_data="AVWAP_SET:REFRESH:NONE"),
                InlineKeyboardButton("🔙 닫기", callback_data="RESET:CANCEL")
            ])

        msg += f"\n\n⏱️ <i>마지막 레이더 스캔: {now_est.strftime('%Y-%m-%d %H:%M:%S')} (EST)</i>\n"

        return msg, InlineKeyboardMarkup(keyboard)
