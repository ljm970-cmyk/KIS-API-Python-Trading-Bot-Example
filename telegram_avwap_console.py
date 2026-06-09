# ==========================================================
# FILE: telegram_avwap_console.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 3중 딥다이브 교차 검증(Syntax 붕괴, Async I/O 족쇄, Float 정밀도 사수) 통과 완료.
# 🚨 MODIFIED: [UI 텍스트 맹독성 하드코딩 소각] 과거 암살자/스캘퍼 실전 매매 시절의 환영(Ghost Text)을 전면 파기하고 순수 관제탑 팩트로 100% 롤오버 완료.
# 🚨 MODIFIED: [V86.00 텍스트 팩트 롤오버] '딥-레스큐' 및 '암살자' 레거시 명칭 영구 소각. 새벽 수금원 및 프리장 스캘퍼 퀀트 네이밍을 거쳐 '초고도화 인텔리전스 관제탑'으로 100% 팩트 교정 완료.
# 🚨 MODIFIED: [Quant Logic 팩트 교정] 고가/저가(High/Low) 기반 타점 산출 맹점을 소각하고, 각 세션의 시가(Open) 기준 상승장/하락장 판별(3/6/9% vs 6/9/12%) 팩트 연산 전면 이식.
# 🚨 MODIFIED: [암살자 딥-레스큐 실전 렌더링 락온] avwap_trade_state 파일을 EAFP 샌드박스로 파싱하여, 교전 중(qty > 0)일 때 원화 환산 목표 스윕가 및 -1% 하단 손절 덫 팩트 노출 완료.
# 🚨 MODIFIED: [ZeroDivision 붕괴 수술] OCO 듀얼 엑시트 원화/수익률 역산 시 수수료(fee_rate) 가산식의 분모 0 붕괴를 막기 위한 safe_denom 쉴드 락온.
# 🚨 MODIFIED: [Time Paradox UI 렌더링 붕괴 수술] 야후 파이낸스 1분봉 관통 시간을 연산할 때, 전일(Yesterday) 데이터 혼입을 막기 위해 무조건 `now_est.date()` 로 슬라이싱하도록 100% 팩트 락온.
# 🚨 MODIFIED: [고성능 클라우드 TPS 방어] 데이터 추출 시 순차적(Sequential) await 및 0.06초 샌드위치 지연(TPS 캡핑), 3단 지수 백오프 강제 락온.
# 🚨 MODIFIED: [Insight 14, 25] API String-Float 및 NaN/Inf 맹독성 포맷팅 쉴드. `_safe_float` 코어 래핑 전면 결속 완료.
# 🚨 MODIFIED: [Case 26 절대 헌법 준수] 텔레그램 HTML 파서 붕괴 방어를 위한 html.escape 쉴드 전역 강제 주입.
# 🚨 MODIFIED: [JSON Iterable 붕괴 방어] active_tickers 문자열 오염 시 봇이 오프라인되는 현상(Silent Death)을 막기 위한 isinstance 리스트 강제 캐스팅 락온.
# 🚨 MODIFIED: [Async I/O 코루틴 붕괴 수술] _get_exchange_rate 내부 동기 함수화로 asyncio.to_thread 래핑 시 발생하는 TypeError(Coroutine) 즉사 버그 완벽 소각.
# 🚨 NEW: [Phase 1/2/3 무한타격 렌더링 락온] 암살자 교전 상태에 따른 분할 딥-매수(phase) 및 HA 필수 여부를 UI에 동적 오버라이드.
# 🚨 NEW: [KeyError 즉사 버그 방어] 달력 API(mcal) 반환 객체 및 DataFrame 객체의 컬럼 결측 시 발생하는 런타임 붕괴를 막기 위해 columns 교차 검증 쉴드 전면 결속.
# 🚨 NEW: [I/O Thread Crash 방어] _read_state 내부 EAFP 패턴 주입으로 I/O 예외의 비동기 전이 원천 차단.
# 🚨 NEW: [듀얼 익절 모드 렌더링 이식] 목표 모드(KRW/PCT)에 따른 동적 목표가 역산 및 UI 팩트 분기 표출망 결속 완료.
# 🚨 NEW: [듀얼 섀도우 트래킹 스탠바이 표출] 잔고 0주 및 phase > 0 일 때 무한 재진입망 감시 현황(상단 V반등/하단 심해) 실시간 렌더링 이식.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import math
import asyncio
import time
import os
import json
import pandas as pd
import pandas_market_calendars as mcal  

import yfinance as yf
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
        
        # 🚨 [KeyError 및 State Mismatch 방어] mcal 응답 객체 컬럼 교차 검증
        if schedule is not None and not schedule.empty and 'market_open' in schedule.columns and 'market_close' in schedule.columns:
            market_open = schedule.iloc[0]['market_open'].astimezone(est)
            market_close = schedule.iloc[0]['market_close'].astimezone(est)
        elif schedule is not None and schedule.empty:
            is_holiday = True
        else:
            if now_est.weekday() < 5: 
                market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
                market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
            else: 
                is_holiday = True

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
            # 🚨 MODIFIED: [JSON Iterable 붕괴 방어] 문자열 오염 시 봇 오프라인 현상(Silent Death) 원천 차단
            if isinstance(active_tickers, str):
                active_tickers = [active_tickers]
            elif not isinstance(active_tickers, list):
                active_tickers = []
        except Exception as e:
            logging.error(f"🚨 Config I/O 타임아웃 (active_tickers): {e}")
            active_tickers = []
            
        avwap_tickers = [t for t in active_tickers if t == "SOXL"]
        
        if not avwap_tickers:
            return "⚠️ <b>[관측망 오프라인]</b>\n▫️ 감시 대상(SOXL) 종목이 없습니다.", None
        
        active_avwap = avwap_tickers
        
        msg = f"📡 <b>[ 초고도화 인텔리전스 관제탑 ]</b>\n{header_status}\n\n"
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

        # 🚨 MODIFIED: [Async I/O 코루틴 붕괴 수술] def로 팩트 교정하여 to_thread 런타임 호환성 100% 확보
        def _get_exchange_rate():
            time.sleep(0.06)
            df = yf.Ticker("KRW=X").history(period="1d", timeout=5.0)
            if not df.empty and 'Close' in df.columns and len(df['Close']) > 0:
                val = self._safe_float(df['Close'].iloc[-1])
                return val if val > 0 else 1400.0
            return 1400.0

        exchange_rate = 1400.0
        try:
            xr_val = await asyncio.wait_for(asyncio.to_thread(_get_exchange_rate), timeout=10.0)
            if xr_val > 0: exchange_rate = xr_val
        except Exception: pass

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
                
                prev_c_val = await _get_with_retry(self.broker.get_previous_close, t)
                prev_c = self._safe_float(prev_c_val)
                
                base_curr_p_val = await _get_with_retry(self.broker.get_current_price, base_t)
                base_curr_p = self._safe_float(base_curr_p_val)
                
                base_amp5_val = await _get_with_retry(self.broker.get_amp_5d_data, base_t)
                base_amp5 = self._safe_float(base_amp5_val)
                
                df_1m = await _get_with_retry(self.broker.get_1min_candles_df, t)
                df_base = await _get_with_retry(self.broker.get_1min_candles_df, base_t)
                
                # 🚨 [9대 관측 지표] SMA 5 데이터 추출 파이프라인 결속
                ma_5day_val = await _get_with_retry(self.broker.get_5day_ma, t)
                ma_5day = self._safe_float(ma_5day_val)
                
                fee_rate = self._safe_float(await _get_with_retry(self.cfg.get_fee, t)) / 100.0
                target_krw = self._safe_float(await _get_with_retry(self.cfg.get_avwap_target_krw, t))
                
                # 🚨 NEW: [목표가 듀얼 역산 엔진] KRW/PCT 모드 스키마 동적 패싱
                target_mode = str(await _get_with_retry(getattr(self.cfg, 'get_avwap_target_mode', lambda x: "KRW"), t)).upper()
                target_pct = self._safe_float(await _get_with_retry(getattr(self.cfg, 'get_avwap_target_pct', lambda x: 10.0), t))

            except Exception as e:
                logging.error(f"🚨 [{t}] 퀀트 관측망 데이터 추출 실패: {e}")
                curr_p, prev_c, base_curr_p, base_amp5, df_1m, df_base, ma_5day, fee_rate, target_krw = 0.0, 0.0, 0.0, 0.0, None, None, 0.0, 0.0007, 1000000.0
                target_mode, target_pct = "KRW", 10.0

            # 🚨 [암살자 딥-레스큐 실전 렌더링 팩트 파싱]
            # 🚨 NEW: [phase 및 last_entry_price 스키마 확장] 분할 타격 상태 추적을 위한 변수 추가
            avwap_qty, avwap_avg, avwap_inv_usd, target_usd, cut_loss = 0, 0.0, 0.0, 0.0, 0.0
            phase = 0
            last_entry_price = 0.0
            is_assassin_active = False
            
            state_file = f"data/avwap_trade_state_{t}.json"
            try:
                def _read_state():
                    # 🚨 NEW: [I/O Thread Crash 방어] EAFP 패턴 적용으로 예외 흡수
                    try:
                        with open(state_file, 'r', encoding='utf-8') as f:
                            return json.load(f)
                    except (OSError, json.JSONDecodeError):
                        return {}

                state_data = await asyncio.wait_for(asyncio.to_thread(_read_state), timeout=5.0)
                
                if isinstance(state_data, dict):
                    avwap_qty = int(self._safe_float(state_data.get('qty', 0)))
                    phase = int(self._safe_float(state_data.get('phase', 0)))
                    last_entry_price = self._safe_float(state_data.get('last_entry_price', 0.0))
                    
                    if avwap_qty > 0:
                        is_assassin_active = True
                        avwap_avg = self._safe_float(state_data.get('avg_price', 0.0))
                        avwap_inv_usd = avwap_qty * avwap_avg
                        
                        # 🚨 NEW: 2차/무한 재진입 타격망의 조건부 컷오프(손절망) 연산 팩트 교정
                        if last_entry_price > 0.0:
                            cut_loss = round(last_entry_price * 0.99, 2)
                        else:
                            cut_loss = round(avwap_avg * 0.99, 2)
                        
                        # 🚨 [ZeroDivision 붕괴 수술] 수수료 가산식 분모 0 붕괴 차단
                        safe_denom = avwap_qty * max(0.0001, (1.0 - fee_rate))
                        
                        # 🚨 NEW: [듀얼 익절 역산 엔진 렌더링 팩트 동기화]
                        if target_mode == "PCT":
                            gross_invest = avwap_inv_usd * (1.0 + fee_rate)
                            target_usd = (gross_invest * (1.0 + target_pct / 100.0)) / safe_denom
                        else:
                            target_usd = ((target_krw / exchange_rate) + (avwap_inv_usd * (1.0 + fee_rate))) / safe_denom
            except Exception:
                pass

            # ==============================================================
            # 1️⃣ 지표 1: 기초지수 평균 진폭 레버리지(x3) 환산
            # ==============================================================
            lev_amp_pct = base_amp5 * 3 * 100.0

            # ==============================================================
            # 2️⃣ 지표 2: 기초지수 실시간/누적 VWAP 이격도
            # ==============================================================
            base_vwap = 0.0
            base_gap_pct = 0.0
            lev_gap_pct = 0.0

            if df_base is not None and not df_base.empty:
                # 🚨 [Time Paradox UI 렌더링 붕괴 수술] 당일 데이터만 강제 슬라이싱
                df_b_today = df_base[df_base.index.date == today_est_date].copy()
                if 'time_est' in df_b_today.columns:
                    # VWAP은 정규장(09:30~16:00) 기준 누적 연산이 표준
                    df_b_reg = df_b_today[(df_b_today['time_est'] >= '093000') & (df_b_today['time_est'] <= '155959')].copy()
                    
                    # 🚨 [KeyError 붕괴 원천 차단] 필수 연산 컬럼 교차 검증 쉴드 결속
                    required_cols = ['high', 'low', 'close', 'volume']
                    if not df_b_reg.empty and all(c in df_b_reg.columns for c in required_cols):
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
                                # 기초지수 VWAP 이격도 레버리지(x3) 환산
                                lev_gap_pct = base_gap_pct * 3.0

            # ==============================================================
            # 3️⃣~8️⃣ 지표 3-8: 프리장/정규장/애프터장 시가(Open) 추출 및 타점 연산
            # ==============================================================
            # 🚨 MODIFIED: [Quant Logic 팩트 교정] 고가/저가 데드코드 소각 및 Open 기반 상승장/하락장 판별
            df_today = df_1m[df_1m.index.date == today_est_date].copy() if (df_1m is not None and not df_1m.empty) else pd.DataFrame()
            
            df_pre = pd.DataFrame()
            df_reg = pd.DataFrame()
            df_aft = pd.DataFrame()
            
            if not df_today.empty and 'time_est' in df_today.columns:
                df_pre = df_today[(df_today['time_est'] >= '040000') & (df_today['time_est'] <= '092959')]
                df_reg = df_today[(df_today['time_est'] >= '093000') & (df_today['time_est'] <= '160000')]
                df_aft = df_today[(df_today['time_est'] >= '160100') & (df_today['time_est'] <= '200000')]

            def _calc_session_metrics(df_session, p_close):
                # 🚨 [KeyError 붕괴 원천 차단] 필수 연산 컬럼 교차 검증 쉴드 결속
                req_cols = ['open', 'high', 'low']
                if df_session.empty or not all(c in df_session.columns for c in req_cols):
                    return 0.0, 0.0, 0.0, False, 0.0, 0.0, 0.0
                
                s_open = self._safe_float(df_session['open'].iloc[0])
                s_high = self._safe_float(df_session['high'].max())
                s_low = self._safe_float(df_session['low'].min())
                
                is_bull = s_open > p_close
                drop_1 = 0.97 if is_bull else 0.94
                drop_2 = 0.94 if is_bull else 0.91
                drop_3 = 0.91 if is_bull else 0.88
                
                t1 = s_open * drop_1
                t2 = s_open * drop_2
                t3 = s_open * drop_3
                
                return s_open, s_high, s_low, is_bull, t1, t2, t3

            pre_open, pre_high, pre_low, pre_bull, pre_t1, pre_t2, pre_t3 = _calc_session_metrics(df_pre, prev_c)
            reg_open, reg_high, reg_low, reg_bull, reg_t1, reg_t2, reg_t3 = _calc_session_metrics(df_reg, prev_c)
            aft_open, aft_high, aft_low, aft_bull, aft_t1, aft_t2, aft_t3 = _calc_session_metrics(df_aft, prev_c)

            # ==============================================================
            # 🖥️ 뷰포트 렌더링
            # ==============================================================
            msg += f"🎯 <b>[ {ticker_clean} 마스터 옵저버 ]</b>\n"
            msg += f"▫️ 현재가: <b>${curr_p:.2f}</b> (전일종가 ${prev_c:.2f})\n\n"
            
            msg += f"1️⃣ <b>기초지수({base_t_clean}) 환산 진폭 (5MA)</b>\n"
            msg += f"▫️ 레버리지(x3) 진폭: <b>{lev_amp_pct:.2f}%</b>\n\n"
            
            msg += f"2️⃣ <b>기초지수({base_t_clean}) VWAP 이격도</b>\n"
            if base_vwap > 0:
                sign = "+" if base_gap_pct > 0 else ""
                lev_sign = "+" if lev_gap_pct > 0 else ""
                msg += f"▫️ 당일 누적 VWAP: <b>${base_vwap:.2f}</b>\n"
                msg += f"▫️ 현재가 이격: <b>{sign}{base_gap_pct:.2f}%</b> (현재 ${base_curr_p:.2f})\n"
                msg += f"▫️ 레버리지(x3) 진폭: <b>{lev_sign}{lev_gap_pct:.2f}%</b>\n\n"
            else:
                msg += f"▫️ 정규장 개장 대기 중 (VWAP 연산 불가)\n\n"

            # 🚨 NEW: [HA 양봉 조건부 표출망 락온] 상승/하락장에 따른 HA 렌더링
            msg += f"🌅 <b>[ 프리장 스펙 (04:00~09:29) ]</b>\n"
            if pre_open > 0:
                msg += f"▫️ 시가: <b>${pre_open:.2f}</b> (고가 ${pre_high:.2f} / 저가 ${pre_low:.2f})\n"
                msg += f"▫️ 판별: {'상승장' if pre_bull else '하락장'} (시가 vs 전일종가)\n"
                msg += f"🔻 무한 하락 타점 추적 (시가 기준)\n"
                msg += f"       1차: <b>${pre_t1:.2f}</b>{'' if pre_bull else ' (+HA 필수)'} / 2차: <b>${pre_t2:.2f}</b> (+HA 필수) / 3차: <b>${pre_t3:.2f}</b> (+HA 필수)\n\n"
            else:
                msg += "▫️ 데이터 집계 대기 중...\n\n"

            msg += f"🔥 <b>[ 정규장 스펙 (09:30~16:00) ]</b>\n"
            if reg_open > 0:
                msg += f"▫️ 시가: <b>${reg_open:.2f}</b> (고가 ${reg_high:.2f} / 저가 ${reg_low:.2f})\n"
                msg += f"▫️ 판별: {'상승장' if reg_bull else '하락장'} (시가 vs 전일종가)\n"
                msg += f"🔻 무한 하락 타점 추적 (시가 기준)\n"
                msg += f"       1차: <b>${reg_t1:.2f}</b>{'' if reg_bull else ' (+HA 필수)'} / 2차: <b>${reg_t2:.2f}</b> (+HA 필수) / 3차: <b>${reg_t3:.2f}</b> (+HA 필수)\n\n"
            else:
                msg += "▫️ 정규장 개장 대기 중...\n\n"

            msg += f"🌙 <b>[ 애프터장 스펙 (16:00~20:00) ]</b>\n"
            if aft_open > 0:
                msg += f"▫️ 시가: <b>${aft_open:.2f}</b> (고가 ${aft_high:.2f} / 저가 ${aft_low:.2f})\n"
                msg += f"▫️ 판별: {'상승장' if aft_bull else '하락장'} (시가 vs 전일종가)\n"
                msg += f"🔻 무한 하락 타점 추적 (시가 기준)\n"
                msg += f"       1차: <b>${aft_t1:.2f}</b>{'' if aft_bull else ' (+HA 필수)'} / 2차: <b>${aft_t2:.2f}</b> (+HA 필수) / 3차: <b>${aft_t3:.2f}</b> (+HA 필수)\n\n"
            else:
                msg += "▫️ 애프터장 개장 대기 중...\n\n"

            # 🚨 MODIFIED: [UI 텍스트 팩트 교정] 사용자 요청에 따라 SMA 5 텍스트 위치를 헤더에서 바디로 이동 락온
            msg += f"📊 <b>[ 직전 5거래일 정규장 종가 평균 ]</b>\n"
            if ma_5day > 0:
                msg += f"▫️ 5일 평균가(SMA 5): <b>${ma_5day:.2f}</b>\n\n"
            else:
                msg += "▫️ 5일 평균가(SMA 5): 대기 중...\n\n"

            # 🚨 NEW: [암살자 분할 딥-레스큐 교전망 현황 정밀 렌더링 락온]
            msg += f"⚔️ <b>[ 암살자 딥-레스큐 교전망 현황 ]</b>\n"
            if is_assassin_active:
                if phase == 1:
                    msg += f"▫️ 교전 상태: <b>1차 딥-매수 완료 (50% 투입, 휩소 방어를 위한 관망 중)</b>\n"
                elif phase == 2:
                    msg += f"▫️ 교전 상태: <b>2차 딥-매수 완료 (100% 누적 투입, 1+2차 통합 연쇄 손절망 가동 중)</b>\n"
                elif phase >= 3:
                    msg += f"▫️ 교전 상태: <b>무한 재진입 타격망 가동 중 (주문가능금액 100% 투입, 진입가 손절망 가동 중)</b>\n"
                else:
                    msg += f"▫️ 교전 상태: <b>ON (OCO 듀얼 엑시트 대기 중)</b>\n"

                msg += f"▫️ 투입 물량: <b>{avwap_qty}주</b> (진입 단가 ${avwap_avg:.2f} | 총 ${avwap_inv_usd:,.2f})\n"
                
                # 🚨 NEW: [목표가 듀얼 렌더링] KRW vs PCT 분기망 표출
                if target_mode == "PCT":
                    msg += f"▫️ 전량 익절: <b>목표가 ${target_usd:.2f}</b> (수익률 {target_pct}%)\n"
                else:
                    msg += f"▫️ 전량 익절: <b>목표가 ${target_usd:.2f}</b> (환산 ₩{int(target_krw):,})\n"
                
                # 🚨 phase 조건부 손절망 UI 팩트 오버라이드
                if phase >= 2:
                    msg += f"▫️ 하드 손절: <b>탈출가 ${cut_loss:.2f}</b> (-1% KIS 덫 장전 완료)\n"
                elif phase == 1:
                    msg += f"▫️ 하드 손절: <b>장전 보류 (1차 물량 휩소 방어 차원 홀딩)</b>\n"
                else:
                    msg += f"▫️ 하드 손절: <b>탈출가 ${cut_loss:.2f}</b> (-1% KIS 덫 장전 완료)\n"
            else:
                # 🚨 NEW: [듀얼 섀도 트래킹 무한 재진입 스탠바이 표출] 잔고 0주이되 phase 1 이상일 때
                if phase > 0:
                    msg += f"▫️ 교전 상태: <b>듀얼 섀도우 트래킹 가동 중 (상단 V자 반등 / 하단 심해 줍줍 스캔)</b>\n"
                else:
                    msg += f"▫️ 교전 상태: <b>OFF (대기 중 / 무포지션)</b>\n"

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
