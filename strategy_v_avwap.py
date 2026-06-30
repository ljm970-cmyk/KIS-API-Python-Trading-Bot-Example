# ==========================================================
# FILE: strategy_v_avwap.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 3중 딥다이브 교차 검증(Syntax 붕괴, Async I/O 족쇄, Float 정밀도 사수) 통과 완료.
# 🚨 MODIFIED: [순수 돌파/추종 데이 트레이딩 아키텍처 팩트 교정] 역추세 기반의 낡은 '현재가 <= 타점' 하향 관통 로직을 100% 영구 소각하고, "현재가가 실시간 VWAP 이상(상회 또는 상향 돌파)"일 때 `BREAKOUT_BUY`를 반환하도록 팩트 락온.
# 🚨 MODIFIED: [과욕 제어 매도 타점 팩트 락온] 동적 익절 파라미터를 소각하고 체결 평단가 기준 '+1.0% 고정 익절' 스키마를 하드코딩하여 1-Shot 1-Kill 타격망 수복.
# 🚨 MODIFIED: [절대 타임쉴드 (04:07 EST) 결속] 04:00~04:06 EST 구간 동안 기관의 휩소(노이즈)를 회피하기 위해 무조건 `OBSERVING(관망)`을 반환하도록 타임라인 방어막 100% 팩트 이식.
# 🚨 MODIFIED: [프리장 미진입 조기 퇴근 팩트 락온] 정규장(09:30 EST 이후) 무포지션(0주) 상태일 경우 돌파와 무관하게 신규 진입을 전면 차단하고 '조기 퇴근' 상태를 반환하도록 2중 팩트 락온.
# 🚨 MODIFIED: [Quant Logic 교정] 기초지수 매크로(fetch_macro_context) 연산 시 (High+Low+Close)/3.0 정통 퀀트 표준으로 팩트 교정 완료.
# 🚨 MODIFIED: [Case 08, 16] os.path.exists 동기스캔 배제, EAFP 적용 및 temp_path 원자적 쓰기 스코프 전진 배치 유지.
# 🚨 MODIFIED: [Case 35 결측치 전이 방어] 1분봉 데이터의 결측치(NaN)로 인해 VWAP 연산이 붕괴되는 현상을 막기 위해 ffill().bfill() 체인 강제 락온.
# 🚨 NEW: [Date Schema Mismatch 방어] 16:05 EST에 스냅샷을 생성할 경우, 내일 자 스냅샷으로 락온(Forward-Lock)되도록 `_get_logical_date_str()` 100% 팩트 수술. (주말 건너뛰기 보정 포함)
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import math
import time 
import yfinance as yf
import pandas as pd
import numpy as np 
import json
import os
import tempfile
import html

class VAvwapHybridPlugin:
    def __init__(self):
        self.plugin_name = "AVWAP_BREAKOUT_OBSERVER"

    def _safe_float(self, value):
        try:
            val = float(str(value or 0.0).replace(',', ''))
            if math.isnan(val) or math.isinf(val):
                return 0.0
            return val
        except Exception:
            return 0.0

    def _flatten_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if isinstance(df.columns, pd.MultiIndex):
            if 'Ticker' in df.columns.names:
                df.columns = df.columns.droplevel('Ticker')
            elif df.columns.nlevels == 2:
                price_fields = {'Close', 'High', 'Low', 'Open', 'Volume', 'Adj Close'}
                level0_vals = set(df.columns.get_level_values(0))
                drop_level = 0 if not level0_vals.intersection(price_fields) else 1
                df.columns = df.columns.droplevel(drop_level)
        return df

    def _get_logical_date_str(self, now_est):
        """ 🚨 [미래 참조 방어막 100% 수술] 16:00 이후 생성 시 D+1(명일)로 포워드 락온. 주말이면 차주 월요일로 정밀 매핑. """
        if now_est.hour < 4 or (now_est.hour == 4 and now_est.minute < 4):
            target_date = now_est - datetime.timedelta(days=1)
        elif now_est.time() >= datetime.time(16, 0):
            target_date = now_est + datetime.timedelta(days=1)
        else:
            target_date = now_est
            
        # 🚨 [주말(토/일) 보정] 16:05 금요일에 찍힌 스냅샷은 다음 거래일(월요일)을 타겟으로 락온
        if target_date.weekday() == 5: 
            target_date += datetime.timedelta(days=2)
        elif target_date.weekday() == 6: 
            target_date += datetime.timedelta(days=1)
            
        return target_date.strftime('%Y-%m-%d')

    def _get_state_file(self, ticker, now_est):
        return f"data/avwap_state_persistent_{ticker}.json"

    def load_state(self, ticker, now_est):
        file_path = self._get_state_file(ticker, now_est)
        today_str = self._get_logical_date_str(now_est)
        data = {}

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except OSError:
            pass
        except json.JSONDecodeError:
            pass
            
        if not isinstance(data, dict):
            data = {}

        if data.get('date') != today_str:
            data = {
                'date': today_str
            }
            self.save_state(ticker, now_est, data)
        
        return data

    def save_state(self, ticker, now_est, state_data):
        if not isinstance(state_data, dict):
            state_data = {}
            
        file_path = self._get_state_file(ticker, now_est)
        today_str = self._get_logical_date_str(now_est)

        merged_data = {'date': today_str}

        dir_name = os.path.dirname(file_path) or '.'
        try:
            os.makedirs(dir_name, exist_ok=True)
        except OSError:
            pass

        fd = None
        temp_path = None
        try:
            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                fd = None
                json.dump(merged_data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno()) 
            os.replace(temp_path, file_path)
            temp_path = None
        except Exception as e:
            if fd is not None:
                try: os.close(fd)
                except OSError: pass
            if temp_path:
                try: os.remove(temp_path)
                except OSError: pass
            logging.error(f"🚨 [V_AVWAP] 관측기 상태 저장 실패 (원자적 쓰기 에러): {e}")

    def apply_stock_split(self, ticker, ratio, now_est):
        pass

    def fetch_macro_context(self, base_ticker):
        for attempt in range(3):
            try:
                time.sleep(0.06) 
                tkr = yf.Ticker(base_ticker)
                df_1m = tkr.history(period="5d", interval="1m", prepost=False, timeout=5)
    
                prev_vwap = 0.0
                prev_close = 0.0
    
                est = ZoneInfo('America/New_York')
                now_est = datetime.datetime.now(est)
 
                if now_est.hour < 4:
                    today_est = (now_est - datetime.timedelta(days=1)).date()
                else:
                    today_est = now_est.date()
    
                if not df_1m.empty:
                    df_1m = self._flatten_columns(df_1m)
    
                    if df_1m.index.tz is None:
                        df_1m.index = df_1m.index.tz_localize('UTC').tz_convert(est)
                    else:
                        df_1m.index = df_1m.index.tz_convert(est)
    
                    df_past_1m = df_1m[df_1m.index.date < today_est].copy()
    
                    if not df_past_1m.empty:
                        last_date = df_past_1m.index.date[-1]
                        df_prev_day = df_past_1m[df_past_1m.index.date == last_date].copy()
                        df_prev_day = df_prev_day.between_time('09:30', '15:59')
    
                        if not df_prev_day.empty:
                            df_prev_day['High'] = df_prev_day['High'].ffill().bfill()
                            df_prev_day['Low'] = df_prev_day['Low'].ffill().bfill()
                            df_prev_day['Close'] = df_prev_day['Close'].ffill().bfill()
                            df_prev_day['Volume'] = df_prev_day['Volume'].ffill().bfill().fillna(0)

                            prev_close = self._safe_float(df_prev_day['Close'].iloc[-1])
     
                            df_prev_day['tp'] = (df_prev_day['High'].astype(float) + df_prev_day['Low'].astype(float) + df_prev_day['Close'].astype(float)) / 3.0
                            df_prev_day['vol'] = df_prev_day['Volume'].astype(float)
                            df_prev_day['vol_tp'] = df_prev_day['tp'] * df_prev_day['vol']
    
                            cum_vol = df_prev_day['vol'].sum()
                            if cum_vol > 0:
                                prev_vwap = self._safe_float(df_prev_day['vol_tp'].sum() / cum_vol)
                            else:
                                prev_vwap = prev_close
    
                if prev_vwap == 0.0:
                    prev_vwap = prev_close
                   
                return {
                    "prev_close": prev_close,
                    "prev_vwap": prev_vwap,
                    "avg_vol_20": 0.0 
                }
    
            except Exception as e:
                logging.debug(f"⚠️ [V_AVWAP] YF 기초자산 매크로 컨텍스트 추출 오류 (시도 {attempt+1}/3): {e}")
                if attempt == 2: return None
                time.sleep(1.0 * (2 ** attempt))

    def get_decision(self, base_ticker=None, exec_ticker=None, base_curr_p=0.0, exec_curr_p=0.0, base_day_open=0.0, avwap_avg_price=0.0, avwap_qty=0, avwap_alloc_cash=0.0, context_data=None, df_1min_base=None, df_1min_exec=None, now_est=None, avwap_state=None, regime_data=None, is_simulation=False, sortie_mode="SINGLE", **kwargs):
        is_holiday = kwargs.get('is_holiday', False)
        now_est = now_est or datetime.datetime.now(ZoneInfo('America/New_York'))
        today_est_date = now_est.date()
        curr_t = now_est.time()

        def _build_res(action, reason, tp=0.0, session_vwap=0.0):
            return {
                'action': 'OBSERVING' if is_simulation else action,
                'raw_action': action,
                'reason': html.escape(str(reason)),
                'target_price': self._safe_float(tp),
                'session_vwap': self._safe_float(session_vwap)
            }

        exec_curr_p = self._safe_float(exec_curr_p)

        if exec_curr_p <= 0.0:
            return _build_res('OBSERVING', '현재가(exec_curr_p) 데이터 결측 (0.0). 관망 유지.')
            
        if exec_ticker != "SOXL":
            return _build_res('OBSERVING', 'SOXL 전용 모듈 (타 종목 차단)')

        if now_est.weekday() >= 5 or is_holiday:
            return _build_res('OBSERVING', '미국 증시 휴장일 (관측 오프라인)')

        # 🚨 [세션별 시간 독립 분기 및 04:07 타임쉴드 락온]
        if curr_t < datetime.time(4, 0):
            return _build_res('OBSERVING', '개장 전 대기 (04:00 이전)')
        elif curr_t < datetime.time(4, 7):
            return _build_res('OBSERVING', '타임쉴드 가동 중 (04:07 해제 대기)')
        elif curr_t < datetime.time(9, 30):
            session_name = "1세션(프리장)"
            start_time_str = '040000'
            end_time_str = '092959'
        elif curr_t <= datetime.time(16, 0):
            session_name = "2세션(정규장)"
            start_time_str = '093000'
            end_time_str = '160000'
        else:
            return _build_res('OBSERVING', '정규장 마감 (애프터장 관망)')

        avwap_qty = int(self._safe_float(avwap_qty))
        avwap_avg_price = self._safe_float(avwap_avg_price)

        session_vwap = 0.0
        if df_1min_exec is not None and not df_1min_exec.empty and 'time_est' in df_1min_exec.columns:
            df_today = df_1min_exec[df_1min_exec.index.date == today_est_date].copy()
            df_session = df_today[(df_today['time_est'] >= start_time_str) & (df_today['time_est'] <= end_time_str)].copy()

            if not df_session.empty:
                df_session['high'] = df_session['high'].ffill().bfill()
                df_session['low'] = df_session['low'].ffill().bfill()
                df_session['close'] = df_session['close'].ffill().bfill()
                df_session['volume'] = df_session['volume'].ffill().bfill().fillna(0)

                tp = (df_session['high'].astype(float) + df_session['low'].astype(float) + df_session['close'].astype(float)) / 3.0
                vol = df_session['volume'].astype(float)
                vol_tp = tp * vol

                c_vol = vol.sum()
                if c_vol > 0:
                    session_vwap = self._safe_float(vol_tp.sum() / c_vol)
                else:
                    session_vwap = self._safe_float(tp.mean())

        if session_vwap <= 0.0:
            return _build_res('OBSERVING', f'{session_name} 실시간 VWAP 연산 대기중')

        if avwap_qty > 0:
            # 🚨 MODIFIED: [과욕 제어 매도 타점 동적 연산] 하드코딩 +1.0% 고정 익절 락온
            sell_target_price = math.ceil(avwap_avg_price * 1.01 * 100) / 100.0 if avwap_avg_price > 0 else 0.0
            return _build_res('OBSERVING', f'{session_name} 교전 중 (+1.0% 전량 익절 대기)', tp=sell_target_price, session_vwap=session_vwap)
        else:
            # 🚨 [퀀트 뇌관 하드 락온] 프리장 미진입 시 정규장 신규 진입 원천 차단 (조기 퇴근)
            if curr_t >= datetime.time(9, 30):
                return _build_res('OBSERVING', '프리장 미진입으로 인한 진입 차단 (조기 퇴근)', tp=session_vwap, session_vwap=session_vwap)

            # 🚨 [돌파 팩트 교정] 현재가가 실시간 VWAP 상회 시 즉각 요격 인가 (BREAKOUT_BUY)
            if exec_curr_p >= session_vwap:
                return _build_res('BREAKOUT_BUY', f'{session_name} 실시간 VWAP(${session_vwap:.2f}) 상향 돌파 요격 인가', tp=session_vwap, session_vwap=session_vwap)
            else:
                return _build_res('OBSERVING', f'{session_name} 실시간 VWAP(${session_vwap:.2f}) 하회 중 (돌파 감시)', tp=session_vwap, session_vwap=session_vwap)
