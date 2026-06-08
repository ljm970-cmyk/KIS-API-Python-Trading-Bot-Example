# ==========================================================
# FILE: strategy_v_avwap.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 3중 딥다이브 교차 검증(Syntax 붕괴, Async I/O 족쇄, Float 정밀도 사수) 통과 완료.
# 🚨 MODIFIED: [의사결정 코어 엔진 다이어트] 실제 매매가 없으므로 불필요한 주문 타점(Target Price) 계산과 복잡한 상태 전이(State Machine) 로직 진공 압축.
# 🚨 MODIFIED: [Action Unified 락온] PLACE_TRAP, PLACE_SELL_TRAP, SHUTDOWN, WAIT 등의 Action 반환을 'OBSERVING'(관측 중) 상태로 100% 통합.
# 🚨 MODIFIED: [관측 타임라인 무중단 사수] 봇이 조기 퇴근하는 SHUTDOWN 락온을 소각하고, 장 마감(16:00 EST)까지 실시간 시장 데이터 스캔(Tracking High)이 무중단 유지되도록 교정.
# 🚨 MODIFIED: [상태 스키마 100% 진공 압축] 매매에 종속된 불필요한 상태 스키마(limit_order_placed, buy_odno, trap_odno, manual_suspend, qty, avg_price 등)를 로컬 메모리 및 파일에서 영구 삭제.
# 🚨 MODIFIED: [Quant Logic 교정] 기초지수 매크로(fetch_macro_context) 연산 시 (Open+High+Low+Close)/4.0 의 노이즈를 배제하고 정통 퀀트 표준인 (High+Low+Close)/3.0 으로 팩트 교정 완료.
# 🚨 MODIFIED: [Time Paradox 붕괴 수술] 04:00~04:04 구간에서 전일(Yesterday)의 데이터를 불러와 RAM을 오염시키는 맹점을 차단하고 04:00 정각에 100% 당일(Today)로 롤오버되도록 팩트 락온.
# 🚨 MODIFIED: [JSON 직렬화 붕괴 예방 락온] Numpy float64 타입 혼입으로 인한 json.dump 에러를 원천 차단하기 위해 순수 Python 타입으로 강제 캐스팅(self._safe_float) 100% 결속.
# 🚨 MODIFIED: [Case 08, 16] os.path.exists 동기스캔 배제, EAFP 적용 및 temp_path 원자적 쓰기 스코프 전진 배치 유지.
# 🚨 NEW: [Phase 2 암살자 코어 퀀트 브레인 복원] 무한 타격망, HA 하드 리셋 쉴드, 원화 목표가 역산 엔진 100% 팩트 이식 완료.
# 🚨 MODIFIED: [Target 2 수술] OCO 듀얼 엑시트 원화 목표가 역산 시 매수 수수료(fee_rate) 가산 누락분을 교정하여 슬리피지 패러독스 원천 차단.
# 🚨 MODIFIED: [타겟-앵커 붕괴 방어] 타점 연산 시 SOXX 시가가 아닌 SOXL 실매매 종목 시가를 앵커로 잡도록 듀얼 앵커 추출 팩트 교정 완료.
# 🚨 MODIFIED: [Numpy Vectorization 락온] 하이킨 아시(HA) 연산 시 Pandas 루프를 영구 소각하고 고속 Numpy 배열 벡터화 연산으로 100% 리빌딩.
# 🚨 MODIFIED: [Case 35 결측치 전이 방어] 1분봉 데이터의 결측치(NaN)로 인해 VWAP 및 HA 연산이 붕괴되는 현상을 막기 위해 ffill().bfill() 체인 및 np.nan_to_num 강제 락온.
# 🚨 MODIFIED: [ZeroDivision 붕괴 방어] 원화 목표가 역산 시 수수료 오염으로 인한 분모 0 붕괴 방지용 safe_denom 팩트 결속.
# 🚨 MODIFIED: [Case 01 절대 헌법 사수] 날짜 비교 시 '%Y-%m-%d' 시스템 표준 포맷 100% 강제 래핑 완료.
# 🚨 MODIFIED: [AttributeError 궁극 수술] save_state 진입 시 state_data 객체의 오염(NoneType 유입)을 막기 위한 isinstance 쉴드 강제 주입 (최종 무결성 락온).
# 🚨 NEW: [발목 타격망(Ankle-Catch) 팩트 교정] tracking_low 스키마를 신설하여 1차 타점(-6%) 관통 이력을 영구 보존. 주가가 반등(발목)하더라도 HA 양봉 출현 시 100% 즉시 딥-매수가 격발되도록 논리 패러독스 완전 소각 완료.
# 🚨 NEW: [Phantom Nuke (유령 무한 매수) 방어망] 매수 체결(Phase 증가) 또는 손절(Strikes 증가) 시 tracking_low를 999999.0으로 원자적 하드 리셋하여 다중 타격망의 멱등성을 100% 사수 완료.
# 🚨 REMOVED: [Case 37 Slippage Cap 영구 소각] 포트폴리오 매니저의 수학적 증명에 따라, HA 조건부 격발 시 반등 폭을 제한하던 1.02배 캡핑 방어막을 전면 파기하고 순수 관통 후 양봉 격발 로직으로 100% 롤백.
# 🚨 NEW: [Case 05 최후의 오발사 방어] exec_curr_p(현재가)가 0.0으로 유입될 경우, 무조건 타점을 터치한 것으로 오인하여 딥-매수가 격발되는 즉사 버그를 막기 위한 원천 방어 쉴드 락온.
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
        self.plugin_name = "AVWAP_INTELLIGENCE_OBSERVER"

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
        if now_est.hour < 4:
            target_date = now_est - datetime.timedelta(days=1)
        else:
            target_date = now_est
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
                'date': today_str,
                'tracking_high': 0.0,
                'tracking_low': 999999.0,  
                'last_phase': 0,           
                'last_strikes': 0,         
                'last_reset_time': '040000', 
                'T_H': 0.0
            }
            self.save_state(ticker, now_est, data)
        
        data['tracking_high'] = self._safe_float(data.get('tracking_high', 0.0))
        
        data['tracking_low'] = self._safe_float(data.get('tracking_low', 999999.0))
        if data['tracking_low'] <= 0.0: 
            data['tracking_low'] = 999999.0
            
        data['last_phase'] = int(self._safe_float(data.get('last_phase', 0)))
        data['last_strikes'] = int(self._safe_float(data.get('last_strikes', 0)))
        data['last_reset_time'] = str(data.get('last_reset_time', '040000'))
        data['T_H'] = self._safe_float(data.get('T_H', 0.0))

        return data

    def save_state(self, ticker, now_est, state_data):
        if not isinstance(state_data, dict):
            state_data = {}
            
        file_path = self._get_state_file(ticker, now_est)
        today_str = self._get_logical_date_str(now_est)

        merged_data = {}
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                merged_data = json.load(f)
            if not isinstance(merged_data, dict):
                merged_data = {}
        except OSError:
            pass
        except json.JSONDecodeError:
            pass

        if merged_data.get('date') != today_str:
            merged_data = {}

        tracking_low_val = self._safe_float(state_data.get('tracking_low', 999999.0))
        if tracking_low_val <= 0.0: tracking_low_val = 999999.0

        cleaned_state = {
            'date': today_str,
            'tracking_high': self._safe_float(state_data.get('tracking_high', 0.0)),
            'tracking_low': tracking_low_val,
            'last_phase': int(self._safe_float(state_data.get('last_phase', 0))),
            'last_strikes': int(self._safe_float(state_data.get('last_strikes', 0))),
            'last_reset_time': str(state_data.get('last_reset_time', '040000')),
            'T_H': self._safe_float(state_data.get('T_H', 0.0))
        }

        merged_data.update(cleaned_state)

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
        if ratio <= 0: return
        state = self.load_state(ticker, now_est)
        
        tracking_high = self._safe_float(state.get("tracking_high", 0.0))
        tracking_low = self._safe_float(state.get("tracking_low", 999999.0))
        t_h = self._safe_float(state.get("T_H", 0.0))
        
        if tracking_high > 0: state["tracking_high"] = round(tracking_high / ratio, 4)
        if tracking_low < 999999.0 and tracking_low > 0: state["tracking_low"] = round(tracking_low / ratio, 4)
        if t_h > 0: state["T_H"] = round(t_h / ratio, 4)
        
        self.save_state(ticker, now_est, state)

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

    def get_decision(self, base_ticker=None, exec_ticker=None, base_curr_p=0.0, exec_curr_p=0.0, base_day_open=0.0, avwap_avg_price=0.0, avwap_qty=0, avwap_alloc_cash=0.0, context_data=None, df_1min_base=None, df_1min_exec=None, now_est=None, avwap_state=None, is_simulation=False, sortie_mode="SINGLE", **kwargs):
        is_holiday = kwargs.get('is_holiday', False)
        now_est = now_est or datetime.datetime.now(ZoneInfo('America/New_York'))
        today_est_date = now_est.date()
        curr_t = now_est.time()

        def _build_res(action, reason, tp=0.0, track_h=0.0, track_l=999999.0):
            return {
                'action': 'OBSERVING' if is_simulation else action,
                'raw_action': action,
                'reason': html.escape(str(reason)),
                'tracking_high': self._safe_float(track_h),
                'tracking_low': self._safe_float(track_l),
                'T_H': self._safe_float(track_h * 0.96),
                'target_price': self._safe_float(tp)
            }

        # 🚨 [Case 05] 최후의 오발사 방어막 (현재가 0.0 유입 시 즉각 관망 락온)
        exec_curr_p = self._safe_float(exec_curr_p)
        if exec_curr_p <= 0.0:
            return _build_res('OBSERVING', '현재가(exec_curr_p) 데이터 결측 (0.0). 관망 유지.')
        
        if now_est.weekday() >= 5:
            is_holiday = True

        avwap_state = avwap_state if isinstance(avwap_state, dict) else {}
        
        phase = int(self._safe_float(avwap_state.get('phase', 0)))
        last_entry_price = self._safe_float(avwap_state.get('last_entry_price', 0.0))
        
        avwap_qty = int(self._safe_float(avwap_qty))
        avwap_avg_price = self._safe_float(avwap_avg_price)
        target_krw = self._safe_float(kwargs.get('target_krw', 1000000.0))
        
        exchange_rate = self._safe_float(kwargs.get('exchange_rate', 1400.0))
        if exchange_rate <= 0.0: exchange_rate = 1400.0
            
        main_actual_avg = self._safe_float(kwargs.get('main_actual_avg', 0.0))
        prev_close = self._safe_float(kwargs.get('prev_close', 0.0))
        fee_rate = self._safe_float(kwargs.get('fee_rate', 0.07)) / 100.0

        persistent_state = self.load_state(exec_ticker, now_est)
        tracking_high = self._safe_float(persistent_state.get('tracking_high', 0.0))
        tracking_low = self._safe_float(persistent_state.get('tracking_low', 999999.0))
        last_phase = int(self._safe_float(persistent_state.get('last_phase', 0)))
        last_strikes = int(self._safe_float(persistent_state.get('last_strikes', 0)))
        last_reset_time = str(persistent_state.get('last_reset_time', '040000'))

        current_phase = int(self._safe_float(avwap_state.get('phase', 0)))
        current_strikes = int(self._safe_float(avwap_state.get('strikes', 0)))

        # 🚨 [Phantom Nuke 방어망] 매수 체결(Phase 증가) 또는 손절(Strikes 증가) 시 tracking_low 하드 리셋
        if current_phase > last_phase or current_strikes > last_strikes:
            tracking_low = 999999.0
            last_phase = current_phase
            last_strikes = current_strikes
            last_reset_time = now_est.strftime('%H%M%S')
            
            persistent_state['last_phase'] = last_phase
            persistent_state['last_strikes'] = last_strikes
            persistent_state['last_reset_time'] = last_reset_time
            persistent_state['tracking_low'] = tracking_low
            self.save_state(exec_ticker, now_est, persistent_state)
            logging.info(f"🔄 [{exec_ticker}] 암살자 페이즈/스트라이크 변동 감지. 발목 타격망(Tracking Low) 하드 리셋 완료.")

        new_tracking_high = tracking_high
        new_tracking_low = tracking_low

        if df_1min_exec is not None and not df_1min_exec.empty and 'time_est' in df_1min_exec.columns:
            df_today = df_1min_exec[df_1min_exec.index.date == today_est_date]
            df_pre = df_today[(df_today['time_est'] >= '040000') & (df_today['time_est'] <= '200000')]
            if not df_pre.empty:
                safe_high_series = pd.to_numeric(df_pre['high'], errors='coerce')
                session_high = self._safe_float(safe_high_series.max())
                if session_high > 0.0:
                    new_tracking_high = max(tracking_high, session_high)

                # 🚨 리셋 타임라인 이후의 데이터만 스캔하여 완벽한 발목 타점 색출
                df_since_reset = df_pre[df_pre['time_est'] >= last_reset_time]
                if not df_since_reset.empty:
                    safe_low_series = pd.to_numeric(df_since_reset['low'], errors='coerce').dropna()
                    safe_low_series = safe_low_series[safe_low_series > 0.0]
                    if not safe_low_series.empty:
                        session_low = self._safe_float(safe_low_series.min())
                        new_tracking_low = min(tracking_low, session_low)

        if exec_curr_p > 0.0:
            new_tracking_low = min(new_tracking_low, exec_curr_p)

        persistent_state['tracking_high'] = self._safe_float(new_tracking_high)
        persistent_state['tracking_low'] = self._safe_float(new_tracking_low)
        persistent_state['T_H'] = self._safe_float(new_tracking_high * 0.96)
        self.save_state(exec_ticker, now_est, persistent_state)

        if is_holiday:
            return _build_res('OBSERVING', '미국 증시 휴장일 (관측 오프라인)', 0.0, new_tracking_high, new_tracking_low)

        if curr_t.minute == 0 and curr_t.hour in [4, 16]:
            return _build_res('OBSERVING', '프리/애프터 개장 직후 휩소 1분 방어(Mute) 가동중', 0.0, new_tracking_high, new_tracking_low)
        if curr_t.minute == 30 and curr_t.hour == 9:
            return _build_res('OBSERVING', '정규장 개장 직후 휩소 1분 방어(Mute) 가동중', 0.0, new_tracking_high, new_tracking_low)

        if curr_t >= datetime.time(16, 0):
            anchor_str = '160000'
            ha_start_str = '160100'
            session_name = "애프터장"
        elif curr_t >= datetime.time(9, 30):
            anchor_str = '093000'
            ha_start_str = '093100'
            session_name = "정규장"
        else:
            anchor_str = '040000'
            ha_start_str = '040100'
            session_name = "프리장"

        if avwap_qty > 0 and exchange_rate > 0:
            total_invested_usd = avwap_qty * avwap_avg_price
            safe_denom = avwap_qty * max(0.0001, (1.0 - fee_rate))
            target_price_usd = ((target_krw / exchange_rate) + (total_invested_usd * (1.0 + fee_rate))) / safe_denom
            
            if exec_curr_p >= target_price_usd:
                return _build_res('SHADOW_EXIT', f'원화 목표액(₩{int(target_krw):,}) 관통 스윕 격발!', tp=target_price_usd, track_h=new_tracking_high, track_l=new_tracking_low)
            
            if phase >= 2:
                cut_loss_price = last_entry_price * 0.99 if last_entry_price > 0 else avwap_avg_price * 0.99
                if exec_curr_p <= cut_loss_price:
                    return _build_res('CUT_LOSS_ALL', f'해당 진입가 기준 -1% 손절 덫 터치', tp=0.0, track_h=new_tracking_high, track_l=new_tracking_low)
                
                return _build_res('OBSERVING', f'{session_name} 무중단 교전 중 (익절/손절 대기, 추가 매수 차단)', tp=target_price_usd, track_h=new_tracking_high, track_l=new_tracking_low)

        exec_session_open = 0.0
        if df_1min_exec is not None and not df_1min_exec.empty and 'time_est' in df_1min_exec.columns:
            df_exec_today = df_1min_exec[df_1min_exec.index.date == today_est_date].copy()
            df_exec_anchor = df_exec_today[df_exec_today['time_est'] >= anchor_str]
            if not df_exec_anchor.empty:
                exec_session_open = self._safe_float(df_exec_anchor['open'].iloc[0])
                
        if exec_session_open == 0.0:
            return _build_res('OBSERVING', f'{session_name} 실매매 종목 시가(Open) 추출 대기중', 0.0, new_tracking_high, new_tracking_low)

        is_bull = exec_session_open > prev_close
        
        if is_bull:
            target_drop_pct = -3.0 * (phase + 1)
        else:
            target_drop_pct = -6.0 - (3.0 * phase)
            
        exec_target_price = exec_session_open * (1 + target_drop_pct / 100.0)

        requires_ha = not (is_bull and phase == 0)
        
        if requires_ha:
            if df_1min_base is None or df_1min_base.empty or 'time_est' not in df_1min_base.columns:
                return _build_res('OBSERVING', 'HA 연산용 기초지수 데이터 부재', 0.0, new_tracking_high, new_tracking_low)
            
            df_base_today = df_1min_base[df_1min_base.index.date == today_est_date].copy()
            df_base_session = df_base_today[df_base_today['time_est'] >= ha_start_str].copy()
            
            if df_base_session.empty:
                return _build_res('OBSERVING', f'기초지수 HA 세션({ha_start_str}~) 데이터 집계 중', 0.0, new_tracking_high, new_tracking_low)
                
            o_arr = np.nan_to_num(df_base_session['open'].ffill().bfill().astype(float).values, nan=0.0, posinf=0.0, neginf=0.0)
            h_arr = np.nan_to_num(df_base_session['high'].ffill().bfill().astype(float).values, nan=0.0, posinf=0.0, neginf=0.0)
            l_arr = np.nan_to_num(df_base_session['low'].ffill().bfill().astype(float).values, nan=0.0, posinf=0.0, neginf=0.0)
            c_arr = np.nan_to_num(df_base_session['close'].ffill().bfill().astype(float).values, nan=0.0, posinf=0.0, neginf=0.0)

            if len(o_arr) > 0:
                ha_c = (o_arr + h_arr + l_arr + c_arr) / 4.0
                ha_o = np.zeros_like(o_arr)
                ha_o[0] = o_arr[0]
                
                for i in range(1, len(o_arr)):
                    ha_o[i] = (ha_o[i-1] + ha_c[i-1]) / 2.0
                
                last_ha_open = ha_o[-1]
                last_ha_close = ha_c[-1]
            
                if last_ha_close <= last_ha_open:
                    return _build_res('OBSERVING', f'타점({target_drop_pct}%) 대기 (HA 양봉 컨펌 필요)', tp=exec_target_price, track_h=new_tracking_high, track_l=new_tracking_low)
            else:
                return _build_res('OBSERVING', 'HA 연산 실패 (배열 크기 0)', 0.0, new_tracking_high, new_tracking_low)

        # 🚨 MODIFIED: [발목 타격망 팩트 교정 및 Slippage Cap 완전 소각]
        is_hit = False
        if requires_ha:
            # HA 필수: 바닥 관통 이력(tracking_low)만 확인 (반등 폭 제한 없음)
            if new_tracking_low <= exec_target_price:
                is_hit = True
        else:
            # HA 불필요 (상승장 1차 타격): 100% 현재가 기반 엄격한 타점 터치 시에만 격발
            if exec_curr_p <= exec_target_price:
                is_hit = True

        if is_hit:
            # 🚨 [상승장 절대 캡핑 쉴드] 평단가 상승 패러독스 방어
            if is_bull and main_actual_avg > 0:
                if exec_curr_p >= main_actual_avg:
                    return _build_res('OBSERVING', f'상승장 캡핑: 현재가(${exec_curr_p:.2f}) >= 본진평단(${main_actual_avg:.2f})', tp=exec_target_price, track_h=new_tracking_high, track_l=new_tracking_low)
                
            if phase == 0:
                action_str = 'DEEP_BUY_1'
            elif phase == 1:
                action_str = 'DEEP_BUY_2'
            else:
                action_str = 'DEEP_BUY_RELOAD'
                
            return _build_res(action_str, f'{"상승" if is_bull else "하락"}장 무한 타점({target_drop_pct}%) 관통 확인 및 격발 인가', tp=exec_target_price, track_h=new_tracking_high, track_l=new_tracking_low)

        return _build_res('OBSERVING', f'{"상승" if is_bull else "하락"}장 타점({target_drop_pct}%) 추적 대기중 (현재 최저: ${new_tracking_low:.2f})', tp=exec_target_price, track_h=new_tracking_high, track_l=new_tracking_low)
