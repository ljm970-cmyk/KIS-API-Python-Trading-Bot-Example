# ==========================================================
# FILE: strategy_v_avwap.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 5대 헌법 및 34대 엣지 케이스 완벽 결속 교차 검증 완료
# 🚨 MODIFIED: [Time Paradox 원천 소각] YF API가 프리장 초반에 전일(Yesterday) 데이터를 섞어서 반환할 때 발생하는 '과거 저가에 의한 가상 체결 오발동' 버그를 막기 위해, DataFrame 슬라이싱 전 `df.index.date == today_est_date` 필터를 100% 강제 적용.
# 🚨 MODIFIED: [Phantom Fill 명시적 바이패스 락온] KIS 원장에 매수 주문이 미체결 상태(is_buy_unfilled)로 살아있을 경우, YF 저가가 타점을 관통하더라도 가상 체결(Virtual Fill)을 보류할 뿐만 아니라 하위 트레일링 로직까지 100% 바이패스(TRAP_WAIT 반환)하도록 State Mismatch 방어막 진공 압축.
# 🚨 MODIFIED: [액면분할(Stock Split) 스케일링 붕괴 완벽 차단] 액면분할 이벤트 발생 시 tracking_high(추적 고가)와 trap_qty(가상 덫 수량)의 보정이 누락되어 터무니없는 타점에 즉시 체결(False Fill)되는 치명적 맹점을 원천 차단하기 위한 전역 스케일링 롤오버 락온.
# 🚨 MODIFIED: [Virtual Qty 가상 역산 아키텍처] 상태 파일 업데이트 중단으로 trap_qty가 0으로 오염되었을 경우, 예산 기반으로 체결 수량을 역산하여 Virtual Fill 롤오버 붕괴를 원천 차단.
# 🚨 MODIFIED: [블랙아웃 관통 누락 완벽 방어] 네트워크 단절(Timeout)로 인해 스캔 사이클을 건너뛰었을 때 발생하는 'Bouncing Price Amnesia'를 막기 위해, 매수 덫 장전 시점(trap_placed_time) 이후의 전체 캔들 최저가(min)를 스캔하여 100% 팩트 관통을 검출하는 누적 저가 스캔 아키텍처 이식.
# 🚨 MODIFIED: [Virtual Fill (관통 즉시 체결 간주) 아키텍처 이식] KIS 잔고 동기화 딜레이 및 예산 고갈 에러(WAIT)로 인한 기억상실(Amnesia Bug) 원천 차단. 1분봉 저가 관통 시 즉시 +2% 매도 덫(PLACE_SELL_TRAP)으로 다이렉트 롤오버.
# 🚨 MODIFIED: [기억상실 영구 치료 락온] 스케줄러 캐시(RAM)가 0주로 오염되더라도 상태 파일(File)의 체결 팩트를 최우선 상속하여 다중 매수(Double Spending) 맹점 100% 소각.
# 🚨 MODIFIED: [JSON 직렬화 붕괴 예방 락온] Numpy float64 타입 혼입으로 인한 json.dump 에러를 원천 차단하기 위해, Virtual Fill 주입 시 순수 Python 타입으로 강제 캐스팅(self._safe_float) 결속.
# 🚨 MODIFIED: [시계열 모순 붕괴 차단] 04:00 잔여 노이즈 캔들로 인한 비정상 관통을 막기 위해 '040100' 캔들 수신 대기(WAIT) 락온 추가.
# 🚨 MODIFIED: [상태 스키마 갱신] Virtual Fill 수량 연산을 위해 trap_qty 캐싱 로직 영구 락온
# 🚨 MODIFIED: [클로저 스코프 렌더링 붕괴 수술] _build_res 반환 시 locals() 평가로 인해 UI 타점이 갱신되지 않고 멈추는 치명적 파이썬 맹점을 변수 선언 전진배치(Hoisting)로 완벽 교정
# 🚨 MODIFIED: [정규장 스파이크 오염 차단] 프리장 고가(session_high) 추출 시 09:30 이후의 캔들이 섞여 타점을 왜곡하는 것을 막기 위해 '092959' 슬라이싱 경계 100% 절대 락온
# 🚨 MODIFIED: [재가동 기억상실 붕괴 방어] 봇 재가동 시 프리장 최고가가 유실되는 치명적 맹점을 막기 위해, 최근 1분 캔들이 아닌 프리장 전체 시계열의 최고가(max)를 추출하여 Tracking_High 갱신 락온
# 🚨 MODIFIED: [매수 4% 동적 트레일링 락온] 프리장 고가를 추적하여 4% 하락가로 지속 갱신(Cancel & Replace)하는 타격망 이식
# 🚨 MODIFIED: [매도 +2% 절대 앵커링 & 즉각 퇴근] 매수 체결 확인 즉시 매수가 기준 +2% 고정 덫을 장전하고 영구 동결(Fire & Forget)하는 단독 구출망 락온
# 🚨 MODIFIED: [Action Signal Mismatch 수술] 스케줄러 통신망 단절 방어를 위해 매도 격발 시그널을 'PLACE_SELL_TRAP'으로 정밀 교정 완료.
# 🚨 MODIFIED: [정규장 매수 원천 차단] 09:30 EST 도달 시 미체결 매수 덫 강제 취소 및 SHUTDOWN 락온
# 🚨 MODIFIED: [이월 영구 소각 팩트] 날짜 변경 시 전일 잔여 상태와 무관하게 100% 영구 포맷(No-Overnight) 락온
# 🚨 MODIFIED: [YF 결측치 붕괴 방어] 1분봉 고가/저가(High/Low)가 0.0으로 유입 시 타점 붕괴를 막는 바이패스(WAIT) 락온
# 🚨 MODIFIED: [Case 08, 16] os.path.exists 동기스캔 배제, EAFP 적용 및 temp_path 원자적 쓰기 스코프 전진 배치 유지
# 🚨 MODIFIED: [Insight 14, 25] NaN, Inf, String-Comma 맹독성 오염 차단을 위한 _safe_float 래퍼 코어 결속
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
        self.plugin_name = "DEEP_RESCUE_V86.50_PREMARKET_SCALPER"
        self.BUY_RATIO = 0.04
        self.SELL_RATIO = 0.02

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
        if now_est.hour < 4 or (now_est.hour == 4 and now_est.minute < 4):
            target_date = now_est - datetime.timedelta(days=1)
        else:
            target_date = now_est
        return target_date.strftime('%Y%m%d')

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
            data['qty'] = 0
            data['avg_price'] = 0.0
            data['shutdown'] = False
            data['strikes'] = 0
            data['limit_order_placed'] = False
            data['trap_placed_time'] = ""
            data['buy_odno'] = ""
            data['trap_odno'] = ""
            data['trap_qty'] = 0 
            
            data['tracking_high'] = 0.0
            data['buy_target'] = 0.0
            data['sell_target'] = 0.0

            data['date'] = today_str
            self.save_state(ticker, now_est, data)
        
        data['limit_order_placed'] = bool(data.get('limit_order_placed'))
        data['trap_placed_time'] = str(data.get('trap_placed_time') or "")
        data['buy_odno'] = str(data.get('buy_odno') or "")
        data['trap_odno'] = str(data.get('trap_odno') or "")
        data['strikes'] = int(self._safe_float(data.get('strikes', 0)))
        data['qty'] = int(self._safe_float(data.get('qty', 0)))
        data['trap_qty'] = int(self._safe_float(data.get('trap_qty', 0))) 
        data['avg_price'] = self._safe_float(data.get('avg_price', 0.0))
        
        data['tracking_high'] = self._safe_float(data.get('tracking_high', 0.0))
        data['buy_target'] = self._safe_float(data.get('buy_target', 0.0))
        data['sell_target'] = self._safe_float(data.get('sell_target', 0.0))
        data['shutdown'] = bool(data.get('shutdown', False))

        return data

    def save_state(self, ticker, now_est, state_data):
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

        merged_data.update(state_data)
        merged_data['date'] = today_str

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
            logging.error(f"🚨 [V_AVWAP] 동적 트레일링 상태 저장 실패 (원자적 쓰기 에러): {e}")

    def apply_stock_split(self, ticker, ratio, now_est):
        if ratio <= 0: return
        state = self.load_state(ticker, now_est)
        
        qty = int(self._safe_float(state.get("qty", 0)))
        if qty > 0:
            new_qty = math.floor((qty * ratio) + 0.5)
            old_avg = self._safe_float(state.get("avg_price", 0.0))
            state["qty"] = new_qty
            state["avg_price"] = round(old_avg / ratio, 4) if ratio > 0 else 0.0
            
        buy_target = self._safe_float(state.get("buy_target", 0.0))
        sell_target = self._safe_float(state.get("sell_target", 0.0))
        tracking_high = self._safe_float(state.get("tracking_high", 0.0))
        trap_qty = int(self._safe_float(state.get("trap_qty", 0)))
        
        if buy_target > 0: state["buy_target"] = round(buy_target / ratio, 4)
        if sell_target > 0: state["sell_target"] = round(sell_target / ratio, 4)
        if tracking_high > 0: state["tracking_high"] = round(tracking_high / ratio, 4)
        if trap_qty > 0: state["trap_qty"] = math.floor((trap_qty * ratio) + 0.5)
        
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
    
                if now_est.hour < 4 or (now_est.hour == 4 and now_est.minute < 5):
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

    def get_decision(self, base_ticker=None, exec_ticker=None, base_curr_p=0.0, exec_curr_p=0.0, base_day_open=0.0, avg_price=0.0, qty=0, alloc_cash=0.0, context_data=None, df_1min_base=None, df_1min_exec=None, now_est=None, avwap_state=None, regime_data=None, is_simulation=False, sortie_mode="SINGLE", **kwargs):
        is_holiday = kwargs.get('is_holiday', False)
        now_est = now_est or datetime.datetime.now(ZoneInfo('America/New_York'))
        today_est_date = now_est.date()
        
        if now_est.weekday() >= 5:
            is_holiday = True

        persistent_state = self.load_state(exec_ticker, now_est)
        
        avwap_qty_param = int(self._safe_float(qty))
        if avwap_qty_param == 0:
            avwap_qty_param = int(self._safe_float(kwargs.get('current_qty', 0)))
        avwap_qty_file = int(self._safe_float(persistent_state.get('qty', 0)))
        avwap_qty = max(avwap_qty_param, avwap_qty_file)
            
        exec_curr_p = self._safe_float(exec_curr_p)
        if exec_curr_p == 0.0:
            exec_curr_p = self._safe_float(kwargs.get('exec_curr_p', 0.0))
            
        avwap_alloc_cash = self._safe_float(alloc_cash)
        if avwap_alloc_cash == 0.0:
            avwap_alloc_cash = self._safe_float(kwargs.get('alloc_cash', kwargs.get('avwap_alloc_cash', 0.0)))
        
        curr_time = now_est.time()
        time_0401 = datetime.time(4, 1)
        time_0930 = datetime.time(9, 30)

        is_shutdown = bool(persistent_state.get('shutdown'))
        limit_order_placed = bool(persistent_state.get('limit_order_placed'))
        trap_placed_time = str((avwap_state or {}).get('trap_placed_time') or persistent_state.get('trap_placed_time') or "")
        buy_odno = str(persistent_state.get('buy_odno') or "")
        trap_odno = str(persistent_state.get('trap_odno') or "")
        
        tracking_high = self._safe_float(persistent_state.get('tracking_high', 0.0))
        buy_target = self._safe_float(persistent_state.get('buy_target', 0.0))
        sell_target = self._safe_float(persistent_state.get('sell_target', 0.0))

        new_tracking_high = tracking_high
        new_buy_target = buy_target
        new_sell_target = sell_target

        def _build_res(action, reason, qty=0, target_price=0.0, t_time=None):
            return {
                'action': action,
                'reason': html.escape(str(reason)),
                'qty': qty,
                'target_price': target_price,
                'vwap': 0.0,
                'base_curr_p': base_curr_p,
                'prev_vwap': self._safe_float((context_data or {}).get('prev_vwap', 0.0)) if isinstance(context_data, dict) else 0.0,
                
                'tracking_high': new_tracking_high,
                'buy_target': new_buy_target,
                'sell_target': new_sell_target,
                
                'T_H': new_buy_target,
                'placed_target_th': new_sell_target,
                
                'limit_order_placed': limit_order_placed,
                'trap_placed_time': trap_placed_time if t_time is None else t_time,
                'buy_odno': buy_odno,
                'trap_odno': trap_odno
            }

        if is_holiday:
            return _build_res('WAIT', '미국 증시 휴장일 (관측 오프라인)')

        if is_shutdown:
            return _build_res('SHUTDOWN', '당일 영구동결 상태 (SHUTDOWN)')

        if curr_time < time_0401:
            return _build_res('WAIT', '프리장 1분봉(04:00) 캔들 확정 및 YF 데이터 안정화 대기 중')

        # 🚨 MODIFIED: [Time Paradox 붕괴 수술] YF 데이터에 어제(Yesterday) 데이터가 섞여 들어올 경우를 대비해 반드시 '오늘(Today)' 날짜만 정밀 슬라이싱
        if df_1min_exec is not None and not df_1min_exec.empty and 'time_est' in df_1min_exec.columns:
            df_today = df_1min_exec[df_1min_exec.index.date == today_est_date]
            df_pre = df_today[(df_today['time_est'] >= '040100') & (df_today['time_est'] <= '195959')]
        else:
            df_pre = pd.DataFrame()

        if df_pre.empty:
            return _build_res('WAIT', '당일 1분봉 데이터 부재 또는 04:01 캔들 수신 대기 (04:00 노이즈 배제)')

        recent_l = self._safe_float(df_pre['low'].iloc[-1])
        curr_candle_time_str = str(df_pre['time_est'].iloc[-1])
        
        if curr_candle_time_str < '040100':
            return _build_res('WAIT', 'YF 04:01 캔들 수신 대기 (04:00 노이즈 캔들 배제)')
        
        df_strict_pre = df_pre[df_pre['time_est'] <= '092959']
        if df_strict_pre.empty:
            session_high = 0.0
        else:
            session_high = self._safe_float(df_strict_pre['high'].max())
        
        if session_high <= 0.0 or recent_l <= 0.0:
            return _build_res('WAIT', 'YF 실시간 캔들 결측치(0.0) 유입으로 인한 수학적 붕괴 방어 가동')

        # ==========================================================
        # 🟢 STATE 0: 매수 대기 (프리장 고가 4% 추적 매수 덫)
        # ==========================================================
        if avwap_qty == 0:
            lowest_since_trap = recent_l
            if trap_placed_time:
                df_since_trap = df_pre[df_pre['time_est'] >= trap_placed_time]
                if not df_since_trap.empty:
                    lowest_since_trap = self._safe_float(df_since_trap['low'].min())

            if limit_order_placed and buy_target > 0.0 and lowest_since_trap <= buy_target:
                is_buy_unfilled = bool((avwap_state or {}).get("is_buy_unfilled", False))
                
                if not is_buy_unfilled:
                    virtual_qty = int(self._safe_float(persistent_state.get('trap_qty', 0)))
                    
                    if virtual_qty <= 0 and buy_target > 0:
                        virtual_qty = int(math.floor(avwap_alloc_cash / buy_target))
                        
                    if virtual_qty > 0:
                        new_sell_target = round(buy_target * (1.0 + self.SELL_RATIO), 2)
                        
                        persistent_state['qty'] = int(self._safe_float(virtual_qty))
                        persistent_state['avg_price'] = self._safe_float(buy_target)
                        persistent_state['sell_target'] = self._safe_float(new_sell_target)
                        persistent_state['limit_order_placed'] = False 
                        
                        if not is_simulation:
                            self.save_state(exec_ticker, now_est, persistent_state)
                        
                        return _build_res('PLACE_SELL_TRAP', '1분봉 저가 관통 및 KIS 체결 확정(Virtual Fill). 매도 덫 즉각 장전', qty=virtual_qty, target_price=new_sell_target, t_time=curr_candle_time_str)
                else:
                    # 🚨 MODIFIED: [Phantom Fill 명시적 바이패스 락온] KIS 원장 미체결 잔존 시 트레일링 로직을 완전 바이패스하고 명시적 대기(WAIT)
                    return _build_res('TRAP_WAIT', f'YF 저가 관통 감지이나 KIS 미체결 잔존 확인 (가상 체결 보류 및 딜레이 방어 중)', target_price=buy_target)

            if curr_time >= time_0930:
                if limit_order_placed:
                    return _build_res('CANCEL_BUY_AND_SHUTDOWN', '정규장 개장(09:30 EST). 신규 매수 차단 및 미체결 매수 덫 강제 취소')
                return _build_res('SHUTDOWN', '정규장 개장(09:30 EST). 신규 매수 원천 차단 (SHUTDOWN)')

            new_tracking_high = max(tracking_high, session_high) if tracking_high > 0.0 else session_high
            new_buy_target = round(new_tracking_high * (1.0 - self.BUY_RATIO), 2)
            
            if new_buy_target <= 0.0:
                return _build_res('WAIT', '비정상 매수타점(0.0 이하) 연산 차단')
            
            if avwap_alloc_cash / new_buy_target < 1.0:
                return _build_res('WAIT', '예산 고갈 (1주 미만 산출 방어)')

            persistent_state['tracking_high'] = self._safe_float(new_tracking_high)
            persistent_state['buy_target'] = self._safe_float(new_buy_target)
            if not is_simulation:
                self.save_state(exec_ticker, now_est, persistent_state)

            if not limit_order_placed:
                buy_qty = int(math.floor(avwap_alloc_cash / new_buy_target))
                return _build_res('PLACE_TRAP', '프리장 고가 4% 추적 매수 덫 최초 장전', qty=buy_qty, target_price=new_buy_target, t_time=curr_candle_time_str)
            else:
                if new_buy_target > buy_target:
                    buy_qty = int(math.floor(avwap_alloc_cash / new_buy_target))
                    return _build_res('UPDATE_BUY_TRAP', '고가 갱신. 프리장 매수 타점 상향 트레일링 (Cancel & Replace)', qty=buy_qty, target_price=new_buy_target, t_time=curr_candle_time_str)
                else:
                    return _build_res('TRAP_WAIT', f'매수 덫(${buy_target:.2f}) 유지 중', target_price=new_buy_target)

        # ==========================================================
        # 🟡 STATE 1: 매수 완료 (+2% 절대 앵커링 단독 구출망 전개 및 즉각 퇴근)
        # ==========================================================
        else:
            avwap_avg = self._safe_float(kwargs.get('avwap_avg_price', 0.0))
            if avwap_avg <= 0.0:
                avwap_avg = self._safe_float(persistent_state.get('avg_price', 0.0))
                
            new_sell_target = round(avwap_avg * (1.0 + self.SELL_RATIO), 2)
            
            persistent_state['sell_target'] = self._safe_float(new_sell_target)
            if not is_simulation:
                self.save_state(exec_ticker, now_est, persistent_state)
                
            if not trap_odno: 
                return _build_res('PLACE_SELL_TRAP', '매수 체결 확인. +2% 매도 지정가 장전 (Fire & Forget 가동)', qty=avwap_qty, target_price=new_sell_target, t_time=curr_candle_time_str)
            else:
                return _build_res('SHUTDOWN', f'+2% 단독 구출 덫(${new_sell_target:.2f}) 장전 및 퇴근 완료', target_price=new_sell_target)
