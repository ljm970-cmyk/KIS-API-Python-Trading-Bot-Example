# ==========================================================
# FILE: strategy_v_avwap.py
# ==========================================================
# 🚨 MODIFIED: [딥-레스큐 아키텍처 V84.00 전면 리빌딩]
# 🚨 MODIFIED: [사후 하락장 절대 판별 락온] main_actual_avg(본진 평단가) > Open(시가) 일 때만 100% 자율 개방.
# 🚨 MODIFIED: [시가 앵커링 덫 산출] PM_H, 가변 오프셋 등 데드코드 전면 소각. 오직 `Open - (Open * Amp5 * 0.45)` 고정 타점(T_H) 도출.
# 🚨 NEW: [Kwargs Overwrite 붕괴 수술] Named Parameter로 전달된 amp5가 내부 kwargs 탐색에 의해 0.0으로 강제 덮어쓰기되어 T_H가 시가(Open)로 고정되던 치명적 파이썬 문법 맹점 원천 교정.
# 🚨 NEW: [Amp5 결측 붕괴 절대 방어] API 지연으로 Amp5가 0.0으로 유입될 경우, 딥-매수 타점(T_H)이 하방 버퍼 없이 시가(Open)로 직결되는 치명적 버그를 원천 차단하기 위해 조기 반환(Early Return) 쉴드 결속.
# 🚨 MODIFIED: [100% 자율 격발] 시가 확정 및 하락장 판별 즉시 T_H 타점에 100% 잉여 현금 지정가 매수(PLACE_TRAP) 즉각 반환.
# 🚨 MODIFIED: [Fire & Forget 락온] 암살자 체결 물량(avwap_qty > 0) 존재 시 더 이상 개입하지 않고 즉시 'HOLD(퇴근)' 반환. 타임스탑 시장가 덤핑 영구 소각.
# 🚨 MODIFIED: [상태 다이어트] 다중 출격(sortie_mode), PM_H, PM_L, T_L, offset, dump_jitter_sec 등 불필요한 레거시 파라미터 100% 진공 압축.
# 🚨 MODIFIED: [Case 08 절대 헌법 준수] 스냅샷 멱등성 훼손 방어를 위해 os.path.exists 레이스 컨디션 유발 코드를 100% 소각하고 EAFP(try-except)로 락온
# 🚨 MODIFIED: [Case 16 위반 교정] 원자적 쓰기(Atomic Write) 실패 시 스코프 붕괴를 막기 위한 임시 파일 식별자 전진 배치(Hoisting)
# 🚨 MODIFIED: [Insight 14] NaN/Inf 및 콤마(,) 맹독성 데이터를 0.0으로 깎아내는 `_safe_float` 코어 래퍼 100% 결속.
# 🚨 MODIFIED: [YF MultiIndex 붕괴 방어] `_flatten_columns` 전단 이식으로 pandas KeyError 즉사 버그 완벽 차단.
# 🚨 MODIFIED: [ZeroDivision 궁극 방어] T_H 타점이 극단적 변동성에 의해 0.0 이하로 산출될 경우 발생하는 연산 마비를 막기 위한 수학적 쉴드 락온.
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

class VAvwapHybridPlugin:
    def __init__(self):
        self.plugin_name = "DEEP_RESCUE_V84.00_AUTONOMOUS"

    # 🚨 [Case 05, Insight 14] NaN, Infinity 및 String-Comma 맹독성 데이터 정밀 필터링 락온
    def _safe_float(self, value):
        try:
            val = float(str(value or 0.0).replace(',', ''))
            if math.isnan(val) or math.isinf(val):
                return 0.0
            return val
        except Exception:
            return 0.0

    # 🚨 [YF MultiIndex 붕괴 방어] DataFrame 멀티인덱스 동적 평탄화 (KeyError 원천 차단)
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

        # 🚨 [Case 08] os.path.exists 소각 및 EAFP 원자적 접근 강제
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
            qty = int(self._safe_float(data.get('qty', 0))) 
            if qty > 0:
                data['shutdown'] = False
            else:
                data['qty'] = 0
                data['avg_price'] = 0.0
                data['shutdown'] = False
                data['strikes'] = 0
                
                data['limit_order_placed'] = False
                data['placed_target_th'] = 0.0
                data['trap_placed_time'] = ""
                data['buy_odno'] = ""  

            # 🚨 MODIFIED: [상태 다이어트] 레거시 파라미터 영구 소각 및 파일 최적화
            data['T_H'] = 0.0
            data['date'] = today_str
            self.save_state(ticker, now_est, data)
        
        # 🚨 [Insight 14] _safe_float 래핑 일괄 락온
        data['T_H'] = self._safe_float(data.get('T_H', 0.0))
        data['limit_order_placed'] = bool(data.get('limit_order_placed'))
        data['placed_target_th'] = self._safe_float(data.get('placed_target_th', 0.0))
        data['trap_placed_time'] = str(data.get('trap_placed_time') or "")
        data['buy_odno'] = str(data.get('buy_odno') or "") 
        data['strikes'] = int(self._safe_float(data.get('strikes', 0)))
        data['qty'] = int(self._safe_float(data.get('qty', 0)))
        data['avg_price'] = self._safe_float(data.get('avg_price', 0.0))

        return data

    def save_state(self, ticker, now_est, state_data):
        file_path = self._get_state_file(ticker, now_est)
        today_str = self._get_logical_date_str(now_est)

        merged_data = {}
        # 🚨 [Case 08] os.path.exists 소각 및 EAFP 원자적 접근 강제
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
        # 🚨 [디스크 I/O 샌드박싱] 무해한 폴더 생성 에러가 트랜잭션을 날리는 과잉 방어(Abort) 차단
        try:
            os.makedirs(dir_name, exist_ok=True)
        except OSError:
            pass

        # 🚨 [Case 16] 임시 파일 고아화 방어 스코프 전진 배치
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
            logging.error(f"🚨 [V_AVWAP] 딥-레스큐 상태 저장 실패 (원자적 쓰기 에러): {e}")

    def apply_stock_split(self, ticker, ratio, now_est):
        if ratio <= 0: return
        state = self.load_state(ticker, now_est)
        
        # 🚨 [Insight 14] _safe_float 래핑 결속
        qty = int(self._safe_float(state.get("qty", 0)))
        if qty > 0:
            new_qty = math.floor((qty * ratio) + 0.5)
            old_avg = self._safe_float(state.get("avg_price", 0.0))
            state["qty"] = new_qty
            state["avg_price"] = round(old_avg / ratio, 4) if ratio > 0 else 0.0
            
            placed_target_th = self._safe_float(state.get("placed_target_th", 0.0))
            if placed_target_th > 0:
                state["placed_target_th"] = round(placed_target_th / ratio, 2)
            
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
                    # 🚨 [YF MultiIndex 붕괴 방어] 계층 평탄화 강제 락온
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
                    "avg_vol_20": 0.0 # 레거시 호환용 더미 유지
                }
    
            except Exception as e:
                logging.debug(f"⚠️ [V_AVWAP] YF 기초자산 매크로 컨텍스트 추출 오류 (시도 {attempt+1}/3): {e}")
                if attempt == 2: return None
                time.sleep(1.0 * (2 ** attempt))

    def get_decision(self, base_ticker=None, exec_ticker=None, base_curr_p=0.0, exec_curr_p=0.0, df_1min_base=None, df_1min_exec=None, avwap_qty=0, avwap_alloc_cash=0.0, now_est=None, avwap_state=None, context_data=None, is_simulation=False, amp5=0.0, prev_close=0.0, ma_5day=0.0, **kwargs):
        is_holiday = kwargs.get('is_holiday', False)
        now_est = now_est or datetime.datetime.now(ZoneInfo('America/New_York'))
        
        if now_est.weekday() >= 5:
            is_holiday = True

        # 🚨 [Insight 11/17] None 객체 유입 시 TypeError 붕괴를 원천 봉쇄하는 _safe_float 강제 캐스팅 선행 락온
        avwap_qty = int(self._safe_float(avwap_qty))
        if avwap_qty == 0:
            avwap_qty = int(self._safe_float(kwargs.get('current_qty', 0)))
            
        exec_curr_p = self._safe_float(exec_curr_p)
        if exec_curr_p == 0.0:
            exec_curr_p = self._safe_float(kwargs.get('exec_curr_p', 0.0))
            
        avwap_alloc_cash = self._safe_float(avwap_alloc_cash)
        if avwap_alloc_cash == 0.0:
            avwap_alloc_cash = self._safe_float(kwargs.get('alloc_cash', kwargs.get('avwap_alloc_cash', 0.0)))
        
        # 🚨 MODIFIED: [Kwargs Overwrite 붕괴 수술] Named Parameter로 전달된 amp5가 내부 kwargs.get 탐색에 의해 0.0으로 덮어쓰여져 T_H가 시가(Open)로 고정되던 치명적 맹점 원천 교정.
        safe_amp5 = self._safe_float(amp5)
        if safe_amp5 == 0.0:
            safe_amp5 = self._safe_float(kwargs.get('amp5', 0.0))
        
        # 🚨 MODIFIED: [사후 하락장 게이트] 본진(V-REV)의 KIS 실제 평단가를 kwargs에서 정밀 추출
        main_actual_avg = self._safe_float(kwargs.get('main_actual_avg', 0.0))

        curr_l = 0.0
        curr_candle_time_str = "" 
        exec_day_open = 0.0
        
        curr_time = now_est.time()
        time_0930 = datetime.time(9, 30)

        persistent_state = self.load_state(exec_ticker, now_est)
        
        is_shutdown = bool(persistent_state.get('shutdown'))
        limit_order_placed = bool(persistent_state.get('limit_order_placed'))
        placed_target_th = self._safe_float(persistent_state.get('placed_target_th', 0.0))
        trap_placed_time = str((avwap_state or {}).get('trap_placed_time') or persistent_state.get('trap_placed_time') or "")
        t_h = self._safe_float(persistent_state.get('T_H', 0.0))
        
        def _build_res(action, reason, qty=0, target_price=0.0, t_time=None):
            return {
                'action': action,
                'reason': reason,
                'qty': qty,
                'target_price': target_price,
                'vwap': 0.0,
                'base_curr_p': base_curr_p,
                'prev_vwap': self._safe_float((context_data or {}).get('prev_vwap', 0.0)) if isinstance(context_data, dict) else 0.0,
                'T_H': t_h,
                'limit_order_placed': limit_order_placed,
                'placed_target_th': placed_target_th,
                'trap_placed_time': trap_placed_time if t_time is None else t_time,
                'buy_odno': str(persistent_state.get('buy_odno') or "") 
            }

        if is_holiday:
            if avwap_qty > 0:
                return _build_res('HOLD', '미국_증시_휴장일(암살자퇴근_이월대기)')
            return _build_res('WAIT', '미국_증시_휴장일(관측_중지)')

        # 🚨 MODIFIED: [Fire & Forget 락온] 체결된 물량(avwap_qty > 0)이 존재하면 무조건 퇴근 모드(HOLD) 반환.
        # 시장가 덤핑 등 추가 개입을 완벽히 차단하고 애프터마켓 종료 시점까지 스케줄러(단독 구출 덫)에 위임.
        if avwap_qty > 0:
            return _build_res('HOLD', '🎯 구출 덫 장전 완료 및 암살자 퇴근 (Fire & Forget)')

        # --- 이 아래부터는 오직 딥-매수 신규 진입(Entry)을 위한 전용 로직입니다 ---
        if is_shutdown:
            return _build_res('WAIT', '당일영구동결_상태(신규진입금지)')

        if curr_time < time_0930:
            return _build_res('WAIT', '프리장 관망 (정규장 시가 및 하락장 확정 대기)')

        # 🚨 MODIFIED: [시가 추출] 정규장 당일 시가(Open) 추출 락온
        if df_1min_exec is not None and not df_1min_exec.empty and 'time_est' in df_1min_exec.columns:
            df_reg = df_1min_exec[(df_1min_exec['time_est'] >= '093000') & (df_1min_exec['time_est'] <= '155959')]
            if not df_reg.empty:
                exec_day_open = self._safe_float(df_reg.iloc[0]['open'])
                curr_l = self._safe_float(df_reg.iloc[-1]['low'])
                curr_candle_time_str = str(df_reg.iloc[-1]['time_est'])

        if exec_day_open <= 0.0:
            return _build_res('WAIT', '정규장 시가(Open) 확정 대기중')

        # 🚨 NEW: [Amp5 결측 붕괴 절대 방어] API 지연 등으로 Amp5가 0.0이 되면 버퍼 0%로 시가(Open)에 덫이 꽂히는 치명적 파괴 현상을 원천 차단.
        if safe_amp5 <= 0.0:
            return _build_res('WAIT', 'Amp5 데이터 결측(0.0). 비정상 타점(버퍼 0%) 산출 방어 대기')

        # 🚨 MODIFIED: [시가 앵커링 고정 덫 산출] Overwrite 붕괴 방어용 safe_amp5 맵핑
        t_h = exec_day_open - (exec_day_open * safe_amp5 * 0.45)
        persistent_state['T_H'] = t_h
        if not is_simulation:
            self.save_state(exec_ticker, now_est, persistent_state)

        # 🚨 MODIFIED: [사후 하락장 게이트 검증]
        # Actual_Avg(main_actual_avg)가 당일 시가(Open)보다 낮거나 같다면 암살자 투입 차단
        if main_actual_avg <= exec_day_open:
            return _build_res('WAIT', f'사후 하락장 조건 미달 (본진 평단가 ${main_actual_avg:.2f} <= 당일 시가 ${exec_day_open:.2f})')

        # 이미 덫을 장전한 상태라면 체결 검증을 위한 분기 진행
        if limit_order_placed:
            is_time_shield_active = False
            if trap_placed_time and curr_candle_time_str:
                try:
                    t1 = datetime.datetime.strptime(trap_placed_time, '%H%M%S')
                    t2 = datetime.datetime.strptime(curr_candle_time_str, '%H%M%S')
                    diff_sec = (t2 - t1).total_seconds()
                    if diff_sec < 0: diff_sec += 86400
                    if 0 <= diff_sec <= 60:
                        is_time_shield_active = True
                except Exception:
                    pass

            if curr_l > 0 and curr_l <= placed_target_th:
                if is_time_shield_active:
                    logging.info(f"🛡️ [Case 31 시차 패러독스 방어] 장전시각({trap_placed_time}) 직후 캔들({curr_candle_time_str}) 노이즈 관통 바이패스.")
                    return _build_res('TRAP_WAIT', f'주문전송_지연방어(1분패러독스)_지정가덫({placed_target_th:.2f})_시장대기중', target_price=placed_target_th)
                return _build_res('VERIFY_TRAP_FILL', '지정가덫_하향관통_실체결검증_및_단독구출덫_투하요청', target_price=placed_target_th)
            else:
                reason_msg = f'선제지정가덫({placed_target_th:.2f})_시장대기중'
                if is_time_shield_active:
                    reason_msg = f'1분봉_시차패러독스_타임쉴드_가동중({placed_target_th:.2f})'
                return _build_res('TRAP_WAIT', reason_msg, target_price=placed_target_th)

        # 🚨 MODIFIED: [수학적 붕괴 방어] T_H 타점이 0.0 이하로 산출 시 ZeroDivision 차단을 위한 쉴드 락온
        if t_h <= 0.0:
            return _build_res('WAIT', '비정상 T_H(0.0 이하) 산출 차단 (수학적 붕괴 방어)')

        # 🚨 MODIFIED: [100% 자율 격발] 조건 달성 즉시 잉여 현금 100% 딥-매수 덫 장전 지시
        if avwap_alloc_cash / t_h < 1.0:
            return _build_res('WAIT', '예산 고갈 ZeroDivision 방어 (1주 미만 산출)')

        buy_qty = int(math.floor(avwap_alloc_cash / t_h))
        logging.info(f"🚀 [V84.00 딥-레스큐 덫 장전] 사후 하락장 팩트 확정. 시가 앵커링 고정 덫 즉시 장전! (타겟가: ${t_h:.2f}, 수량: {buy_qty}주)")
        
        return _build_res('PLACE_TRAP', '사후 하락장 판별 확정. 시가 앵커링 고정 덫 즉시 장전', qty=buy_qty, target_price=t_h, t_time=curr_candle_time_str)
