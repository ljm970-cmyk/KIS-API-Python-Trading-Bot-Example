# ==========================================================
# FILE: strategy_v_avwap.py
# ==========================================================
# 🚨 MODIFIED: [V59.00 AVWAP 암살자 예산 100% 수혈 및 15:25 전량 덤핑 팩트 교정]
# 🚨 MODIFIED: [V60.00 옴니 매트릭스 진입 차단망 전면 폐기 및 데드코드 소각]
# 🚨 MODIFIED: [V61.00 숏(SOXS) 전면 소각 작전 지시서 적용]
# 🚨 NEW: [V65.00 AVWAP 동적 하드스탑 락온]
# 🚨 NEW: [V66.00 AVWAP 암살자 덤핑 지터(Jitter) 분산 락온]
# 🚨 NEW: [V75.04 상태 캐시 기억상실(Amnesia) 완벽 수술]
# 🚨 NEW: [V7.4 Assassin Lock-on] 낡은 Apex/V-Turn 전면 소각 및 V7.4 암살자 엔진 탑재
# - 프리마켓 1분봉 스캔 기반 PM_H, PM_L 및 ATR5 오프셋 타겟(T_H, T_L) 연산 락온.
# - 정규장 실시간 1분봉 스캔, 일반 하락 셧다운 및 갭상승 휩소 방어(5분봉 HA 양봉 검증) 이식.
# - 95% 가용 자금 투입 및 +2.0% 익절 / 15:20 EST 전량 덤핑 투트랙 청산 파이프라인 개통.
# 🚨 MODIFIED: [V76.01 ATR5 동적 하드스탑 영구 소각 및 투트랙 엑시트 절대 락온]
# 🚨 MODIFIED: [V76.02 타점 역전 패러독스 하드 마진 락온 (매니저 제안 수혈)]
# - 프리마켓 진폭이 극도로 좁아 T_H < T_L 역전이 발생할 경우, 
#   T_L을 T_H보다 무조건 $0.01 낮게 강제 캡핑(Clamping)하여 수학적 모순 원천 차단.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import math
import random
import yfinance as yf
import pandas as pd
import json
import os
import tempfile

class VAvwapHybridPlugin:
    def __init__(self):
        # NEW: [V7.4 플러그인 닉네임 교체]
        self.plugin_name = "AVWAP_V7.4_ASSASSIN"
        self.leverage = 3.0       

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

        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                if data.get('date') != today_str:
                    qty = data.get('qty', 0)
                    if qty > 0:
                        data['bought'] = True
                        data['shutdown'] = False
                    else:
                        data['qty'] = 0
                        data['avg_price'] = 0.0
                        data['shutdown'] = False
                        data['strikes'] = 0
                        data['bought'] = False
                        data['daily_bought_qty'] = 0
                        data['daily_sold_qty'] = 0

                    # NEW: [V7.4 상태 변수 초기화] 낡은 잔재 소각 및 V7.4 팩트 인젝션
                    data['PM_H'] = 0.0
                    data['PM_L'] = 0.0
                    data['T_H'] = 0.0
                    data['T_L'] = 0.0
                    data['offset'] = 0.0
                    data['whipsaw_mode'] = False
                    data['whipsaw_armed'] = False
                    data['whipsaw_checked'] = False
                    data['dump_jitter_sec'] = random.randint(0, 180)

                    data['date'] = today_str
                    self.save_state(ticker, now_est, data)
                
                # 안전 형변환 보장
                data['PM_H'] = float(data.get('PM_H', 0.0))
                data['PM_L'] = float(data.get('PM_L', 0.0))
                data['T_H'] = float(data.get('T_H', 0.0))
                data['T_L'] = float(data.get('T_L', 0.0))
                data['offset'] = float(data.get('offset', 0.0))
                data['whipsaw_mode'] = bool(data.get('whipsaw_mode', False))
                data['whipsaw_armed'] = bool(data.get('whipsaw_armed', False))
                data['whipsaw_checked'] = bool(data.get('whipsaw_checked', False))

                return data
            except Exception:
                pass

        # NEW: [V7.4 초기 상태값 구성]
        return {
            "executed_buy": False, "shutdown": False, "strikes": 0, "qty": 0, 
            "avg_price": 0.0, "daily_bought_qty": 0, "daily_sold_qty": 0, 
            "dump_jitter_sec": random.randint(0, 180),
            "PM_H": 0.0, "PM_L": 0.0, "T_H": 0.0, "T_L": 0.0, "offset": 0.0,
            "whipsaw_mode": False, "whipsaw_armed": False, "whipsaw_checked": False
        }

    def save_state(self, ticker, now_est, state_data):
        file_path = self._get_state_file(ticker, now_est)
        today_str = self._get_logical_date_str(now_est)

        merged_data = {}
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    merged_data = json.load(f)
            except Exception:
                pass

        if merged_data.get('date') != today_str:
            merged_data = {}

        merged_data.update(state_data)
        merged_data['date'] = today_str

        try:
            dir_name = os.path.dirname(file_path)
            if dir_name and not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)

            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(merged_data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, file_path)
        except Exception as e:
            logging.error(f"🚨 [V_AVWAP] 상태 저장 실패 (원자적 쓰기 에러): {e}")

    def fetch_macro_context(self, base_ticker):
        try:
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
                        prev_close = float(df_prev_day['Close'].iloc[-1])
                        df_prev_day['tp'] = (df_prev_day['High'].astype(float) + df_prev_day['Low'].astype(float) + df_prev_day['Close'].astype(float)) / 3.0
                        df_prev_day['vol'] = df_prev_day['Volume'].astype(float)
                        df_prev_day['vol_tp'] = df_prev_day['tp'] * df_prev_day['vol']

                        cum_vol = df_prev_day['vol'].sum()
                        if cum_vol > 0:
                            prev_vwap = df_prev_day['vol_tp'].sum() / cum_vol
                        else:
                            prev_vwap = prev_close

            df_30m = tkr.history(period="60d", interval="30m", timeout=5)
            avg_vol_20 = 0.0

            if not df_30m.empty:
                if df_30m.index.tz is None:
                    df_30m.index = df_30m.index.tz_localize('UTC').tz_convert(est)
                else:
                    df_30m.index = df_30m.index.tz_convert(est)

                first_30m = df_30m[df_30m.index.time == datetime.time(9, 30)]
                past_first_30m = first_30m[first_30m.index.date < today_est]

                if len(past_first_30m) >= 20:
                    avg_vol_20 = float(past_first_30m['Volume'].tail(20).mean())
                elif len(past_first_30m) > 0:
                    avg_vol_20 = float(past_first_30m['Volume'].mean())

            if prev_vwap == 0.0:
                prev_vwap = prev_close

            return {
                "prev_close": prev_close,
                "prev_vwap": prev_vwap,
                "avg_vol_20": avg_vol_20
            }

        except Exception as e:
            logging.error(f"🚨 [V_AVWAP] YF 기초자산 매크로 컨텍스트 추출 실패 ({base_ticker}): {e}")
            return None

    def get_decision(self, base_ticker=None, exec_ticker=None, base_curr_p=0.0, exec_curr_p=0.0, base_day_open=0.0, avwap_avg_price=0.0, avwap_qty=0, avwap_alloc_cash=0.0, context_data=None, df_1min_base=None, now_est=None, avwap_state=None, regime_data=None, is_apex_on=True, is_simulation=False, **kwargs):
        # NEW: [V7.4 스코프 상단 선언 원칙 준수] UnboundLocalError 차단을 위한 초기값 명시
        avwap_qty = avwap_qty if avwap_qty != 0 else kwargs.get('current_qty', 0)
        exec_curr_p = exec_curr_p if exec_curr_p > 0 else kwargs.get('exec_curr_p', 0.0)
        avwap_avg_price = avwap_avg_price if avwap_avg_price > 0 else kwargs.get('avwap_avg_price', kwargs.get('avg_price', 0.0))
        avwap_alloc_cash = avwap_alloc_cash if avwap_alloc_cash > 0 else kwargs.get('alloc_cash', kwargs.get('avwap_alloc_cash', 0.0))
        atr5 = float(kwargs.get('atr5', 0.0))
        prev_c = float(kwargs.get('prev_close', 0.0))
        
        now_est = now_est or datetime.datetime.now(ZoneInfo('America/New_York'))
        curr_time = now_est.time()
        
        time_0925 = datetime.time(9, 25)
        time_0930 = datetime.time(9, 30)
        time_1520 = datetime.time(15, 20)

        persistent_state = self.load_state(exec_ticker, now_est)
        is_shutdown = persistent_state.get('shutdown', False)
        
        pm_h = persistent_state.get('PM_H', 0.0)
        pm_l = persistent_state.get('PM_L', 0.0)
        t_h = persistent_state.get('T_H', 0.0)
        t_l = persistent_state.get('T_L', 0.0)
        offset = persistent_state.get('offset', 0.0)
        whipsaw_mode = persistent_state.get('whipsaw_mode', False)
        whipsaw_armed = persistent_state.get('whipsaw_armed', False)
        whipsaw_checked = persistent_state.get('whipsaw_checked', False)

        def _build_res(action, reason, qty=0, target_price=0.0):
            return {
                'action': action,
                'reason': reason,
                'qty': qty,
                'target_price': target_price,
                'vwap': 0.0,
                'base_curr_p': base_curr_p,
                'prev_vwap': context_data.get('prev_vwap', 0.0) if context_data else 0.0
            }

        # ---------------------------------------------------------
        # 1. 매도 (보유 중일 때) 로직 - V7.4 투트랙 자동 청산
        # ---------------------------------------------------------
        if avwap_qty > 0:
            safe_avg = avwap_avg_price if avwap_avg_price > 0 else exec_curr_p

            if safe_avg <= 0:
                return _build_res('SELL', 'CORRUPT_PRICE_EMERGENCY_DUMP', qty=avwap_qty, target_price=exec_curr_p)

            # MODIFIED: [V76.01 ATR5 동적 하드스탑 영구 소각 (투트랙 엑시트 원칙 사수)]

            # 🚨 [V7.4 룰 7] 체결되지 않고 15:20 EST 도달 시 미체결 지정가 매도 취소 후 즉시 전량 시장가 덤핑
            if curr_time >= time_1520:
                persistent_state["shutdown"] = True
                if not is_simulation:
                    self.save_state(exec_ticker, now_est, persistent_state)
                return _build_res('SELL', '15:20_도달_전량_시장가_덤핑', qty=avwap_qty, target_price=exec_curr_p)

            # 🚨 [V7.4 룰 7] 매수 체결 즉시, 평단가 대비 +2.0% 지정가 매도(Limit Order) 전송
            # scheduler_sniper가 SELL action과 target_price를 받으면 지정가를 즉시 꽂아버립니다.
            exit_target_price = round(safe_avg * 1.02, 2)
            if exec_curr_p >= exit_target_price:
                return _build_res('SELL', '목표가(+2.0%)_도달_익절_격발', qty=avwap_qty, target_price=exit_target_price)

            return _build_res('HOLD', '보유중_익절(+2.0%)_및_15:20덤핑_감시중')

        # ---------------------------------------------------------
        # 2. 매수 (포지션 0주 일 때) 로직 - V7.4 암살자 스캔 및 격발
        # ---------------------------------------------------------
        if is_shutdown:
            return _build_res('WAIT', '당일영구동결_상태(신규진입금지)')

        if curr_time >= time_1520:
            persistent_state["shutdown"] = True
            if not is_simulation:
                self.save_state(exec_ticker, now_est, persistent_state)
            return _build_res('SHUTDOWN', '15:20_도달_신규진입_영구동결')

        if prev_c <= 0 or atr5 <= 0:
            return _build_res('WAIT', '진입_평가용_필수데이터_결측_대기')

        # NEW: [V7.4 룰 2] YF 1분봉 데이터 패치 헬퍼 함수
        def _get_exec_1m_data():
            try:
                df = yf.download(exec_ticker, period="1d", interval="1m", prepost=True, progress=False, timeout=5)
                if not df.empty:
                    if isinstance(df.columns, pd.MultiIndex):
                        if 'Ticker' in df.columns.names:
                            df.columns = df.columns.droplevel('Ticker')
                        elif df.columns.nlevels == 2:
                            price_fields = {'Close', 'High', 'Low', 'Open', 'Volume', 'Adj Close'}
                            level0_vals = set(df.columns.get_level_values(0))
                            drop_level = 0 if not level0_vals.intersection(price_fields) else 1
                            df.columns = df.columns.droplevel(drop_level)
                    est = ZoneInfo('America/New_York')
                    if df.index.tz is None:
                        df.index = df.index.tz_localize('UTC').tz_convert(est)
                    else:
                        df.index = df.index.tz_convert(est)
                return df
            except Exception as e:
                logging.error(f"🚨 [V_AVWAP] YF {exec_ticker} 1분봉 파싱 에러: {e}")
                return pd.DataFrame()

        # NEW: [V7.4 룰 2] 장 시작 전(Pre-Market) 타겟 연산 (09:25 EST 이후 단 1회)
        if curr_time >= time_0925 and pm_h == 0.0:
            df_1m = _get_exec_1m_data()
            if not df_1m.empty:
                df_pre = df_1m.between_time('04:00', '09:29')
                if not df_pre.empty:
                    pm_h = float(df_pre['High'].max())
                    pm_l = float(df_pre['Low'].min())
                    # ATR5는 퍼센트 단위(예: 5.0 = 5%)이므로 비율 연산을 위해 100으로 나눔
                    offset = prev_c * (atr5 / 100.0) * 0.40
                    t_h = pm_h - offset
                    t_l = pm_l + offset
                    
                    # 🚨 NEW: [V76.02 타점 역전 패러독스 하드 마진 락온 (매니저 제안)]
                    # 프리마켓 진폭이 극도로 좁아 T_H < T_L 역전이 발생할 경우, 
                    # T_L을 T_H보다 무조건 $0.01 낮게 강제 캡핑(Clamping)하여 수학적 모순 원천 차단
                    if t_l >= t_h:
                        t_l = max(0.01, t_h - 0.01)

                    persistent_state['PM_H'] = pm_h
                    persistent_state['PM_L'] = pm_l
                    persistent_state['T_H'] = t_h
                    persistent_state['T_L'] = t_l
                    persistent_state['offset'] = offset

                    if not is_simulation:
                        self.save_state(exec_ticker, now_est, persistent_state)
                    logging.info(f"🎯 [V7.4 암살자 락온] {exec_ticker} PM_H: {pm_h:.2f}, PM_L: {pm_l:.2f}, Offset: {offset:.2f} | T_H: {t_h:.2f}, T_L: {t_l:.2f}")
                else:
                    return _build_res('WAIT', '프리마켓_데이터_결측_대기중')

        if pm_h == 0.0 or t_h == 0.0:
            return _build_res('WAIT', '프리마켓_타겟_연산_대기중')

        if curr_time < time_0930:
            return _build_res('WAIT', '정규장_개장_대기중')

        # NEW: [V7.4 룰 3] 정규장 실시간 타점 감시 (09:30~15:20 EST)
        df_1m = _get_exec_1m_data()
        if df_1m.empty:
            return _build_res('WAIT', '정규장_실시간_1분봉_결측')

        df_reg = df_1m.between_time('09:30', '15:20')
        if df_reg.empty:
            return _build_res('WAIT', '정규장_캔들_형성대기')

        # NEW: [V7.4 룰 6] 엣지 케이스 (갭상승 휩소 방어 절대 규칙) 체크
        if not whipsaw_checked:
            first_open = float(df_reg['Open'].iloc[0])
            if first_open > t_h:
                whipsaw_mode = True
                persistent_state['whipsaw_mode'] = whipsaw_mode
                logging.warning(f"🚨 [V7.4 엣지 케이스 발동] 시가({first_open:.2f})가 T_H({t_h:.2f}) 상회. 갭상승 휩소 방어(Whipsaw Mode) 락온!")
            whipsaw_checked = True
            persistent_state['whipsaw_checked'] = whipsaw_checked
            if not is_simulation:
                self.save_state(exec_ticker, now_est, persistent_state)

        # NEW: [V7.4 룰 6] 엣지 케이스 진행 궤도
        if whipsaw_mode:
            if exec_curr_p < t_h and not whipsaw_armed:
                whipsaw_armed = True
                persistent_state['whipsaw_armed'] = whipsaw_armed
                if not is_simulation:
                    self.save_state(exec_ticker, now_est, persistent_state)
                logging.info(f"🎯 [V7.4 휩소 방어] 현재가({exec_curr_p:.2f})가 T_H({t_h:.2f}) 하회. 상승 돌파 대기(Armed) 락온!")

            if whipsaw_armed and exec_curr_p >= t_h:
                # 실시간 5분봉 하이킨아시 확인
                df_5m = df_reg.resample('5min', label='left', closed='left').agg({
                    'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
                }).dropna()

                if not df_5m.empty:
                    ha_close = (df_5m['Open'].astype(float) + df_5m['High'].astype(float) + df_5m['Low'].astype(float) + df_5m['Close'].astype(float)) / 4.0
                    ha_open = []
                    for i in range(len(df_5m)):
                        if i == 0:
                            ha_open.append((float(df_5m['Open'].iloc[i]) + float(df_5m['Close'].iloc[i])) / 2.0)
                        else:
                            ha_open.append((ha_open[i-1] + float(ha_close.iloc[i-1])) / 2.0)
                    
                    df_5m['HA_Open'] = pd.Series(ha_open, index=df_5m.index)
                    df_5m['HA_Close'] = ha_close
                    
                    is_bullish = float(df_5m['HA_Close'].iloc[-1]) >= float(df_5m['HA_Open'].iloc[-1])

                    if is_bullish:
                        # NEW: [V7.4 룰 1] 가용 자금의 95% 비중 투입
                        safe_budget = avwap_alloc_cash * 0.95
                        buy_qty = int(math.floor(safe_budget / exec_curr_p)) if exec_curr_p > 0 else 0
                        if buy_qty > 0:
                            return _build_res('BUY', '엣지케이스_휩소방어통과_HA양봉_상승돌파_격발', qty=buy_qty, target_price=exec_curr_p)
                    else:
                        return _build_res('WAIT', '휩소진행중_HA음봉감지_매수금지')
            
            return _build_res('WAIT', '휩소방어모드_조건달성_대기중')
            
        else:
            # NEW: [V7.4 룰 4] 일반 하락장 스킵
            if exec_curr_p <= t_l:
                persistent_state["shutdown"] = True
                if not is_simulation:
                    self.save_state(exec_ticker, now_est, persistent_state)
                logging.info(f"🛑 [V7.4 하락 락온] 현재가({exec_curr_p:.2f})가 T_L({t_l:.2f}) 하향 돌파. 당일 매매 셧다운!")
                return _build_res('SHUTDOWN', '일반하락장_T_L하향돌파_매매종료')

            # NEW: [V7.4 룰 5] 일반 상승 격발
            if exec_curr_p >= t_h:
                safe_budget = avwap_alloc_cash * 0.95
                buy_qty = int(math.floor(safe_budget / exec_curr_p)) if exec_curr_p > 0 else 0
                if buy_qty > 0:
                    logging.info(f"🚀 [V7.4 상승 격발] 현재가({exec_curr_p:.2f})가 T_H({t_h:.2f}) 상향 돌파. 매수 진입!")
                    return _build_res('BUY', '일반상승장_T_H상향돌파_격발', qty=buy_qty, target_price=exec_curr_p)

        return _build_res('WAIT', '타격선_도달_감시중')
