# ==========================================================
# FILE: telegram_avwap_console.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 3중 딥다이브 교차 검증(Syntax 붕괴, Async I/O 족쇄, Float 정밀도 사수) 통과 완료.
# 🚨 MODIFIED: [프리장 데이터 공백 패러독스 방어] 거래량 0(Zero-Volume) 유입 시 VWAP이 0.0으로 즉사하는 맹점을 차단하고, TWAP(시간가중평균단가)으로 즉각 폴백(Fallback)하는 수리적 방어망 결속.
# 🚨 MODIFIED: [렌더링 팩트 조건 확장] 거래량이 없더라도 가격 틱(High)만 존재하면 관제탑 시야가 밝혀지도록 렌더링 조건을 `if pre_vwap > 0 or pre_high > 0:`으로 전면 상향 락온.
# 🚨 MODIFIED: [제2헌법 준수] 사용되지 않는 유령 임포트(os, yfinance)를 정적 분석기의 관점에서 영구 소각하여 파일 응집도 극대화.
# 🚨 MODIFIED: [AttributeError 궁극 수술] config.py에서 영구 삭제된 암살자 수동 타겟팅(KRW/PCT) 설정 호출 데드코드를 전면 소각하여 렌더링 즉사(모든 지표 0.0 표출) 버그 완벽 차단.
# 🚨 MODIFIED: [순수 리버전 +2% 익절 팩트 락온] 과거의 복잡한 환율 및 수수료 역산 스키마를 소각하고, 코어 엔진과 100% 동일한 진입가(+2%) 기반 하드코딩 익절 타점 명시.
# 🚨 MODIFIED: [Phase 2 관제탑 인텔리전스 동적 렌더링 팩트 교정] 암살자 ON/OFF(is_avwap_hybrid) 상태에 따라 관제탑 텍스트가 동적으로 변환되도록 UI 렌더링 로직을 전면 수술.
# 🚨 MODIFIED: [UX 패러독스 원천 소각] 암살자가 OFF일 때 -3% 타점이 '진입 덫'으로 표출되던 오해를 막기 위해, OFF 시 "하방 이격(-3%) 감시선"으로 명칭을 강제 전환하는 동적 분기망 결속.
# 🚨 MODIFIED: [암살자 셧다운 팩트 브리핑] 암살자가 OFF 상태일 경우, 하단 교전망 상태에 "⚠️ [ 암살자 타격망 OFF (단순 관측 모드) ]"를 명시하여 보조 타격망이 대기 상태임을 직관적으로 렌더링.
# 🚨 MODIFIED: [UI 텍스트 맹독성 하드코딩 궁극 소각] 과거 휩소 방어(HA 컨펌), 다중 페이즈(Phase), 듀얼 섀도우 컷오프, 수수료(fee_rate) 역산 등 낡은 텍스트 및 변수 렌더링 로직을 100% 영구 삭제.
# 🚨 MODIFIED: [Case 17 순수 리버전 관제탑 롤오버] '데이 트레이딩 리버전' 통제소로 UI를 리빌딩하고 세션별 VWAP, -3% 타점, +2% 익절가, 15:59 강제 청산 팩트만 직관적으로 렌더링.
# 🚨 MODIFIED: [타 종목 오염 원천 차단] aVWAP 암살자 모듈의 SOXL 전용 가동 원칙에 따라 TQQQ 등 타 종목 유입 시 '관측망 오프라인' 처리 락온.
# 🚨 MODIFIED: [Quant Logic 교정] 1분봉 데이터를 1세션(04:00~09:29) 및 2세션(09:30~16:00)으로 완벽히 분할 슬라이싱하여 독립된 VWAP(순수 거래대금/거래량 기반) 동적 산출 이식.
# 🚨 MODIFIED: [고성능 클라우드 TPS 방어] 데이터 추출 시 순차적(Sequential) await 및 0.06초 샌드위치 지연(TPS 캡핑), 3단 지수 백오프 강제 락온.
# 🚨 MODIFIED: [Case 26 절대 헌법 준수] 텔레그램 HTML 파서 붕괴(Silent Death) 방어를 위한 html.escape 쉴드 전역 강제 주입.
# 🚨 MODIFIED: [Case 24 결측치 방어] 정규장/프리장 1분봉 데이터(df_pre, df_reg) 부재 시 ValueError를 막기 위해 단락 평가(if not empty) 강제.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import math
import asyncio
import time
import json
import pandas as pd
import pandas_market_calendars as mcal  
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
            if isinstance(active_tickers, str):
                active_tickers = [active_tickers]
            elif not isinstance(active_tickers, list):
                active_tickers = []
        except Exception as e:
            logging.error(f"🚨 Config I/O 타임아웃 (active_tickers): {e}")
            active_tickers = []
            
        # 🚨 [Case 11] 순수 리버전 데이 트레이딩은 오직 SOXL 종목만 감시
        avwap_tickers = [t for t in active_tickers if t == "SOXL"]
        
        if not avwap_tickers:
            return "⚠️ <b>[관측망 오프라인]</b>\n▫️ 감시 대상(SOXL) 종목이 없습니다.", None
        
        t = avwap_tickers[0]
        ticker_clean = html.escape(str(t)) 

        base_t = 'SOXX'
        base_t_clean = html.escape(str(base_t))
        
        msg = f"📡 <b>[ 순수 리버전 데이트레이딩 관제탑 ]</b>\n{header_status}\n\n"
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

        try:
            # 🚨 데이터 추출 및 병목 방지
            curr_p_val = await _get_with_retry(self.broker.get_current_price, t)
            curr_p = self._safe_float(curr_p_val)
            
            base_amp5_val = await _get_with_retry(self.broker.get_amp_5d_data, base_t)
            base_amp5 = self._safe_float(base_amp5_val)
            
            df_1m = await _get_with_retry(self.broker.get_1min_candles_df, t)
            
            ma_5day_val = await _get_with_retry(self.broker.get_5day_ma, t)
            ma_5day = self._safe_float(ma_5day_val)
            
            bal_res = await _get_with_retry(self.broker.get_account_balance)
            holdings = bal_res[1] if isinstance(bal_res, (list, tuple)) and len(bal_res) > 1 else {}
            safe_holdings = holdings if isinstance(holdings, dict) else {}
            kis_avg = self._safe_float(safe_holdings.get(t, {}).get('avg', 0.0))

            # 🚨 NEW: [Phase 2 암살자 ON/OFF 플래그 동기화]
            is_avwap_hybrid = bool(await _get_with_retry(getattr(self.cfg, 'get_avwap_hybrid_mode', lambda x: False), t))

        except Exception as e:
            logging.error(f"🚨 [{t}] 퀀트 관측망 데이터 추출 실패: {e}")
            curr_p, base_amp5, df_1m, ma_5day, kis_avg = 0.0, 0.0, None, 0.0, 0.0
            is_avwap_hybrid = False

        # 🚨 [암살자 1-Shot 1-Kill 실전 렌더링 팩트 파싱]
        avwap_qty, avwap_avg, target_usd, avwap_inv_usd = 0, 0.0, 0.0, 0.0
        is_assassin_active = False
        
        state_file = f"data/avwap_trade_state_{t}.json"
        try:
            def _read_state():
                # 🚨 [EAFP 패턴 적용] os.path.exists 소각 (제2헌법 os 모듈 100% 진공 압축으로 인한 Try-Except 처리)
                try:
                    with open(state_file, 'r', encoding='utf-8') as f:
                        return json.load(f)
                except Exception:
                    return {}

            state_data = await asyncio.wait_for(asyncio.to_thread(_read_state), timeout=5.0)
            
            if isinstance(state_data, dict):
                avwap_qty = int(self._safe_float(state_data.get('qty', 0)))
                
                if avwap_qty > 0:
                    is_assassin_active = True
                    avwap_avg = self._safe_float(state_data.get('avg_price', 0.0))
                    avwap_inv_usd = avwap_qty * avwap_avg
                    
                    # 🚨 MODIFIED: [순수 리버전 +2% 익절 팩트 락온] 복잡한 수수료 역산식 소각 및 진입가 * 1.02 하드코딩
                    target_usd = math.ceil(avwap_avg * 1.02 * 100) / 100.0
        except Exception:
            pass

        # ==============================================================
        # 1️⃣ 지표 1: 기초지수 평균 진폭 레버리지(x3) 환산
        # ==============================================================
        lev_amp_pct = base_amp5 * 3 * 100.0

        # ==============================================================
        # 2️⃣ 지표 2: 본진 역추세 KIS 장부 평단가 대비 현재가 등락률(%)
        # ==============================================================
        kis_gap_pct = ((curr_p - kis_avg) / kis_avg * 100.0) if kis_avg > 0 else 0.0

        # ==============================================================
        # 3️⃣ ~ 5️⃣ 세션별 VWAP, 고가/저가/진폭 스캔 (당일 1분봉 팩트)
        # ==============================================================
        pre_vwap, pre_target, pre_high, pre_low, pre_amp = 0.0, 0.0, 0.0, 0.0, 0.0
        reg_vwap, reg_target, reg_high, reg_low, reg_amp = 0.0, 0.0, 0.0, 0.0, 0.0

        if df_1m is not None and not df_1m.empty and 'time_est' in df_1m.columns:
            df_today = df_1m[df_1m.index.date == today_est_date].copy()
            
            def _calc_session_metrics(df_session):
                if df_session.empty:
                    return 0.0, 0.0, 0.0, 0.0, 0.0
                
                # 🚨 [Case 35 결측치 방어] ffill().bfill()
                df_session['high'] = df_session['high'].ffill().bfill()
                df_session['low'] = df_session['low'].ffill().bfill()
                df_session['close'] = df_session['close'].ffill().bfill()
                df_session['volume'] = df_session['volume'].ffill().bfill().fillna(0)
                
                s_high = self._safe_float(df_session['high'].max())
                s_low = self._safe_float(df_session['low'].min())
                s_amp = ((s_high - s_low) / s_low * 100.0) if s_low > 0 else 0.0
                
                df_session['tp'] = (df_session['high'].astype(float) + df_session['low'].astype(float) + df_session['close'].astype(float)) / 3.0
                df_session['vol'] = df_session['volume'].astype(float)
                df_session['vol_tp'] = df_session['tp'] * df_session['vol']
                
                c_vol = df_session['vol'].sum()
                
                # 🚨 MODIFIED: [프리장 데이터 공백 패러독스 방어] Zero-Volume일 경우 TWAP(시간가중평균) 폴백 가동
                if c_vol > 0:
                    s_vwap = self._safe_float(df_session['vol_tp'].sum() / c_vol)
                else:
                    s_vwap = self._safe_float(df_session['tp'].mean())
                
                # 🚨 [-3% 진입 타점 내림 연산]
                s_target = math.floor(s_vwap * 0.97 * 100) / 100.0 if s_vwap > 0 else 0.0
                
                return s_vwap, s_target, s_high, s_low, s_amp

            df_pre = df_today[(df_today['time_est'] >= '040000') & (df_today['time_est'] <= '092959')].copy()
            df_reg = df_today[(df_today['time_est'] >= '093000') & (df_today['time_est'] <= '160000')].copy()
            
            pre_vwap, pre_target, pre_high, pre_low, pre_amp = _calc_session_metrics(df_pre)
            reg_vwap, reg_target, reg_high, reg_low, reg_amp = _calc_session_metrics(df_reg)

        # ==============================================================
        # 🖥️ 뷰포트 렌더링
        # ==============================================================
        msg += f"🎯 <b>[ {ticker_clean} 데이 트레이딩 관측소 ]</b>\n"
        msg += f"▫️ 현재가: <b>${curr_p:.2f}</b>\n\n"
        
        msg += f"1️⃣ <b>기초지수({base_t_clean}) 환산 진폭 (5MA)</b>\n"
        msg += f"▫️ 레버리지(x3) 5일 평균 진폭: <b>{lev_amp_pct:.2f}%</b>\n\n"
        
        msg += f"2️⃣ <b>본진 V-REV 평단가 등락률</b>\n"
        if kis_avg > 0:
            sign = "+" if kis_gap_pct > 0 else ""
            msg += f"▫️ KIS 평단가: <b>${kis_avg:.2f}</b>\n"
            msg += f"▫️ 현재가 등락률: <b>{sign}{kis_gap_pct:.2f}%</b>\n\n"
        else:
            msg += f"▫️ 본진 물량 보유 없음 (관망)\n\n"

        # 🚨 NEW: [Phase 2 암살자 상태 연동 동적 텍스트 결속]
        target_label = "암살자(-3%) 진입 덫" if is_avwap_hybrid else "하방 이격(-3%) 감시선"

        msg += f"🌅 <b>[ 1세션 - 프리장 (04:00~09:29) ]</b>\n"
        # 🚨 MODIFIED: [렌더링 팩트 조건 확장] VWAP 0.0 폴백 시에도 고점(틱)이 존재하면 렌더링 유지
        if pre_vwap > 0 or pre_high > 0:
            msg += f"▫️ 고가: ${pre_high:.2f} / 저가: ${pre_low:.2f} (진폭 {pre_amp:.2f}%)\n"
            msg += f"▫️ 누적 VWAP: <b>${pre_vwap:.2f}</b>\n"
            msg += f"🔻 {target_label}: <b>${pre_target:.2f}</b>\n\n"
        else:
            msg += "▫️ 데이터 집계 대기 중...\n\n"

        msg += f"🔥 <b>[ 2세션 - 정규장 (09:30~16:00) ]</b>\n"
        # 🚨 MODIFIED: [렌더링 팩트 조건 확장] VWAP 0.0 폴백 시에도 고점(틱)이 존재하면 렌더링 유지
        if reg_vwap > 0 or reg_high > 0:
            msg += f"▫️ 고가: ${reg_high:.2f} / 저가: ${reg_low:.2f} (진폭 {reg_amp:.2f}%)\n"
            msg += f"▫️ 누적 VWAP: <b>${reg_vwap:.2f}</b>\n"
            msg += f"🔻 {target_label}: <b>${reg_target:.2f}</b>\n\n"
        else:
            msg += "▫️ 정규장 개장 대기 중...\n\n"

        msg += f"📊 <b>[ 직전 5거래일 정규장 종가 평균 ]</b>\n"
        if ma_5day > 0:
            msg += f"▫️ 5일 평균가(SMA 5): <b>${ma_5day:.2f}</b>\n\n"
        else:
            msg += "▫️ 5일 평균가(SMA 5): 대기 중...\n\n"

        # 🚨 NEW: [Phase 2 암살자 ON/OFF 토글 상태 팩트 브리핑]
        if is_avwap_hybrid or is_assassin_active:
            msg += f"⚔️ <b>[ 암살자(aVWAP) 85% 예산 교전망 (🟢 가동중) ]</b>\n"
        else:
            msg += f"⚠️ <b>[ 암살자 타격망 OFF (단순 관측 모드) ]</b>\n"

        if is_assassin_active:
            # 🚨 MODIFIED: [UI 텍스트 맹독성 하드코딩 궁극 소각] 1-Shot 1-Kill 아키텍처에 맞춘 진공 압축 렌더링 팩트 교정
            msg += f"▫️ 교전 상태: <b>ON (-3% 타점 관통 및 진입 완료)</b>\n"
            msg += f"▫️ 투입 물량: <b>{avwap_qty}주</b> (진입 단가 ${avwap_avg:.2f} | 총 ${avwap_inv_usd:,.2f})\n"
            msg += f"▫️ 전량 익절: <b>목표가 ${target_usd:.2f}</b> (+2% 지정가 락온)\n"
            msg += f"▫️ 자본 잠김 방어: <b>15:59 EST 도달 시 전량 강제 덤핑 대기 중</b>\n"
        else:
            if is_avwap_hybrid:
                msg += f"▫️ 교전 상태: <b>ON (세션 VWAP -3% 타점 관통 대기 중)</b>\n"
            else:
                msg += f"▫️ 교전 상태: <b>OFF (수동 가동 대기)</b>\n"

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
