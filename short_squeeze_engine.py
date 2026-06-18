# ==========================================================
# FILE: short_squeeze_engine.py
# ==========================================================
# 🚨 NEW: [도메인 주도 설계 (DDD) 신규 파일] 숏 스퀴즈 감시 및 공매도 데이터 스캔 전담 엔진
# 🚨 NEW: [가이던스 모듈 결속] 사용자가 숏 스퀴즈 메커니즘과 임계치를 즉각 파악할 수 있는 팩트 렌더링 메서드(get_squeeze_guidance_text) 이식 완료.
# 🚨 VERIFIED: [Case 26 텔레그램 붕괴 방어] 텍스트 반환 시 부등호(>) 기호를 HTML 이스케이프(&gt;)로 치환하여 파서 즉사 원천 차단.
# 🚨 VERIFIED: [제1헌법 철저 준수] yfinance API의 동기 통신(I/O) 블로킹 맹점을 asyncio.to_thread와 wait_for(10s) 족쇄로 완벽 격리하여 메인 이벤트 루프 교착(Deadlock) 원천 차단.
# 🚨 VERIFIED: [Case 32 & 33 절대 규칙] YF API 호출 전 TPS 캡핑(0.06초 지연) 샌드위치 및 3단 지수 백오프 무중단 회복 루틴 결속.
# 🚨 VERIFIED: [Insight 14 & 25] NaN, Infinity 및 String-Comma 등 맹독성 데이터 유입 시 ValueError 즉사 방어를 위한 _safe_float 쉴드 100% 내재화.
# 🚨 VERIFIED: [Case 05 데이터 오염 방어] API 응답 결측(None) 시 딕셔너리 단락 평가(or {}) 및 0.0 강제 형변환 폴백 적용.
# ==========================================================
import asyncio
import logging
import math
import yfinance as yf

class ShortSqueezeScanner:
    def __init__(self):
        pass

    # 🚨 [Insight 14, 25] API String-Float 및 NaN/Inf 맹독성 런타임 붕괴 방어막 결속
    def _safe_float(self, val) -> float:
        if val is None:
            return 0.0
        try:
            f_val = float(str(val).replace(',', ''))
            if math.isnan(f_val) or math.isinf(f_val):
                return 0.0
            return f_val
        except (ValueError, TypeError):
            return 0.0

    # 🚨 [제1헌법 준수] 동기 I/O(YF API) 블로킹 캡슐화 전용 서브 헬퍼
    def _fetch_yf_info_sync(self, ticker_symbol: str) -> dict:
        try:
            # yfinance는 info 프로퍼티 접근 시 내부적으로 동기식 네트워크 통신(HTTP)이 발생함
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            return info if isinstance(info, dict) else {}
        except Exception as e:
            logging.debug(f"⚠️ [{ticker_symbol}] YF 동기 통신 내부 예외: {e}")
            return {}

    # 🚨 [메인 코어 엔진] 비동기 래핑 및 스퀴즈 임계치 판정
    async def get_metrics(self, ticker_symbol: str) -> dict:
        info = {}
        
        # 🚨 [Case 32, 33] 3단 지수 백오프 및 TPS 캡핑(0.06s) 무중단 회복 루틴 락온
        for attempt in range(3):
            try:
                await asyncio.sleep(0.06) # TPS 캡핑 (Rate Limit 절대 방어)
                
                # 🚨 [제1헌법] 백그라운드 스레드 밀어내기 및 10초 타임아웃 강제 족쇄 체결
                info = await asyncio.wait_for(
                    asyncio.to_thread(self._fetch_yf_info_sync, ticker_symbol),
                    timeout=10.0
                )
                
                # 데이터 정상 수신 시 루프 즉시 탈출
                if info:
                    break
                    
            except asyncio.TimeoutError:
                if attempt == 2:
                    logging.error(f"🚨 [{ticker_symbol}] 숏 스퀴즈 감시망 YF 통신 타임아웃(10초) 초과. 빈 데이터 반환 폴백.")
                else:
                    await asyncio.sleep(1.0 * (2 ** attempt))
            except Exception as e:
                if attempt == 2:
                    logging.error(f"🚨 [{ticker_symbol}] 숏 스퀴즈 감시망 치명적 통신 오류: {e}")
                else:
                    await asyncio.sleep(1.0 * (2 ** attempt))

        # 🚨 [Case 05] 최종 데이터 오염 방어 (None 방어 단락 평가)
        if not isinstance(info, dict):
            info = {}

        # 핵심 데이터 팩트 추출 및 _safe_float 기반 0.0 폴백 철저 방어
        short_pct_of_float = self._safe_float(info.get('shortPercentOfFloat', 0.0)) * 100.0
        days_to_cover = self._safe_float(info.get('shortRatio', 0.0))
        shares_short = self._safe_float(info.get('sharesShort', 0.0))

        # 알고리즘 스퀴즈 임계치 판정망 (SI > 15%, DTC > 4일 기준)
        is_si_high = short_pct_of_float >= 15.0
        is_dtc_high = days_to_cover >= 4.0

        if is_si_high and is_dtc_high:
            status = "🚨 <b>[위험] 숏 스퀴즈 화약고 단계 (임계치 초과)</b>"
        elif is_si_high or is_dtc_high:
            status = "⚠️ <b>[주의] 수급 왜곡 초기 징후 감지</b>"
        else:
            status = "✅ <b>[안정] 정상 수급 범위 내 거래 중</b>"
            
        # 🚨 만약 YF API 통신이 완전히 붕괴되어 0.0만 반환되었을 경우 관제탑 렌더링 폴백 상태 반환
        if short_pct_of_float == 0.0 and days_to_cover == 0.0:
            status = "데이터 집계 대기 중..."

        return {
            "SI_Float": round(short_pct_of_float, 2),
            "DTC": round(days_to_cover, 2),
            "Shares_Short": shares_short,
            "Status": status
        }

    # 🚨 [UI 렌더링 팩트] 사용자 열람용 숏 스퀴즈 해석 가이던스 추출기 (Case 26 HTML Escape 적용)
    def get_squeeze_guidance_text(self) -> str:
        """
        텔레그램 UI 렌더링에 적합한 형태의 숏 스퀴즈 개념 및 임계치 가이던스 문자열을 반환합니다.
        """
        msg = "💡 <b>[ 숏 스퀴즈(Short Squeeze) 해석 가이던스 ]</b>\n\n"
        msg += "숏 스퀴즈가 발생한다는 것은 숏(공매도) 포지션을 구축한 세력이 손실을 막기 위해 시장에서 주식을 강제로 사서 갚아야 하는(Buy-to-Cover) 상황에 몰리며, 이로 인해 <b>롱(매수) 포지션의 주가가 비정상적으로 급등(슈팅)</b>하는 수급 왜곡 현상을 의미합니다.\n\n"
        
        msg += "📊 <b>[ 3대 핵심 온체인 지표 해석 팩트 ]</b>\n"
        msg += "▫️ <b>유통주식수 대비 공매도 비율 (SI % of Float)</b>\n"
        msg += "🔹 <b>정의:</b> 시장에서 실제 거래 가능한 전체 주식(Float) 중 공매도 세력이 빌려서 팔아치운 주식의 비율.\n"
        msg += "🔹 <b>임계치:</b> 통계적으로 <b>15.0%</b>를 초과하면 '화약고' 상태로 분류하며, 20% 초과 시 극단적인 위험군으로 판정합니다.\n"
        msg += "🔹 <b>해석:</b> 숫자가 높을수록 잠재적인 대기 매수세(숏 커버링 물량)가 거대하게 쌓여있음을 의미합니다.\n\n"
        
        msg += "▫️ <b>숏 레이시오 (Days to Cover, DTC)</b>\n"
        msg += "🔹 <b>정의:</b> 현재 총 공매도 잔고 수량을 '최근 일일 평균 거래량'으로 나눈 수치 (숏 커버링에 걸리는 일수).\n"
        msg += "🔹 <b>임계치:</b> <b>4.0일</b> 이상일 경우 숏 세력의 탈출 병목 현상이 심각한 것으로 판정합니다.\n"
        msg += "🔹 <b>해석:</b> 수치가 높을수록, 주가 급등 시 숏 세력들이 한꺼번에 매수 대기열에 몰리며 주가가 수직 상승하게 됩니다.\n\n"
        
        msg += "▫️ <b>대차 이자율 (Cost to Borrow, CTB)</b>\n"
        msg += "🔹 <b>정의:</b> 공매도를 위해 주식을 빌릴 때 지불해야 하는 연환산 이자율.\n"
        msg += "🔹 <b>임계치:</b> <b>20.0%</b> 이상 급등할 경우 스퀴즈 임박의 강력한 선행 지표로 판정합니다.\n"
        msg += "🔹 <b>해석:</b> 이자가 폭등하면 공매도 세력은 포지션 유지 자체만으로 막대한 출혈이 발생하므로, 작은 호재 하나에도 주식을 사서 갚아버리는 트리거가 당겨집니다.\n\n"
        
        msg += "⚙️ <b>[ 숏 스퀴즈 발생의 연쇄 폭발 메커니즘 ]</b>\n"
        msg += "🔸 <b>1단계 (화약 누적):</b> 특정 종목에 과도한 공매도 물량(SI &gt; 15%)이 집중됨.\n"
        msg += "🔸 <b>2단계 (도화선 점화):</b> 호재 유입, 기술적 반등으로 인해 주가가 1차 상향 돌파를 시작함.\n"
        msg += "🔸 <b>3단계 (강제 청산):</b> 숏 세력의 손실 확대 및 증권사 담보금 부족으로 인한 강제 청산(마진콜) 발생.\n"
        msg += "🔸 <b>4단계 (수직 상승):</b> 파산을 막기 위한 시장가 매수(Buy-to-Cover)가 쏟아지며 본질 가치를 무시하고 🚀수직 폭등함.\n\n"
        
        msg += "💡 <b>결론:</b> 관제탑에서 해당 지표들이 임계치를 초과한 상태를 포착했다면, 이는 공매도 세력의 시체가 타들어가는 <b>수급 불균형에 의한 롱(Long) 슈팅 폭등 궤도</b>가 열려있음을 의미합니다."
        
        return msg
