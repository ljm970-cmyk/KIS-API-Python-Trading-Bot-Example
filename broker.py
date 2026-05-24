# ==========================================================
# FILE: broker.py
# ==========================================================
# MODIFIED: [V28.15 장부 2배 뻥튀기(Double Counting) 원천 차단]
# 🚨 MODIFIED: [Case 03 준수] 동일 종목 유령 중복 응답 누적 합산 절대 금지 및 무시 처리 팩트 교정 완료.
# 🚨 MODIFIED: [제2헌법 준수] yfinance 네임스페이스 오염을 유발하는 중복 임포트 영구 소각 및 단일화 락온
# 🚨 NEW: [Case 32] 고성능 서버 KIS API 초당 20건 통신 제한(TPS) 완벽 방어를 위한 0.06초 캡핑 주입
# 🚨 NEW: [Case 33] yfinance 및 외부 통신 타임아웃 대응 3단 지수 백오프(Exponential Backoff) 및 재시도 엔진 전면 이식
# 🚨 MODIFIED: [Case 30 팩트 교정] 취소 주문(cancel_order) API 응답 객체 누락 맹점을 소각하고 return 배선 개통 완료.
# 🚨 MODIFIED: [Insight 14 팩트 교정] KIS 응답값 콤마(,) 맹독성 런타임 붕괴 방어용 _safe_float 래핑 전면 결속.
# 🚨 MODIFIED: [Case 16 위반 교정] 원자적 쓰기 실패 시 임시 파일 고아화 및 OS 용량 고갈을 막기 위한 스코프 전진 배치(Hoisting).
# 🚨 MODIFIED: [치명적 논리 결함 수술] 딕셔너리 언패킹 오버라이드로 인한 체결량 누락 맹점 완벽 교정.
# 🚨 MODIFIED: [엣지 케이스 수술] yf 중복 인덱스 유입 시 pd.Series 반환으로 인한 JSON 직렬화 붕괴 방어.
# 🚨 MODIFIED: [데드코드 소각] 정적 분석 결과 호출되지 않는 유령 함수 7종 영구 소각 완료.
# ==========================================================

import requests
import json
import time
import datetime
import os
import math
import yfinance as yf
from zoneinfo import ZoneInfo
import tempfile
import shutil  
import pandas as pd   
import numpy as np
import volatility_engine as ve
import logging  

def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """ 🚨 [수술 완료] 야후 파이낸스 API 업데이트로 인한 MultiIndex 순서 붕괴 방어 """
    if isinstance(df.columns, pd.MultiIndex):
        if 'Ticker' in df.columns.names:
            df.columns = df.columns.droplevel('Ticker')
        elif df.columns.nlevels == 2:
            price_fields = {'Close', 'High', 'Low', 'Open', 'Volume', 'Adj Close'}
            level0_vals = set(df.columns.get_level_values(0))
            drop_level = 0 if not level0_vals.intersection(price_fields) else 1
            df.columns = df.columns.droplevel(drop_level)
    return df

class KoreaInvestmentBroker:
  
    def __init__(self, app_key, app_secret, cano, acnt_prdt_cd="01"):
        self.app_key = app_key
        self.app_secret = app_secret
        self.cano = cano
        self.acnt_prdt_cd = acnt_prdt_cd
        self.base_url = "https://openapi.koreainvestment.com:9443"
        self.token_file = f"data/token_{cano}.dat" 
        self.token = None
        self._excg_cd_cache = {} 
        self._get_access_token()

    def _get_access_token(self, force=False):
        kst = ZoneInfo('Asia/Seoul')
        
        if not force and os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r') as f:
                    saved = json.load(f)
               
                expire_time = datetime.datetime.strptime(saved['expire'], '%Y-%m-%d %H:%M:%S')
                now_kst_naive = datetime.datetime.now(kst).replace(tzinfo=None)
        
                if expire_time > now_kst_naive + datetime.timedelta(hours=1):
                    self.token = saved['token']
                    return
            except Exception: pass

        if force and os.path.exists(self.token_file):
            try: os.remove(self.token_file)
            except Exception: pass

        url = f"{self.base_url}/oauth2/tokenP"
        body = {"grant_type": "client_credentials", "appkey": self.app_key, "appsecret": self.app_secret}
        
        # 🚨 MODIFIED: [Case 33] 3단 지수 백오프 토큰 발급 이식
        for attempt in range(3):
            try:
                # 🚨 NEW: [Case 32] KIS TPS 캡핑
                time.sleep(0.06)
                res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body), timeout=10)
                data = res.json()
     
                if 'access_token' in data:
                    self.token = data['access_token']
                    expire_str = (datetime.datetime.now(kst).replace(tzinfo=None) + datetime.timedelta(seconds=int(data['expires_in']))).strftime('%Y-%m-%d %H:%M:%S')
                
                    dir_name = os.path.dirname(self.token_file)
                   
                    if dir_name and not os.path.exists(dir_name):
                        os.makedirs(dir_name, exist_ok=True)
       
                    # 🚨 MODIFIED: [Case 16 & 제4헌법 준수] 원자적 쓰기(Atomic Write) 강제 및 POSIX 표준 os.replace 주입
                    fd = None
                    temp_path = None
                    try:
                        fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
                        with os.fdopen(fd, 'w', encoding='utf-8') as f:
                            fd = None
                            json.dump({'token': self.token, 'expire': expire_str}, f)
                            f.flush()
                            os.fsync(f.fileno())
                        os.replace(temp_path, self.token_file)
                        temp_path = None
                    except Exception as inner_e:
                        if fd is not None:
                            try: os.close(fd)
                            except OSError: pass
                        if temp_path and os.path.exists(temp_path):
                            try: os.remove(temp_path)
                            except OSError: pass
                        raise inner_e
                    break # 성공 시 루프 탈출
                else:
                    if attempt == 2:
                        print(f"❌ [Broker] 토큰 발급 실패: {data.get('error_description', '알 수 알 없는 오류')}")
                    time.sleep(1.0 * (2 ** attempt))
            except Exception as e:
                if attempt == 2:
                    print(f"❌ [Broker] 토큰 통신 에러: {e}")
                time.sleep(1.0 * (2 ** attempt))

    def _get_header(self, tr_id):
        return {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self.token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
            "custtype": "P"
        }

    # 🚨 MODIFIED: [Case 32 & 33] 3단 지수 백오프 및 TPS 초 초과 방어 로직 주입
    def _api_request(self, method, url, headers, params=None, data=None):
        TOKEN_EXPIRY_KEYWORDS = frozenset([
            'expired', '인증', 'authorization', 'egt0001', 'egt0002', 'oauth', 
            '접근토큰이 만료', '토큰이 유효하지'
        ])
    
        for attempt in range(3): 
            try:
                # 🚨 NEW: [Case 32] 고성능 인스턴스 TPS 초과 방어 (KIS 서버 거절 차단)
                time.sleep(0.06)
                
                if method.upper() == "GET":
                    res = requests.get(url, headers=headers, params=params, timeout=10)
                else:
                    res = requests.post(url, headers=headers, data=json.dumps(data) if data else None, timeout=10)
                 
                if res.status_code == 429 or res.status_code >= 500:
                    raise Exception(f"HTTP Error {res.status_code}")
                    
                resp_json = res.json()
         
                if resp_json.get('rt_cd') != '0':
                    msg1_lower = resp_json.get('msg1', '').lower()
                    msg_cd = resp_json.get('msg_cd', '').lower()
                    
                    if any(x in msg1_lower or x in msg_cd for x in TOKEN_EXPIRY_KEYWORDS):
                        if attempt == 0: 
                            old_token = self.token 
                            print(f"\n🚨 [안전장치 가동] API 토큰 만료 감지! : {msg1_lower}")
                            self._get_access_token(force=True)
                            
                            if self.token == old_token or self.token is None:
                                print("🚨 [Broker] 토큰 갱신 실패. 재시도 중단.")
                                return res, resp_json
                            
                            headers["authorization"] = f"Bearer {self.token}"
                            time.sleep(1.0)
                            continue
 
                return res, resp_json
            except Exception as e:
                logging.debug(f"⚠️ [Broker] API 통신 중 예외 발생 (시도 {attempt+1}/3): {e}")
                if attempt == 2: return None, {}
                # 🚨 NEW: [Case 33] 3단 지수 백오프 적용
                time.sleep(1.0 * (2 ** attempt))
        return None, {}

    def _call_api(self, tr_id, url_path, method="GET", params=None, body=None):
        headers = self._get_header(tr_id)
        url = f"{self.base_url}{url_path}"
        res, resp_json = self._api_request(method, url, headers, params=params, data=body)
        if not resp_json: return {'rt_cd': '999', 'msg1': '통신 오류 또는 최대 재시도 횟수 초과'}
        return resp_json

    def _ceil_2(self, value):
        if value is None: return 0.0
        return max(0.01, math.ceil(round(float(value) * 100, 4)) / 100.0)

    def _safe_float(self, value):
        try: return float(str(value).replace(',', ''))
        except Exception: return 0.0

    def _get_exchange_code(self, ticker, target_api="PRICE"):
        if ticker in self._excg_cd_cache:
            codes = self._excg_cd_cache[ticker]
            return codes['PRICE'] if target_api == "PRICE" else codes['ORDER']

        price_cd = "NAS"
        order_cd = "NASD"
        dynamic_success = False

        for attempt in range(3):
            try:
                for prdt_type in ["512", "513", "529"]:
                    # 🚨 NEW: [Case 32] 동적 스캔 시에도 TPS 캡핑 강제
                    time.sleep(0.06)
                    params = {"PRDT_TYPE_CD": prdt_type, "PDNO": ticker}
                    res = self._call_api("CTPF1702R", "/uapi/overseas-price/v1/quotations/search-info", "GET", params=params)
                    
                    if res.get('rt_cd') == '0' and res.get('output'):
                        excg_name = str(res['output'].get('ovrs_excg_cd', '')).upper()
                
                        if "NASD" in excg_name or "NASDAQ" in excg_name:
                            price_cd, order_cd = "NAS", "NASD"
                            dynamic_success = True
                            break
                        elif "NYSE" in excg_name or "NEW YORK" in excg_name:
                            price_cd, order_cd = "NYS", "NYSE"
                            dynamic_success = True
                            break
                        elif "AMEX" in excg_name:
                            price_cd, order_cd = "AMS", "AMEX"
                            dynamic_success = True
                            break
                if dynamic_success: break
            except Exception as e:
                if attempt == 2:
                    print(f"⚠️ [Broker] 거래소 동적 획득 실패: {ticker} - {e}")
                time.sleep(1.0 * (2 ** attempt))

        if not dynamic_success:
            if ticker == "SOXL": price_cd, order_cd = "AMS", "AMEX"
            elif ticker == "TQQQ": price_cd, order_cd = "NAS", "NASD"

        self._excg_cd_cache[ticker] = {'PRICE': price_cd, 'ORDER': order_cd}
        return price_cd if target_api == "PRICE" else order_cd

    def get_account_balance(self):
        """ 🚨 [Case 03 준수] API 잔고 응답 중복 합산 절대 방어 락온 """
        cash = 0.0
        holdings = {}
        api_success = False 
  
        params = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "WCRC_FRCR_DVSN_CD": "02", "NATN_CD": "840", "TR_MKET_CD": "00", "INQR_DVSN_CD": "00"}
        res = self._call_api("CTRP6504R", "/uapi/overseas-stock/v1/trading/inquire-present-balance", "GET", params=params)
   
        if res.get('rt_cd') == '0':
            api_success = True
            o2 = res.get('output2', {})
            if isinstance(o2, list): o2 = o2[0] if len(o2) > 0 else {}
            
            dncl_amt = self._safe_float(o2.get('frcr_dncl_amt_2', 0))     
            sll_amt = self._safe_float(o2.get('frcr_sll_amt_smtl', 0))      
            buy_amt = self._safe_float(o2.get('frcr_buy_amt_smtl', 0))      
            raw_bp = dncl_amt + sll_amt - buy_amt
            cash = max(0.0, math.floor((raw_bp * 0.9945) * 100) / 100.0)

        target_excgs = ["NASD", "AMEX", "NYSE"] 
  
        for excg in target_excgs:
            fk200, nk200 = "", ""
            for attempt in range(20): 
                # 🚨 MODIFIED: [Case 32] 다중 스캔 루프 내 Rate Limit 캡핑
                time.sleep(0.06)
                
                params_hold = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg, "TR_CRCY_CD": "USD", "CTX_AREA_FK200": fk200, "CTX_AREA_NK200": nk200}
          
                headers = self._get_header("TTTS3012R")
                url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-balance"
                res_hold, resp_json = self._api_request("GET", url, headers, params=params_hold)
    
                if res_hold and resp_json.get('rt_cd') == '0':
                    api_success = True
  
                    if cash <= 0:
                        o2 = resp_json.get('output2', {})
                        if isinstance(o2, list): o2 = o2[0] if len(o2) > 0 else {}
                        new_cash = self._safe_float(o2.get('ovrs_ord_psbl_amt', 0))
                        if new_cash > cash: cash = new_cash
                   
                    for item in (resp_json.get('output1') or []):
                        ticker = item.get('ovrs_pdno')
                        if not ticker: continue
 
                        qty = int(self._safe_float(item.get('ovrs_cblc_qty', 0)))
                        ord_psbl_qty = int(self._safe_float(item.get('ord_psbl_qty', 0)))
                        avg = self._safe_float(item.get('pchs_avg_pric', 0))
                
                        if qty > 0 and ord_psbl_qty == 0: ord_psbl_qty = qty
                   
                        if qty > 0:
                            if ticker not in holdings: 
                                holdings[ticker] = {'qty': qty, 'ord_psbl_qty': ord_psbl_qty, 'avg': avg}
                            else:
                                # 🚨 MODIFIED: [Case 03] 유령 중복 합산 누적 무시 (영구 소각)
                                continue 

                    tr_cont = res_hold.headers.get('tr_cont', '') if hasattr(res_hold, 'headers') else ''
                    fk200 = (resp_json.get('ctx_area_fk200', '') or '').strip()
                    nk200 = (resp_json.get('ctx_area_nk200', '') or '').strip()
                    if tr_cont in ['M', 'F'] and nk200:
                        time.sleep(0.06)
                        continue
                    else: break
                else: break
        
        if api_success: return cash, holdings
        else: return cash, None

    # 🚨 MODIFIED: [Case 33] 3단 지수 백오프 이식
    def get_daily_vwap_info(self, ticker):
        """ 기초자산의 정규장 거래 내역 기반 일자별 순수 VWAP 계산 """
        for attempt in range(3):
            try:
                time.sleep(0.06) # TPS 캡핑
                stock = yf.Ticker(ticker)
                df = stock.history(period="5d", interval="1m", prepost=False, timeout=10)
                if df.empty: 
                    if attempt == 2: return 0.0, 0.0
                    time.sleep(1.0 * (2 ** attempt))
                    continue
                df = _flatten_columns(df)
                est = ZoneInfo('America/New_York') 
                if df.index.tz is None: df.index = df.index.tz_localize('UTC').tz_convert(est)
                else: df.index = df.index.tz_convert(est)
    
                regular_market = df.between_time('09:30', '15:59').copy()
                if regular_market.empty: return 0.0, 0.0
                regular_market['Typical_Price'] = (regular_market['High'] + regular_market['Low'] + regular_market['Close']) / 3.0
                regular_market['Vol_x_Price'] = regular_market['Typical_Price'] * regular_market['Volume']
        
                regular_market['Date'] = regular_market.index.date
               
                daily_stats = regular_market.groupby('Date').agg(Total_Vol_Price=('Vol_x_Price', 'sum'), Total_Vol=('Volume', 'sum'))
                # 🚨 MODIFIED: [Case 05] 결측치 방어용 0.0 강제 형변환
                daily_stats['VWAP'] = np.where(daily_stats['Total_Vol'] > 0, daily_stats['Total_Vol_Price'] / daily_stats['Total_Vol'], np.nan)
                daily_stats = daily_stats.dropna(subset=['VWAP'])
    
                if len(daily_stats) >= 2:
                    return round(float(daily_stats['VWAP'].iloc[-2]), 4), round(float(daily_stats['VWAP'].iloc[-1]), 4)
                elif len(daily_stats) == 1:
                    return 0.0, round(float(daily_stats['VWAP'].iloc[-1]), 4)
                return 0.0, 0.0
            except Exception as e:
                if attempt == 2:
                    logging.error(f"⚠️ [Broker] 일별 VWAP 파싱 실패 ({ticker}): {e}")
                    return 0.0, 0.0
                time.sleep(1.0 * (2 ** attempt))
        return 0.0, 0.0

    def get_current_price(self, ticker, is_market_closed=False):
        for attempt in range(3):
            try:
                time.sleep(0.06)
                stock = yf.Ticker(ticker)
                hist = stock.history(period="1d", interval="1m", prepost=True, timeout=5)
                if not hist.empty: return float(hist['Close'].iloc[-1])
                elif attempt == 2: raise ValueError("YF 실시간 데이터 응답 지연 (timeout)") 
            except Exception as e:
                logging.debug(f"⚠️ [야후] 현재가 에러 (시도 {attempt+1}/3): {e}")
                if attempt == 2: break
                time.sleep(1.0 * (2 ** attempt))

        for attempt in range(3):
            try:
                time.sleep(0.06)
                excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
                params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
                res = self._call_api("HHDFS76200200", "/uapi/overseas-price/v1/quotations/price", "GET", params=params)
               
                # 🚨 MODIFIED: [Insight 14] String-Float 맹독성 포맷팅 쉴드 래핑
                if res.get('rt_cd') == '0': return self._safe_float(res.get('output', {}).get('last', 0.0))
                break
            except Exception:
                if attempt == 2: pass
                else: time.sleep(1.0 * (2 ** attempt))
        return 0.0

    def get_ask_price(self, ticker):
        for attempt in range(3):
            try:
                time.sleep(0.06)
                excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
                params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
                res = self._call_api("HHDFS76200100", "/uapi/overseas-price/v1/quotations/inquire-asking-price", "GET", params=params)
         
                # 🚨 MODIFIED: [Insight 14] String-Float 맹독성 포맷팅 쉴드 래핑
                if res.get('rt_cd') == '0':
                    o2 = res.get('output2', [])
                    if isinstance(o2, list) and len(o2) > 0: return self._safe_float(o2[0].get('pask1', 0.0))
                    elif isinstance(o2, dict): return self._safe_float(o2.get('pask1', 0.0))
                break
            except Exception:
                if attempt == 2: pass
                else: time.sleep(1.0 * (2 ** attempt))
        return 0.0

    def get_bid_price(self, ticker):
        for attempt in range(3):
            try:
                time.sleep(0.06)
                excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
                params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
                res = self._call_api("HHDFS76200100", "/uapi/overseas-price/v1/quotations/inquire-asking-price", "GET", params=params)
                # 🚨 MODIFIED: [Insight 14] String-Float 맹독성 포맷팅 쉴드 래핑
                if res.get('rt_cd') == '0':
                    o2 = res.get('output2', [])
                    if isinstance(o2, list) and len(o2) > 0: return self._safe_float(o2[0].get('pbid1', 0.0))
                    elif isinstance(o2, dict): return self._safe_float(o2.get('pbid1', 0.0))
                break
            except Exception:
                if attempt == 2: pass
                else: time.sleep(1.0 * (2 ** attempt))
        return 0.0

    def get_previous_close(self, ticker):
        for attempt in range(3):
            try:
                time.sleep(0.06)
                stock = yf.Ticker(ticker)
                hist = stock.history(period="5d", timeout=5)
                if not hist.empty:
                    est = ZoneInfo('America/New_York')
                    now_est = datetime.datetime.now(est)
                    cutoff_date = now_est.date()
                    if now_est.time() <= datetime.time(16, 0, 30): cutoff_date -= datetime.timedelta(days=1)
                    if hist.index.tzinfo is None: hist.index = hist.index.tz_localize('UTC').tz_convert(est)
                    else: hist.index = hist.index.tz_convert(est)
                    past_hist = hist[hist.index.date <= cutoff_date]
                    if not past_hist.empty: return float(past_hist['Close'].dropna().iloc[-1])
                break
            except Exception as e:
                logging.debug(f"⚠️ [야후] 전일 종가 에러 (시도 {attempt+1}/3): {e}")
                if attempt == 2: break
                time.sleep(1.0 * (2 ** attempt))

        for attempt in range(3):
            try:
                time.sleep(0.06)
                excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
                params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
                res = self._call_api("HHDFS76200200", "/uapi/overseas-price/v1/quotations/price", "GET", params=params)
                # 🚨 MODIFIED: [Insight 14] String-Float 맹독성 포맷팅 쉴드 래핑
                if res.get('rt_cd') == '0': 
                    return self._safe_float(res.get('output', {}).get('base', 0.0))
                break
            except Exception:
                if attempt == 2: pass
                else: time.sleep(1.0 * (2 ** attempt))
        return 0.0
       
    def get_5day_ma(self, ticker):
        for attempt in range(3):
            try:
                time.sleep(0.06)
                stock = yf.Ticker(ticker)
                hist = stock.history(period="10d", timeout=5) 
                if len(hist) >= 5: return float(hist['Close'][-5:].mean())
                break
            except Exception:
                if attempt == 2: break
                time.sleep(1.0 * (2 ** attempt))
                
        for attempt in range(3):
            try:
                time.sleep(0.06)
                excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
                params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker, "GUBN": "0", "BYMD": "", "MODP": "1"}
                res = self._call_api("HHDFS76240000", "/uapi/overseas-price/v1/quotations/dailyprice", "GET", params=params)
                # 🚨 MODIFIED: [Insight 14] String-Float 맹독성 포맷팅 쉴드 래핑
                if res.get('rt_cd') == '0':
                    o2 = res.get('output2', [])
                    if isinstance(o2, list) and len(o2) >= 5:
                        closes = [self._safe_float(x['clos']) for x in o2[:5]]
                        return sum(closes) / len(closes)
                break
            except Exception:
                if attempt == 2: pass
                else: time.sleep(1.0 * (2 ** attempt))
        return 0.0

    def get_1min_candles_df(self, ticker):
        """ 🚨 [제5경고] 하이킨아시 연산을 위한 open 컬럼 강제 보존 리턴 """
        for attempt in range(3):
            try:
                time.sleep(0.06)
                stock = yf.Ticker(ticker)
                df = stock.history(period="1d", interval="1m", prepost=True, timeout=5)
                if df.empty: 
                    if attempt == 2: return None
                    time.sleep(1.0 * (2 ** attempt))
                    continue
                    
                df = _flatten_columns(df)
          
                est = ZoneInfo('America/New_York')
                if df.index.tz is None: df.index = df.index.tz_localize('UTC').tz_convert(est)
                else: df.index = df.index.tz_convert(est)
                df = df.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'})
                df['time_est'] = df.index.strftime('%H%M00')
                
                try:
                    max_high, min_low = float(df['high'].max()), float(df['low'].min())
                    time_high_idx, time_low_idx = df['high'].astype(float).idxmax(), df['low'].astype(float).idxmin()
                    
                    # 🚨 MODIFIED: [엣지 케이스 수술] yf 중복 인덱스 유입 시 pd.Series 반환으로 인한 JSON 직렬화 붕괴 방어
                    time_high_raw = df.loc[time_high_idx, 'time_est'] if not pd.isna(time_high_idx) else ""
                    time_high_str = str(time_high_raw.iloc[0] if isinstance(time_high_raw, pd.Series) else time_high_raw)
                    
                    time_low_raw = df.loc[time_low_idx, 'time_est'] if not pd.isna(time_low_idx) else ""
                    time_low_str = str(time_low_raw.iloc[0] if isinstance(time_low_raw, pd.Series) else time_low_raw)
        
                    cache_file = "data/avwap_cache.json"
                    cache_data = {}
                    if os.path.exists(cache_file):
                        try:
                            with open(cache_file, 'r', encoding='utf-8') as f: cache_data = json.load(f)
                        except Exception: pass
                    cache_data[ticker] = {'day_high': max_high, 'day_low': min_low, 'time_high': time_high_str, 'time_low': time_low_str, 'date': datetime.datetime.now(est).strftime("%Y-%m-%d")}
                    os.makedirs('data', exist_ok=True)
                    # 🚨 MODIFIED: [Case 16] 원자적 쓰기 실패 시 OS 스토리지 고갈 방어를 위한 temp_path 스코프 전진 배치 및 finally 정리
                    fd = None
                    tmp_path = None
                    try:
                        fd, tmp_path = tempfile.mkstemp(dir='data', text=True)
                        with os.fdopen(fd, 'w', encoding='utf-8') as f_out:
                            fd = None
                            json.dump(cache_data, f_out, ensure_ascii=False, indent=4)
                            f_out.flush()
                            os.fsync(f_out.fileno())
                        os.replace(tmp_path, cache_file)
                        tmp_path = None
                    except Exception as e:
                        if fd is not None:
                            try: os.close(fd)
                            except OSError: pass
                        if tmp_path and os.path.exists(tmp_path):
                            try: os.remove(tmp_path)
                            except OSError: pass
                        logging.debug(f"🚨 [{ticker}] 시계열 체력 팩트 캐싱 실패: {e}")
                except Exception as e: logging.debug(f"🚨 [{ticker}] 체력 팩트 캐싱 연산 에러: {e}")
                return df[['open', 'high', 'low', 'close', 'volume', 'time_est']]
            except Exception:
                if attempt == 2: return None
                time.sleep(1.0 * (2 ** attempt))

    def get_unfilled_orders_detail(self, ticker):
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        valid_orders = []
        fk200, nk200 = "", ""
     
        for attempt in range(10):
            time.sleep(0.06) # 🚨 [Case 32] 루프 내부 TPS 캡핑
            params = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd, "SORT_SQN": "DS", "CTX_AREA_FK200": fk200, "CTX_AREA_NK200": nk200}
            res, resp_json = self._api_request("GET", f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-nccs", self._get_header("TTTS3018R"), params=params)
            if res and resp_json.get('rt_cd') == '0':
                output = resp_json.get('output', [])
            
                if isinstance(output, dict): output = [output]
                valid_orders.extend([item for item in output if item.get('pdno') == ticker])
                tr_cont = res.headers.get('tr_cont', '') if hasattr(res, 'headers') else ''
                fk200 = (resp_json.get('ctx_area_fk200', '') or '').strip()
                nk200 = (resp_json.get('ctx_area_nk200', '') or '').strip()
                if tr_cont in ['M', 'F'] and nk200: time.sleep(0.3); continue
                else: break
            else: return False
        return valid_orders

    # 🚨 MODIFIED: [Case 18] 로컬 예약 스냅샷 폐기 및 KIS 원장 직접 연동
    def get_reservation_orders(self, ticker, start_date, end_date):
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        valid_orders = []
  
        fk200, nk200 = "", ""
        
        for attempt in range(15):
            time.sleep(0.06) # 🚨 [Case 32] 루프 내부 TPS 캡핑
            params = {
                "CANO": self.cano,
                "ACNT_PRDT_CD": self.acnt_prdt_cd,
                "INQR_STRT_DT": start_date,
                "INQR_END_DT": end_date,
                "INQR_DVSN_CD": "00",
                "OVRS_EXCG_CD": excg_cd,
                "PRDT_TYPE_CD": "",
                "CTX_AREA_FK200": fk200,
                "CTX_AREA_NK200": nk200
            }
            
            res, resp_json = self._api_request("GET", f"{self.base_url}/uapi/overseas-stock/v1/trading/order-resv-list", self._get_header("TTTT3039R"), params=params)
            
            if res and resp_json.get('rt_cd') == '0':
                output = resp_json.get('output', [])
     
                if isinstance(output, dict): output = [output]
                
                valid_orders.extend([item for item in output if item.get('pdno') == ticker])
                
                tr_cont = res.headers.get('tr_cont', '') if hasattr(res, 'headers') else ''
 
                fk200 = (resp_json.get('ctx_area_fk200', '') or '').strip()
                nk200 = (resp_json.get('ctx_area_nk200', '') or '').strip()
                
                if tr_cont in ['M', 'F'] and nk200:
                    time.sleep(0.3)
                    continue
                else:
                    break
            else:
                break
           
        return valid_orders

    def cancel_targeted_orders(self, ticker, side, target_ord_dvsn):
        sll_buy_cd = '02' if side == "BUY" else '01'
       
        orders = self.get_unfilled_orders_detail(ticker)
        if not orders: return 0
        target_orders = [o for o in orders if o.get('sll_buy_dvsn_cd') == sll_buy_cd and (o.get('ord_dvsn_cd') or o.get('ord_dvsn') or '') == target_ord_dvsn]
        for o in target_orders: 
            time.sleep(0.06)
            self.cancel_order(ticker, o.get('odno'))
            time.sleep(0.3)
        return len(target_orders)

    def send_order(self, ticker, side, qty, price, order_type="LIMIT", start_time=None, end_time=None):
        # 🚨 MODIFIED: [Insight 14] String-Float 맹독성 쉴드 래핑
        try: order_qty = int(self._safe_float(qty))
        except (TypeError, ValueError): return {'rt_cd': '999', 'msg1': f'유효하지 않은 주문 수량: {qty!r}'}
        if order_qty <= 0: return {'rt_cd': '999', 'msg1': f'수량 오류: {qty}'}

        for attempt in range(3):
            time.sleep(0.06)
            tr_id = "TTTT1002U" if side == "BUY" else "TTTT1006U"
            excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
            ord_dvsn = {"LOC": "34", "MOC": "33", "LOO": "32", "MOO": "31", "VWAP": "36"}.get(order_type, "00")
            final_price = 0 if order_type in ["MOC", "MOO"] else self._ceil_2(price)
         
            if order_type not in ["MOC", "MOO"] and final_price <= 0.0: return {'rt_cd': '999', 'msg1': f'가격 오류: {price}'}
            
            sll_type = "00" if side == "SELL" else ""
            
            body = {
                "CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd, 
                "PDNO": ticker, "ORD_QTY": str(order_qty), "OVRS_ORD_UNPR": str(final_price), 
                "ORD_SVR_DVSN_CD": "0", "ORD_DVSN": ord_dvsn, "SLL_TYPE": sll_type
            }
            
            if order_type == "VWAP":
                if start_time and end_time:
                    body["ALGO_ORD_TMD_DVSN_CD"] = "00"
                    body["START_TIME"] = start_time
                    body["END_TIME"] = end_time
                else:
                    body["ALGO_ORD_TMD_DVSN_CD"] = "02"

            res = self._call_api(tr_id, "/uapi/overseas-stock/v1/trading/order", "POST", body=body)
            if res.get('rt_cd') != '0' and attempt < 2 and any(x in res.get('msg1', '') for x in ["거래소", "시장", "exchange", "코드"]):
                if ticker in self._excg_cd_cache: del self._excg_cd_cache[ticker]
                time.sleep(1.0 * (2 ** attempt))
                continue
            return {'rt_cd': res.get('rt_cd', '999'), 'msg1': res.get('msg1', '오류'), 'odno': res.get('output', {}).get('ODNO', '') if isinstance(res.get('output'), dict) else ''}
        return {'rt_cd': '999', 'msg1': '거래소 캐시 재시도 초과'}

    def cancel_order(self, ticker, order_id):
        time.sleep(0.06)
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        body = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd, "PDNO": ticker, "ORGN_ODNO": order_id, "RVSE_CNCL_DVSN_CD": "02", "ORD_QTY": "0", "OVRS_ORD_UNPR": "0", "ORD_SVR_DVSN_CD": "0"}
        # 🚨 MODIFIED: [Case 30 팩트 교정] 취소 주문 API 응답 객체 반환 배선 강제 이식
        return self._call_api("TTTT1004U", "/uapi/overseas-stock/v1/trading/order-rvsecncl", "POST", body=body)

    def send_reservation_order(self, ticker, side, qty, price, order_type="LIMIT"):
        time.sleep(0.06)
        # 🚨 MODIFIED: [Insight 14] String-Float 맹독성 쉴드 래핑
        try: order_qty = int(self._safe_float(qty))
        except: return {'rt_cd': '999', 'msg1': '수량 오류'}
        
        tr_id = "TTTT3014U" if side == "BUY" else "TTTT3016U"
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        final_price = str(self._ceil_2(price))
        
        body = {
            "CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "PDNO": ticker,
            "OVRS_EXCG_CD": excg_cd, "FT_ORD_QTY": str(order_qty)
        }
        
        if order_type == "LOC":
            body["ORD_DVSN"] = "34" 
            body["FT_ORD_UNPR3"] = final_price
        else:
            body["ORD_DVSN"] = "00" 
            body["FT_ORD_UNPR3"] = final_price
            
        res = self._call_api(tr_id, "/uapi/overseas-stock/v1/trading/order-resv", "POST", body=body)
        rt_cd = res.get('rt_cd', '999')
        msg1 = res.get('msg1', '오류')
      
        odno = res.get('output', {}).get('ODNO', '') if isinstance(res.get('output'), dict) else ''
        return {'rt_cd': rt_cd, 'msg1': msg1, 'odno': odno}

    def cancel_reservation_order(self, order_date, order_id):
        time.sleep(0.06)
        body = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "RSVN_ORD_RCIT_DT": order_date, "OVRS_RSVN_ODNO": order_id}
        return self._call_api("TTTT3017U", "/uapi/overseas-stock/v1/trading/order-resv-ccnl", "POST", body=body)

    def get_execution_history(self, ticker, start_date, end_date):
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        odno_map = {}
       
        for attempt in range(10): 
            time.sleep(0.06)
            params = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "PDNO": ticker, "ORD_STRT_DT": start_date, "ORD_END_DT": end_date, "SLL_BUY_DVSN": "00", "CCLD_NCCS_DVSN": "00", "OVRS_EXCG_CD": excg_cd, "SORT_SQN": "DS", "CTX_AREA_FK200": "", "CTX_AREA_NK200": ""}
            res, resp_json = self._api_request("GET", f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-ccnl", self._get_header("TTTS3035R"), params=params)
 
            if res and resp_json.get('rt_cd') == '0':
                output = resp_json.get('output', [])
  
                if isinstance(output, dict): output = [output] 
                for item in output:
                    try:
                        # 🚨 MODIFIED: [Insight 14] String-Float 맹독성 쉴드 래핑
                        iq, ip = self._safe_float(item.get('ft_ccld_qty', 0)), self._safe_float(item.get('ft_ccld_unpr3', 0))
                        odno = item.get('odno', f"__nk_{id(item)}")
                        if iq > 0:
                            if odno not in odno_map: 
                                odno_map[odno] = {"item": dict(item), "total_qty": iq, "total_amt": iq * ip}
                            else: 
                                odno_map[odno]["total_qty"] += iq
                                odno_map[odno]["total_amt"] += (iq * ip)
                    except: continue
                if res.headers.get('tr_cont', '') in ['M', 'F']: time.sleep(0.3); continue
                else: break
            else: break
        # 🚨 MODIFIED: [치명적 논리 결함 수술] 딕셔너리 언패킹 오버라이드 맹점 교정 (원본 데이터 우선 배치)
        return [{**d["item"], "ft_ccld_qty": str(d["total_qty"]), "ft_ccld_unpr3": str(d["total_amt"]/d["total_qty"] if d["total_qty"]>0 else 0)} for d in odno_map.values()]

    def get_recent_stock_split(self, ticker, last_date_str):
        for attempt in range(3):
            try:
                time.sleep(0.06)
                splits = yf.Ticker(ticker).splits
                if splits is not None and not splits.empty:
                    safe_last_date = last_date_str if last_date_str else (datetime.datetime.now(ZoneInfo('America/New_York')) - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
                    for dt, ratio in splits.items():
                        sd = dt[:10] if isinstance(dt, str) else pd.Timestamp(dt).strftime('%Y-%m-%d')
                        if sd > safe_last_date: return float(ratio), sd
                break
            except Exception:
                if attempt == 2: break
                time.sleep(1.0 * (2 ** attempt))
        return 0.0, ""

    def get_dynamic_sniper_target(self, index_ticker):
        target_index = "SOXX" if index_ticker in ["SOXX", "SOXL"] else index_ticker
        try:
            class TargetFloat(float): pass
            if target_index == "SOXX":
                hv, w, td, ba = ve.get_soxl_target_drop_full()
                ret = TargetFloat(td); ret.metric_val, ret.weight, ret.base_amp, ret.metric_name, ret.metric_base = hv, w, ba, "SOXX HV", round(hv/w, 2) if w>0 else 25.0
            else:
                vxn, w, td, ba = ve.get_tqqq_target_drop_full()
                ret = TargetFloat(td); ret.metric_val, ret.weight, ret.base_amp, ret.metric_name, ret.metric_base = vxn, w, ba, "실시간 VXN", round(vxn/w, 2) if w>0 else 20.0
     
            ret.is_panic, ret.gap_pct = False, 0.0; return ret
        except:
            fb = -8.79 if target_index == "SOXX" else -4.95
            ret = TargetFloat(fb); ret.metric_val, ret.weight, ret.base_amp, ret.metric_name, ret.metric_base = 0.0, 1.0, fb, "통신오류", 25.0 if target_index == "SOXX" else 20.0
            ret.is_panic, ret.gap_pct = False, 0.0; return ret

    def get_day_high_low(self, ticker):
        for attempt in range(3):
            try:
                time.sleep(0.06)
                hist = yf.Ticker(ticker).history(period="1d", interval="1m", prepost=True, timeout=5)
                if not hist.empty:
                    hist = _flatten_columns(hist)
                    return float(hist['High'].max()), float(hist['Low'].min())
                break
            except Exception:
                if attempt == 2: break
                time.sleep(1.0 * (2 ** attempt))
                
        for attempt in range(3):
            try:
                time.sleep(0.06)
                res = self._call_api("HHDFS76200200", "/uapi/overseas-price/v1/quotations/price", "GET", params={"AUTH": "", "EXCD": self._get_exchange_code(ticker, target_api="PRICE"), "SYMB": ticker})
                # 🚨 MODIFIED: [Insight 14] String-Float 맹독성 쉴드 래핑
                if res.get('rt_cd') == '0':
                    out = res.get('output', {})
                    return self._safe_float(out.get('high', 0.0)), self._safe_float(out.get('low', 0.0))
                break
            except Exception:
                if attempt == 2: pass
                else: time.sleep(1.0 * (2 ** attempt))
        return 0.0, 0.0

    def get_amp_5d_data(self, ticker):
        for attempt in range(3):
            try:
                time.sleep(0.06)
                hist = yf.Ticker(ticker).history(period="15d", prepost=False, timeout=5)
                if hist.empty or len(hist) < 6:
                    if attempt == 2: return 0.0
                    time.sleep(1.0 * (2 ** attempt))
                    continue
                
                hist['Prev_Close'] = hist['Close'].shift(1)
                hist = hist.dropna(subset=['High', 'Low', 'Prev_Close']).copy()
                hist['Prev_Close'] = hist['Prev_Close'].replace(0, np.nan)
                
                hist['Amp'] = (hist['High'] - hist['Low']) / hist['Prev_Close']
                hist['Amp_5d'] = hist['Amp'].rolling(window=5).mean()
                
                last = hist.iloc[-1]
                if not pd.isna(last['Amp_5d']):
                    return round(float(last['Amp_5d']), 6)
                break
            except Exception as e:
                if attempt == 2:
                    logging.error(f"⚠️ [Broker] Amp 5MA 파싱 에러 ({ticker}): {e}")
                    break
                time.sleep(1.0 * (2 ** attempt))
        return 0.0
