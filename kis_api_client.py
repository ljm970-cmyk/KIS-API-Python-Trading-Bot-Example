# ==========================================================
# FILE: kis_api_client.py
# 🚨 MODIFIED: 분산된 time.sleep(0.06) 땜질 소각 및 GlobalThrottle 중앙 제어 락온
# 🚨 MODIFIED: 비동기 썬더링 허드(Thundering Herd) 발생 시에도 TPS 18건 절대 사수
# ==========================================================
import requests
import json
import time
import datetime
import os
import math
import tempfile
import logging
from zoneinfo import ZoneInfo
from global_throttle import GlobalThrottle  # 🚨 중앙 통제소 결속

class KisApiClient:
    # ... (초기화 메서드 __init__ 및 _safe_float 등 기존 코드 동일 유지) ...

    def _get_access_token(self, force=False):
        est = ZoneInfo('America/New_York')
        
        if not force:
            try:
                # 🚨 파일 I/O Lock 적용 (토큰 파일 읽기)
                with GlobalThrottle.get_file_lock(self.token_file):
                    with open(self.token_file, 'r') as f:
                        saved = json.load(f)
                
                expire_time = datetime.datetime.strptime(saved['expire'], '%Y-%m-%d %H:%M:%S')
                now_est_naive = datetime.datetime.now(est).replace(tzinfo=None)
        
                if expire_time > now_est_naive + datetime.timedelta(hours=1):
                    self.token = saved['token']
                    return
            except OSError: pass
            except json.JSONDecodeError: pass
            except Exception: pass

        if force:
            try: os.remove(self.token_file)
            except OSError: pass

        url = f"{self.base_url}/oauth2/tokenP"
        body = {"grant_type": "client_credentials", "appkey": self.app_key, "appsecret": self.app_secret}
        
        for attempt in range(3):
            try:
                # 🚨 NEW: 무식한 sleep 소각 및 중앙 통제소 스로틀링 락온
                GlobalThrottle.wait_api_sync()
                
                res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body), timeout=10)
                data = res.json()
                
                if not isinstance(data, dict):
                    data = {}
     
                if 'access_token' in data:
                    self.token = data['access_token']
                    safe_expires_in = int(self._safe_float(data.get('expires_in', 86400)))
                    expire_str = (datetime.datetime.now(est).replace(tzinfo=None) + datetime.timedelta(seconds=safe_expires_in)).strftime('%Y-%m-%d %H:%M:%S')
                
                    dir_name = os.path.dirname(self.token_file)
                    if dir_name:
                        try: os.makedirs(dir_name, exist_ok=True)
                        except OSError: pass
       
                    # 🚨 파일 I/O Lock 적용 (토큰 파일 쓰기)
                    with GlobalThrottle.get_file_lock(self.token_file):
                        fd, temp_path = tempfile.mkstemp(dir=dir_name or '.', text=True)
                        try:
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
                            if temp_path:
                                try: os.remove(temp_path)
                                except OSError: pass
                            raise inner_e
                    break 
                else:
                    if attempt == 2:
                        logging.error(f"❌ [Broker] 토큰 발급 실패: {data.get('error_description') or '알 수 없는 오류'}")
                    time.sleep(1.0 * (2 ** attempt))
            except Exception as e:
                if attempt == 2:
                    logging.error(f"❌ [Broker] 토큰 통신 에러: {e}")
                time.sleep(1.0 * (2 ** attempt))

    def _api_request(self, method, url, headers, params=None, data=None):
        TOKEN_EXPIRY_KEYWORDS = frozenset([
            'expired', '인증', 'authorization', 'egt0001', 'egt0002', 'oauth', 
            '접근토큰이 만료', '토큰이 유효하지'
        ])
    
        for attempt in range(3): 
            try:
                # 🚨 NEW: 병목 유발하던 sleep 소각, 글로벌 스로틀(TPS 18) 락온
                GlobalThrottle.wait_api_sync()
                
                if method.upper() == "GET":
                    res = requests.get(url, headers=headers, params=params, timeout=10)
                else:
                    res = requests.post(url, headers=headers, data=json.dumps(data) if data else None, timeout=10)
              
                if res.status_code == 429 or res.status_code >= 500:
                    raise Exception(f"HTTP Error {res.status_code}")
   
                resp_json = res.json()
                if not isinstance(resp_json, dict):
                    resp_json = {}
         
                if resp_json.get('rt_cd') != '0':
                    msg1_lower = str(resp_json.get('msg1') or '').lower()
                    msg_cd = str(resp_json.get('msg_cd') or '').lower()
       
                    if any(x in msg1_lower or x in msg_cd for x in TOKEN_EXPIRY_KEYWORDS):
                        if attempt == 0: 
                            old_token = self.token 
                            logging.warning(f"🚨 [안전장치 가동] API 토큰 만료 감지! : {msg1_lower}")
                            self._get_access_token(force=True)
                            
                            if self.token == old_token or self.token is None:
                                logging.error("🚨 [Broker] 토큰 갱신 실패. 재시도 중단.")
                                return res, resp_json
                    
                            headers["authorization"] = f"Bearer {self.token}"
                            time.sleep(1.0)
                            continue
 
                return res, resp_json
            except Exception as e:
                logging.debug(f"⚠️ [Broker] API 통신 중 예외 발생 (시도 {attempt+1}/3): {e}")
                if attempt == 2: return None, {}
                time.sleep(1.0 * (2 ** attempt))
        return None, {}
    
    # ... (이하 _call_api 및 기타 메서드 유지) ...
