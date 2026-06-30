# ==========================================================
# FILE: global_throttle.py
# 🚨 NEW: 1인용 로컬 봇 극한 최적화를 위한 중앙 통제소
# 🚨 1. KIS API 글로벌 토큰 버킷 (초당 18건 캡핑 강제)
# 🚨 2. JSON 파일 병렬 I/O 충돌 방어용 File Mutex (경쟁 조건 차단)
# ==========================================================
import time
import threading
from collections import defaultdict

class GlobalThrottle:
    _instance = None
    _lock = threading.Lock()
    
    # 🚨 API TPS 제어 (초당 20건 제한 -> 여유 버퍼 고려 초당 18건: 0.055초 간격)
    _api_lock = threading.Lock()
    _last_api_call = 0.0
    _min_api_interval = 0.055 
    
    # 🚨 파일 I/O 충돌 방지용 경로별 독립 Lock
    _file_locks = defaultdict(threading.Lock)

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(GlobalThrottle, cls).__new__(cls)
        return cls._instance

    @classmethod
    def wait_api_sync(cls):
        """ 
        🚨 [API 썬더링 허드 완벽 방어] 
        비동기 태스크 50개가 동시에 to_thread로 깨어나더라도, 
        이 Lock을 통해 KIS 서버에는 무조건 0.055초 간격으로 1발씩 정밀하게 발사됩니다.
        """
        with cls._api_lock:
            now = time.perf_counter()
            elapsed = now - cls._last_api_call
            if elapsed < cls._min_api_interval:
                time.sleep(cls._min_api_interval - elapsed)
            cls._last_api_call = time.perf_counter()

    @classmethod
    def get_file_lock(cls, filepath: str) -> threading.Lock:
        """ 
        🚨 [Lost Update 원천 차단] 
        파일 경로별로 독립적인 Mutex Lock을 반환하여, 
        A 스레드가 읽고 쓰는 동안 B 스레드가 개입하여 데이터가 증발하는 현상을 차단합니다.
        """
        # 경로 정규화로 동일 파일에 대한 완벽한 Lock 매핑 보장
        normalized_path = filepath.strip().lower()
        return cls._file_locks[normalized_path]
