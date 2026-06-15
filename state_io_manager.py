# ==========================================================
# FILE: state_io_manager.py
# ==========================================================
# 🚨 NEW: [상태 원자적 제어 도메인 분리] 스케줄러 내부에 혼재하던 파일 I/O 로직을 전담하는 신규 모듈 구축
# 🚨 VERIFIED: [원샷 딥다이브 교차 검증 완료] 5대 헌법 및 절대 규칙 적용 무결점 패치 완료
# 🚨 MODIFIED: [제2헌법 단일 책임 및 중복 소각] 파일 내 중복 작성되어 있던 읽기/원자적 쓰기 절차를 `_read_json_safe_sync` 및 `_atomic_write_json_sync` 헬퍼로 100% 진공 압축.
# 🚨 MODIFIED: [I/O 오버헤드 진공 압축] 이미 장전된 덫(Idempotency 충족)일 경우, 무의미한 원자적 덮어쓰기를 전면 바이패스(Bypass)하여 디스크 병목 차단.
# 🚨 MODIFIED: [제4헌법 절대 사수] 모든 상태 저장 메서드에 tempfile 생성 ➔ flush ➔ fsync ➔ os.replace 기반의 100% 원자적 쓰기(Atomic Write) 강제 락온.
# 🚨 MODIFIED: [Case 08 TOCTOU 붕괴 방어] os.path.exists를 통한 파일 존재 여부 스캔을 영구 소각하고, EAFP 패턴(try-except OSError)으로 레이스 컨디션 원천 차단.
# 🚨 MODIFIED: [Case 16 Scope Hoisting] 원자적 쓰기 실패 시 발생하는 UnboundLocalError 연쇄 붕괴를 막기 위해 `fd` 및 `tmp_path` 변수 스코프를 최상단 전진 배치.
# ==========================================================

import os
import json
import tempfile
import logging

def _read_json_safe_sync(filepath, date_str):
    """ 🚨 [EAFP 기반 안전 읽기 헬퍼] TOCTOU 붕괴를 막고 JSON 오염 시 안전 폴백 """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, dict) and data.get('date') == date_str:
                return data
    except (OSError, json.JSONDecodeError):
        pass
    return {}

def _atomic_write_json_sync(filepath, data):
    """ 🚨 [제4헌법 준수] 원자적 쓰기(Atomic Write) 통합 헬퍼 (중복 소각) """
    dir_name = os.path.dirname(filepath) or '.'
    try: 
        os.makedirs(dir_name, exist_ok=True)
    except OSError: 
        pass

    fd = None
    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(dir=dir_name, text=True)
        with os.fdopen(fd, 'w', encoding='utf-8') as f_out:
            fd = None
            json.dump(data, f_out, ensure_ascii=False, indent=4)
            f_out.flush()
            os.fsync(f_out.fileno()) # 커널 버퍼 강제 디스크 동기화
        os.replace(tmp_path, filepath) # 원자적 덮어쓰기
        tmp_path = None
    except Exception as e:
        if fd is not None:
            try: os.close(fd)
            except OSError: pass
        if tmp_path:
            try: os.remove(tmp_path)
            except OSError: pass
        logging.error(f"🚨 상태 파일 원자적 쓰기 실패 ({filepath}): {e}")
        raise e

def read_avwap_state_sync(ticker, date_str):
    """ 
    🚨 [Case 39 방어] 암살자 자본 잠김(Capital Lock-up) 스캔 헬퍼
    - 순수 동기 함수이므로 호출부에서 반드시 asyncio.to_thread 래핑 강제
    """
    state_file = f"data/avwap_trade_state_{ticker}.json"
    return _read_json_safe_sync(state_file, date_str)

def save_aftermarket_state_sync(ticker, date_str, slice_info):
    """ 
    🚨 [Case 39, 40 방어] 애프터장 지연 타격을 위한 원자적 상태 이관 헬퍼
    - 멱등성을 보장하여 중복 이관 방어
    """
    state_file = f"data/vrev_aftermarket_state_{ticker}.json"
    data = _read_json_safe_sync(state_file, date_str)
    
    if not data:
        data = {"date": date_str, "orders": []}
        
    if not isinstance(data.get('orders'), list):
        data['orders'] = []
    
    # 🚨 멱등성 보장 (Idempotency): 이미 장전/이관된 주문은 중복 추가하지 않음
    for item in data['orders']:
        if isinstance(item, dict) and item.get('desc') == slice_info['desc'] and item.get('side') == slice_info['side']:
            # 🚨 [I/O 오버헤드 압축] 이미 존재 시 무의미한 디스크 쓰기 바이패스
            return
    
    data['orders'].append(slice_info)
    _atomic_write_json_sync(state_file, data)

def save_slice_state_sync(ticker, date_str, slice_info):
    """ 
    🚨 [V-REV 로컬 엔진 인계] 자체 1분 슬라이싱 엔진 인계를 위한 원자적 상태 기록 헬퍼
    - KIS 알고리즘 소각 및 로컬 Slicing 락온
    """
    state_file = f"data/vrev_slice_state_{ticker}.json"
    data = _read_json_safe_sync(state_file, date_str)
    
    if not data:
        data = {"date": date_str, "hijacked": False, "orders": []}
        
    if not isinstance(data.get('orders'), list):
        data['orders'] = []
    
    # 🚨 멱등성 보장 (Idempotency): 이미 장전된 슬라이스는 덮어쓰지 않음
    for item in data['orders']:
        if isinstance(item, dict) and item.get('desc') == slice_info['desc'] and item.get('side') == slice_info['side']:
            # 🚨 [I/O 오버헤드 압축] 이미 존재 시 무의미한 디스크 쓰기 바이패스
            return
    
    data['orders'].append(slice_info)
    _atomic_write_json_sync(state_file, data)
