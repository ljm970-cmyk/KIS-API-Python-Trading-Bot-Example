# ==========================================================
# FILE: strategy.py
# ==========================================================
# [strategy.py] - 🌟 V61.00 롱 단일 모멘텀 암살자 전용 라우터 🌟
# 🚨 MODIFIED: [V54.06 SSOT 코어 통일 및 Split-Brain 영구 소각]
# 🚨 MODIFIED: [순수익 2.0% 절대 보장 타점 공식 릴레이 배선 개통]
# 🚨 MODIFIED: [Case 11] 다중 출격(Multi-Sortie) 모드 파라미터 수혈 배선 이식
# 🚨 MODIFIED: [라우팅 누수 방어] V14 스나이퍼 감시 라우터 배선 개통 완료
# 🚨 MODIFIED: [I/O 붕괴 방어] V_REV 라우팅 내 QueueLedger 동기 호출부 샌드박싱 강화
# 🚨 MODIFIED: [Insight 14] get_plan 진입점 및 capture_vrev_snapshot 내 _safe_float 절대 방어막 전면 이식
# 🚨 MODIFIED: [AttributeError 붕괴 방어] get_plan 내 ticker 파라미터 결측치(None) 유입 시 upper() 호출 즉사 버그 완벽 차단
# 🚨 MODIFIED: [Cascading Failure 전면 차단] 모든 라우팅 메서드에 safe_ticker 락온을 주입하여 하위 플러그인 오염 전파 원천 봉쇄
# 🚨 MODIFIED: [유령 종목 I/O 차단] 빈 문자열 렌더링 시 하위 플러그인 I/O 호출을 차단하는 조기 종료(Early Return) 락온
# 🚨 MODIFIED: [Config Null-Pointer 방어] get_plan 진입 시 cfg 인스턴스 결측으로 인한 연쇄 붕괴를 막는 절대 쉴드 락온
# 🚨 MODIFIED: [음수 오염 방어] capture_vrev_snapshot 내 수수료 음수 오입력 시 수익률이 뻥튀기되는 논리적 결함 원천 봉쇄 (max 0.0 바운딩)
# 🚨 MODIFIED: [데드코드 소각] 정적 분석 결과 호출되지 않는 유령 함수 4종 영구 소각 완료.
# ==========================================================
import logging
import pandas as pd
import numpy as np
from zoneinfo import ZoneInfo
from strategy_v14 import V14Strategy
from strategy_v_avwap import VAvwapHybridPlugin  
from strategy_reversion import ReversionStrategy
from strategy_v14_vwap import V14VwapStrategy

class InfiniteStrategy:
    
    def __init__(self, config):
        self.cfg = config
        self.v14_plugin = V14Strategy(config)
        self.v_avwap_plugin = VAvwapHybridPlugin()
        self.v_rev_plugin = ReversionStrategy(config)
        self.v14_vwap_plugin = V14VwapStrategy(config)

    def _safe_float(self, val):
        """ 🚨 [Insight 14 & 25] String-Comma 오염 및 NaN/Inf 수학적 런타임 붕괴 원천 차단 쉴드 """
        try:
            f_val = float(str(val or 0.0).replace(',', ''))
            if pd.isna(f_val) or np.isinf(f_val):
                return 0.0
            return f_val
        except Exception:
            return 0.0

    def get_plan(self, ticker, current_price, avg_price, qty, prev_close, ma_5day=0.0, market_type="REG", available_cash=0, is_simulation=False, vwap_status=None, is_snapshot_mode=False, regime_data=None):
        # 🚨 MODIFIED: [Config Null-Pointer 방어] 의존성 주입 실패 시 하위 연쇄 붕괴(AttributeError) 원천 차단
        if not self.cfg:
            logging.error("🚨 [FATAL] Config 객체 결측. 플랜 생성을 강제 중단하고 빈 지시서를 반환합니다.")
            return {
                'core_orders': [], 'bonus_orders': [], 'orders': [],
                't_val': 0.0, 'is_reverse': False, 'star_price': 0.0, 'one_portion': 0.0
            }

        # 🚨 MODIFIED: [AttributeError 방어] ticker가 None일 경우 발생하는 upper() 호출 즉사 버그 방어
        safe_ticker = str(ticker or "").strip().upper()
        
        # 🚨 MODIFIED: [유령 종목 차단] 빈 문자열일 경우 하위 I/O 실행을 차단하고 빈 플랜 반환
        if not safe_ticker:
            return {
                'core_orders': [], 'bonus_orders': [], 'orders': [],
                't_val': 0.0, 'is_reverse': False, 'star_price': 0.0, 'one_portion': 0.0
            }

        # 🚨 MODIFIED: [Insight 14] 라우터 진입점 맹독성 오염(String-Comma, None) 방어 절대 쉴드
        current_price = self._safe_float(current_price)
        avg_price = self._safe_float(avg_price)
        qty = int(self._safe_float(qty))
        prev_close = self._safe_float(prev_close)
        ma_5day = self._safe_float(ma_5day)
        available_cash = self._safe_float(available_cash)

        # 🚨 MODIFIED: [Insight 06/07] cfg.get_version 결측치(None) 유입으로 인한 AttributeError 즉사 버그 차단
        version = str(self.cfg.get_version(safe_ticker) or "V14").strip().upper()
        
        # 🚨 MODIFIED: [Case 04 준수] TQQQ는 오직 V14로만 강제 라우팅 (SOXS 소각)
        if safe_ticker == "TQQQ" and version != "V14":
            logging.warning(f"🚨 [{safe_ticker}] 절대 헌법 위반 감지. V14 모드로 강제 라우팅합니다.")
            self.cfg.set_version(safe_ticker, "V14")
            version = "V14"

        if version in ["V13", "V17", "V_VWAP", "V_AVWAP"]:
            logging.warning(f"[{safe_ticker}] 폐기된 레거시 모드({version}) 감지. V14 엔진으로 강제 라우팅합니다.")
            self.cfg.set_version(safe_ticker, "V14")
            version = "V14"

        try:
            is_vwap_enabled = getattr(self.cfg, 'get_manual_vwap_mode', lambda x: False)(safe_ticker)
        except Exception:
            is_vwap_enabled = False
        
        if version == "V14" and is_vwap_enabled:
            plan = self.v14_vwap_plugin.get_plan(
                ticker=safe_ticker, current_price=current_price, avg_price=avg_price, qty=qty,
                prev_close=prev_close, ma_5day=ma_5day, market_type=market_type,
                available_cash=available_cash, is_simulation=is_simulation,
                is_snapshot_mode=is_snapshot_mode
            )
        elif version == "V_REV":
            try:
                # 🚨 MODIFIED: [I/O 붕괴 방어] QueueLedger 동기 호출 시 발생 가능한 파일 접근 에러 샌드박싱 및 단락 평가 쉴드
                from queue_ledger import QueueLedger
                try:
                    ql = QueueLedger()
                    q_data = ql.get_queue(safe_ticker) or []
                except Exception as q_err:
                    logging.error(f"🚨 V-REV 큐 장부 파일 로드 실패: {q_err}")
                    q_data = []
                
                plan = self.v_rev_plugin.get_dynamic_plan(
                    ticker=safe_ticker, curr_p=current_price, prev_c=prev_close,
                    current_weight=1.0, vwap_status={}, min_idx=-1,
                    alloc_cash=available_cash, q_data=q_data,
                    is_snapshot_mode=is_snapshot_mode, market_type=market_type
                )
                
                plan['core_orders'] = [o for o in plan.get('orders', [])]
                plan['bonus_orders'] = []
                plan['is_reverse'] = True
                plan['t_val'] = 0.0
                plan['star_price'] = 0.0
                plan['one_portion'] = 0.0
            except Exception as e:
                logging.error(f"🚨 V-REV 플랜 생성 실패 (런타임 예외): {e}")
                plan = {
                    'core_orders': [], 'bonus_orders': [], 'orders': [],
                    't_val': 0.0, 'is_reverse': True, 'star_price': 0.0, 'one_portion': 0.0
                }
        else:
            plan = self.v14_plugin.get_plan(
                ticker=safe_ticker, current_price=current_price, avg_price=avg_price, qty=qty,
                prev_close=prev_close, ma_5day=ma_5day, market_type=market_type,
                available_cash=available_cash, is_simulation=is_simulation, vwap_status=vwap_status,
                is_snapshot_mode=is_snapshot_mode
            )
            
        return plan

    # 🚨 MODIFIED: [라우팅 누수 방어] V14 스나이퍼 감시 라우터 배선 개통 및 맹독성 오염 차단
    def check_sniper_condition(self, ticker, cfg, broker, chat_id):
        safe_ticker = str(ticker or "").strip().upper()
        if not safe_ticker: 
            return {"action": "HOLD", "reason": "스나이퍼 감시 대기(종목결측)", "limit_price": 0.0, "qty": 0}
        
        # 🚨 MODIFIED: [스나이퍼 Config 결측 방어] 매 1분 감시망에서 cfg 객체 부재 시 발생하는 즉사 버그 완벽 차단
        if not self.cfg:
            return {"action": "HOLD", "reason": "스나이퍼 감시 대기(Config 결측)", "limit_price": 0.0, "qty": 0}

        try:
            version = str(self.cfg.get_version(safe_ticker) or "V14").strip().upper()
        except Exception:
            version = "V14"

        if version == "V14":
            if hasattr(self.v14_plugin, 'check_sniper_condition'):
                return self.v14_plugin.check_sniper_condition(safe_ticker, cfg, broker, chat_id)
                
        return {"action": "HOLD", "reason": "스나이퍼 감시 대기(또는 모듈 없음)", "limit_price": 0.0, "qty": 0}

    def capture_vrev_snapshot(self, ticker, clear_price, avg_price, qty):
        safe_ticker = str(ticker or "").strip().upper()
        if not safe_ticker: return None
        
        qty = int(self._safe_float(qty))
        if qty <= 0: return None
        
        clear_price = self._safe_float(clear_price)
        avg_price = self._safe_float(avg_price)
        
        # 🚨 MODIFIED: [I/O 붕괴 방어 및 음수 오염 차단] cfg.get_fee 호출 실패 및 오입력(음수) 시 수익률 뻥튀기 원천 봉쇄
        try:
            if not self.cfg: raise ValueError("Config 인스턴스 결측")
            fee_rate = max(0.0, self._safe_float(self.cfg.get_fee(safe_ticker)) / 100.0)
        except Exception as e:
            logging.debug(f"⚠️ [{safe_ticker}] 수수료율 캐싱 실패. 기본값 0.07% 락온: {e}")
            fee_rate = 0.0007
            
        raw_total_buy = avg_price * qty
        raw_total_sell = clear_price * qty
        
        net_invested = raw_total_buy * (1.0 + fee_rate)
        net_revenue = raw_total_sell * (1.0 - fee_rate)
        
        realized_pnl = net_revenue - net_invested
        # 🚨 MODIFIED: [ZeroDivision 방어] net_invested가 0보다 클 때만 수익률 산출
        realized_pnl_pct = (realized_pnl / net_invested) * 100 if net_invested > 0 else 0.0
    
        return {
            "ticker": safe_ticker,
            "clear_price": clear_price,
            "avg_price": avg_price,
            "cleared_qty": qty,
            "realized_pnl": realized_pnl,
            "realized_pnl_pct": realized_pnl_pct,
            "captured_at": pd.Timestamp.now(tz=ZoneInfo('America/New_York'))
        }

    def get_avwap_decision(self, base_ticker, exec_ticker, base_curr_p, exec_curr_p, base_day_open, avg_price, qty, alloc_cash, context_data, df_1min_base, now_est, avwap_state=None, regime_data=None, is_simulation=False, df_1min_exec=None, sortie_mode="SINGLE", **kwargs):
        safe_base_ticker = str(base_ticker or "").strip().upper()
        safe_exec_ticker = str(exec_ticker or "").strip().upper()
        
        if not safe_base_ticker or not safe_exec_ticker: return {}
        
        # 🚨 MODIFIED: [Case 11] 다중 출격(Multi-Sortie) 모드 파라미터 수혈 배선
        return self.v_avwap_plugin.get_decision(
            base_ticker=safe_base_ticker, exec_ticker=safe_exec_ticker, base_curr_p=base_curr_p, exec_curr_p=exec_curr_p, 
            base_day_open=base_day_open, avwap_avg_price=avg_price, avwap_qty=qty, avwap_alloc_cash=alloc_cash,
            context_data=context_data, df_1min_base=df_1min_base, df_1min_exec=df_1min_exec, now_est=now_est, avwap_state=avwap_state, is_simulation=is_simulation, sortie_mode=sortie_mode, **kwargs
        )
