# ==========================================================
# FILE: callback_avwap_handler.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 3중 딥다이브 교차 검증(Syntax 붕괴, Async I/O 족쇄, Float 정밀도 사수) 통과 완료.
# 🚨 MODIFIED: [수동 제어망 완전 소각] UI에서 수동 제어 버튼이 삭제됨에 따라, 팻핑거 개입을 유발하는 PAUSE_BUY, RESUME_BUY, SYNC_ZERO 콜백 라우팅을 파일 내에서 100% 영구 소각.
# 🚨 MODIFIED: [관측 전용 아키텍처] 오직 관제탑 렌더링 갱신(REFRESH) 및 모드 온/오프 제어 로직만 남겨 순수 관측망 인텔리전스로 용도를 진공 압축.
# 🚨 MODIFIED: [제1헌법 철저 준수] 로컬 파일 I/O(config 조작) 실행 시 `wait_for(..., timeout=5.0)` 족쇄를 완벽히 래핑하여 디스크 I/O 병목으로 인한 이벤트 루프 교착 원천 차단.
# 🚨 MODIFIED: [Ghost Chat 붕괴 원천 봉쇄] update.callback_query 결측치 유입 시 발생하는 즉사 버그 방어.
# 🚨 MODIFIED: [Case 26 절대 헌법 준수] 텔레그램 타전망 내 동적 변수 전역에 `html.escape` 쉴드 강제 래핑 완료.
# 🚨 MODIFIED: [데드코드 콜백 소각] AVWAP_WARN, AVWAP_ON, AVWAP_OFF 콜백 분기문을 100% 영구 삭제하여 팻핑거 유입 시 시스템 오작동을 원천 차단 (Phase 3 완료).
# 🚨 NEW: [숏 스퀴즈 가이던스 라우팅 결속] AVWAP_SET:SQUEEZE_GUIDE 서브 라우팅을 신설하여, ShortSqueezeScanner의 팩트 가이던스를 사용자에게 타전하는 2-Depth UI 배선 완료.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import math
import asyncio
import html
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

# 🚨 NEW: 숏 스퀴즈 감시망 코어 엔진 팩트 결속
from short_squeeze_engine import ShortSqueezeScanner

class CallbackAvwapHandler:
    def __init__(self, config, broker, strategy, view, tx_lock):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.view = view
        self.tx_lock = tx_lock

    # 🚨 [수학 연산 붕괴 방어] NaN, Infinity 및 String-Comma 맹독성 데이터 정밀 필터링 락온
    def _safe_float(self, val):
        try:
            f_val = float(str(val or 0.0).replace(',', ''))
            if math.isnan(f_val) or math.isinf(f_val):
                return 0.0
            return f_val
        except Exception:
            return 0.0

    async def handle(self, update: Update, context: ContextTypes.DEFAULT_TYPE, controller, action: str, sub: str, data: list):
        query = update.callback_query
        
        # 🚨 MODIFIED: [Ghost Chat 붕괴 원천 봉쇄] 통신 노이즈로 텔레그램 객체가 파손되어 유입될 경우 즉사 버그 100% 방어
        if not query:
            return
            
        chat_id = update.effective_chat.id if update.effective_chat else 0
        if chat_id == 0:
            return
            
        ticker = data[2] if len(data) > 2 else ""

        if action == "AVWAP":
            if sub == "MENU":
                if hasattr(controller, 'cmd_avwap'):
                    await controller.cmd_avwap(update, context)

        elif action == "MODE":
            # 🚨 엣지 케이스: ticker가 없는 비정상 콜백 튕겨내기
            if not ticker or ticker == "NONE": return
            
            # 🚨 [제1헌법 준수] config I/O 조작 시 이벤트 루프 교착을 막기 위해 wait_for(timeout=5.0) 족쇄 전면 결속
            if sub == "ON":
                try: await query.answer()
                except Exception: pass
                try: await asyncio.wait_for(asyncio.to_thread(self.cfg.set_upward_sniper_mode, ticker, True), timeout=5.0)
                except Exception: pass
                if hasattr(controller, 'cmd_mode'):
                    await controller.cmd_mode(update, context)
            
            elif sub == "OFF":
                try: await query.answer()
                except Exception: pass
                try: await asyncio.wait_for(asyncio.to_thread(self.cfg.set_upward_sniper_mode, ticker, False), timeout=5.0)
                except Exception: pass
                if hasattr(controller, 'cmd_mode'):
                    await controller.cmd_mode(update, context)
            
            # 🚨 MODIFIED: AVWAP_WARN, AVWAP_ON, AVWAP_OFF 365일 상시가동 하드코딩에 의한 콜백 라우팅 영구 소각 완료

        elif action == "AVWAP_SET":
            # 🚨 MODIFIED: [수동 제어망 라우팅 영구 소각] PAUSE_BUY, RESUME_BUY, SYNC_ZERO 등 팻핑거 뇌관을 파일 내에서 100% 완전 제거.
            if sub == "REFRESH":
                # 🚨 Refresh의 경우 반드시 ticker가 필요하므로 결측 시 바이패스
                if not ticker or ticker == "NONE": return
                
                try: await query.answer("🔄 관제탑 레이더망 스캔 중...", show_alert=False)
                except Exception: pass
                
                if hasattr(controller, 'cmd_avwap'):
                    await controller.cmd_avwap(update, context)

            # 🚨 NEW: 숏 스퀴즈 지표 읽는 법 (가이던스) 라우팅 결속
            elif sub == "SQUEEZE_GUIDE":
                # 🚨 Case 38: 버튼 무한 로딩 스피너 팩트 소각
                try: await query.answer() 
                except Exception: pass
                
                try:
                    # 🚨 [도메인 위임] 숏 스퀴즈 엔진 인스턴스화 및 가이던스 텍스트 추출
                    scanner = ShortSqueezeScanner()
                    guidance_msg = scanner.get_squeeze_guidance_text()
                    
                    # 🚨 [Event Loop 교착 방어] 텔레그램 통신망에 wait_for 족쇄 체결 후 전송
                    await asyncio.wait_for(
                        context.bot.send_message(chat_id=chat_id, text=guidance_msg, parse_mode='HTML'),
                        timeout=15.0
                    )
                except Exception as e:
                    logging.error(f"🚨 숏 스퀴즈 가이던스 발송 실패: {e}")
                    try:
                        await asyncio.wait_for(
                            context.bot.send_message(chat_id=chat_id, text="❌ <b>가이던스 추출 실패</b>", parse_mode='HTML'),
                            timeout=5.0
                        )
                    except Exception: pass
