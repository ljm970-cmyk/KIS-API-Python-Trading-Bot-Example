# ==========================================================
# FILE: telegram_states.py
# ==========================================================
# 🚨 MODIFIED: [보안 무결성 팩트 교정] 관리자 검증 코루틴 호출 시 await 누락으로 인한 보안망 우회 맹점 완벽 수술
# 🚨 MODIFIED: [V44.30 수동 입력 렌더링 수술] 텔레그램 상태 제어 후 제자리 렌더링 및 응답 무결성 확보
# 🚨 MODIFIED: [V44.44 이벤트 루프 교착 방어] 큐 장부 지층 수동 수정(EDIT_Q) 시 발생하는 직접적인 파일 I/O 작업을 비동기 래핑
# 🚨 MODIFIED: [Case 26 절대 헌법 준수] 텔레그램 HTML 파서 붕괴 방어를 위한 html.escape 쉴드 전역 강제 주입
# 🚨 MODIFIED: [Case 32 & 33 절대 규칙] 팻핑거 스캔 시 TPS 캡핑(0.06s) 및 3단 지수 백오프, 타임아웃(10s) 샌드위치 락온
# 🚨 MODIFIED: [NoneType 붕괴 원천 봉쇄] update.message 다이렉트 참조 소각 및 update.effective_message 단락 평가 락온
# 🚨 MODIFIED: [Fat-Finger 쉴드 재조정] 3배수 레버리지 극단적 갭(Gap) 변동성 수용을 위해 ±30% -> ±60% 로 임계치 대폭 확장
# 🚨 NEW: [Phase 1 암살자 설정 UI 결속] CONF_AVWAP_KRW 라우팅 분기 신설 및 콤마 맹독성 방어 후 원자적 I/O 기록 팩트 이식 완료
# 🚨 MODIFIED: [Case 37 UX 무결성 사수] 모든 설정(시드, 분할, 수수료, 암살자 목표액 등) 입력 완료 시, 즉각 cmd_settlement를 호출하여 최신 관제탑 화면으로 복귀하도록 팩트 락온.
# 🚨 MODIFIED: [Case 38 렌더링 충돌 절대 방어] 제자리 렌더링 호출(cmd_settlement) 시 발생하는 텔레그램 BadRequest(Message is not modified) 에러를 흡수하는 샌드박스 정밀 래핑.
# 🚨 MODIFIED: [NameError 즉사 방어] PTB 최신 규격에 맞춰 from telegram.error import BadRequest 명시적 임포트 및 샌드박스 문법 100% 교정 완료.
# 🚨 MODIFIED: [제1헌법 철저 준수] 텔레그램 메세지 발송(reply_text) 및 파일 I/O 스레드 전역에 asyncio.wait_for(timeout=10.0) 족쇄를 100% 래핑하여 텔레그램 서버 지연으로 인한 메인 이벤트 루프 교착(Deadlock) 원천 봉쇄.
# 🚨 MODIFIED: [Insight 14 & 25] 클래스 내부에 _safe_float 래퍼를 전격 이식하여, 문자열 치환(ValueError 의존) 방식의 맹점을 소각하고 NaN/Inf 맹독성 데이터 유입 시 즉각 0.0 폴백 방어막 가동.
# 🚨 MODIFIED: [제4헌법 무결성 수복] 다이렉트 JSON I/O(Split, Target) 연산 시 누락되었던 self.cfg._io_lock 뮤텍스를 강제 결속하여 TOCTOU 레이스 컨디션 데이터 증발 현상 완벽 차단.
# 🚨 MODIFIED: [제2헌법 절대 준수] _safe_float 도입으로 인해 영원히 도달 불가능해진 하단 `except ValueError:` 데드코드 블록 100% 영구 소각 완료.
# 🚨 NEW: [Phase 7 암살자 듀얼 익절 스키마 결속] CONF_AVWAP_PCT 라우팅 분기 신설. 수익률 목표 수동 입력 팻핑거 쉴드 락온 및 cmd_settlement 롤백 연계 완료.
# ==========================================================

import logging
import datetime
from zoneinfo import ZoneInfo
import os
import json
import asyncio
import tempfile
import html
import math 
from telegram import Update
from telegram.ext import ContextTypes
from telegram.error import BadRequest

class TelegramStates:
    def __init__(self, config, broker, queue_ledger, sync_engine):
        self.cfg = config
        self.broker = broker
        self.queue_ledger = queue_ledger
        self.sync_engine = sync_engine

    # 🚨 NEW: [Insight 14 & 25] NaN, Infinity 및 String-Comma 맹독성 데이터 정밀 필터링 절대 쉴드 내재화
    def _safe_float(self, val):
        try:
            f_val = float(str(val or 0.0).replace(',', ''))
            if math.isnan(f_val) or math.isinf(f_val):
                return 0.0
            return f_val
        except Exception:
            return 0.0

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, controller):
        # 🚨 MODIFIED: [보안 팩트 교정] await 키워드 강제 락온으로 코루틴 경고 소각 및 관리자 인증망 수복
        if not await controller._is_admin(update):
            return
            
        chat_id = update.effective_chat.id
        
        # 🚨 MODIFIED: 미디어(사진 등) 수신 시 text 속성이 None이 되어 발생하는 TypeError 단락 평가 방어
        text = update.effective_message.text.strip() if update.effective_message and update.effective_message.text else ""
        
        # 일반 명령어 라우팅 우회
        if "통합 지시서" in text or "지시서 조회" in text:
            return await controller.cmd_sync(update, context)
        elif "장부 동기화" in text or "장부 조회" in text:
            return await controller.cmd_record(update, context)
        elif "명예의 전당" in text:
            return await controller.cmd_history(update, context)
        elif "코어 스위칭" in text or "전술 설정" in text or "모드변환" in text or "분할변경" in text:
            return await controller.cmd_settlement(update, context)
        elif "시드머니" in text or "시드 변경" in text or "시드 관리" in text:
            return await controller.cmd_seed(update, context)
        elif "종목 선택" in text:
            return await controller.cmd_ticker(update, context)
        elif "스나이퍼" in text:
            return await controller.cmd_mode(update, context)
        elif "버전" in text or "업데이트 내역" in text:
            return await controller.cmd_version(update, context)
        elif "비상 해제" in text:
            return await controller.cmd_reset(update, context)
        elif "시스템 업데이트" in text or "엔진 업데이트" in text:
            return await controller.cmd_update(update, context)

        state = controller.user_states.get(chat_id)
        
        if not state:
            return

        try:
            # ==========================================================
            # 🛠️ 지층(Queue) 수동 편집 모드 팻핑거 쉴드
            # ==========================================================
            if state.startswith("EDITQ_"):
                parts = state.split("_", 2)
                ticker = parts[1]
                target_date = parts[2]
                safe_ticker = html.escape(str(ticker))
                
                input_parts = text.split()
                if len(input_parts) != 2:
                    del controller.user_states[chat_id]
                    try: await asyncio.wait_for(update.effective_message.reply_text("❌ 입력 형식 오류입니다. 띄어쓰기로 수량과 평단가를 입력해주세요. (수정 취소됨)"), timeout=10.0)
                    except Exception: pass
                    return
                
                # 🚨 MODIFIED: [Insight 14 & 25] ValueError 의존 맹점 소각 및 _safe_float를 통한 완벽한 필터링 락온
                qty = int(self._safe_float(input_parts[0]))
                price = self._safe_float(input_parts[1])
                
                if qty <= 0 or price <= 0.0:
                    del controller.user_states[chat_id]
                    try: await asyncio.wait_for(update.effective_message.reply_text("❌ 수량/평단가는 0보다 큰 숫자로 입력하세요. (수정 취소됨)"), timeout=10.0)
                    except Exception: pass
                    return
                
                try:
                    curr_p = 0.0
                    # 🚨 MODIFIED: [Case 32, 33, 14] 팻핑거 스캔 시 TPS 캡핑, 3단 백오프, 타임아웃 10초 샌드위치 락온
                    for attempt in range(3):
                        try:
                            await asyncio.sleep(0.06)
                            curr_p_val = await asyncio.wait_for(
                                asyncio.to_thread(self.broker.get_current_price, ticker), 
                                timeout=10.0
                            )
                            curr_p = self._safe_float(curr_p_val)
                            break
                        except Exception:
                            if attempt == 2: curr_p = 0.0
                            else: await asyncio.sleep(1.0 * (2 ** attempt))
                            
                        # 🚨 MODIFIED: [Fat-Finger 쉴드 재조정] 3배수 레버리지 극단적 갭(Gap) 변동성 수용을 위해 ±30% -> ±60% 로 임계치 확장
                        if curr_p and curr_p > 0 and (price < curr_p * 0.4 or price > curr_p * 1.6):
                            del controller.user_states[chat_id]
                            try: await asyncio.wait_for(update.effective_message.reply_text(f"🚨 <b>팻핑거 방어 가동:</b> 입력가(${price:.2f})가 현재가(${curr_p:.2f}) 대비 ±60%를 초과합니다. 다시 시도해주세요.", parse_mode='HTML'), timeout=10.0)
                            except Exception: pass
                            return
                except Exception:
                    pass

                # 🚨 MODIFIED: [제1헌법] 큐 장부 파일 I/O 작업 비동기 및 타임아웃 래핑
                if getattr(self, 'queue_ledger', None):
                    try: await asyncio.wait_for(asyncio.to_thread(self.queue_ledger.edit_lot, ticker, target_date, qty, price), timeout=10.0)
                    except Exception as e: logging.error(f"🚨 지층 수정 파일 I/O 에러: {e}")
                
                del controller.user_states[chat_id]
                short_date = html.escape(str(target_date[:10]))
                try: await asyncio.wait_for(update.effective_message.reply_text(f"✅ <b>[{safe_ticker}] 지층 정밀 수정 완료! KIS 원장과 동기화합니다.</b>\n▫️ {short_date} | {qty}주 | ${price:.2f}", parse_mode='HTML'), timeout=10.0)
                except Exception: pass
                
                if ticker not in self.sync_engine.sync_locks:
                    self.sync_engine.sync_locks[ticker] = asyncio.Lock()
                if not self.sync_engine.sync_locks[ticker].locked():
                    await self.sync_engine.process_auto_sync(ticker, chat_id, context, silent_ledger=False)
                
                return

            # ==========================================================
            # ⚙️ 관제탑 일반 설정 모드 (콤마 맹독성 방어 공통 적용)
            # ==========================================================
            # 🚨 MODIFIED: [Insight 14 & 25] String-Float 콤마 및 NaN 맹독성 절대 방어 쉴드 래핑
            val = self._safe_float(text)
            parts = state.split("_")
            
            if state.startswith("SEED"):
                if val <= 0:
                    try: await asyncio.wait_for(update.effective_message.reply_text("❌ 오류: 시드머니는 0보다 커야 합니다."), timeout=10.0)
                    except Exception: pass
                    return
                    
                action, ticker = parts[1], parts[2]
                safe_ticker = html.escape(str(ticker))
                
                # 🚨 MODIFIED: [맹점 4] cfg.get_seed 동기 I/O 블로킹 비동기 및 타임아웃 래핑
                try: curr = self._safe_float(await asyncio.wait_for(asyncio.to_thread(self.cfg.get_seed, ticker), timeout=10.0))
                except Exception: curr = 0.0

                new_v = curr + val if action == "ADD" else (max(0.0, curr - val) if action == "SUB" else val)
                
                try: await asyncio.wait_for(asyncio.to_thread(self.cfg.set_seed, ticker, new_v), timeout=10.0)
                except Exception as e: logging.error(f"🚨 시드 설정 에러: {e}")
                
                try: await asyncio.wait_for(update.effective_message.reply_text(f"✅ [{safe_ticker}] 시드 변경: ${new_v:,.0f}"), timeout=10.0)
                except Exception: pass
                
                if hasattr(controller, 'cmd_seed'):
                    try:
                        await controller.cmd_seed(update, context)
                    except BadRequest as e:
                        if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                    except Exception: pass
                
            elif state.startswith("CONF_SPLIT"):
                if val < 1:
                    try: await asyncio.wait_for(update.effective_message.reply_text("❌ 오류: 분할 횟수는 1 이상이어야 합니다."), timeout=10.0)
                    except Exception: pass
                    return
                    
                ticker = parts[2]
                safe_ticker = html.escape(str(ticker))
                
                def _set_split():
                    # 🚨 MODIFIED: [제4헌법] 다이렉트 I/O 시 TOCTOU 붕괴 방어용 스레드 잠금 결속
                    with self.cfg._io_lock:
                        d = self.cfg._load_json(self.cfg.FILES["SPLIT"], self.cfg.DEFAULT_SPLIT)
                        d[ticker] = val
                        self.cfg._save_json(self.cfg.FILES["SPLIT"], d)
                
                # 🚨 MODIFIED: [제1헌법] 파일 I/O 타임아웃 족쇄 래핑
                try: await asyncio.wait_for(asyncio.to_thread(_set_split), timeout=10.0)
                except Exception as e: logging.error(f"🚨 분할 설정 에러: {e}")
                
                try: await asyncio.wait_for(update.effective_message.reply_text(f"✅ [{safe_ticker}] 분할: {int(val)}회"), timeout=10.0)
                except Exception: pass
                
                if hasattr(controller, 'cmd_settlement'):
                    try:
                        await controller.cmd_settlement(update, context)
                    except BadRequest as e:
                        if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                    except Exception: pass
                
            elif state.startswith("CONF_TARGET"):
                ticker = parts[2]
                safe_ticker = html.escape(str(ticker))
                
                def _set_target():
                    # 🚨 MODIFIED: [제4헌법] 다이렉트 I/O 시 TOCTOU 붕괴 방어용 스레드 잠금 결속
                    with self.cfg._io_lock:
                        d = self.cfg._load_json(self.cfg.FILES["PROFIT_CFG"], self.cfg.DEFAULT_TARGET)
                        d[ticker] = val
                        self.cfg._save_json(self.cfg.FILES["PROFIT_CFG"], d)
                
                try: await asyncio.wait_for(asyncio.to_thread(_set_target), timeout=10.0)
                except Exception as e: logging.error(f"🚨 목표치 설정 에러: {e}")
                
                try: await asyncio.wait_for(update.effective_message.reply_text(f"✅ [{safe_ticker}] 목표 수익률: {val}%"), timeout=10.0)
                except Exception: pass
                
                if hasattr(controller, 'cmd_settlement'):
                    try:
                        await controller.cmd_settlement(update, context)
                    except BadRequest as e:
                        if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                    except Exception: pass

            elif state.startswith("CONF_COMPOUND"):
                if val < 0:
                    try: await asyncio.wait_for(update.effective_message.reply_text("❌ 오류: 복리율은 0 이상이어야 합니다."), timeout=10.0)
                    except Exception: pass
                    return
                    
                ticker = parts[2]
                safe_ticker = html.escape(str(ticker))
                
                try: await asyncio.wait_for(asyncio.to_thread(self.cfg.set_compound_rate, ticker, val), timeout=10.0)
                except Exception as e: logging.error(f"🚨 복리 설정 에러: {e}")
                
                try: await asyncio.wait_for(update.effective_message.reply_text(f"✅ [{safe_ticker}] 졸업 시 자동 복리율: {val}%"), timeout=10.0)
                except Exception: pass
                
                if hasattr(controller, 'cmd_settlement'):
                    try:
                        await controller.cmd_settlement(update, context)
                    except BadRequest as e:
                        if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                    except Exception: pass

            elif state.startswith("CONF_FEE"):
                if val < 0.0 or val > 10.0:
                    try: await asyncio.wait_for(update.effective_message.reply_text("🚨 <b>오입력 차단:</b> 수수료율은 0.0% ~ 10.0% 사이여야 합니다.", parse_mode='HTML'), timeout=10.0)
                    except Exception: pass
                    return
                    
                ticker = parts[2]
                safe_ticker = html.escape(str(ticker))
                
                try: await asyncio.wait_for(asyncio.to_thread(self.cfg.set_fee, ticker, val), timeout=10.0)
                except Exception as e: logging.error(f"🚨 수수료 설정 에러: {e}")
                
                try: await asyncio.wait_for(update.effective_message.reply_text(f"💳 <b>[{safe_ticker}] 증권사 거래 수수료: {val}% 적용 완료!</b>\n▫️ 다음 명예의 전당 정산부터 수익 연산 시 해당 수수료가 적용됩니다.", parse_mode='HTML'), timeout=10.0)
                except Exception: pass
                
                if hasattr(controller, 'cmd_settlement'):
                    try:
                        await controller.cmd_settlement(update, context)
                    except BadRequest as e:
                        if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                    except Exception: pass
                
            elif state.startswith("CONF_STOCK_SPLIT"):
                if val <= 0:
                    try: await asyncio.wait_for(update.effective_message.reply_text("❌ 오류: 액면 보정 비율은 0보다 커야 합니다."), timeout=10.0)
                    except Exception: pass
                    return
                    
                ticker = parts[2]
                safe_ticker = html.escape(str(ticker))
                
                try: await asyncio.wait_for(asyncio.to_thread(self.cfg.apply_stock_split, ticker, val), timeout=15.0)
                except Exception as e: logging.error(f"🚨 수동 액면보정 에러: {e}")
                
                # 🚨 MODIFIED: [Case 03] ZoneInfo('America/New_York') EST 단일 소스 락온
                est = ZoneInfo('America/New_York')
                today_str = datetime.datetime.now(est).strftime('%Y-%m-%d')
                
                try: await asyncio.wait_for(asyncio.to_thread(self.cfg.set_last_split_date, ticker, today_str), timeout=10.0)
                except Exception as e: logging.error(f"🚨 분할 날짜 기록 에러: {e}")
                
                try: await asyncio.wait_for(update.effective_message.reply_text(f"✅ [{safe_ticker}] 수동 액면 보정 완료\n▫️ 모든 장부 기록이 {val}배 비율로 정밀하게 소급 조정되었습니다."), timeout=10.0)
                except Exception: pass
                
                if hasattr(controller, 'cmd_settlement'):
                    try:
                        await controller.cmd_settlement(update, context)
                    except BadRequest as e:
                        if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                    except Exception: pass

            elif state.startswith("VREV_GAP"):
                ticker = parts[2]
                safe_ticker = html.escape(str(ticker))
                if val > 0: val = -val
                
                if hasattr(self.cfg, 'set_vrev_gap_threshold'):
                    try: await asyncio.wait_for(asyncio.to_thread(self.cfg.set_vrev_gap_threshold, ticker, val), timeout=10.0)
                    except Exception as e: logging.error(f"🚨 VREV 갭 임계치 설정 에러: {e}")
                    
                try: await asyncio.wait_for(update.effective_message.reply_text(f"📉 <b>[{safe_ticker}] V-REV 장막판 갭 스위칭 임계치 설정 완료!</b>\n▫️ 팩트 타격선: 기초자산 VWAP 대비 <b>{val}%</b>\n▫️ 다음 타임 슬라이싱 스케줄부터 즉시 적용됩니다.", parse_mode='HTML'), timeout=10.0)
                except Exception: pass
                
                if hasattr(controller, 'cmd_settlement'):
                    try:
                        await controller.cmd_settlement(update, context)
                    except BadRequest as e:
                        if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                    except Exception: pass
                
            # ==========================================================
            # 🎯 [Phase 1 NEW] 암살자 원화 목표 수익금 파싱 및 장부 기록
            # ==========================================================
            elif state.startswith("CONF_AVWAP_KRW"):
                if val <= 0:
                    try: await asyncio.wait_for(update.effective_message.reply_text("❌ 오류: 암살자 목표 수익금은 0보다 커야 합니다."), timeout=10.0)
                    except Exception: pass
                    return
                    
                # State 형식: CONF_AVWAP_KRW_SOXL -> parts: ["CONF", "AVWAP", "KRW", "SOXL"]
                ticker = parts[3]
                safe_ticker = html.escape(str(ticker))
                
                try: await asyncio.wait_for(asyncio.to_thread(self.cfg.set_avwap_target_krw, ticker, val), timeout=10.0)
                except Exception as e: logging.error(f"🚨 AVWAP 원화 목표치 설정 에러: {e}")
                
                try: await asyncio.wait_for(update.effective_message.reply_text(f"🎯 <b>[{safe_ticker}] 암살자 목표 수익금 ₩{int(val):,} 적용 완료!</b>\n▫️ 다음 섀도우 연산부터 KIS 원화 환산 팩트가 적용됩니다.", parse_mode='HTML'), timeout=10.0)
                except Exception: pass
                
                if hasattr(controller, 'cmd_settlement'):
                    try:
                        await controller.cmd_settlement(update, context)
                    except BadRequest as e:
                        if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                    except Exception: pass

            # ==========================================================
            # 🎯 [Phase 7 NEW] 암살자 수익률(%) 목표 파싱 및 장부 기록
            # ==========================================================
            elif state.startswith("CONF_AVWAP_PCT"):
                if val <= 0.0:
                    try: await asyncio.wait_for(update.effective_message.reply_text("❌ 오류: 암살자 목표 수익률은 0보다 커야 합니다."), timeout=10.0)
                    except Exception: pass
                    return
                    
                # State 형식: CONF_AVWAP_PCT_SOXL -> parts: ["CONF", "AVWAP", "PCT", "SOXL"]
                ticker = parts[3]
                safe_ticker = html.escape(str(ticker))
                
                try: await asyncio.wait_for(asyncio.to_thread(self.cfg.set_avwap_target_pct, ticker, val), timeout=10.0)
                except Exception as e: logging.error(f"🚨 AVWAP 수익률 목표치 설정 에러: {e}")
                
                try: await asyncio.wait_for(update.effective_message.reply_text(f"🎯 <b>[{safe_ticker}] 암살자 목표 수익률 {val}% 적용 완료!</b>\n▫️ 다음 섀도우 연산부터 수익률 기반 팩트가 적용됩니다.", parse_mode='HTML'), timeout=10.0)
                except Exception: pass
                
                if hasattr(controller, 'cmd_settlement'):
                    try:
                        await controller.cmd_settlement(update, context)
                    except BadRequest as e:
                        if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                    except Exception: pass

        except Exception as e:
            safe_err = html.escape(str(e))
            try: await asyncio.wait_for(update.effective_message.reply_text(f"❌ 알 수 없는 오류 발생: {safe_err}"), timeout=10.0)
            except Exception: pass
        finally:
            if chat_id in controller.user_states:
                del controller.user_states[chat_id]
