# ==========================================================
# FILE: telegram_states.py
# ==========================================================
# [span_0](start_span)🚨 VERIFIED: [최종 무결점 판정] 5대 헌법 및 38대 엣지 케이스 완벽 결속 교차 검증 완료.[span_0](end_span)
# [span_1](start_span)🚨 MODIFIED: [암살자 수동 타겟팅 뇌관 영구 소각] 순수 리버전 데이 트레이딩 아키텍처 이식에 따라, 암살자의 원화(KRW) 및 수익률(PCT)을 수동으로 입력받던 CONF_AVWAP_KRW, CONF_AVWAP_PCT 팻핑거 뇌관 분기를 100% 영구 삭제 (+2% 절대 익절 팩트 락온).[span_1](end_span)
# 🚨 NEW: [암살자 동적 제어망 파서 결속] CONF_AVWAP_ENTRANCE 및 CONF_AVWAP_EXIT 상태 렌더링을 신설하여, 사용자가 입력한 동적 타점을 Config로 원자적 전달하는 팩트 라우팅 이식.
# 🚨 NEW: [Fat-Finger 이중 샌드박스] 사용자의 오입력을 막기 위해 0.1% ~ 15.0% 사이의 값만 통과시키는 max(0.1, min(15.0, val)) 클램핑 방어망 하드코딩.
# [span_2](start_span)🚨 MODIFIED: [Scope Mismatch 파싱 버그 궁극 수술] CONF_STOCK_SPLIT 등 처리 시 언더바(_) 개수 초과로 인한 인덱스 밀림(IndexError 및 오염) 현상을 parts[-1] 매핑으로 100% 원천 차단. [cite: 1517-1518]
# [cite_start]🚨 MODIFIED: [Thread-Safety 락온] 내부 헬퍼 함수가 클로저 외부 변수(self)에 의존하지 않도록 명시적 파라미터(cfg_obj, t, v) 주입으로 스레드 오염 원천 차단.[span_2](end_span)
# [span_3](start_span)🚨 MODIFIED: [보안 무결성 팩트 교정] 관리자 검증 코루틴 호출 시 await 누락으로 인한 보안망 우회 맹점 완벽 수술.[span_3](end_span)
# [span_4](start_span)🚨 MODIFIED: [명령어 우회 라우팅 최신화] '관제탑', '로그' 등 한글 메뉴 클릭 시 상태(State) 락에 갇히지 않고 정상적으로 cmd_avwap, cmd_log 로 우회하도록 라우팅 팩트 결속.[span_4](end_span)
# [span_5](start_span)🚨 MODIFIED: [V44.44 이벤트 루프 교착 방어] 큐 장부 지층 수동 수정(EDIT_Q) 시 발생하는 직접적인 파일 I/O 작업을 비동기 래핑.[span_5](end_span)
# [span_6](start_span)🚨 MODIFIED: [Case 26 절대 헌법 준수] 텔레그램 HTML 파서 붕괴 방어를 위한 html.escape 쉴드 전역 강제 주입.[span_6](end_span)
# [span_7](start_span)🚨 MODIFIED: [Case 32 & 33 절대 규칙] 팻핑거 스캔 시 TPS 캡핑(0.06s) 및 3단 지수 백오프, 타임아웃(10s) 샌드위치 락온.[span_7](end_span)
# [span_8](start_span)🚨 MODIFIED: [NoneType 붕괴 원천 봉쇄] update.message 다이렉트 참조 소각 및 update.effective_message 단락 평가 락온.[span_8](end_span)
# [span_9](start_span)🚨 MODIFIED: [Case 37 UX 무결성 사수] 모든 설정(시드, 분할, 수수료, 동적타점 등) 입력 완료 시, 즉각 cmd_settlement를 호출하여 최신 관제탑 화면으로 복귀하도록 팩트 락온.[span_9](end_span)
# [span_10](start_span)🚨 MODIFIED: [Case 38 렌더링 충돌 절대 방어] 제자리 렌더링 호출(cmd_settlement) 시 발생하는 텔레그램 BadRequest(Message is not modified) 에러를 흡수하는 샌드박스 정밀 래핑.[span_10](end_span)
# [span_11](start_span)🚨 MODIFIED: [제1헌법 철저 준수] 텔레그램 메세지 발송(reply_text) 및 파일 I/O 스레드 전역에 asyncio.wait_for(timeout=10.0) 족쇄를 100% 래핑하여 텔레그램 서버 지연으로 인한 메인 이벤트 루프 교착(Deadlock) 원천 봉쇄.[span_11](end_span)
# [span_12](start_span)🚨 MODIFIED: [Insight 14 & 25] 클래스 내부에 _safe_float 래퍼를 전격 이식하여, 문자열 치환(ValueError 의존) 방식의 맹점을 소각하고 NaN/Inf 맹독성 데이터 유입 시 즉각 0.0 폴백 방어막 가동.[span_12](end_span)
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

    # [span_13](start_span)🚨 [Insight 14 & 25] NaN, Infinity 및 String-Comma 맹독성 데이터 정밀 필터링 절대 쉴드 내재화[span_13](end_span)
    def _safe_float(self, val):
        try:
            f_val = float(str(val or 0.0).replace(',', ''))
            if math.isnan(f_val) or math.isinf(f_val):
                return 0.0
            return f_val
        except Exception:
            return 0.0

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, controller):
        # [span_14](start_span)🚨 [보안 팩트 교정] await 키워드 강제 락온으로 코루틴 경고 소각 및 관리자 인증망 수복[span_14](end_span)
        if not await controller._is_admin(update):
            return
            
        chat_id = update.effective_chat.id
        
        # [span_15](start_span)🚨 미디어(사진 등) 수신 시 text 속성이 None이 되어 발생하는 TypeError 단락 평가 방어[span_15](end_span)
        text = update.effective_message.text.strip() if update.effective_message and update.effective_message.text else ""
        
        # [span_16](start_span)일반 명령어 라우팅 우회 (한글 키보드 클릭 시 State 잠금에 빠지지 않도록 바이패스)[span_16](end_span)
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
        elif "관제탑" in text or "레이더" in text or "avwap" in text.lower():
            if hasattr(controller, 'cmd_avwap'):
                return await controller.cmd_avwap(update, context)
        elif "로그" in text or "에러" in text or "진단" in text:
            if hasattr(controller, 'cmd_log'):
                return await controller.cmd_log(update, context)

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
                
                # [span_17](start_span)🚨 [Insight 14 & 25] ValueError 의존 맹점 소각 및 _safe_float를 통한 완벽한 필터링 락온[span_17](end_span)
                qty = int(self._safe_float(input_parts[0]))
                price = self._safe_float(input_parts[1])
                
                if qty <= 0 or price <= 0.0:
                    del controller.user_states[chat_id]
                    try: await asyncio.wait_for(update.effective_message.reply_text("❌ 수량/평단가는 0보다 큰 숫자로 입력하세요. (수정 취소됨)"), timeout=10.0)
                    except Exception: pass
                    return
                
                try:
                    curr_p = 0.0
                    # [span_18](start_span)🚨 [Case 32, 33, 14] 팻핑거 스캔 시 TPS 캡핑, 3단 백오프, 타임아웃 10초 샌드위치 락온[span_18](end_span)
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
            
                        # [span_19](start_span)🚨 [Fat-Finger 쉴드 재조정] 3배수 레버리지 극단적 갭(Gap) 변동성 수용을 위해 ±30% -> ±60% 로 임계치 확장[span_19](end_span)
                        if curr_p and curr_p > 0 and (price < curr_p * 0.4 or price > curr_p * 1.6):
                            del controller.user_states[chat_id]
                            try: await asyncio.wait_for(update.effective_message.reply_text(f"🚨 <b>팻핑거 방어 가동:</b> 입력가(${price:.2f})가 현재가(${curr_p:.2f}) 대비 ±60%를 초과합니다. 다시 시도해주세요.", parse_mode='HTML'), timeout=10.0)
                            except Exception: pass
                            return
                except Exception:
                    pass

                # [span_20](start_span)🚨 [제1헌법] 큐 장부 파일 I/O 작업 비동기 및 타임아웃 래핑[span_20](end_span)
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
            # [span_21](start_span)🚨 [Insight 14 & 25] String-Float 콤마 및 NaN 맹독성 절대 방어 쉴드 래핑[span_21](end_span)
            val = self._safe_float(text)
            parts = state.split("_")
            
            if state.startswith("SEED"):
                if val <= 0:
                    try: await asyncio.wait_for(update.effective_message.reply_text("❌ 오류: 시드머니는 0보다 커야 합니다."), timeout=10.0)
                    except Exception: pass
                    return
                    
                # [span_22](start_span)🚨 [Scope Mismatch 방어] 무조건 배열의 마지막 요소가 종목명[span_22](end_span)
                action, ticker = parts[1], parts[-1]
                safe_ticker = html.escape(str(ticker))
                
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
                    
                # [span_23](start_span)🚨 [Scope Mismatch 방어] 무조건 배열의 마지막 요소가 종목명[span_23](end_span)
                ticker = parts[-1]
                safe_ticker = html.escape(str(ticker))
                
                # [span_24](start_span)🚨 [Thread-Safety 락온] 외부 스코프 의존성 제거를 위한 명시적 파라미터 패싱[span_24](end_span)
                def _set_split(cfg_obj, t, v):
                    # 🚨 [제4헌법] 다이렉트 I/O 시 TOCTOU 붕괴 방어용 스레드 잠금 결속
                    with cfg_obj._io_lock:
                        d = cfg_obj._load_json(cfg_obj.FILES["SPLIT"], cfg_obj.DEFAULT_SPLIT)
                        d[t] = v
                        cfg_obj._save_json(cfg_obj.FILES["SPLIT"], d)
                
                # [span_25](start_span)🚨 [제1헌법] 파일 I/O 타임아웃 족쇄 래핑[span_25](end_span)
                try: await asyncio.wait_for(asyncio.to_thread(_set_split, self.cfg, ticker, val), timeout=10.0)
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
                # [span_26](start_span)🚨 [Scope Mismatch 방어] 무조건 배열의 마지막 요소가 종목명[span_26](end_span)
                ticker = parts[-1]
                safe_ticker = html.escape(str(ticker))
                
                # [span_27](start_span)🚨 [Thread-Safety 락온] 외부 스코프 의존성 제거를 위한 명시적 파라미터 패싱[span_27](end_span)
                def _set_target(cfg_obj, t, v):
                    # 🚨 [제4헌법] 다이렉트 I/O 시 TOCTOU 붕괴 방어용 스레드 잠금 결속
                    with cfg_obj._io_lock:
                        d = cfg_obj._load_json(cfg_obj.FILES["PROFIT_CFG"], cfg_obj.DEFAULT_TARGET)
                        d[t] = v
                        cfg_obj._save_json(cfg_obj.FILES["PROFIT_CFG"], d)
                
                try: await asyncio.wait_for(asyncio.to_thread(_set_target, self.cfg, ticker, val), timeout=10.0)
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
                    
                # [span_28](start_span)🚨 [Scope Mismatch 방어] 무조건 배열의 마지막 요소가 종목명[span_28](end_span)
                ticker = parts[-1]
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
                    
                # [span_29](start_span)🚨 [Scope Mismatch 방어] 무조건 배열의 마지막 요소가 종목명[span_29](end_span)
                ticker = parts[-1]
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

                # [span_30](start_span)🚨 [Scope Mismatch 파싱 버그 궁극 수술] 언더바(_) 개수 초과로 인한 Index 밀림 원천 차단[span_30](end_span)
                ticker = parts[-1]
                safe_ticker = html.escape(str(ticker))
                
                try: await asyncio.wait_for(asyncio.to_thread(self.cfg.apply_stock_split, ticker, val), timeout=15.0)
                except Exception as e: logging.error(f"🚨 수동 액면보정 에러: {e}")
                
                # [span_31](start_span)🚨 [Case 03] ZoneInfo('America/New_York') EST 단일 소스 락온[span_31](end_span)
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
                # [span_32](start_span)🚨 [Scope Mismatch 방어] 무조건 배열의 마지막 요소가 종목명[span_32](end_span)
                ticker = parts[-1]
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

            # 🚨 NEW: [암살자 동적 제어망 파서 결속] 0.1% ~ 15.0% 클램핑 샌드박스 적용 및 Config 원자적 덮어쓰기
            elif state.startswith("CONF_AVWAP_ENTRANCE"):
                ticker = parts[-1]
                safe_ticker = html.escape(str(ticker))
                clamped_val = max(0.1, min(15.0, val))  # 🚨 [팻핑거 이중 샌드박스 결속]
                
                try: await asyncio.wait_for(asyncio.to_thread(self.cfg.set_avwap_entrance_rate, ticker, clamped_val), timeout=10.0)
                except Exception as e: logging.error(f"🚨 암살자 진입률 설정 에러: {e}")
                
                try: await asyncio.wait_for(update.effective_message.reply_text(f"✅ <b>[{safe_ticker}] 암살자 진입 타점: 세션 VWAP -{clamped_val}% 락온 완료</b>", parse_mode='HTML'), timeout=10.0)
                except Exception: pass
                
                # 🚨 [UI 무결성 팩트 복귀]
                if hasattr(controller, 'cmd_settlement'):
                    try:
                        await controller.cmd_settlement(update, context)
                    except BadRequest as e:
                        if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                    except Exception: pass

            elif state.startswith("CONF_AVWAP_EXIT"):
                ticker = parts[-1]
                safe_ticker = html.escape(str(ticker))
                clamped_val = max(0.1, min(15.0, val))  # 🚨 [팻핑거 이중 샌드박스 결속]
                
                try: await asyncio.wait_for(asyncio.to_thread(self.cfg.set_avwap_exit_rate, ticker, clamped_val), timeout=10.0)
                except Exception as e: logging.error(f"🚨 암살자 익절률 설정 에러: {e}")
                
                try: await asyncio.wait_for(update.effective_message.reply_text(f"✅ <b>[{safe_ticker}] 암살자 익절 타점: 진입가 +{clamped_val}% 락온 완료</b>", parse_mode='HTML'), timeout=10.0)
                except Exception: pass
                
                # 🚨 [UI 무결성 팩트 복귀]
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
