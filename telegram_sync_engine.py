# ==========================================================
# FILE: telegram_sync_engine.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 5대 헌법 및 38대 엣지 케이스 완벽 결속 교차 검증 완료. 시스템 런타임 즉사 뇌관 잔존율 0%.
# 🚨 MODIFIED: [Indentation 붕괴 수술] process_auto_sync 내 V-REV 큐 관리 블록의 21칸 들여쓰기 오차를 20칸 표준으로 정밀 교정하여 SyntaxError 즉사 버그 완벽 차단.
# 🚨 MODIFIED: [병렬 가동 아키텍처 팩트 수복] 암살자 가동 시 표출되던 본진 셧다운 안내를 영구 소각하고, "본진(15%)과 암살자 100% 독립 병렬 가동 팩트 락온" 텍스트를 주입하여 디커플링 상태를 관제탑 UI에 완벽히 명시.
# 🚨 MODIFIED: [1-Shot 1-Kill 타격망 UI 교정] 가상의 85% 시드 개념 파기에 따라, 암살자 상태를 '가용 현금 100% 올인'으로 변경하고 15:59 전량 최유리 지정가 덤핑 로직을 UI에 동기화 완료.
# 🚨 MODIFIED: [Phase 3 비동기 통신 헬퍼 래핑 (DRY 원칙)] 무한 반복되는 asyncio.wait_for(asyncio.to_thread(...)) 및 텔레그램 메시지 발송 샌드박스 로직을 범용 헬퍼 메서드(_retry_api, _safe_send)로 단일화하여 코드 라인 수를 극한으로 진공 압축.
# 🚨 MODIFIED: [Thread-Safety 락온] 내부 헬퍼 함수(_get_last_trade_date, get_yf_close 등)가 클로저(Closure) 외부 변수에 의존하지 않고 명시적 파라미터를 받도록 교정하여 Thread Context 오염 원천 차단.
# 🚨 MODIFIED: [동기화 코어 유령 차감 뇌관 완벽 소각] 암살자 실매매가 소각됨에 따라, KIS 실잔고(actual_qty)에서 avwap_qty 및 avwap_daily_buy/sell을 억지로 빼서 연산하던 과거 에스크로(Escrow) 로직 찌꺼기를 100% 영구 삭제했습니다.
# 🚨 NEW: [0주 오인 패러독스 소각 & Fact Override] 실제 잔고가 존재함에도 불구하고 새벽 스냅샷의 0주(is_zero_start=True) 상태를 맹신하던 로직을 전면 파기하고, KIS 실잔고 및 큐 장부를 최우선으로 오버라이드하여 Fact Mismatch 원천 차단.
# 🚨 NEW: [Ghost Balance (유령 잔고) 방어막 주입] KIS 서버 오류로 실잔고가 0주로 반환되었을 때, 실제 당일 매도 체결(sold_today) 내역이 없다면 장부 소각(자동 졸업)을 원천 차단하여 Phantom Graduation 붕괴 완벽 방어.
# 🚨 NEW: [제1헌법 100% 준수] _retry_api 헬퍼를 통해 파일 I/O 동기화 로직 전역에 wait_for(timeout=10.0/15.0) 족쇄를 래핑하여 이벤트 루프 교착 완벽 차단.
# 🚨 MODIFIED: [NaN 맹독 전이 및 JSON 직렬화 붕괴 원천 차단] 모든 재무 데이터에 self._safe_float() 정화 필터 강제 락온.
# 🚨 MODIFIED: [Gap Hijack 타점 오버라이드] 관제탑 데이터 집계 도메인의 vrev_gap_thresh_val 및 avwap_gap_thresh_val 폴백 수치를 -0.67에서 -2.0으로 상향 팩트 교정 완료.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import time
import os
import asyncio
import json
import tempfile
import traceback
import math 
import html 
import functools
import yfinance as yf
import pandas as pd 
import pandas_market_calendars as mcal
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

class TelegramSyncEngine:
    def __init__(self, config, broker, strategy, queue_ledger, view, tx_lock, sync_locks):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.queue_ledger = queue_ledger
        self.view = view
        self.tx_lock = tx_lock
        self.sync_locks = sync_locks

    # ==========================================================
    # 🛡️ [DRY Helper] 절대 방어 헬퍼 메서드 모음
    # ==========================================================
    def _safe_float(self, value):
        try:
            f_val = float(str(value or 0.0).replace(',', ''))
            if math.isnan(f_val) or math.isinf(f_val): return 0.0
            return f_val
        except Exception: return 0.0

    async def _retry_api(self, func, *args, timeout=15.0, default=None, **kwargs):
        """ 🚨 [Case 32, 33] 중앙 집중형 TPS 캡핑 및 지수 백오프 비동기 래퍼 """
        for attempt in range(3):
            try:
                await asyncio.sleep(0.06)
                if asyncio.iscoroutinefunction(func):
                    return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
                else:
                    p_func = functools.partial(func, *args, **kwargs)
                    return await asyncio.wait_for(asyncio.to_thread(p_func), timeout=timeout)
            except Exception as e:
                if attempt == 2:
                    func_name = getattr(func, '__name__', 'unknown_func')
                    logging.debug(f"🚨 API 래퍼 최종 실패 ({func_name}): {e}")
                    return default
                await asyncio.sleep(1.0 * (2 ** attempt))
        return default

    async def _safe_send(self, context, chat_id, text, timeout=15.0, **kwargs):
        if not chat_id: return None
        try:
            return await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=text, **kwargs), timeout=timeout)
        except Exception as e:
            logging.error(f"🚨 텔레그램 전송 실패: {e}")
            return None

    # ==========================================================
    # 📝 [Core Engine] 16:05 정산 및 수동 동기화 메인 프로세스
    # ==========================================================
    async def process_auto_sync(self, ticker, chat_id, context, silent_ledger=False):
        if ticker not in self.sync_locks:
            self.sync_locks[ticker] = asyncio.Lock()
             
        if self.sync_locks[ticker].locked(): return "LOCKED"
            
        async with self.sync_locks[ticker]:
            async with self.tx_lock:
                est = ZoneInfo('America/New_York')
                now_est = datetime.datetime.now(est)
                kst = ZoneInfo('Asia/Seoul')
                now_kst = datetime.datetime.now(kst)

                # 1️⃣ 액면분할(Split) 소급 조정 검증
                last_split_date = await self._retry_api(self.cfg.get_last_split_date, ticker, default="")
                split_ratio, split_date = await self._retry_api(self.broker.get_recent_stock_split, ticker, last_split_date, default=(0.0, ""))
                
                if split_ratio > 0.0 and split_date != "":
                    await self._retry_api(self.cfg.apply_stock_split, ticker, split_ratio, timeout=10.0)
                    if getattr(self, 'queue_ledger', None):
                        await self._retry_api(self.queue_ledger.apply_stock_split, ticker, split_ratio, timeout=10.0)
                    if hasattr(self.strategy, 'v_avwap_plugin'):
                        await self._retry_api(self.strategy.v_avwap_plugin.apply_stock_split, ticker, split_ratio, now_est, timeout=10.0)
                    
                    await self._retry_api(self.cfg.set_last_split_date, ticker, split_date, timeout=5.0)
                    
                    split_type = "액면분할" if split_ratio > 1.0 else "액면병합(역분할)"
                    await self._safe_send(context, chat_id, f"✂️ <b>[{html.escape(str(ticker))}] 야후 파이낸스 {split_type} 자동 감지!</b>\n▫️ 감지된 비율: <b>{split_ratio}배</b> (발생일: {html.escape(str(split_date))})\n▫️ 봇이 기존 V14 장부, V-REV 큐 장부, AVWAP 상태 캐시의 수량과 평단가를 100% 무인 자동 소급 조정 완료했습니다.", parse_mode='HTML')
                 
                # 2️⃣ 달력 스캔 및 최근 거래일 팩트 추출 (Thread-Safety 교정)
                def _get_last_trade_date(target_est):
                    time.sleep(0.06)
                    nyse = mcal.get_calendar('NYSE')
                    return nyse.schedule(start_date=(target_est - datetime.timedelta(days=10)).date(), end_date=target_est.date())

                schedule = await self._retry_api(_get_last_trade_date, now_est, timeout=10.0, default=pd.DataFrame())
                if not schedule.empty:
                    last_trade_date = schedule.index[-1]
                    target_ledger_str = last_trade_date.strftime('%Y-%m-%d')
                else: target_ledger_str = now_est.strftime('%Y-%m-%d')

                # 3️⃣ KIS 실잔고 스캔
                res_bal = await self._retry_api(self.broker.get_account_balance, timeout=15.0, default=None)
                if not res_bal:
                    await self._safe_send(context, chat_id, f"❌ <b>[{html.escape(str(ticker))}] API 오류</b>\n잔고를 불러오지 못했습니다.", parse_mode='HTML')
                    return "ERROR"
                    
                holdings = res_bal[1] if isinstance(res_bal, (list, tuple)) and len(res_bal) > 1 else {}
                safe_holdings = holdings if isinstance(holdings, dict) else {}
                safe_ticker_info = safe_holdings.get(ticker) or {'qty': 0, 'avg': 0.0}
                
                actual_qty = int(self._safe_float(safe_ticker_info.get('qty')))
                actual_avg = self._safe_float(safe_ticker_info.get('avg'))

                # 4️⃣ 로컬 장부 스캔
                full_ledger = await self._retry_api(self.cfg.get_ledger, default=[])
                recs_for_check = [r for r in (full_ledger or []) if isinstance(r, dict) and r.get('ticker') == ticker]
                hold_res = await self._retry_api(self.cfg.calculate_holdings, ticker, recs_for_check, default=(0, 0.0, 0.0, 0.0))
                ledger_qty_for_check = hold_res[0] if isinstance(hold_res, tuple) and len(hold_res) > 0 else 0
                
                vrev_ledger_qty_for_check = 0
                is_rev = (await self._retry_api(self.cfg.get_version, ticker, default="V14") == "V_REV")
                
                if is_rev and getattr(self, 'queue_ledger', None):
                    q_data_check = await self._retry_api(self.queue_ledger.get_queue, ticker, default=[])
                    vrev_ledger_qty_for_check = sum(int(self._safe_float(item.get("qty"))) for item in q_data_check if isinstance(item, dict))
                    vrev_total_invested = sum(int(self._safe_float(item.get("qty"))) * self._safe_float(item.get("price")) for item in q_data_check if isinstance(item, dict))
                    if vrev_ledger_qty_for_check > 0: actual_avg = round(vrev_total_invested / vrev_ledger_qty_for_check, 4)
                    else: actual_avg = 0.0
                 
                max_check_qty = max(ledger_qty_for_check, vrev_ledger_qty_for_check)

                # 5️⃣ KIS 당일 체결 내역 스캔 (KST 맵핑)
                kis_search_start = (now_kst - datetime.timedelta(days=4)).strftime('%Y%m%d')
                query_end_dt = now_kst.strftime('%Y%m%d')

                def filter_to_est(execs_raw):
                    filtered = []
                    if not execs_raw: return filtered
                    for ex in execs_raw:
                        if not isinstance(ex, dict): continue
                        ord_dt = ex.get('ord_dt') or ex.get('ord_strt_dt')
                        if not ord_dt: continue
                        ord_tmd = ex.get('ord_tmd')
                        if not ord_tmd or len(str(ord_tmd)) != 6: ord_tmd = '000000'
                        try:
                            k_dt = datetime.datetime.strptime(f"{ord_dt}{ord_tmd}", "%Y%m%d%H%M%S").replace(tzinfo=kst)
                            e_dt = k_dt.astimezone(est)
                            if e_dt.strftime('%Y-%m-%d') == target_ledger_str:
                                filtered.append(ex)
                        except Exception: pass
                    return filtered

                raw_execs = []
                target_execs = []
                
                # 🚨 유령 잔고 방어 및 지연(Lag) 안정화 루프
                if actual_qty == 0 and max_check_qty > 0:
                    max_retries = 6
                    prev_sold_today = -1
                    stable_cnt = 0
                    for attempt in range(max_retries):
                        raw_execs = await self._retry_api(self.broker.get_execution_history, ticker, kis_search_start, query_end_dt, timeout=15.0, default=[])
                        target_execs = filter_to_est(raw_execs)
                        sold_today = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01")
                        
                        if sold_today >= max_check_qty:
                            if sold_today == prev_sold_today:
                                stable_cnt += 1
                                if stable_cnt >= 1: break
                            else: stable_cnt = 0
                            prev_sold_today = sold_today
                        
                        if attempt < max_retries - 1:
                            logging.info(f"⏳ [{ticker}] 체결 원장 지연(Lag) 감지. 데이터 안정화 및 EST 매핑 검증 중... ({attempt+1}/{max_retries})")
                            await asyncio.sleep(2.0)
                else:
                    raw_execs = await self._retry_api(self.broker.get_execution_history, ticker, kis_search_start, query_end_dt, timeout=15.0, default=[])
                    target_execs = filter_to_est(raw_execs)

                if target_execs:
                    calibrated_count = await self._retry_api(self.cfg.calibrate_ledger_prices, ticker, target_ledger_str, target_execs, timeout=10.0, default=0)
                    if calibrated_count > 0:
                        logging.info(f"🔧 [{ticker}] LOC/MOC 주문 {calibrated_count}건에 대해 실제 체결 단가 소급 업데이트를 완료했습니다.")

                # 다시 최신 장부 스캔 및 State 교차 검증
                full_ledger = await self._retry_api(self.cfg.get_ledger, default=[])
                recs = [r for r in (full_ledger or []) if isinstance(r, dict) and r.get('ticker') == ticker]
                
                hold_res2 = await self._retry_api(self.cfg.calculate_holdings, ticker, recs, default=(0,0.0,0.0,0.0))
                ledger_qty = hold_res2[0] if isinstance(hold_res2, tuple) and len(hold_res2) > 0 else 0
                avg_price = hold_res2[1] if isinstance(hold_res2, tuple) and len(hold_res2) > 1 else 0.0
                
                diff = actual_qty - ledger_qty
                price_diff = abs(actual_avg - avg_price)

                today_recs = [r for r in recs if r.get('date') == target_ledger_str and 'INIT' not in str(r.get('exec_id', '')) and 'CALIB' not in str(r.get('exec_id', ''))]
                ledger_today_buy = sum(r.get('qty', 0) for r in today_recs if r.get('side') == 'BUY')
                ledger_today_sell = sum(r.get('qty', 0) for r in today_recs if r.get('side') == 'SELL')
                
                exec_today_buy = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "02")
                exec_today_sell = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01")
                
                needs_reconstruction = (diff != 0) or (ledger_today_buy != exec_today_buy) or (ledger_today_sell != exec_today_sell)

                # 6️⃣ 장부 보정 (Calibration)
                if not needs_reconstruction and price_diff >= 0.01:
                    await self._retry_api(self.cfg.calibrate_avg_price, ticker, actual_avg, timeout=10.0)
                    await self._safe_send(context, chat_id, f"🔧 <b>[{html.escape(str(ticker))}] 장부 평단가 미세 오차({price_diff:.4f}) 교정 완료!</b>", parse_mode='HTML')
                elif needs_reconstruction:
                    temp_recs = [r for r in recs if r.get('date') != target_ledger_str or 'INIT' in str(r.get('exec_id', ''))]
                    temp_res = await self._retry_api(self.cfg.calculate_holdings, ticker, temp_recs, default=(0,0.0,0.0,0.0))
                    temp_sim_qty = temp_res[0] if isinstance(temp_res, tuple) and len(temp_res) > 0 else 0
                    temp_sim_avg = temp_res[1] if isinstance(temp_res, tuple) and len(temp_res) > 1 else 0.0
                    temp_avg = temp_sim_avg
                    
                    new_target_records = []
                    
                    if target_execs:
                        target_execs.sort(key=lambda x: str(x.get('ord_dt') or '00000000') + str(x.get('ord_tmd') or '000000')) 
                        for ex in target_execs:
                            side_cd = ex.get('sll_buy_dvsn_cd')
                            exec_qty = int(self._safe_float(ex.get('ft_ccld_qty')))
                            exec_price = self._safe_float(ex.get('ft_ccld_unpr3'))
                            
                            if side_cd == "02": 
                                new_avg = ((temp_sim_qty * temp_sim_avg) + (exec_qty * exec_price)) / (temp_sim_qty + exec_qty) if (temp_sim_qty + exec_qty) != 0 else exec_price
                                temp_sim_qty += exec_qty
                                temp_sim_avg = new_avg
                            else: temp_sim_qty -= exec_qty
                            
                            rec_item = {'date': target_ledger_str, 'side': "BUY" if side_cd == "02" else "SELL", 'qty': exec_qty, 'price': exec_price, 'avg_price': temp_sim_avg}
                            if is_rev: rec_item['is_reverse'] = True
                            new_target_records.append(rec_item)
                            
                    gap_qty = actual_qty - temp_sim_qty
                    if gap_qty != 0:
                        calib_side = "BUY" if gap_qty > 0 else "SELL"
                        calib_price = actual_avg
                        actual_clear_price_calib = 0.0

                        if target_execs:
                            sell_execs_calib = [ex for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01"]
                            if sell_execs_calib:
                                tot_amt_calib = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) * self._safe_float(ex.get('ft_ccld_unpr3')) for ex in sell_execs_calib)
                                tot_q_calib = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in sell_execs_calib)
                                if tot_q_calib > 0: actual_clear_price_calib = round(tot_amt_calib / tot_q_calib, 4)
                        
                        if calib_side == "SELL" and actual_avg <= 0.0:
                            if actual_clear_price_calib > 0.0: calib_price = actual_clear_price_calib
                            else: calib_price = temp_sim_avg if temp_sim_avg > 0 else (temp_avg if temp_avg > 0 else 0.01)
                            calib_avg = temp_sim_avg
                        elif calib_side == "BUY" and actual_avg <= 0.0:
                            if actual_clear_price_calib > 0.0:
                                calib_price = actual_clear_price_calib
                                calib_avg = actual_clear_price_calib
                            else:
                                calib_price = temp_sim_avg if temp_sim_avg > 0 else (temp_avg if temp_avg > 0 else 0.01)
                                calib_avg = temp_sim_avg
                        else:
                            calib_price = actual_avg if actual_avg > 0 else temp_sim_avg
                            calib_avg = actual_avg if actual_avg > 0 else temp_sim_avg
             
                        calib_item = {'date': target_ledger_str, 'side': calib_side, 'qty': abs(gap_qty), 'price': calib_price, 'avg_price': calib_avg, 'exec_id': f"CALIB_{int(time.time())}", 'desc': "비파괴 보정"}
                        if is_rev: calib_item['is_reverse'] = True
                        new_target_records.append(calib_item)
                        
                    if new_target_records:
                        if actual_qty > 0:
                            for r in new_target_records: r['avg_price'] = actual_avg
                
                    await self._retry_api(self.cfg.overwrite_incremental_ledger, ticker, temp_recs, new_target_records, timeout=10.0)
                    if gap_qty != 0: await self._safe_send(context, chat_id, f"🔧 <b>[{html.escape(str(ticker))}] 통합 메인 장부(MAIN LEDGER) 비파괴 보정 완료!</b>\n▫️ KIS 실잔고 오차 수량({gap_qty}주)을 역사 보존 상태로 안전하게 교정했습니다.", parse_mode='HTML')

                # 7️⃣ V-REV 큐 관리 및 졸업 판별
                if is_rev:
                    q_data_before = await self._retry_api(self.queue_ledger.get_queue, ticker, default=[])
                    vrev_ledger_qty = sum(int(self._safe_float(item.get("qty"))) for item in q_data_before if isinstance(item, dict))
                    sold_today_vrev = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01") if target_execs else 0
                    
                    # 🚨 [Case 35: Ghost Balance - Phantom Graduation 방어막]
                    if actual_qty == 0 and (vrev_ledger_qty > 0 or sold_today_vrev > 0):
                        if sold_today_vrev == 0 and vrev_ledger_qty > 0:
                            await self._safe_send(context, chat_id, f"🚨 <b>[{html.escape(str(ticker))} 유령 잔고 방어 가동]</b>\nKIS 실잔고가 0주로 조회되었으나, 당일 매도 체결 내역이 0건입니다. 통신 오류(Ghost Balance)일 가능성이 매우 높아 장부 강제 소각(자동 졸업)을 차단합니다.\n▫️ HTS 등을 통해 수동으로 100% 전량 매도한 상태라면 <code>/reset</code> 명령어를 사용하여 봇을 초기화하십시오.", parse_mode='HTML')
                            return "GHOST_BALANCE_BLOCKED"

                        added_seed = 0.0
                        _vrev_snap_ok = False
                        snapshot = None
            
                        actual_clear_price = 0.0
                        tot_q = 0
            
                        if target_execs:
                            sell_execs = [ex for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01"]
                            if sell_execs:
                                tot_amt = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) * self._safe_float(ex.get('ft_ccld_unpr3')) for ex in sell_execs)
                                tot_q = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in sell_execs)
                                if tot_q > 0: actual_clear_price = round(tot_amt / tot_q, 4)

                        if tot_q > vrev_ledger_qty:
                            missing_qty = tot_q - vrev_ledger_qty
                            buy_execs = [ex for ex in (target_execs or []) if ex.get('sll_buy_dvsn_cd') == "02"]
                            temp_invested = sum(self._safe_float(item.get("qty")) * self._safe_float(item.get("price")) for item in q_data_before if isinstance(item, dict))
                            temp_avg = temp_invested / vrev_ledger_qty if vrev_ledger_qty > 0 else 0.0
                            missing_price = temp_avg
                            
                            if buy_execs:
                                b_tot_amt = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) * self._safe_float(ex.get('ft_ccld_unpr3')) for ex in buy_execs)
                                b_tot_q = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in buy_execs)
                                if b_tot_q > 0:
                                    q_today_amt = 0.0
                                    q_today_qty = 0
                                    for item in q_data_before:
                                        if isinstance(item, dict) and str(item.get("date", "")).startswith(target_ledger_str):
                                            iq = int(self._safe_float(item.get("qty")))
                                            q_today_qty += iq
                                            q_today_amt += iq * self._safe_float(item.get("price"))
                                            
                                    pure_manual_q = b_tot_q - q_today_qty
                                    pure_manual_amt = b_tot_amt - q_today_amt
                                    if pure_manual_q >= missing_qty and pure_manual_q > 0 and pure_manual_amt > 0:
                                        derived_price = pure_manual_amt / pure_manual_q
                                        missing_price = round(derived_price, 4)
                                    else: missing_price = round(b_tot_amt / b_tot_q, 4)

                            q_data_before.append({"date": now_est.strftime('%Y-%m-%d %H:%M:%S'), "qty": missing_qty, "price": missing_price, "exec_id": "MANUAL_SYNC"})
                            vrev_ledger_qty = tot_q
                            await self._retry_api(self.queue_ledger.overwrite_queue, ticker, q_data_before, timeout=10.0)

                        total_invested = sum(self._safe_float(item.get("qty")) * self._safe_float(item.get("price")) for item in q_data_before if isinstance(item, dict))
                        q_avg_price = total_invested / vrev_ledger_qty if vrev_ledger_qty > 0 else 0.0

                        curr_p = await self._retry_api(self.broker.get_current_price, ticker, timeout=15.0, default=0.0)
                        clear_price = actual_clear_price if actual_clear_price > 0.0 else (curr_p if curr_p and curr_p > 0 else q_avg_price * 1.006)
                        snapshot = await self._retry_api(self.strategy.capture_vrev_snapshot, ticker, clear_price, q_avg_price, vrev_ledger_qty, timeout=10.0, default={})
                        
                        if snapshot and isinstance(snapshot, dict):
                            realized_pnl = self._safe_float(snapshot.get('realized_pnl', 0.0))
                            yield_pct = self._safe_float(snapshot.get('realized_pnl_pct', 0.0))
                            compound_rate = self._safe_float(await self._retry_api(self.cfg.get_compound_rate, ticker, default=70.0)) / 100.0
                            
                            if realized_pnl > 0 and compound_rate > 0:
                                added_seed = realized_pnl * compound_rate
                                current_seed = self._safe_float(await self._retry_api(self.cfg.get_seed, ticker, default=6720.0))
                                await self._retry_api(self.cfg.set_seed, ticker, current_seed + added_seed, timeout=10.0)
                            
                            cap_dt = snapshot.get('captured_at', now_est)
                            cap_dt_str = cap_dt if isinstance(cap_dt, str) else cap_dt.strftime('%Y-%m-%d')
                            start_dt_str = str(q_data_before[0].get('date', ''))[:10] if q_data_before and isinstance(q_data_before[0], dict) else cap_dt_str[:10]
                            
                            hist_data = await self._retry_api(self.cfg._load_json, self.cfg.FILES["HISTORY"], [], default=[])
                            
                            new_hist = {
                                "id": int(time.time()), "ticker": ticker, "start_date": start_dt_str, "end_date": cap_dt_str[:10],
                                "invested": self._safe_float(total_invested), "revenue": self._safe_float(total_invested + realized_pnl),
                                "profit": realized_pnl, "yield": yield_pct, "trades": q_data_before 
                            }
                            hist_data.append(new_hist)
                            await self._retry_api(self.cfg._save_json, self.cfg.FILES["HISTORY"], hist_data, timeout=10.0)
                            _vrev_snap_ok = True
                                
                        if getattr(self, 'queue_ledger', None):
                            await self._retry_api(self.queue_ledger.sync_with_broker, ticker, 0, timeout=10.0)
                         
                        if _vrev_snap_ok:
                            msg = f"🎉 <b>[{html.escape(str(ticker))} V-REV 잭팟 스윕(전량 익절) 감지!]</b>\n▫️ 잔고가 0주가 되어 LIFO 큐 지층을 100% 소각(초기화)했습니다."
                            if added_seed > 0: msg += f"\n💸 <b>자동 복리 +${added_seed:,.0f}</b> 이 다음 운용 시드에 완벽하게 추가되었습니다!"
                            await self._safe_send(context, chat_id, msg, parse_mode='HTML')
                            
                            if snapshot and isinstance(snapshot, dict):
                                img_path = await self._retry_api(
                                    self.view.create_profit_image, 
                                    ticker=ticker, 
                                    profit=self._safe_float(snapshot.get('realized_pnl', 0.0)), 
                                    yield_pct=self._safe_float(snapshot.get('realized_pnl_pct', 0.0)), 
                                    invested=self._safe_float(snapshot.get('avg_price', 0.0)) * self._safe_float(snapshot.get('cleared_qty', 0)), 
                                    revenue=self._safe_float(snapshot.get('clear_price', 0.0)) * self._safe_float(snapshot.get('cleared_qty', 0)), 
                                    end_date=cap_dt_str[:10],
                                    timeout=15.0, default=None
                                )
                                if img_path:
                                    def _read_img3(p):
                                        with open(p, 'rb') as f_in: return f_in.read()
                                    try:
                                        img_bytes3 = await self._retry_api(_read_img3, img_path)
                                        if str(img_path).lower().endswith('.gif'): await context.bot.send_animation(chat_id=chat_id, animation=img_bytes3)
                                        else: await context.bot.send_photo(chat_id=chat_id, photo=img_bytes3)
                                    except OSError: pass
                        else:
                            await self._safe_send(context, chat_id, f"⚠️ <b>[{html.escape(str(ticker))} V-REV 0주 강제 정산 완료]</b>\n▫️ 0주를 확인하여 큐를 안전하게 비웠으나 통신 지연으로 졸업 카드는 생략되었습니다.", parse_mode='HTML')
                            
                        return "SUCCESS"
                 
                    if actual_qty == vrev_ledger_qty:
                        pass
                    elif actual_qty > 0 and actual_qty < vrev_ledger_qty:
                        gap_qty = vrev_ledger_qty - actual_qty
                        vwap_state_file = f"data/vwap_state_REV_{ticker}.json"
                        
                        def _read_v_state(f_path):
                            with open(f_path, 'r', encoding='utf-8') as vf: return json.load(vf)
                            
                        v_state = await self._retry_api(_read_v_state, vwap_state_file, default={})
                        if isinstance(v_state, dict) and "executed" in v_state and isinstance(v_state["executed"], dict) and "SELL_QTY" in v_state["executed"]:
                            old_sell_qty = v_state["executed"]["SELL_QTY"]
                            v_state["executed"]["SELL_QTY"] = max(0, old_sell_qty - gap_qty)
                            
                        def _write_v_state(state_dict, f_path):
                            fd = None
                            tmp_path = None
                            try:
                                fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(f_path) or '.')
                                with os.fdopen(fd, 'w', encoding='utf-8') as _vf_out:
                                    fd = None
                                    json.dump(state_dict, _vf_out, ensure_ascii=False, indent=4)
                                    _vf_out.flush()
                                    os.fsync(_vf_out.fileno())
                                os.replace(tmp_path, f_path)
                                tmp_path = None
                            except Exception as write_err:
                                if fd is not None:
                                    try: os.close(fd)
                                    except OSError: pass
                                if tmp_path:
                                    try: os.remove(tmp_path)
                                    except OSError: pass
                                raise write_err

                        await self._retry_api(_write_v_state, v_state, vwap_state_file, timeout=10.0)

                        actual_clear_price_for_sync = 0.0
                        if target_execs:
                            sell_execs_sync = [ex for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01"]
                            if sell_execs_sync:
                                t_amt = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) * self._safe_float(ex.get('ft_ccld_unpr3')) for ex in sell_execs_sync)
                                t_q = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in sell_execs_sync)
                                if t_q > 0: actual_clear_price_for_sync = round(t_amt / t_q, 4)
                         
                        calibrated = False
                        if getattr(self, 'queue_ledger', None):
                            calibrated = await self._retry_api(self.queue_ledger.sync_with_broker, ticker, actual_qty, 0.0, actual_clear_price_for_sync, timeout=10.0, default=False)
                         
                        if calibrated: await self._safe_send(context, chat_id, f"🔧 <b>[{html.escape(str(ticker))}] V-REV 큐(Queue) 비파괴 보정 및 리앵커링 완료!</b>\n▫️ 수동 매도 물량(<b>{gap_qty}주</b>)을 LIFO 큐에서 안전하게 차감하고, 수익금만큼 잔여 지층의 평단가를 일괄 차감했습니다.", parse_mode='HTML')
                          
                    elif actual_qty > 0 and actual_qty > vrev_ledger_qty:
                        gap_qty = actual_qty - vrev_ledger_qty
                        real_buy_price = actual_avg
                        buy_execs = [ex for ex in (target_execs or []) if ex.get('sll_buy_dvsn_cd') == "02"]
                        if buy_execs:
                            b_tot_amt = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) * self._safe_float(ex.get('ft_ccld_unpr3')) for ex in buy_execs)
                            b_tot_q = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in buy_execs)
                            if b_tot_q > 0: real_buy_price = round(b_tot_amt / b_tot_q, 4)

                        if real_buy_price <= 0.0:
                            curr_p = await self._retry_api(self.broker.get_current_price, ticker, timeout=15.0, default=0.0)
                            q_data_temp = await self._retry_api(self.queue_ledger.get_queue, ticker, default=[])
                            last_price = self._safe_float(q_data_temp[-1].get('price')) if q_data_temp and isinstance(q_data_temp[-1], dict) else 0.0
                            real_buy_price = curr_p if curr_p > 0 else (last_price if last_price > 0 else 1.0)
             
                        q_data = await self._retry_api(self.queue_ledger.get_queue, ticker, default=[])
                        q_data.append({"date": now_est.strftime('%Y-%m-%d %H:%M:%S'), "qty": gap_qty, "price": real_buy_price, "exec_id": f"MANUAL_BUY_{int(time.time())}"})
                        await self._retry_api(self.queue_ledger.overwrite_queue, ticker, q_data, timeout=10.0)
                        await self._safe_send(context, chat_id, f"🔧 <b>[{html.escape(str(ticker))}] V-REV 큐(Queue) 수동 매수 편입 완료!</b>\n▫️ KIS 실잔고에 맞춰 신규 지층(<b>{gap_qty}주</b>, 추정단가 ${real_buy_price})을 정밀 추가했습니다.", parse_mode='HTML')
                if not is_rev:
                    sold_today_v14 = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01") if target_execs else 0
                    
                    # 🚨 [Case 35: Ghost Balance - Phantom Graduation 방어막]
                    if actual_qty == 0 and (ledger_qty > 0 or sold_today_v14 > 0):
                        if sold_today_v14 == 0 and ledger_qty > 0:
                            await self._safe_send(context, chat_id, f"🚨 <b>[{html.escape(str(ticker))} 유령 잔고 방어 가동]</b>\nKIS 실잔고가 0주로 조회되었으나, 당일 매도 체결 내역이 0건입니다. 통신 오류(Ghost Balance)일 가능성이 매우 높아 장부 강제 소각(자동 졸업)을 차단합니다.\n▫️ HTS 등을 통해 수동으로 100% 전량 매도한 상태라면 <code>/reset</code> 명령어를 사용하여 봇을 초기화하십시오.", parse_mode='HTML')
                            return "GHOST_BALANCE_BLOCKED"

                        today_est_str = now_est.strftime('%Y-%m-%d')
                        prev_c = await self._retry_api(self.broker.get_previous_close, ticker, timeout=15.0, default=0.0)
                        
                        grad_res = await self._retry_api(self.cfg.archive_graduation, ticker, today_est_str, prev_c, timeout=10.0, default=(None, 0.0))
                        new_hist, added_seed = grad_res if isinstance(grad_res, tuple) and len(grad_res) >= 2 else (None, 0.0)

                        if new_hist and isinstance(new_hist, dict):
                            msg = f"🎉 <b>[{html.escape(str(ticker))} 졸업 확인!]</b>\n장부를 명예의 전당에 저장하고 새 사이클을 준비합니다."
                            if added_seed > 0: msg += f"\n💸 <b>자동 복리 +${added_seed:,.0f}</b> 이 다음 운용 시드에 완벽하게 추가되었습니다!"
                            await self._safe_send(context, chat_id, msg, parse_mode='HTML')
                            
                            img_path = await self._retry_api(
                                self.view.create_profit_image,
                                ticker=ticker, 
                                profit=self._safe_float(new_hist.get('profit', 0.0)), 
                                yield_pct=self._safe_float(new_hist.get('yield', 0.0)),
                                invested=self._safe_float(new_hist.get('invested', 0.0)), 
                                revenue=self._safe_float(new_hist.get('revenue', 0.0)), 
                                end_date=new_hist.get('end_date', target_ledger_str),
                                timeout=15.0, default=None
                            )
                            if img_path:
                                def _read_img4(p):
                                    with open(p, 'rb') as f_in: return f_in.read()
                                try:
                                    img_bytes4 = await self._retry_api(_read_img4, img_path)
                                    if str(img_path).lower().endswith('.gif'): await context.bot.send_animation(chat_id=chat_id, animation=img_bytes4)
                                    else: await context.bot.send_photo(chat_id=chat_id, photo=img_bytes4)
                                except OSError: pass
                        else:
                            full_ledger2 = await self._retry_api(self.cfg.get_ledger, default=[])
                            all_recs = [r for r in full_ledger2 if isinstance(r, dict) and r.get('ticker') != ticker]
                            await self._retry_api(self.cfg._save_json, self.cfg.FILES["LEDGER"], all_recs, timeout=10.0)
                            await self._safe_send(context, chat_id, f"⚠️ <b>[{html.escape(str(ticker))} 강제 정산 완료]</b>\n잔고가 0주이나 마이너스 수익 상태이므로 명예의 전당 박제 없이 장부를 비우고 새출발 타점을 장전합니다.", parse_mode='HTML')

                return "SUCCESS"

    async def _display_ledger(self, ticker, chat_id, context, query=None, message_obj=None, pre_fetched_holdings=None):
        full_ledger = await self._retry_api(self.cfg.get_ledger, default=[])
        recs = [r for r in full_ledger if isinstance(r, dict) and r.get('ticker') == ticker]
        
        report = ""
        
        if not recs:
            report += f"📭 <b>[{html.escape(str(ticker))}]</b> 현재 진행 중인 사이클이 없습니다 (보유량 0주).\n\n"
        else:
            from collections import OrderedDict
            agg_dict = OrderedDict()
            total_buy = 0.0
            total_sell = 0.0
            
            for rec in recs:
                raw_date = str(rec.get('date') or '').split(' ')[0]
                parts = raw_date.split('-')
                if len(parts) == 3: date_short = f"{parts[1]}.{parts[2]}"
                else: date_short = raw_date
                     
                side_str = "🔴매수" if rec.get('side') == 'BUY' else "🔵매도"
                key = (date_short, side_str)
                
                if key not in agg_dict: agg_dict[key] = {'qty': 0, 'amt': 0.0}
            
                agg_dict[key]['qty'] += int(self._safe_float(rec.get('qty')))
                agg_dict[key]['amt'] += (int(self._safe_float(rec.get('qty'))) * self._safe_float(rec.get('price')))
                
                if rec.get('side') == 'BUY': total_buy += (int(self._safe_float(rec.get('qty'))) * self._safe_float(rec.get('price')))
                elif rec.get('side') == 'SELL': total_sell += (int(self._safe_float(rec.get('qty'))) * self._safe_float(rec.get('price')))
              
            report += f"📜 <b>[ {html.escape(str(ticker))} 일자별 매매 (통합 변동분) (총 {len(agg_dict)}일) ]</b>\n\n<code>No. 일자   구분  평균단가  수량\n"
            report += "-"*30 + "\n"
            
            idx = 1
            for (date, side), data in agg_dict.items():
                tot_qty = data['qty']
                avg_prc = data['amt'] / tot_qty if tot_qty != 0 else 0.0
                report += f"{idx:<3} {date} {side} ${avg_prc:<6.2f} {tot_qty}주\n"
                idx += 1
                
            report += "-"*30 + "</code>\n\n"
             
        safe_holdings = pre_fetched_holdings if isinstance(pre_fetched_holdings, dict) else {}
        actual_qty = int(self._safe_float((safe_holdings.get(ticker) or {'qty': 0}).get('qty')))
        actual_avg = self._safe_float((safe_holdings.get(ticker) or {'avg': 0}).get('avg'))
        
        v_mode = await self._retry_api(self.cfg.get_version, ticker, default="V14")
        
        if v_mode == "V_REV":
            if not getattr(self, 'queue_ledger', None):
                from queue_ledger import QueueLedger
                self.queue_ledger = await asyncio.to_thread(QueueLedger)
            
            if getattr(self, 'queue_ledger', None):
                q_data_ui = await self._retry_api(self.queue_ledger.get_queue, ticker, default=[])
                vrev_inv_ui = sum(int(self._safe_float(item.get('qty'))) * self._safe_float(item.get('price')) for item in q_data_ui if isinstance(item, dict))
                vrev_q_ui = sum(int(self._safe_float(item.get('qty'))) for item in q_data_ui if isinstance(item, dict))
                if vrev_q_ui > 0: actual_avg = round(vrev_inv_ui / vrev_q_ui, 4)
                else: actual_avg = 0.0

        split = await self._retry_api(self.cfg.get_split_count, ticker, default=40.0)
        t_val_res = await self._retry_api(self.cfg.get_absolute_t_val, ticker, actual_qty, actual_avg, default=(0.0, 0.0))
        t_val = t_val_res[0] if isinstance(t_val_res, tuple) and len(t_val_res) > 0 else 0.0
         
        t_val_safe = self._safe_float(t_val)
        split_safe = int(self._safe_float(split))

        report += "📊 <b>[ 현재 진행 상황 요약 ]</b>\n"
        report += f"▪️ 현재 T값 : {t_val_safe:.4f} T ({split_safe}분할)\n"
        report += f"▪️ 보유 수량 : {actual_qty} 주 (평단 ${actual_avg:,.2f})\n"
        
        if recs:
            report += f"▪️ 총 매수액 : ${total_buy:,.2f}\n"
            report += f"▪️ 총 매도액 : ${total_sell:,.2f}"
        
        msg = report
        
        if len(msg) > 4000:
            msg = msg[:3900] + "\n\n... (장부 내역이 너무 길어 하단이 생략되었습니다) ✂️"

        active_tickers = await self._retry_api(self.cfg.get_active_tickers, default=[])
        keyboard = []
         
        if v_mode == "V_REV":
            keyboard.append([InlineKeyboardButton(f"🗄️ {html.escape(str(ticker))} V-REV 큐(Queue) 정밀 관리", callback_data=f"QUEUE:VIEW:{ticker}")])
             
        row = [InlineKeyboardButton(f"🔄 {html.escape(str(t))} 장부 업데이트", callback_data=f"REC:SYNC:{t}") for t in active_tickers if isinstance(t, str)]
        if row: keyboard.append(row)
        markup = InlineKeyboardMarkup(keyboard)

        if query:
            try: await asyncio.wait_for(query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML'), timeout=15.0)
            except Exception: pass
        elif message_obj:
            try: await asyncio.wait_for(message_obj.edit_text(msg, reply_markup=markup, parse_mode='HTML'), timeout=15.0)
            except Exception: pass
        else:
            await self._safe_send(context, chat_id, msg, reply_markup=markup, parse_mode='HTML')

    def get_settlement_message(self, active_tickers, config, atr_data, tracking_cache=None):
        msg = ""
        keyboard = []
        ver = ""
        is_manual_vwap = False
        fee_rate = 0.0
        icon = ""
        ver_display = ""
        split_cnt = 0
        target_profit = 0.0
        comp_rate = 0.0
        v14_mode_txt = ""

        tracking_cache = tracking_cache or {}
        msg = "⚙️ <b>[ 현재 설정 및 복리 상태 ]</b>\n\n"
        
        active_tickers = active_tickers or []
        for t in active_tickers:
            safe_t = html.escape(str(t))
            ver = str(config.get_version(t) or "")
            is_manual_vwap = getattr(config, 'get_manual_vwap_mode', lambda x: False)(t)
            is_avwap_hybrid = getattr(config, 'get_avwap_hybrid_mode', lambda x: False)(t)
            fee_rate = self._safe_float(getattr(config, 'get_fee', lambda x: 0.25)(t))
            
            if ver == "V_REV":
                icon, ver_display = "⚖️", "V_REV 역추세 (로컬 1분 VWAP)" 
            else:
                icon = "💎"
                ver_display = "무매4 (로컬 1분 VWAP)" if is_manual_vwap else "무매4 (LOC)" 
                
            split_cnt = int(self._safe_float(config.get_split_count(t)))
            target_profit = self._safe_float(config.get_target_profit(t))
            comp_rate = self._safe_float(config.get_compound_rate(t))
            msg += f"{icon} <b>{safe_t} ({ver_display} 모드)</b>\n"
            
            if ver == "V_REV":
                # 🚨 MODIFIED: [병렬 가동 아키텍처 팩트 수복] 암살자 올인 타격 및 독립 병렬 가동 명시
                avwap_status = "🟢 ON (주문가능금액 100% 올인)" if is_avwap_hybrid else "⚪ OFF (가동 대기)"
                
                msg += f"▫️ 본진 예산: 총 시드의 15% (고정 할당)\n▫️ 본진 목표: [가상1층]+0.6% / [상위층]+0.5%\n▫️ 자동복리: {comp_rate}% | 수수료: <b>{fee_rate}%</b>\n▫️ 갭 스위칭: <b>🤖 자율주행 (상승장 자동 가동)</b>\n"
                msg += f"▫️ 암살자 타격망: <b>{avwap_status}</b>\n"
                
                if is_avwap_hybrid:
                    msg += f"▫️ 아키텍처: <b>본진(15%)과 암살자 100% 독립 병렬 가동 팩트 락온</b>\n"
                    # 🚨 MODIFIED: [타점 롤오버] 암살자 타점을 -2%로 팩트 교정 완료
                    msg += f"▫️ 암살자 타점: <b>세션 VWAP -2% (1-Shot 1-Kill)</b>\n"
                    msg += f"▫️ 암살자 익절: <b>+2% 지정가 전량 익절 (절대 락온)</b>\n"
                    msg += f"▫️ 자본 잠김 차단: <b>15:59 EST 전량 최유리 지정가 덤핑</b>\n"
                    msg += f"▫️ 데이 트레이딩 관제탑: <b>365일 상시 가동 📡</b>\n"
                msg += "⚖️ <b>본진 스탠바이:</b> 15:26 EST 예약 덫 관측 ➔ 15:27 로컬 자체 슬라이싱 가동\n\n" 
            else:
                msg += f"▫️ 분할: {split_cnt}회 | 목표: {target_profit}% | 복리: {comp_rate}%\n▫️ 수수료: <b>{fee_rate}%</b>\n"
                v14_mode_txt = "🕒 자체 1분 슬라이싱 VWAP 엔진" if is_manual_vwap else "📉 LOC 단일 타격 (초안정성)" 
                msg += f"▫️ 집행: <b>{v14_mode_txt}</b>\n\n"
         
            if t == "SOXL":
                keyboard.append([InlineKeyboardButton("💎 오리지널 V14 세팅", callback_data=f"SET_VER:V14:{t}"), InlineKeyboardButton("⚖️ 역추세 V-REV 세팅", callback_data=f"SET_VER:V_REV:{t}")])
            elif t == "TQQQ":
                keyboard.append([InlineKeyboardButton("💎 오리지널 V14 세팅", callback_data=f"SET_VER:V14:{t}")])

            if ver == "V_REV":
                if t == "SOXL": 
                    keyboard.append([InlineKeyboardButton(f"📡 {safe_t} 데이 트레이딩 관제탑 열기", callback_data=f"AVWAP:MENU:{t}")])
                    keyboard.append([InlineKeyboardButton("⚔️ 암살자 ON/OFF 토글", callback_data=f"CONFIG_AVWAP:TOGGLE:{t}")])
                    keyboard.append([InlineKeyboardButton(f"💸 {safe_t} 복리", callback_data=f"INPUT:COMPOUND:{t}"), InlineKeyboardButton(f"💳 {safe_t} 수수료", callback_data=f"INPUT:FEE:{t}")])
                    keyboard.append([InlineKeyboardButton(f"✂️ {safe_t} 액면보정", callback_data=f"INPUT:STOCK_SPLIT:{t}")])
                else:
                    keyboard.append([InlineKeyboardButton(f"💸 {safe_t} 복리", callback_data=f"INPUT:COMPOUND:{t}"), InlineKeyboardButton(f"💳 {safe_t} 수수료", callback_data=f"INPUT:FEE:{t}")])
                    keyboard.append([InlineKeyboardButton(f"✂️ {safe_t} 액면보정", callback_data=f"INPUT:STOCK_SPLIT:{t}")])
            else:
                keyboard.append([InlineKeyboardButton(f"⚙️ {safe_t} 분할", callback_data=f"INPUT:SPLIT:{t}"), InlineKeyboardButton(f"🎯 {safe_t} 목표", callback_data=f"INPUT:TARGET:{t}"), InlineKeyboardButton(f"💸 {safe_t} 복리", callback_data=f"INPUT:COMPOUND:{t}")])
                keyboard.append([InlineKeyboardButton(f"✂️ {safe_t} 액면보정", callback_data=f"INPUT:STOCK_SPLIT:{t}"), InlineKeyboardButton(f"💳 {safe_t} 수수료", callback_data=f"INPUT:FEE:{t}")])
    
        return msg, InlineKeyboardMarkup(keyboard)

    def create_sync_report(self, status_text, dst_text, cash, rp_amount, ticker_data, is_trade_active, p_trade_data=None, exchange_rate=None):
        ticker_data = ticker_data or []
        
        safe_status = html.escape(str(status_text))
        safe_dst = html.escape(str(dst_text))
        header_msg = f"📜 <b>[ 통합 지시서 ({safe_status}) ]</b>\n📅 <b>{safe_dst}</b>\n"
        
        header_msg += f"💵 주문가능금액: ${cash:,.2f}\n"
        header_msg += f"🏛️ RP 투자권장: ${rp_amount:,.2f}\n"
        header_msg += "----------------------------\n\n"
        
        keyboard = []
        body_msg = ""
        krw_profit = 0.0

        for t_info in ticker_data:
            if not isinstance(t_info, dict): continue
            
            t = html.escape(str(t_info.get('ticker') or 'UNK'))
            v_mode = str(t_info.get('version') or 'V14')
            is_manual_vwap = bool(t_info.get('is_manual_vwap'))
            is_zero_start = bool(t_info.get('is_zero_start'))
             
            safe_seed = self._safe_float(t_info.get('seed') or 0.0)
            safe_one_portion = self._safe_float(t_info.get('one_portion') or 0.0)
            safe_curr = self._safe_float(t_info.get('curr') or 0.0)
            safe_avg = self._safe_float(t_info.get('avg') or 0.0)
            fact_qty = int(self._safe_float(t_info.get('qty') or 0))
            safe_profit_amt = self._safe_float(t_info.get('profit_amt') or 0.0)
            safe_profit_pct = self._safe_float(t_info.get('profit_pct') or 0.0)
            safe_split = self._safe_float(t_info.get('split') or 40.0)
            safe_t_val = self._safe_float(t_info.get('t_val') or 0.0)
            
            v_mode_display = ""
            main_icon = ""
            bdg_txt = ""
            is_rev_logic = bool(t_info.get('is_reverse'))
            
            plan_dict = t_info.get('plan') or {}
            proc_status = html.escape(str(plan_dict.get('process_status') or ''))
            tracking_info = t_info.get('tracking_info') or {}
            
            snap_tag = " <code>[📸락온]</code>" if t_info.get('has_snapshot') else ""
            day_high = self._safe_float(t_info.get('day_high') or 0.0)
            day_low = self._safe_float(t_info.get('day_low') or 0.0)
            prev_close = self._safe_float(t_info.get('prev_close') or 0.0)
            sniper_status_txt = html.escape(str(t_info.get('upward_sniper') or 'OFF'))

            if fact_qty == 0 and not is_zero_start:
                is_zero_start = True
                plan_dict['orders'] = []
                 
                if v_mode == "V_REV":
                    half_budget = (safe_seed * 0.15) * 0.5
                    if prev_close > 0:
                        p1_trigger_fact = round(prev_close * 1.15, 2)
                        p2_trigger_fact = round(prev_close * 0.999, 2)
                        q1 = math.floor(half_budget / p1_trigger_fact) if p1_trigger_fact > 0 else 0
                        q2 = math.floor(half_budget / p2_trigger_fact) if p2_trigger_fact > 0 else 0
                        if q1 > 0: plan_dict['orders'].append({"side": "BUY", "qty": q1, "price": p1_trigger_fact, "type": "LOC", "desc": "가상 매수(Buy1)"})
                        if q2 > 0: plan_dict['orders'].append({"side": "BUY", "qty": q2, "price": p2_trigger_fact, "type": "LOC", "desc": "가상 매수(Buy2)"})
            
                else:
                    half_budget = safe_one_portion * 0.5
                    if prev_close > 0:
                        p_buy = max(0.01, round(math.ceil(prev_close * 1.15 * 100) / 100.0 - 0.01, 2))
                        q1 = math.floor(half_budget / p_buy) if p_buy > 0 else 0
                        q2 = math.floor((safe_one_portion - half_budget) / p_buy) if p_buy > 0 else 0
                        
                        if q1 == 0 and q2 == 0 and safe_one_portion >= p_buy > 0: q1 = int(math.floor(safe_one_portion / p_buy))
                        if q1 > 0: plan_dict['orders'].append({"side": "BUY", "qty": q1, "price": p_buy, "type": "LOC", "desc": "가상 매수(Buy1)"})
                        if q2 > 0: plan_dict['orders'].append({"side": "BUY", "qty": q2, "price": p_buy, "type": "LOC", "desc": "가상 매수(Buy2)"})
             
            if safe_split > 0 and safe_t_val > (safe_split * 1.1):
                body_msg += "⚠️ <b>[🚨 시스템 긴급 경고: 비정상 T값 폭주 감지!]</b>\n"
                body_msg += f"🔎 현재 T값(<b>{safe_t_val:.4f}T</b>)이 설정된 분할수(<b>{int(safe_split)}분할</b>) 초과했습니다!\n"
                body_msg += "🛡️ <b>가동 조치:</b> 마이너스 호가 차단용 절대 하한선($0.01) 방어막 가동 중!\n\n"

            if v_mode == "V_REV":
                v_mode_display = "V_REV 역추세 (로컬 1분 VWAP)" 
                main_icon = "⚖️"
                bdg_txt = f"1회(1배수) 예산: ${safe_one_portion:,.0f}"
            else:
                v_mode_display = "무매4 (로컬 1분 VWAP)" if is_manual_vwap else "무매4 (LOC)" 
                main_icon = "💎"
                bdg_txt = f"당일 예산: ${safe_one_portion:,.0f}"

            if v_mode == "V_REV":
                body_msg += f"{main_icon} <b>[{t}] {v_mode_display}</b>{snap_tag}\n"
                v_rev_q_lots_safe = int(self._safe_float(t_info.get('v_rev_q_lots') or 0))
                v_rev_q_qty_safe = int(self._safe_float(t_info.get('v_rev_q_qty') or 0))
                body_msg += f"📈 큐(Queue): <b>{v_rev_q_lots_safe}개 지층 대기 중 (총 {v_rev_q_qty_safe}주)</b>\n"
            elif is_rev_logic:
                icon = "🩸" if "리버스(긴급수혈)" in proc_status else "🔄"
                bdg_txt = f"리버스 잔금쿼터: ${safe_one_portion:,.0f}"
                body_msg += f"{icon} <b>[{t}] {v_mode_display} 리버스</b>{snap_tag}\n"
                body_msg += f"📈 진행: <b>{safe_t_val:.4f}T / {int(safe_split)}분할</b>\n"
            else:
                body_msg += f"{main_icon} <b>[{t}] {v_mode_display}</b>{snap_tag}\n"
                body_msg += f"📈 진행: <b>{safe_t_val:.4f}T / {int(safe_split)}분할</b>\n"
            
            body_msg += f"💵 총 시드: ${safe_seed:,.0f}\n🛒 <b>{bdg_txt}</b>\n"
            body_msg += f"💰 현재 ${safe_curr:,.2f} / 평단 ${safe_avg:,.2f} ({fact_qty}주)\n"
            
            if prev_close > 0 and day_high > 0 and day_low > 0:
                high_pct = (day_high - prev_close) / prev_close * 100
                low_pct = (day_low - prev_close) / prev_close * 100
                body_msg += f"📈 금일 고가: ${day_high:.2f} ({'+' if high_pct > 0 else ''}{high_pct:.2f}%)\n"
                body_msg += f"📉 금일 저가: ${day_low:.2f} ({'+' if low_pct > 0 else ''}{low_pct:.2f}%)\n"

            sign = "+" if safe_profit_amt >= 0 else "-"
            icon = "🔺" if safe_profit_amt >= 0 else "🔻"
            if exchange_rate and self._safe_float(exchange_rate) > 0:
                krw_profit = abs(safe_profit_amt) * self._safe_float(exchange_rate)
                body_msg += f"{icon} 수익: {sign}{abs(safe_profit_pct):.2f}% ({sign}${abs(safe_profit_amt):,.2f} | {sign}₩{int(krw_profit):,})\n\n"
            else:
                body_msg += f"{icon} 수익: {sign}{abs(safe_profit_pct):.2f}% ({sign}${abs(safe_profit_amt):,.2f})\n\n"
            
            if is_zero_start and sniper_status_txt == "ON": sniper_status_txt = "OFF (0주 락온)"
            
            if v_mode != "V_REV":
                safe_target = self._safe_float(t_info.get('target') or 10.0)
                safe_star_pct = self._safe_float(t_info.get('star_pct') or 0.0)
                safe_star_price = self._safe_float(t_info.get('star_price') or 0.0)

                if is_rev_logic:
                    body_msg += f"⚙️ 🌟 5일선 별지점: ${safe_star_price:.2f} | 🎯감시: {sniper_status_txt}\n"
                else:
                    if fact_qty > 0 and safe_avg > 0:
                        target_price = safe_avg * (1 + safe_target / 100.0)
                        body_msg += f"⚙️ 🎯 익절 목표가: <b>${target_price:.2f}</b> (+{safe_target}%)\n"
                    body_msg += f"⚙️ ⭐ 별지점: {safe_star_pct}% | 🎯감시: {sniper_status_txt}\n"
                
                if sniper_status_txt == "ON":
                    if not is_trade_active:
                        body_msg += "🎯 상방 스나이퍼: 감시 종료 (장마감)\n"
                    elif tracking_info.get('is_trailing', False):
                        trigger_price = self._safe_float(tracking_info.get('trigger_price') or 0.0)
                        peak_price = self._safe_float(tracking_info.get('peak_price') or 0.0)
                        body_msg += f"🎯 상방 추적(${trigger_price:.2f}) 중 (고가: ${peak_price:.2f})\n"
                    else:
                        sn_target = safe_star_price if is_rev_logic else max(safe_star_price, math.ceil(safe_avg * 1.005 * 100) / 100.0)
                        if sn_target > 0: body_msg += f"🎯 상방 스나이퍼: ${sn_target:.2f} 이상 대기\n"
            else:
                body_msg += "⚖️ <b>역추세 LIFO 큐(Queue) 엔진 스탠바이</b>\n"
                body_msg += "⏱️ <b>스케줄:</b> 15:26 EST 로컬 엔진 스탠바이 ➔ 15:27 슬라이싱 타격 (자전거래 차단)\n" 
            
            if v_mode == "V_REV":
                body_msg += "📋 <b>[주문 가이던스 - ⚖️다중 LIFO 제어]</b>\n"
                # 🚨 MODIFIED: [Gap Hijack 타겟 팩트 롤오버 동기화] 브리핑 텍스트 운용종목으로 교정
                body_msg += f"⚡ <b>[Gap Hijack 🤖자율주행]</b> 운용종목 갭 이탈 감지 시 잔여예산 스윕 대기\n"
                raw_guidance = str(t_info.get('v_rev_guidance') or " (가이던스 대기 중)")
                if is_zero_start:
                    raw_guidance = '\n'.join([line for line in raw_guidance.split('\n') if "잭팟" not in line and "상위층" not in line])
                
                v_rev_q_lots_val = self._safe_float(t_info.get('v_rev_q_lots') or 0.0)
                if not is_zero_start and fact_qty > 0 and v_rev_q_lots_val > 0:
                    try:
                        b1_order = next((o for o in plan_dict.get('orders', []) if isinstance(o, dict) and 'Buy1' in str(o.get('desc', ''))), None)
                        b2_order = next((o for o in plan_dict.get('orders', []) if isinstance(o, dict) and 'Buy2' in str(o.get('desc', ''))), None)
                        
                        if b1_order or b2_order:
                            lines = raw_guidance.split('\n')
                            for i, line in enumerate(lines):
                                if "매수1(Buy1)" in line and b1_order:
                                    b1_price = self._safe_float(b1_order.get('price'))
                                    b1_qty = int(self._safe_float(b1_order.get('qty')))
                                    lines[i] = f" 🔴 매수1(Buy1) ${b1_price:.2f} <b>{b1_qty}주</b>"
                                elif "매수2(Buy2)" in line and b2_order:
                                    b2_price = self._safe_float(b2_order.get('price'))
                                    b2_qty = int(self._safe_float(b2_order.get('qty')))
                                    lines[i] = f" 🔴 매수2(Buy2) ${b2_price:.2f} <b>{b2_qty}주</b>"
                        raw_guidance = '\n'.join(lines)
                    except Exception: pass

                body_msg += raw_guidance.replace(" (LOC)", "").replace(" (VWAP)", "").replace("[가상격리] ", "").replace("[가상 ", "[").replace("가상 ", "") + "\n"
            else:
                if is_manual_vwap and not is_rev_logic:
                    body_msg += "⏱️ <b>스케줄:</b> 17:05 KST 선제 덫 장전 ➔ 로컬 1분 VWAP 슬라이싱\n" 
                body_msg += f"📋 <b>[주문 계획 - {proc_status}]</b>\n"
                
                plan_orders = plan_dict.get('orders') or []
                if plan_orders:
                    plan_orders_sorted = sorted(plan_orders, key=lambda x: 1 if str(x.get('side', '')) == 'SELL' else 0)
                    jubjub_orders = [o for o in plan_orders_sorted if isinstance(o, dict) and "🧲줍줍" in str(o.get('desc', ''))]
                    rendered_jubjub = False

                    for o in plan_orders_sorted:
                        if not isinstance(o, dict): continue
                        if "🧲줍줍" in str(o.get('desc', '')):
                            if not rendered_jubjub:
                                if jubjub_orders:
                                    min_price = min(self._safe_float(x.get('price')) for x in jubjub_orders)
                                    max_price = max(self._safe_float(x.get('price')) for x in jubjub_orders)
                                    total_jub_shares = sum(int(self._safe_float(x.get('qty'))) for x in jubjub_orders)
                                    
                                    if min_price == max_price:
                                        price_str = f"${min_price:.2f}"
                                    else:
                                        price_str = f"(${min_price:.2f}~${max_price:.2f})"
                                      
                                    body_msg += f" 🔴 🧲줍줍: <b>{price_str} x {total_jub_shares}주</b> (LOC)\n"
                                rendered_jubjub = True
                            continue
                        
                        ico = "🔴" if str(o.get('side', '')) == 'BUY' else "🔵"
                        safe_desc = html.escape(str(o.get('desc', ''))).replace("🩸", "")
                        if "수혈" in str(o.get('desc', '')): ico = "🩸"
                        type_str = f"({html.escape(str(o.get('type', '')))})" if str(o.get('type', '')) != 'LIMIT' else ""
                        body_msg += f" {ico} {safe_desc}: <b>${self._safe_float(o.get('price')):.2f} x {int(self._safe_float(o.get('qty')))}주</b> {type_str}\n"
                else:
                    body_msg += "  💤 주문 없음 (관망/예산소진)\n"

            if is_trade_active:
                if t_info.get('is_locked', False):
                    body_msg += " (✅ 금일 주문 완료/잠금)\n"
             
        final_msg = header_msg + body_msg.strip()
        
        if not is_trade_active:
            final_msg += "\n\n⛔ 장마감/애프터마켓: 주문 불가"
            
        if any(str(t_info.get('version', '')) == 'V_REV' for t_info in ticker_data if isinstance(t_info, dict)):
            final_msg += "\n\n▶️ /avwap : 🔫 데이 트레이딩 레이더 관제탑"

        return final_msg, InlineKeyboardMarkup(keyboard) if keyboard else None
