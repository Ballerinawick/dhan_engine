from __future__ import annotations
import logging, time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional
logger = logging.getLogger(__name__)

@dataclass
class TriWaveV2Signal:
    action:str="NO_TRADE"; side:Optional[str]=None; reason:str="TRI_WAVE_V2:NOOP"; confidence:float=0.0; diagnostics:dict=field(default_factory=dict)
@dataclass
class TriWaveStreamState:
    stream:str; secid:int=0; last_ltp:float=0.0; ticks:Deque[float]=field(default_factory=lambda: deque(maxlen=120)); phase:str="INIT"; prev_phase:str="INIT"; phase_ts:float=0.0; last_turn_ts:float=0.0; stats:dict=field(default_factory=dict)
@dataclass
class TriWavePositionState:
    active_side:Optional[str]=None; entry_price:float=0.0; entry_ts:float=0.0; best_price:float=0.0; worst_price:float=0.0; peak_pnl_pct:float=0.0; owner:str="TRI_WAVE_V2"

class TriWaveV2Brain:
    MIN_BREATHING_HOLD_SEC=20;SOFT_EXIT_MIN_HOLD_SEC=45;NORMAL_EXIT_MIN_HOLD_SEC=60;PROFIT_EXIT_MIN_HOLD_SEC=45
    FAST_ADVERSE_PCT=-4.0;FAST_ADVERSE_MIN_HOLD_SEC=10;ADVERSE_EXIT_PCT=-2.0;ADVERSE_EXIT_MIN_HOLD_SEC=30
    LOT_SIZE=65;ROUND_TRIP_FEE=60.0;MIN_NET_PROFIT_EXIT=100.0;MIN_GROSS_POINTS_FOR_PROFIT_EXIT=2.50
    TIME_LOSS_EXIT_SEC=120;TIME_LOSS_EXIT_PCT=-1.0;DEAD_TRADE_EXIT_SEC=180;DEAD_TRADE_MIN_PROFIT_PCT=0.20
    PROFIT_ARM_PCT=1.20;PROFIT_GIVEBACK_RATIO=0.50;MIN_PEAK_PNL_FOR_GIVEBACK_PCT=1.20;MAX_HOLD_SEC=600
    ENTRY_CONFIRM_TICKS=3;ENTRY_CONFIRM_MAX_WINDOW_SEC=8;ENTRY_CONFIRM_MIN_INTERVAL_SEC=0.8;ENTRY_MIN_HOLD_AFTER_PHASE_CHANGE_SEC=2.0
    def __init__(self):
        self.streams=defaultdict(lambda:{k:TriWaveStreamState(stream=k) for k in ("FUT","CE","PE")}); self.pos=defaultdict(TriWavePositionState); self._exit_conf=defaultdict(int); self._last_wait_log=defaultdict(float); self._last_visual=defaultdict(float); self._entry_confirm=defaultdict(lambda:{"side":None,"count":0,"first_ts":0.0,"last_ts":0.0,"last_reason":None})
    def _update(self,index,stream,secid,ltp,features):
        s=self.streams[index][stream]; s.secid=int(secid); s.last_ltp=float(ltp); s.ticks.append(float(ltp))
        p=list(s.ticks); n=len(p); rng=max(max(p)-min(p),0.01) if p else 1.0
        last5=(p[-1]-p[-5]) if n>=5 else 0.0; prev5=(p[-5]-p[-10]) if n>=10 else 0.0; last10=(p[-1]-p[-10]) if n>=10 else 0.0
        turn_up=prev5<0 and last5>0; turn_down=prev5>0 and last5<0
        stats={"last":p[-1],"min_price":min(p),"max_price":max(p),"recent_low":min(p[-10:]),"recent_high":max(p[-10:]),"position_in_range":(p[-1]-min(p))/rng,"last_5_delta":last5,"previous_5_delta":prev5,"last_10_delta":last10,"turn_up":turn_up,"turn_down":turn_down,"strength":(p[-1]-p[0])/rng if n else 0.0,"velocity":(p[-1]-p[-2]) if n>=2 else 0.0,"acceleration":((p[-1]-p[-2])-(p[-2]-p[-3])) if n>=3 else 0.0}
        for k in ["recovery_score","exhaustion_score","clean_trade_score","spoof_risk","spread_pct","depth_imbalance_5","top_depth_imbalance","market_queue_imbalance","volume_change_tick","oi_change_tick","flow","real_flow","ofi","pressure_score","ltq","day_position","ltp_vs_avg_pct","ask_pressure_score"]: stats[k]=float(features.get(k,0.0) or 0.0)
        stats["feature_source"]=features.get("feature_source","UNKNOWN")
        stats["has_full_data"]=bool("total_buy_quantity" in features or "total_sell_quantity" in features or "volume_change_tick" in features or "oi_change_tick" in features or "ltq" in features)
        s.stats=stats; self._phase(index,s)
        key=f"{index}:{stream}"
        if time.time()-self._last_wait_log.get(f"diag:{key}",0.0)>=5:
            self._last_wait_log[f"diag:{key}"]=time.time()
            logger.info("TRI_WAVE_V2_FEATURE_DIAG | index=%s | stream=%s | source=%s | has_full=%s | ltp=%.2f | recovery=%.2f | clean=%.2f | exhaustion=%.2f | flow=%.2f | ofi=%.2f | depth_imb=%.2f | spread_pct=%.4f | volume_change=%s | oi_change=%s | feature_keys=%s",index,stream,stats.get("feature_source","UNKNOWN"),stats.get("has_full_data",False),s.last_ltp,stats.get("recovery_score",0.0),stats.get("clean_trade_score",0.0),stats.get("exhaustion_score",0.0),stats.get("flow",0.0),stats.get("ofi",0.0),stats.get("depth_imbalance_5",0.0),stats.get("spread_pct",0.0),features.get("volume_change_tick"),features.get("oi_change_tick"),sorted(features.keys()))
    def _phase(self,index,s):
        st=s.stats; old=s.phase; new="NOISE"
        pfq=(1 if st.get("flow",0)>0 else 0)+(1 if st.get("ofi",0)>0 else 0)+(1 if st.get("depth_imbalance_5",0)>0 else 0)
        if len(s.ticks)<8: new="INIT"
        elif st["turn_up"] and st["position_in_range"]<=0.70 and st["last_5_delta"]>0 and st["clean_trade_score"]>=0.40 and st["exhaustion_score"]<0.60: new="RECOVERY"
        elif st["strength"]>=0.22 and st["last_5_delta"]>0 and (st["recovery_score"]>=0.40 or pfq>=2) and st["position_in_range"]<0.85: new="EXPANSION"
        elif (st["position_in_range"]>=0.78 and st["last_5_delta"]<=0) or st["exhaustion_score"]>=0.60 or (old=="EXPANSION" and st["clean_trade_score"]<0.35): new="EXHAUSTION"
        elif st["turn_down"] or (st["last_5_delta"]<0 and st["strength"]<=-0.12) or st.get("ask_pressure_score",0)>st.get("recovery_score",0): new="REVERSAL"
        elif st["last_5_delta"]<0 or st["strength"]<=-0.12: new="PULLBACK"
        if new!=old:
            s.prev_phase=old; s.phase=new; s.phase_ts=time.time(); logger.info("TRI_WAVE_V2_PHASE_CHANGE | index=%s | stream=%s | old=%s | new=%s | ltp=%.2f | pos=%.2f | last5=%.2f | strength=%.2f | recovery=%.2f | exhaustion=%.2f | clean=%.2f | spread_pct=%.4f",index,s.stream,old,new,s.last_ltp,st.get("position_in_range",0),st.get("last_5_delta",0),st.get("strength",0),st.get("recovery_score",0),st.get("exhaustion_score",0),st.get("clean_trade_score",0),st.get("spread_pct",0))
    def on_future_tick(self,index,secid,ltp,features): self._update(index,"FUT",secid,ltp,features or {})
    def on_option_tick(self,index,side,secid,ltp,features): self._update(index,side,secid,ltp,features or {})
    def reset_trade_state(self,index,side,entry_price): self.pos[index]=TriWavePositionState(active_side=side,entry_price=entry_price,entry_ts=time.time(),best_price=entry_price,worst_price=entry_price)
    def clear_trade_state(self,index): self.pos[index]=TriWavePositionState()
    def _entry_check(self, side, fut, ce, pe, now):
        if side=="CE":
            if ce.phase not in {"RECOVERY","EXPANSION"}: return False,"CE_PHASE_NOT_READY"
            if ce.phase in {"RECOVERY","EXPANSION"} and (now-ce.phase_ts)<self.ENTRY_MIN_HOLD_AFTER_PHASE_CHANGE_SEC: return False,"CE_RECOVERY_TOO_NEW"
            if ce.prev_phase not in {"PULLBACK","REVERSAL","NOISE","INIT"}: return False,"CE_NOT_FRESH_TURN"
            if ce.stats["last_5_delta"]<=0 or ce.stats["velocity"]<0: return False,"CE_NO_PRICE_FOLLOW_THROUGH"
            if ce.stats["position_in_range"]>=0.78: return False,"CE_TOP_ZONE"
            if ce.stats["clean_trade_score"]<0.45: return False,"CE_CLEAN_LOW"
            if not (ce.stats["recovery_score"]>=0.40 or sum([ce.stats["flow"]>0,ce.stats["ofi"]>0,ce.stats["depth_imbalance_5"]>0])>=2): return False,"CE_RECOVERY_LOW"
            if pe.phase=="EXPANSION" and pe.stats["strength"]>0.25: return False,"PE_OPPOSITE_EXPANDING"
            if not (fut.stats["strength"]>=0.10 or fut.phase in {"RECOVERY","EXPANSION"}): return False,"FUT_NOT_SUPPORTING_CE"
            return True,"CE_OK"
        if pe.phase not in {"RECOVERY","EXPANSION"}: return False,"PE_PHASE_NOT_READY"
        if pe.phase in {"RECOVERY","EXPANSION"} and (now-pe.phase_ts)<self.ENTRY_MIN_HOLD_AFTER_PHASE_CHANGE_SEC: return False,"PE_RECOVERY_TOO_NEW"
        if pe.prev_phase not in {"PULLBACK","REVERSAL","NOISE","INIT"}: return False,"PE_NOT_FRESH_TURN"
        if pe.stats["last_5_delta"]<=0 or pe.stats["velocity"]<0: return False,"PE_NO_PRICE_FOLLOW_THROUGH"
        if pe.stats["position_in_range"]>=0.78: return False,"PE_TOP_ZONE"
        if pe.stats["clean_trade_score"]<0.45: return False,"PE_CLEAN_LOW"
        if not (pe.stats["recovery_score"]>=0.40 or sum([pe.stats["flow"]>0,pe.stats["ofi"]>0,pe.stats["depth_imbalance_5"]>0])>=2): return False,"PE_RECOVERY_LOW"
        if ce.phase=="EXPANSION" and ce.stats["strength"]>0.25: return False,"CE_OPPOSITE_EXPANDING"
        if not (fut.stats["strength"]<=-0.10 or fut.phase in {"PULLBACK","REVERSAL"}): return False,"FUT_NOT_SUPPORTING_PE"
        return True,"PE_OK"


    def _confirm_entry(self,index:str,side:str,reason:str,now:float)->tuple[bool,int]:
        state=self._entry_confirm[index]
        if state["side"]!=side or state["last_reason"]!=reason:
            state["side"]=side; state["count"]=0; state["first_ts"]=now; state["last_ts"]=0.0; state["last_reason"]=reason
        if now-state["first_ts"]>self.ENTRY_CONFIRM_MAX_WINDOW_SEC:
            state["count"]=0; state["first_ts"]=now; state["last_ts"]=0.0
        if state.get("last_ts",0.0) and now-state["last_ts"]<self.ENTRY_CONFIRM_MIN_INTERVAL_SEC: return False,state["count"]
        state["count"]+=1; state["last_ts"]=now
        return state["count"]>=self.ENTRY_CONFIRM_TICKS,state["count"]

    def evaluate(self,index,active_position=None):
        fut,ce,pe=[self.streams[index][x] for x in ("FUT","CE","PE")]; now=time.time()
        if not ce.stats or not pe.stats or not fut.stats: return TriWaveV2Signal()
        if active_position:
            side=active_position.get("side"); tgt=ce if side=="CE" else pe; p=self.pos[index]; entry=active_position.get("entry",p.entry_price or tgt.last_ltp); pnl=((tgt.last_ltp-entry)/max(entry,1e-9))*100.0; hold=now-float(active_position.get("entry_ts",p.entry_ts or now)); p.best_price=max(p.best_price,tgt.last_ltp); p.peak_pnl_pct=max(p.peak_pnl_pct,pnl)
            gross_points=tgt.last_ltp-entry; gross_rupees=gross_points*self.LOT_SIZE; net_rupees=gross_rupees-self.ROUND_TRIP_FEE
            profit_enough=(net_rupees>=self.MIN_NET_PROFIT_EXIT or gross_points>=self.MIN_GROSS_POINTS_FOR_PROFIT_EXIT)
            fast_adverse_allowed=hold>=self.FAST_ADVERSE_MIN_HOLD_SEC and pnl<=self.FAST_ADVERSE_PCT and tgt.last_ltp<entry
            if hold<self.MIN_BREATHING_HOLD_SEC:
                candidate="FAST_ADVERSE" if pnl<=self.FAST_ADVERSE_PCT else "NONE"; allowed=(candidate=="FAST_ADVERSE" and fast_adverse_allowed); blocked_reason="BELOW_MIN_BREATHING_HOLD"
                if candidate=="FAST_ADVERSE" and tgt.last_ltp>=entry:
                    logger.info("TRI_WAVE_V2_EXIT_BUG_BLOCKED | reason=FAST_ADVERSE_NOT_NEGATIVE | hold=%.2f | entry=%.2f | price=%.2f | pnl_pct=%.2f",hold,entry,tgt.last_ltp,pnl)
                    allowed=False; blocked_reason="FAST_ADVERSE_NOT_NEGATIVE"
                logger.info("TRI_WAVE_V2_EXIT_WATCH | index=%s | side=%s | hold=%.2f | pnl_pct=%.2f | peak_pnl_pct=%.2f | gross_points=%.2f | gross_rupees=%.2f | net_rupees=%.2f | target_phase=%s | target_last5=%.2f | target_exh=%.2f | target_clean=%.2f | candidate=%s | allowed=%s | blocked_reason=%s | confirm=%s",index,side,hold,pnl,p.peak_pnl_pct,gross_points,gross_rupees,net_rupees,tgt.phase,tgt.stats.get("last_5_delta",0.0),tgt.stats.get("exhaustion_score",0.0),tgt.stats.get("clean_trade_score",0.0),candidate,allowed,blocked_reason,"NA")
                if fast_adverse_allowed: return TriWaveV2Signal(action=f"EXIT_{side}",side=side,reason="TRI_WAVE_V2_EXIT:FAST_ADVERSE",confidence=0.95)
                return TriWaveV2Signal()
            reasons=[]
            if hold>=self.ADVERSE_EXIT_MIN_HOLD_SEC and pnl<=self.ADVERSE_EXIT_PCT: reasons.append("ADVERSE_MOVE")
            if hold>=self.TIME_LOSS_EXIT_SEC and pnl<=self.TIME_LOSS_EXIT_PCT: reasons.append("TIME_LOSS")
            if hold>=self.DEAD_TRADE_EXIT_SEC and pnl<self.DEAD_TRADE_MIN_PROFIT_PCT: reasons.append("DEAD_TRADE")
            if hold>=self.SOFT_EXIT_MIN_HOLD_SEC and pnl>0 and tgt.phase in {"EXHAUSTION","REVERSAL"} and tgt.stats.get("last_5_delta",0.0)<0 and (tgt.stats.get("exhaustion_score",0.0)>=0.55 or tgt.stats.get("clean_trade_score",1.0)<0.35): reasons.append("WAVE_PROFIT_EXHAUSTION")
            if hold>=self.SOFT_EXIT_MIN_HOLD_SEC and pnl<=-1.0 and tgt.phase in {"EXHAUSTION","REVERSAL"} and tgt.stats.get("last_5_delta",0.0)<0: reasons.append("WAVE_FAILURE_EXIT")
            giveback=p.peak_pnl_pct-pnl
            if hold>=self.PROFIT_EXIT_MIN_HOLD_SEC and p.peak_pnl_pct>=self.MIN_PEAK_PNL_FOR_GIVEBACK_PCT and giveback>=p.peak_pnl_pct*self.PROFIT_GIVEBACK_RATIO: reasons.append("PROFIT_GIVEBACK")
            if hold>=self.MAX_HOLD_SEC: reasons.append("MAX_HOLD")
            if hold<self.NORMAL_EXIT_MIN_HOLD_SEC: reasons=[r for r in reasons if r in {"ADVERSE_MOVE","PROFIT_GIVEBACK","WAVE_PROFIT_EXHAUSTION","WAVE_FAILURE_EXIT","MAX_HOLD","TIME_LOSS","DEAD_TRADE"}]
            candidate=reasons[0] if reasons else "NONE"; required=0.0; blocked_reason="NONE"; allowed=bool(reasons)
            if candidate=="ADVERSE_MOVE" and hold<self.ADVERSE_EXIT_MIN_HOLD_SEC: required=self.ADVERSE_EXIT_MIN_HOLD_SEC
            elif candidate in {"WAVE_PROFIT_EXHAUSTION","WAVE_FAILURE_EXIT"} and hold<self.SOFT_EXIT_MIN_HOLD_SEC: required=self.SOFT_EXIT_MIN_HOLD_SEC
            elif candidate=="PROFIT_GIVEBACK" and hold<self.PROFIT_EXIT_MIN_HOLD_SEC: required=self.PROFIT_EXIT_MIN_HOLD_SEC
            elif hold<self.MIN_BREATHING_HOLD_SEC: required=self.MIN_BREATHING_HOLD_SEC
            if candidate=="FAST_ADVERSE" and tgt.last_ltp>=entry:
                logger.info("TRI_WAVE_V2_EXIT_BUG_BLOCKED | reason=FAST_ADVERSE_NOT_NEGATIVE | hold=%.2f | entry=%.2f | price=%.2f | pnl_pct=%.2f",hold,entry,tgt.last_ltp,pnl)
                allowed=False; blocked_reason="FAST_ADVERSE_NOT_NEGATIVE"
            if candidate in {"PROFIT_GIVEBACK","WAVE_PROFIT_EXHAUSTION"} or (candidate=="FLOW_QUALITY_COLLAPSE" and pnl>0):
                if not profit_enough:
                    allowed=False; blocked_reason="PROFIT_BELOW_FEES"
                    logger.info("TRI_WAVE_V2_PROFIT_EXIT_BLOCKED_BY_FEES | index=%s | side=%s | candidate=%s | gross_rupees=%.2f | net_rupees=%.2f | gross_points=%.2f | required_net=%.2f",index,side,candidate,gross_rupees,net_rupees,gross_points,self.MIN_NET_PROFIT_EXIT)
            if required>0 and hold<required:
                allowed=False; blocked_reason="EXIT_TOO_EARLY"
                logger.info("TRI_WAVE_V2_EXIT_BLOCKED | reason=EXIT_TOO_EARLY | candidate=%s | hold=%.2f | required=%.2f | pnl_pct=%.2f | peak_pnl_pct=%.2f",candidate,hold,required,pnl,p.peak_pnl_pct)
            if (now-self._last_visual[index]>=3) or reasons:
                logger.info("TRI_WAVE_V2_EXIT_WATCH | index=%s | side=%s | hold=%.2f | pnl_pct=%.2f | peak_pnl_pct=%.2f | gross_points=%.2f | gross_rupees=%.2f | net_rupees=%.2f | target_phase=%s | target_last5=%.2f | target_exh=%.2f | target_clean=%.2f | candidate=%s | allowed=%s | blocked_reason=%s | confirm=%s",index,side,hold,pnl,p.peak_pnl_pct,gross_points,gross_rupees,net_rupees,tgt.phase,tgt.stats.get("last_5_delta",0.0),tgt.stats.get("exhaustion_score",0.0),tgt.stats.get("clean_trade_score",0.0),candidate,allowed,blocked_reason,self._exit_conf.get(f"{index}:{side}:{candidate}",0))
                self._last_visual[index]=now
            if reasons and allowed:
                key=f"{index}:{side}:{candidate}"; self._exit_conf[key]+=1
                if self._exit_conf[key]>=2: self._exit_conf[key]=0; return TriWaveV2Signal(action=f"EXIT_{side}",side=side,reason=f"TRI_WAVE_V2_EXIT:{candidate}",confidence=0.8)
            return TriWaveV2Signal()
        ce_ok,ce_reason=self._entry_check("CE",fut,ce,pe,now)
        pe_ok,pe_reason=self._entry_check("PE",fut,ce,pe,now)
        if now-self._last_wait_log[index]>=5:
            self._last_wait_log[index]=now; logger.info("TRI_WAVE_V2_ENTRY_BLOCK | index=%s | ce_ok=%s | ce_reason=%s | pe_ok=%s | pe_reason=%s | fut_phase=%s | fut_strength=%.2f | ce_phase=%s | ce_prev=%s | ce_pos=%.2f | ce_rec=%.2f | ce_clean=%.2f | ce_flow=%.2f | ce_ofi=%.2f | ce_imb=%.2f | ce_source=%s | pe_phase=%s | pe_prev=%s | pe_pos=%.2f | pe_rec=%.2f | pe_clean=%.2f | pe_flow=%.2f | pe_ofi=%.2f | pe_imb=%.2f | pe_source=%s",index,ce_ok,ce_reason,pe_ok,pe_reason,fut.phase,fut.stats.get("strength",0.0),ce.phase,ce.prev_phase,ce.stats["position_in_range"],ce.stats["recovery_score"],ce.stats["clean_trade_score"],ce.stats["flow"],ce.stats["ofi"],ce.stats["depth_imbalance_5"],ce.stats.get("feature_source","UNKNOWN"),pe.phase,pe.prev_phase,pe.stats["position_in_range"],pe.stats["recovery_score"],pe.stats["clean_trade_score"],pe.stats["flow"],pe.stats["ofi"],pe.stats["depth_imbalance_5"],pe.stats.get("feature_source","UNKNOWN"))
        if not ce_ok and not pe_ok:
            self._entry_confirm[index]={"side":None,"count":0,"first_ts":0.0,"last_ts":0.0,"last_reason":None}
            return TriWaveV2Signal()
        if now-self._last_visual[index]>=5:
            self._last_visual[index]=now; logger.info("TRI_WAVE_V2_VISUAL | index=%s | FUT phase=%s strength=%.2f | CE phase=%s ltp=%.2f pos=%.2f rec=%.2f exh=%.2f clean=%.2f | PE phase=%s ltp=%.2f pos=%.2f rec=%.2f exh=%.2f clean=%.2f | active=%s pnl_pct=%.2f",index,fut.phase,fut.stats['strength'],ce.phase,ce.last_ltp,ce.stats['position_in_range'],ce.stats['recovery_score'],ce.stats['exhaustion_score'],ce.stats['clean_trade_score'],pe.phase,pe.last_ltp,pe.stats['position_in_range'],pe.stats['recovery_score'],pe.stats['exhaustion_score'],pe.stats['clean_trade_score'],"NONE",0.0)
        if ce_ok and not pe_ok:
            confirmed,count=self._confirm_entry(index,"CE",ce_reason,now)
            if not confirmed:
                logger.info("TRI_WAVE_V2_ENTRY_CONFIRM_WAIT | index=%s | side=CE | count=%s | required=%s | reason=%s | phase=%s | pos=%.2f | rec=%.2f | clean=%.2f | flow=%.2f | ofi=%.2f",index,count,self.ENTRY_CONFIRM_TICKS,ce_reason,ce.phase,ce.stats["position_in_range"],ce.stats["recovery_score"],ce.stats["clean_trade_score"],ce.stats["flow"],ce.stats["ofi"])
                return TriWaveV2Signal()
            return TriWaveV2Signal(action="BUY_CE",side="CE",reason="TRI_WAVE_V2_ENTRY:CE_WAVE_RECOVERY",confidence=0.8)
        if pe_ok and not ce_ok:
            confirmed,count=self._confirm_entry(index,"PE",pe_reason,now)
            if not confirmed:
                logger.info("TRI_WAVE_V2_ENTRY_CONFIRM_WAIT | index=%s | side=PE | count=%s | required=%s | reason=%s | phase=%s | pos=%.2f | rec=%.2f | clean=%.2f | flow=%.2f | ofi=%.2f",index,count,self.ENTRY_CONFIRM_TICKS,pe_reason,pe.phase,pe.stats["position_in_range"],pe.stats["recovery_score"],pe.stats["clean_trade_score"],pe.stats["flow"],pe.stats["ofi"])
                return TriWaveV2Signal()
            return TriWaveV2Signal(action="BUY_PE",side="PE",reason="TRI_WAVE_V2_ENTRY:PE_WAVE_RECOVERY",confidence=0.8)
        best="CE" if (ce.stats["recovery_score"],ce.stats["clean_trade_score"],-ce.stats["position_in_range"])> (pe.stats["recovery_score"],pe.stats["clean_trade_score"],-pe.stats["position_in_range"]) else "PE"
        reason=ce_reason if best=="CE" else pe_reason
        confirmed,count=self._confirm_entry(index,best,reason,now)
        target=ce if best=="CE" else pe
        if not confirmed:
            logger.info("TRI_WAVE_V2_ENTRY_CONFIRM_WAIT | index=%s | side=%s | count=%s | required=%s | reason=%s | phase=%s | pos=%.2f | rec=%.2f | clean=%.2f | flow=%.2f | ofi=%.2f",index,best,count,self.ENTRY_CONFIRM_TICKS,reason,target.phase,target.stats["position_in_range"],target.stats["recovery_score"],target.stats["clean_trade_score"],target.stats["flow"],target.stats["ofi"])
            return TriWaveV2Signal()
        return TriWaveV2Signal(action=f"BUY_{best}",side=best,reason=f"TRI_WAVE_V2_ENTRY:{best}_WAVE_RECOVERY",confidence=0.78)
