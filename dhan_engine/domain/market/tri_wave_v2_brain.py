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
    TIME_LOSS_EXIT_SEC=120;TIME_LOSS_EXIT_PCT=-1.0;DEAD_TRADE_EXIT_SEC=180;DEAD_TRADE_MIN_PROFIT_PCT=0.20
    PROFIT_ARM_PCT=1.20;PROFIT_GIVEBACK_RATIO=0.50;MIN_PEAK_PNL_FOR_GIVEBACK_PCT=1.20;MAX_HOLD_SEC=600
    def __init__(self):
        self.streams=defaultdict(lambda:{k:TriWaveStreamState(stream=k) for k in ("FUT","CE","PE")}); self.pos=defaultdict(TriWavePositionState); self._exit_conf=defaultdict(int); self._last_wait_log=defaultdict(float); self._last_visual=defaultdict(float)
    def _update(self,index,stream,secid,ltp,features):
        s=self.streams[index][stream]; s.secid=int(secid); s.last_ltp=float(ltp); s.ticks.append(float(ltp))
        p=list(s.ticks); n=len(p); rng=max(max(p)-min(p),0.01) if p else 1.0
        last5=(p[-1]-p[-5]) if n>=5 else 0.0; prev5=(p[-5]-p[-10]) if n>=10 else 0.0; last10=(p[-1]-p[-10]) if n>=10 else 0.0
        turn_up=prev5<0 and last5>0; turn_down=prev5>0 and last5<0
        stats={"last":p[-1],"min_price":min(p),"max_price":max(p),"recent_low":min(p[-10:]),"recent_high":max(p[-10:]),"position_in_range":(p[-1]-min(p))/rng,"last_5_delta":last5,"previous_5_delta":prev5,"last_10_delta":last10,"turn_up":turn_up,"turn_down":turn_down,"strength":(p[-1]-p[0])/rng if n else 0.0,"velocity":(p[-1]-p[-2]) if n>=2 else 0.0,"acceleration":((p[-1]-p[-2])-(p[-2]-p[-3])) if n>=3 else 0.0}
        for k in ["recovery_score","exhaustion_score","clean_trade_score","spoof_risk","spread_pct","depth_imbalance_5","top_depth_imbalance","market_queue_imbalance","volume_change_tick","oi_change_tick","flow","real_flow","ofi","pressure_score","ltq","day_position","ltp_vs_avg_pct","ask_pressure_score"]: stats[k]=float(features.get(k,0.0) or 0.0)
        s.stats=stats; self._phase(index,s)
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
    def evaluate(self,index,active_position=None):
        fut,ce,pe=[self.streams[index][x] for x in ("FUT","CE","PE")]; now=time.time()
        if not ce.stats or not pe.stats or not fut.stats: return TriWaveV2Signal()
        if active_position:
            side=active_position.get("side"); tgt=ce if side=="CE" else pe; p=self.pos[index]; entry=active_position.get("entry",p.entry_price or tgt.last_ltp); pnl=((tgt.last_ltp-entry)/max(entry,1e-9))*100.0; hold=now-float(active_position.get("entry_ts",p.entry_ts or now)); p.best_price=max(p.best_price,tgt.last_ltp); p.peak_pnl_pct=max(p.peak_pnl_pct,pnl)
            fast_adverse_allowed=hold>=self.FAST_ADVERSE_MIN_HOLD_SEC and pnl<=self.FAST_ADVERSE_PCT
            if hold<self.MIN_BREATHING_HOLD_SEC:
                candidate="FAST_ADVERSE" if pnl<=self.FAST_ADVERSE_PCT else "NONE"; allowed=(candidate=="FAST_ADVERSE" and fast_adverse_allowed); blocked_reason="BELOW_MIN_BREATHING_HOLD"
                logger.info("TRI_WAVE_V2_EXIT_WATCH | index=%s | side=%s | hold=%.2f | pnl_pct=%.2f | peak_pnl_pct=%.2f | target_phase=%s | target_last5=%.2f | target_exh=%.2f | target_clean=%.2f | candidate=%s | allowed=%s | blocked_reason=%s | confirm=%s",index,side,hold,pnl,p.peak_pnl_pct,tgt.phase,tgt.stats.get("last_5_delta",0.0),tgt.stats.get("exhaustion_score",0.0),tgt.stats.get("clean_trade_score",0.0),candidate,allowed,blocked_reason,"NA")
                if fast_adverse_allowed: return TriWaveV2Signal(action=f"EXIT_{side}",side=side,reason="TRI_WAVE_V2_EXIT:FAST_ADVERSE",confidence=0.95)
                return TriWaveV2Signal()
            reasons=[]
            if hold>=self.ADVERSE_EXIT_MIN_HOLD_SEC and pnl<=self.ADVERSE_EXIT_PCT: reasons.append("ADVERSE_MOVE")
            if hold>=self.TIME_LOSS_EXIT_SEC and pnl<=self.TIME_LOSS_EXIT_PCT: reasons.append("TIME_LOSS")
            if hold>=self.DEAD_TRADE_EXIT_SEC and pnl<self.DEAD_TRADE_MIN_PROFIT_PCT: reasons.append("DEAD_TRADE")
            if hold>=self.SOFT_EXIT_MIN_HOLD_SEC and tgt.phase in {"EXHAUSTION","REVERSAL"} and tgt.stats.get("last_5_delta",0.0)<0 and (tgt.stats.get("exhaustion_score",0.0)>=0.55 or tgt.stats.get("clean_trade_score",1.0)<0.35): reasons.append("WAVE_EXHAUSTION")
            giveback=p.peak_pnl_pct-pnl
            if hold>=self.PROFIT_EXIT_MIN_HOLD_SEC and p.peak_pnl_pct>=self.MIN_PEAK_PNL_FOR_GIVEBACK_PCT and giveback>=p.peak_pnl_pct*self.PROFIT_GIVEBACK_RATIO: reasons.append("PROFIT_GIVEBACK")
            if hold>=self.MAX_HOLD_SEC: reasons.append("MAX_HOLD")
            if hold<self.NORMAL_EXIT_MIN_HOLD_SEC: reasons=[r for r in reasons if r in {"ADVERSE_MOVE","PROFIT_GIVEBACK","WAVE_EXHAUSTION","MAX_HOLD","TIME_LOSS","DEAD_TRADE"}]
            candidate=reasons[0] if reasons else "NONE"; required=0.0; blocked_reason="NONE"; allowed=bool(reasons)
            if candidate=="ADVERSE_MOVE" and hold<self.ADVERSE_EXIT_MIN_HOLD_SEC: required=self.ADVERSE_EXIT_MIN_HOLD_SEC
            elif candidate=="WAVE_EXHAUSTION" and hold<self.SOFT_EXIT_MIN_HOLD_SEC: required=self.SOFT_EXIT_MIN_HOLD_SEC
            elif candidate=="PROFIT_GIVEBACK" and hold<self.PROFIT_EXIT_MIN_HOLD_SEC: required=self.PROFIT_EXIT_MIN_HOLD_SEC
            elif hold<self.MIN_BREATHING_HOLD_SEC: required=self.MIN_BREATHING_HOLD_SEC
            if required>0 and hold<required:
                allowed=False; blocked_reason="EXIT_TOO_EARLY"
                logger.info("TRI_WAVE_V2_EXIT_BLOCKED | reason=EXIT_TOO_EARLY | candidate=%s | hold=%.2f | required=%.2f | pnl_pct=%.2f | peak_pnl_pct=%.2f",candidate,hold,required,pnl,p.peak_pnl_pct)
            if (now-self._last_visual[index]>=3) or reasons:
                logger.info("TRI_WAVE_V2_EXIT_WATCH | index=%s | side=%s | hold=%.2f | pnl_pct=%.2f | peak_pnl_pct=%.2f | target_phase=%s | target_last5=%.2f | target_exh=%.2f | target_clean=%.2f | candidate=%s | allowed=%s | blocked_reason=%s | confirm=%s",index,side,hold,pnl,p.peak_pnl_pct,tgt.phase,tgt.stats.get("last_5_delta",0.0),tgt.stats.get("exhaustion_score",0.0),tgt.stats.get("clean_trade_score",0.0),candidate,allowed,blocked_reason,self._exit_conf.get(f"{index}:{side}:{candidate}",0))
                self._last_visual[index]=now
            if reasons and allowed:
                key=f"{index}:{side}:{candidate}"; self._exit_conf[key]+=1
                if self._exit_conf[key]>=2: self._exit_conf[key]=0; return TriWaveV2Signal(action=f"EXIT_{side}",side=side,reason=f"TRI_WAVE_V2_EXIT:{candidate}",confidence=0.8)
            return TriWaveV2Signal()
        ce_ok=(ce.phase in {"RECOVERY","EXPANSION"} and ce.prev_phase in {"PULLBACK","REVERSAL","NOISE","INIT"} and ce.stats["position_in_range"]<0.78 and ce.stats["clean_trade_score"]>=0.45 and (ce.stats["recovery_score"]>=0.40 or sum([ce.stats["flow"]>0,ce.stats["ofi"]>0,ce.stats["depth_imbalance_5"]>0])>=2) and not (pe.phase=="EXPANSION" and pe.stats["strength"]>0.25) and (fut.stats["strength"]>=0.10 or fut.phase in {"RECOVERY","EXPANSION"}))
        pe_ok=(pe.phase in {"RECOVERY","EXPANSION"} and pe.prev_phase in {"PULLBACK","REVERSAL","NOISE","INIT"} and pe.stats["position_in_range"]<0.78 and pe.stats["clean_trade_score"]>=0.45 and (pe.stats["recovery_score"]>=0.40 or sum([pe.stats["flow"]>0,pe.stats["ofi"]>0,pe.stats["depth_imbalance_5"]>0])>=2) and not (ce.phase=="EXPANSION" and ce.stats["strength"]>0.25) and (fut.stats["strength"]<=-0.10 or fut.phase in {"PULLBACK","REVERSAL"}))
        if not ce_ok and not pe_ok and now-self._last_wait_log[index]>=5:
            self._last_wait_log[index]=now; logger.info("TRI_WAVE_V2_FLOW_WAIT | index=%s | fut_phase=%s | ce_phase=%s | pe_phase=%s | ce_reason=%s | pe_reason=%s | ce_pos=%.2f | pe_pos=%.2f | ce_rec=%.2f | pe_rec=%.2f | ce_clean=%.2f | pe_clean=%.2f",index,fut.phase,ce.phase,pe.phase,"NO_ENTRY","NO_ENTRY",ce.stats["position_in_range"],pe.stats["position_in_range"],ce.stats["recovery_score"],pe.stats["recovery_score"],ce.stats["clean_trade_score"],pe.stats["clean_trade_score"])
            return TriWaveV2Signal()
        if now-self._last_visual[index]>=5:
            self._last_visual[index]=now; logger.info("TRI_WAVE_V2_VISUAL | index=%s | FUT phase=%s strength=%.2f | CE phase=%s ltp=%.2f pos=%.2f rec=%.2f exh=%.2f clean=%.2f | PE phase=%s ltp=%.2f pos=%.2f rec=%.2f exh=%.2f clean=%.2f | active=%s pnl_pct=%.2f",index,fut.phase,fut.stats['strength'],ce.phase,ce.last_ltp,ce.stats['position_in_range'],ce.stats['recovery_score'],ce.stats['exhaustion_score'],ce.stats['clean_trade_score'],pe.phase,pe.last_ltp,pe.stats['position_in_range'],pe.stats['recovery_score'],pe.stats['exhaustion_score'],pe.stats['clean_trade_score'],"NONE",0.0)
        if ce_ok and not pe_ok: return TriWaveV2Signal(action="BUY_CE",side="CE",reason="TRI_WAVE_V2_ENTRY:CE_WAVE_RECOVERY",confidence=0.8)
        if pe_ok and not ce_ok: return TriWaveV2Signal(action="BUY_PE",side="PE",reason="TRI_WAVE_V2_ENTRY:PE_WAVE_RECOVERY",confidence=0.8)
        best="CE" if (ce.stats["recovery_score"],ce.stats["clean_trade_score"],-ce.stats["position_in_range"])> (pe.stats["recovery_score"],pe.stats["clean_trade_score"],-pe.stats["position_in_range"]) else "PE"
        return TriWaveV2Signal(action=f"BUY_{best}",side=best,reason=f"TRI_WAVE_V2_ENTRY:{best}_WAVE_RECOVERY",confidence=0.78)
