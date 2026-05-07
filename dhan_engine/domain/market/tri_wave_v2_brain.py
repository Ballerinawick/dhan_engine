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
    stream:str; secid:int=0; last_ltp:float=0.0; ticks:Deque[float]=field(default_factory=lambda: deque(maxlen=120)); feature_ticks:Deque[dict]=field(default_factory=lambda: deque(maxlen=180)); phase:str="INIT"; prev_phase:str="INIT"; phase_ts:float=0.0; last_turn_ts:float=0.0; stats:dict=field(default_factory=dict)
@dataclass
class TriWavePositionState:
    active_side:Optional[str]=None; entry_price:float=0.0; entry_ts:float=0.0; best_price:float=0.0; worst_price:float=0.0; peak_pnl_pct:float=0.0; owner:str="TRI_WAVE_V2"

class TriWaveV2Brain:
    MIN_BREATHING_HOLD_SEC=20;SOFT_EXIT_MIN_HOLD_SEC=45;NORMAL_EXIT_MIN_HOLD_SEC=60;PROFIT_EXIT_MIN_HOLD_SEC=45
    FAST_ADVERSE_PCT=-4.0;FAST_ADVERSE_MIN_HOLD_SEC=10;ADVERSE_EXIT_PCT=-2.0;ADVERSE_EXIT_MIN_HOLD_SEC=30
    LOT_SIZE=65;ROUND_TRIP_FEE=60.0;MIN_NET_PROFIT_EXIT=100.0;MIN_GROSS_POINTS_FOR_PROFIT_EXIT=2.50
    TIME_LOSS_EXIT_SEC=120;TIME_LOSS_EXIT_PCT=-1.0;DEAD_TRADE_EXIT_SEC=180;DEAD_TRADE_MIN_PROFIT_PCT=0.20
    PROFIT_ARM_PCT=1.20;PROFIT_GIVEBACK_RATIO=0.50;MIN_PEAK_PNL_FOR_GIVEBACK_PCT=1.20;MAX_HOLD_SEC=600
    ENTRY_CONFIRM_TICKS=3;ENTRY_CONFIRM_MAX_WINDOW_SEC=8;ENTRY_CONFIRM_MIN_INTERVAL_SEC=0.8;ENTRY_MIN_HOLD_AFTER_PHASE_CHANGE_SEC=0.0
    def __init__(self):
        self.streams=defaultdict(lambda:{k:TriWaveStreamState(stream=k) for k in ("FUT","CE","PE")}); self.pos=defaultdict(TriWavePositionState); self._exit_conf=defaultdict(int); self._last_wait_log=defaultdict(float); self._last_visual=defaultdict(float); self._entry_confirm=defaultdict(lambda:{"side":None,"count":0,"first_ts":0.0,"last_ts":0.0,"last_reason":None})

    def _field_values(self,s,field,window=30):
        rows=list(s.feature_ticks); vals=[float(r.get(field,0.0) or 0.0) for r in rows[-window:]]; return vals
    def _field_slope(self,s,field,window=5):
        vals=self._field_values(s,field,window)
        if len(vals)<2: return 0.0
        return vals[-1]-vals[0]
    def _field_mean(self,s,field,window=30):
        vals=self._field_values(s,field,window)
        return sum(vals)/max(len(vals),1)
    def _field_std(self,s,field,window=30):
        vals=self._field_values(s,field,window)
        if len(vals)<2: return 0.0
        mean=sum(vals)/len(vals)
        return (sum((v-mean)**2 for v in vals)/len(vals))**0.5
    def _zscore(self,s,field,window=30):
        vals=self._field_values(s,field,window)
        if len(vals)<10: return 0.0
        current=vals[-1]; mean=self._field_mean(s,field,window); std=self._field_std(s,field,window)
        return (current-mean)/max(std,1e-6)
    def _percentile_rank(self,s,field,window=30):
        vals=self._field_values(s,field,window)
        if len(vals)<10: return 0.50
        current=vals[-1]
        return sum(1 for v in vals if v<=current)/len(vals)

    def _update(self,index,stream,secid,ltp,features):
        s=self.streams[index][stream]; s.secid=int(secid); s.last_ltp=float(ltp); s.ticks.append(float(ltp))
        p=list(s.ticks); n=len(p); rng=max(max(p)-min(p),0.01) if p else 1.0
        last5=(p[-1]-p[-5]) if n>=5 else 0.0; prev5=(p[-5]-p[-10]) if n>=10 else 0.0; last10=(p[-1]-p[-10]) if n>=10 else 0.0
        turn_up=prev5<0 and last5>0; turn_down=prev5>0 and last5<0
        stats={"last":p[-1],"min_price":min(p),"max_price":max(p),"recent_low":min(p[-10:]),"recent_high":max(p[-10:]),"position_in_range":(p[-1]-min(p))/rng,"last_5_delta":last5,"previous_5_delta":prev5,"last_10_delta":last10,"turn_up":turn_up,"turn_down":turn_down,"strength":(p[-1]-p[0])/rng if n else 0.0,"velocity":(p[-1]-p[-2]) if n>=2 else 0.0,"acceleration":((p[-1]-p[-2])-(p[-2]-p[-3])) if n>=3 else 0.0}
        tracked_fields=["recovery_score","exhaustion_score","clean_trade_score","flow","real_flow","ofi","depth_imbalance_5","top_depth_imbalance","market_queue_imbalance","volume_change_tick","oi_change_tick","pressure_score","ask_pressure_score","spread_pct","spoof_risk","ltq","day_position","ltp_vs_avg_pct"]
        for k in tracked_fields: stats[k]=float(features.get(k,0.0) or 0.0)
        stats["feature_source"]=features.get("feature_source","UNKNOWN")
        stats["has_full_data"]=bool("total_buy_quantity" in features or "total_sell_quantity" in features or "volume_change_tick" in features or "oi_change_tick" in features or "ltq" in features)
        snap={k:stats.get(k,0.0) for k in tracked_fields}; snap.update({"ltp":s.last_ltp,"ts":time.time()}); s.feature_ticks.append(snap)
        dynamic={}
        for field in tracked_fields:
            dynamic[f"{field}_slope5"]=self._field_slope(s,field,5)
            dynamic[f"{field}_z30"]=self._zscore(s,field,30)
            dynamic[f"{field}_pct30"]=self._percentile_rank(s,field,30)
        support_components={"price_follow":stats["last_5_delta"]>0 and stats["velocity"]>=0,"recovery_above_baseline":dynamic["recovery_score_z30"]>0 or dynamic["recovery_score_slope5"]>0,"clean_not_deteriorating":dynamic["clean_trade_score_slope5"]>=0,"flow_improving":dynamic["flow_slope5"]>0 or dynamic["real_flow_slope5"]>0,"ofi_improving":dynamic["ofi_slope5"]>0,"depth_improving":dynamic["depth_imbalance_5_slope5"]>0 or dynamic["top_depth_imbalance_slope5"]>0,"queue_improving":dynamic["market_queue_imbalance_slope5"]>0,"pressure_improving":dynamic["pressure_score_slope5"]>0,"spread_not_worsening":dynamic["spread_pct_slope5"]<=0,"exhaustion_not_rising":dynamic["exhaustion_score_slope5"]<=0}
        risk_components={"price_weak":stats["last_5_delta"]<0 or stats["velocity"]<0,"recovery_falling":dynamic["recovery_score_slope5"]<0,"clean_falling":dynamic["clean_trade_score_slope5"]<0,"flow_worsening":dynamic["flow_slope5"]<0 and dynamic["real_flow_slope5"]<0,"ofi_worsening":dynamic["ofi_slope5"]<0,"depth_worsening":dynamic["depth_imbalance_5_slope5"]<0 and dynamic["top_depth_imbalance_slope5"]<0,"queue_worsening":dynamic["market_queue_imbalance_slope5"]<0,"ask_pressure_rising":dynamic["ask_pressure_score_slope5"]>0,"spread_widening":dynamic["spread_pct_slope5"]>0,"exhaustion_rising":dynamic["exhaustion_score_slope5"]>0}
        support_score=sum(1 for v in support_components.values() if v)/len(support_components)
        risk_score=sum(1 for v in risk_components.values() if v)/len(risk_components)
        dynamic_edge=support_score-risk_score
        stats["dynamic"]=dynamic; stats["support_components"]=support_components; stats["risk_components"]=risk_components
        stats["dynamic_support_score"]=support_score; stats["dynamic_risk_score"]=risk_score; stats["dynamic_edge"]=dynamic_edge
        s.stats=stats; self._phase(index,s)
        key=f"{index}:{stream}"
        if time.time()-self._last_wait_log.get(f"diag:{key}",0.0)>=5:
            self._last_wait_log[f"diag:{key}"]=time.time()
            logger.info("TRI_WAVE_V2_FEATURE_DIAG | index=%s | stream=%s | source=%s | has_full=%s | ltp=%.2f | recovery=%.2f | clean=%.2f | exhaustion=%.2f | flow=%.2f | ofi=%.2f | depth_imb=%.2f | spread_pct=%.4f | volume_change=%s | oi_change=%s | feature_keys=%s",index,stream,stats.get("feature_source","UNKNOWN"),stats.get("has_full_data",False),s.last_ltp,stats.get("recovery_score",0.0),stats.get("clean_trade_score",0.0),stats.get("exhaustion_score",0.0),stats.get("flow",0.0),stats.get("ofi",0.0),stats.get("depth_imbalance_5",0.0),stats.get("spread_pct",0.0),features.get("volume_change_tick"),features.get("oi_change_tick"),sorted(features.keys()))
        if time.time()-self._last_wait_log.get(f"dynamic:{key}",0.0)>=5:
            self._last_wait_log[f"dynamic:{key}"]=time.time()
            d=stats["dynamic"]
            logger.info("TRI_WAVE_V2_DYNAMIC_INTEL | index=%s | stream=%s | phase=%s | ltp=%.2f | support=%.2f | risk=%.2f | edge=%.2f | last5=%.2f | velocity=%.2f | rec=%.2f rec_z=%.2f rec_slope=%.2f | clean=%.2f clean_z=%.2f clean_slope=%.2f | exh=%.2f exh_z=%.2f exh_slope=%.2f | flow=%.2f flow_z=%.2f flow_slope=%.2f | ofi=%.2f ofi_z=%.2f ofi_slope=%.2f | depth=%.2f depth_slope=%.2f | mqi=%.2f mqi_slope=%.2f | spread=%.4f spread_slope=%.4f | support_components=%s | risk_components=%s",index,stream,s.phase,s.last_ltp,stats["dynamic_support_score"],stats["dynamic_risk_score"],stats["dynamic_edge"],stats["last_5_delta"],stats["velocity"],stats["recovery_score"],d["recovery_score_z30"],d["recovery_score_slope5"],stats["clean_trade_score"],d["clean_trade_score_z30"],d["clean_trade_score_slope5"],stats["exhaustion_score"],d["exhaustion_score_z30"],d["exhaustion_score_slope5"],stats["flow"],d["flow_z30"],d["flow_slope5"],stats["ofi"],d["ofi_z30"],d["ofi_slope5"],stats["depth_imbalance_5"],d["depth_imbalance_5_slope5"],stats["market_queue_imbalance"],d["market_queue_imbalance_slope5"],stats["spread_pct"],d["spread_pct_slope5"],stats["support_components"],stats["risk_components"])
    def _phase(self,index,s):
        st=s.stats; old=s.phase; new="NOISE"; dynamic=st.get("dynamic",{}); support_components=st.get("support_components",{}); risk_components=st.get("risk_components",{})
        support_score=st.get("dynamic_support_score",0.0); risk_score=st.get("dynamic_risk_score",0.0); dynamic_edge=st.get("dynamic_edge",0.0)
        supported_recovery=(dynamic_edge>0 and support_score>risk_score and not risk_components.get("exhaustion_rising",False) and not risk_components.get("spread_widening",False) and st["position_in_range"]<0.90 and st["last_5_delta"]>=0)
        supported_expansion=(dynamic_edge>0 and support_score>risk_score and st["last_5_delta"]>0 and dynamic.get("recovery_score_slope5",0.0)>=0 and dynamic.get("flow_slope5",0.0)>=0)
        supported_exhaustion=(risk_score>support_score and (risk_components.get("exhaustion_rising",False) or risk_components.get("clean_falling",False) or risk_components.get("ofi_worsening",False) or risk_components.get("depth_worsening",False)))
        supported_reversal=(risk_score>support_score and st["last_5_delta"]<=0 and (risk_components.get("flow_worsening",False) or risk_components.get("ofi_worsening",False) or risk_components.get("depth_worsening",False)))
        if len(s.ticks)<8: new="INIT"
        elif supported_recovery: new="RECOVERY"
        elif supported_expansion: new="EXPANSION"
        elif supported_exhaustion: new="EXHAUSTION"
        elif supported_reversal: new="REVERSAL"
        elif st["last_5_delta"]<0 or st["strength"]<=-0.12: new="PULLBACK"
        if new!=old:
            s.prev_phase=old; s.phase=new; s.phase_ts=time.time(); logger.info("TRI_WAVE_V2_PHASE_CHANGE | index=%s | stream=%s | old=%s | new=%s | ltp=%.2f | pos=%.2f | last5=%.2f | strength=%.2f | support=%.2f | risk=%.2f | edge=%.2f",index,s.stream,old,new,s.last_ltp,st.get("position_in_range",0),st.get("last_5_delta",0),st.get("strength",0),support_score,risk_score,dynamic_edge)
    def on_future_tick(self,index,secid,ltp,features): self._update(index,"FUT",secid,ltp,features or {})
    def on_option_tick(self,index,side,secid,ltp,features): self._update(index,side,secid,ltp,features or {})
    def reset_trade_state(self,index,side,entry_price): self.pos[index]=TriWavePositionState(active_side=side,entry_price=entry_price,entry_ts=time.time(),best_price=entry_price,worst_price=entry_price)
    def clear_trade_state(self,index): self.pos[index]=TriWavePositionState()
    def _entry_check(self, index, side, fut, ce, pe, now):
        if side=="CE":
            if ce.phase not in {"RECOVERY","EXPANSION"}: return False,"CE_PHASE_NOT_READY"
            if ce.stats["dynamic_support_score"]<=ce.stats["dynamic_risk_score"]: return False,"CE_SUPPORT_NOT_ABOVE_RISK"
            if ce.stats.get("last_5_delta",0.0)<0: return False,"CE_LAST5_NEGATIVE"
            ce_edge=ce.stats.get("dynamic_edge",0.0); pe_edge=pe.stats.get("dynamic_edge",0.0)
            if not (ce_edge>0 or ce_edge>=pe_edge): return False,"CE_EDGE_NOT_POSITIVE_OR_IMPROVING"
            fut_supports_ce=(fut.stats.get("last_5_delta",0.0)>=0 or fut.stats.get("velocity",0.0)>=0 or fut.phase in {"RECOVERY","EXPANSION"})
            if not fut_supports_ce and fut.phase in {"PULLBACK","REVERSAL"}: return False,"FUT_FLOW_AGAINST_CE"
            logger.info("TRI_WAVE_V2_ENTRY_CANDIDATE_OK | index=%s | side=%s | phase=%s | support=%.2f | risk=%.2f | edge=%.2f | fut_phase=%s | fut_last5=%.2f | fut_velocity=%.2f | opposite_edge=%.2f",index,"CE",ce.phase,ce.stats.get("dynamic_support_score",0.0),ce.stats.get("dynamic_risk_score",0.0),ce.stats.get("dynamic_edge",0.0),fut.phase,fut.stats.get("last_5_delta",0.0),fut.stats.get("velocity",0.0),pe.stats.get("dynamic_edge",0.0))
            return True,"CE_OK"
        if pe.phase not in {"RECOVERY","EXPANSION"}: return False,"PE_PHASE_NOT_READY"
        if pe.stats["dynamic_support_score"]<=pe.stats["dynamic_risk_score"]: return False,"PE_SUPPORT_NOT_ABOVE_RISK"
        if pe.stats.get("last_5_delta",0.0)<0: return False,"PE_LAST5_NEGATIVE"
        pe_edge=pe.stats.get("dynamic_edge",0.0); ce_edge=ce.stats.get("dynamic_edge",0.0)
        if not (pe_edge>0 or pe_edge>=ce_edge): return False,"PE_EDGE_NOT_POSITIVE_OR_IMPROVING"
        fut_supports_pe=(fut.stats.get("last_5_delta",0.0)<=0 or fut.stats.get("velocity",0.0)<=0 or fut.phase in {"PULLBACK","REVERSAL","EXHAUSTION"})
        if not fut_supports_pe and fut.phase in {"RECOVERY","EXPANSION"}: return False,"FUT_FLOW_AGAINST_PE"
        logger.info("TRI_WAVE_V2_ENTRY_CANDIDATE_OK | index=%s | side=%s | phase=%s | support=%.2f | risk=%.2f | edge=%.2f | fut_phase=%s | fut_last5=%.2f | fut_velocity=%.2f | opposite_edge=%.2f",index,"PE",pe.phase,pe.stats.get("dynamic_support_score",0.0),pe.stats.get("dynamic_risk_score",0.0),pe.stats.get("dynamic_edge",0.0),fut.phase,fut.stats.get("last_5_delta",0.0),fut.stats.get("velocity",0.0),ce.stats.get("dynamic_edge",0.0))
        return True,"PE_OK"


    def _confirm_entry(self,index:str,side:str,reason:str,now:float)->tuple[bool,int,bool]:
        state=self._entry_confirm[index]
        started=False
        if state["side"]!=side or state["last_reason"]!=reason:
            state["side"]=side; state["count"]=0; state["first_ts"]=now; state["last_ts"]=0.0; state["last_reason"]=reason; started=True
        if now-state["first_ts"]>self.ENTRY_CONFIRM_MAX_WINDOW_SEC:
            state["count"]=0; state["first_ts"]=now; state["last_ts"]=0.0; started=True
        if state.get("last_ts",0.0) and now-state["last_ts"]<self.ENTRY_CONFIRM_MIN_INTERVAL_SEC: return False,state["count"],started
        state["count"]+=1; state["last_ts"]=now
        return state["count"]>=self.ENTRY_CONFIRM_TICKS,state["count"],(started or state["count"]==1)

    def evaluate(self,index,active_position=None):
        fut,ce,pe=[self.streams[index][x] for x in ("FUT","CE","PE")]; now=time.time()
        if not ce.stats or not pe.stats or not fut.stats: return TriWaveV2Signal()
        if active_position:
            side=active_position.get("side"); tgt=ce if side=="CE" else pe; p=self.pos[index]; entry=active_position.get("entry",p.entry_price or tgt.last_ltp); pnl=((tgt.last_ltp-entry)/max(entry,1e-9))*100.0; hold=now-float(active_position.get("entry_ts",p.entry_ts or now)); p.best_price=max(p.best_price,tgt.last_ltp); p.peak_pnl_pct=max(p.peak_pnl_pct,pnl)
            gross_points=tgt.last_ltp-entry; gross_rupees=gross_points*self.LOT_SIZE; net_rupees=gross_rupees-self.ROUND_TRIP_FEE
            profit_enough=(net_rupees>=self.MIN_NET_PROFIT_EXIT or gross_points>=self.MIN_GROSS_POINTS_FOR_PROFIT_EXIT)
            fast_adverse_allowed=hold>=self.FAST_ADVERSE_MIN_HOLD_SEC and pnl<=self.FAST_ADVERSE_PCT and tgt.last_ltp<entry
            if hold<self.MIN_BREATHING_HOLD_SEC:
                target_support=tgt.stats.get("dynamic_support_score",0.0); target_risk=tgt.stats.get("dynamic_risk_score",0.0); target_edge=tgt.stats.get("dynamic_edge",0.0)
                wave_healthy=(target_support>=target_risk or target_edge>=0)
                candidate="FAST_ADVERSE" if pnl<=self.FAST_ADVERSE_PCT else "NONE"; allowed=(candidate=="FAST_ADVERSE" and fast_adverse_allowed); blocked_reason="BELOW_MIN_BREATHING_HOLD"
                if candidate=="FAST_ADVERSE" and tgt.last_ltp>=entry:
                    logger.info("TRI_WAVE_V2_EXIT_BUG_BLOCKED | reason=FAST_ADVERSE_NOT_NEGATIVE | hold=%.2f | entry=%.2f | price=%.2f | pnl_pct=%.2f",hold,entry,tgt.last_ltp,pnl)
                    allowed=False; blocked_reason="FAST_ADVERSE_NOT_NEGATIVE"
                logger.info("TRI_WAVE_V2_EXIT_WATCH | index=%s | side=%s | hold=%.2f | pnl_pct=%.2f | peak_pnl_pct=%.2f | gross_points=%.2f | gross_rupees=%.2f | net_rupees=%.2f | target_phase=%s | target_last5=%.2f | target_exh=%.2f | target_clean=%.2f | target_support=%.2f | target_risk=%.2f | target_edge=%.2f | wave_healthy=%s | candidate=%s | allowed=%s | blocked_reason=%s | confirm=%s",index,side,hold,pnl,p.peak_pnl_pct,gross_points,gross_rupees,net_rupees,tgt.phase,tgt.stats.get("last_5_delta",0.0),tgt.stats.get("exhaustion_score",0.0),tgt.stats.get("clean_trade_score",0.0),target_support,target_risk,target_edge,wave_healthy,candidate,allowed,blocked_reason,"NA")
                if fast_adverse_allowed: return TriWaveV2Signal(action=f"EXIT_{side}",side=side,reason="TRI_WAVE_V2_EXIT:FAST_ADVERSE",confidence=0.95)
                return TriWaveV2Signal()
            target_support=tgt.stats.get("dynamic_support_score",0.0)
            target_risk=tgt.stats.get("dynamic_risk_score",0.0)
            target_edge=tgt.stats.get("dynamic_edge",0.0)
            wave_healthy=(target_support>=target_risk or target_edge>=0)
            reasons=[]
            adverse_wave_failed=(tgt.phase in {"PULLBACK","REVERSAL","EXHAUSTION"} and target_risk>target_support and target_edge<0)
            if hold>=self.ADVERSE_EXIT_MIN_HOLD_SEC and pnl<=self.ADVERSE_EXIT_PCT and adverse_wave_failed: reasons.append("ADVERSE_MOVE")
            if hold>=self.TIME_LOSS_EXIT_SEC and pnl<=self.TIME_LOSS_EXIT_PCT: reasons.append("TIME_LOSS")
            if hold>=self.DEAD_TRADE_EXIT_SEC and pnl<self.DEAD_TRADE_MIN_PROFIT_PCT: reasons.append("DEAD_TRADE")
            if hold>=self.SOFT_EXIT_MIN_HOLD_SEC and net_rupees>=self.MIN_NET_PROFIT_EXIT and p.peak_pnl_pct>=self.MIN_PEAK_PNL_FOR_GIVEBACK_PCT and tgt.phase in {"EXHAUSTION","REVERSAL"} and target_risk>target_support and target_edge<0: reasons.append("WAVE_PROFIT_EXHAUSTION")
            if hold>=self.SOFT_EXIT_MIN_HOLD_SEC and pnl<=-1.0 and tgt.phase in {"EXHAUSTION","REVERSAL","PULLBACK"} and target_risk>target_support and target_edge<0: reasons.append("WAVE_FAILURE_EXIT")
            giveback=p.peak_pnl_pct-pnl
            if hold>=self.PROFIT_EXIT_MIN_HOLD_SEC and p.peak_pnl_pct>=self.MIN_PEAK_PNL_FOR_GIVEBACK_PCT and giveback>=p.peak_pnl_pct*self.PROFIT_GIVEBACK_RATIO: reasons.append("PROFIT_GIVEBACK")
            if hold>=self.MAX_HOLD_SEC: reasons.append("MAX_HOLD")
            if wave_healthy:
                filtered=[]
                for r in reasons:
                    if r=="ADVERSE_MOVE" and pnl<=self.FAST_ADVERSE_PCT:
                        filtered.append(r)
                    elif r not in {"ADVERSE_MOVE","WAVE_FAILURE_EXIT","WAVE_PROFIT_EXHAUSTION"}:
                        filtered.append(r)
                if len(filtered)!=len(reasons):
                    logger.info("TRI_WAVE_V2_HOLD_HEALTHY_WAVE | index=%s | side=%s | hold=%.2f | pnl_pct=%.2f | support=%.2f | risk=%.2f | edge=%.2f | phase=%s",index,side,hold,pnl,target_support,target_risk,target_edge,tgt.phase)
                reasons=filtered
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
                logger.info("TRI_WAVE_V2_EXIT_WATCH | index=%s | side=%s | hold=%.2f | pnl_pct=%.2f | peak_pnl_pct=%.2f | gross_points=%.2f | gross_rupees=%.2f | net_rupees=%.2f | target_phase=%s | target_last5=%.2f | target_exh=%.2f | target_clean=%.2f | target_support=%.2f | target_risk=%.2f | target_edge=%.2f | wave_healthy=%s | candidate=%s | allowed=%s | blocked_reason=%s | confirm=%s",index,side,hold,pnl,p.peak_pnl_pct,gross_points,gross_rupees,net_rupees,tgt.phase,tgt.stats.get("last_5_delta",0.0),tgt.stats.get("exhaustion_score",0.0),tgt.stats.get("clean_trade_score",0.0),target_support,target_risk,target_edge,wave_healthy,candidate,allowed,blocked_reason,self._exit_conf.get(f"{index}:{side}:{candidate}",0))
                self._last_visual[index]=now
            if reasons and allowed:
                key=f"{index}:{side}:{candidate}"; self._exit_conf[key]+=1
                if self._exit_conf[key]>=2: self._exit_conf[key]=0; return TriWaveV2Signal(action=f"EXIT_{side}",side=side,reason=f"TRI_WAVE_V2_EXIT:{candidate}",confidence=0.8)
            return TriWaveV2Signal()
        ce_ok,ce_reason=self._entry_check(index,"CE",fut,ce,pe,now)
        pe_ok,pe_reason=self._entry_check(index,"PE",fut,ce,pe,now)
        if now-self._last_wait_log[index]>=5:
            self._last_wait_log[index]=now; logger.info("TRI_WAVE_V2_ENTRY_BLOCK | index=%s | ce_ok=%s | ce_reason=%s | pe_ok=%s | pe_reason=%s | ce_support=%.2f | ce_risk=%.2f | ce_edge=%.2f | pe_support=%.2f | pe_risk=%.2f | pe_edge=%.2f | fut_support=%.2f | fut_risk=%.2f | fut_edge=%.2f | fut_phase=%s | fut_strength=%.2f | ce_phase=%s | ce_prev=%s | ce_pos=%.2f | ce_rec=%.2f | ce_clean=%.2f | ce_flow=%.2f | ce_ofi=%.2f | ce_imb=%.2f | ce_source=%s | pe_phase=%s | pe_prev=%s | pe_pos=%.2f | pe_rec=%.2f | pe_clean=%.2f | pe_flow=%.2f | pe_ofi=%.2f | pe_imb=%.2f | pe_source=%s",index,ce_ok,ce_reason,pe_ok,pe_reason,ce.stats.get("dynamic_support_score",0.0),ce.stats.get("dynamic_risk_score",0.0),ce.stats.get("dynamic_edge",0.0),pe.stats.get("dynamic_support_score",0.0),pe.stats.get("dynamic_risk_score",0.0),pe.stats.get("dynamic_edge",0.0),fut.stats.get("dynamic_support_score",0.0),fut.stats.get("dynamic_risk_score",0.0),fut.stats.get("dynamic_edge",0.0),fut.phase,fut.stats.get("strength",0.0),ce.phase,ce.prev_phase,ce.stats["position_in_range"],ce.stats["recovery_score"],ce.stats["clean_trade_score"],ce.stats["flow"],ce.stats["ofi"],ce.stats["depth_imbalance_5"],ce.stats.get("feature_source","UNKNOWN"),pe.phase,pe.prev_phase,pe.stats["position_in_range"],pe.stats["recovery_score"],pe.stats["clean_trade_score"],pe.stats["flow"],pe.stats["ofi"],pe.stats["depth_imbalance_5"],pe.stats.get("feature_source","UNKNOWN"))
        if now-self._last_wait_log.get(f"entry_confirm:{index}",0.0)>=5:
            self._last_wait_log[f"entry_confirm:{index}"]=now
            entry_state=self._entry_confirm.get(index,{})
            logger.info("TRI_WAVE_V2_ENTRY_CONFIRM_STATE | index=%s | side=%s | count=%s | reason=%s | age=%.2f | ce_phase=%s | pe_phase=%s | ce_last5=%.2f | pe_last5=%.2f | ce_velocity=%.2f | pe_velocity=%.2f",index,entry_state.get("side"),entry_state.get("count",0),entry_state.get("last_reason"),(now-entry_state.get("first_ts",now)) if entry_state.get("first_ts",0.0) else 0.0,ce.phase,pe.phase,ce.stats.get("last_5_delta",0.0),pe.stats.get("last_5_delta",0.0),ce.stats.get("velocity",0.0),pe.stats.get("velocity",0.0))
        if not ce_ok and not pe_ok:
            pending=self._entry_confirm.get(index,{})
            pending_side=pending.get("side")
            if pending_side=="CE" and ce.phase in {"RECOVERY","EXPANSION"}:
                pass
            elif pending_side=="PE" and pe.phase in {"RECOVERY","EXPANSION"}:
                pass
            else:
                logger.info("TRI_WAVE_V2_ENTRY_CONFIRM_RESET | index=%s | pending_side=%s | ce_phase=%s | ce_reason=%s | pe_phase=%s | pe_reason=%s",index,pending_side,ce.phase,ce_reason,pe.phase,pe_reason)
                self._entry_confirm[index]={"side":None,"count":0,"first_ts":0.0,"last_ts":0.0,"last_reason":None}
            return TriWaveV2Signal()
        if now-self._last_visual[index]>=5:
            self._last_visual[index]=now; logger.info("TRI_WAVE_V2_VISUAL | index=%s | FUT phase=%s strength=%.2f | CE phase=%s ltp=%.2f pos=%.2f rec=%.2f exh=%.2f clean=%.2f | PE phase=%s ltp=%.2f pos=%.2f rec=%.2f exh=%.2f clean=%.2f | active=%s pnl_pct=%.2f",index,fut.phase,fut.stats['strength'],ce.phase,ce.last_ltp,ce.stats['position_in_range'],ce.stats['recovery_score'],ce.stats['exhaustion_score'],ce.stats['clean_trade_score'],pe.phase,pe.last_ltp,pe.stats['position_in_range'],pe.stats['recovery_score'],pe.stats['exhaustion_score'],pe.stats['clean_trade_score'],"NONE",0.0)
        if ce_ok and not pe_ok:
            confirmed,count,started=self._confirm_entry(index,"CE",ce_reason,now)
            if started:
                logger.info("TRI_WAVE_V2_ENTRY_CONFIRM_START | index=%s | side=%s | phase=%s | support=%.2f | risk=%.2f | edge=%.2f | last5=%.2f | flow=%.2f | ofi=%.2f",index,"CE",ce.phase,ce.stats.get("dynamic_support_score",0.0),ce.stats.get("dynamic_risk_score",0.0),ce.stats.get("dynamic_edge",0.0),ce.stats.get("last_5_delta",0.0),ce.stats.get("flow",0.0),ce.stats.get("ofi",0.0))
            if not confirmed:
                logger.info("TRI_WAVE_V2_ENTRY_CONFIRM_WAIT | index=%s | side=CE | count=%s | required=%s | reason=%s | phase=%s | pos=%.2f | rec=%.2f | clean=%.2f | flow=%.2f | ofi=%.2f",index,count,self.ENTRY_CONFIRM_TICKS,ce_reason,ce.phase,ce.stats["position_in_range"],ce.stats["recovery_score"],ce.stats["clean_trade_score"],ce.stats["flow"],ce.stats["ofi"])
                return TriWaveV2Signal()
            return TriWaveV2Signal(action="BUY_CE",side="CE",reason="TRI_WAVE_V2_ENTRY:CE_WAVE_RECOVERY",confidence=0.8)
        if pe_ok and not ce_ok:
            confirmed,count,started=self._confirm_entry(index,"PE",pe_reason,now)
            if started:
                logger.info("TRI_WAVE_V2_ENTRY_CONFIRM_START | index=%s | side=%s | phase=%s | support=%.2f | risk=%.2f | edge=%.2f | last5=%.2f | flow=%.2f | ofi=%.2f",index,"PE",pe.phase,pe.stats.get("dynamic_support_score",0.0),pe.stats.get("dynamic_risk_score",0.0),pe.stats.get("dynamic_edge",0.0),pe.stats.get("last_5_delta",0.0),pe.stats.get("flow",0.0),pe.stats.get("ofi",0.0))
            if not confirmed:
                logger.info("TRI_WAVE_V2_ENTRY_CONFIRM_WAIT | index=%s | side=PE | count=%s | required=%s | reason=%s | phase=%s | pos=%.2f | rec=%.2f | clean=%.2f | flow=%.2f | ofi=%.2f",index,count,self.ENTRY_CONFIRM_TICKS,pe_reason,pe.phase,pe.stats["position_in_range"],pe.stats["recovery_score"],pe.stats["clean_trade_score"],pe.stats["flow"],pe.stats["ofi"])
                return TriWaveV2Signal()
            return TriWaveV2Signal(action="BUY_PE",side="PE",reason="TRI_WAVE_V2_ENTRY:PE_WAVE_RECOVERY",confidence=0.8)
        best="CE" if (ce.stats.get("dynamic_edge",0.0),ce.stats.get("dynamic_support_score",0.0),-ce.stats.get("dynamic_risk_score",0.0))>(pe.stats.get("dynamic_edge",0.0),pe.stats.get("dynamic_support_score",0.0),-pe.stats.get("dynamic_risk_score",0.0)) else "PE"
        reason=ce_reason if best=="CE" else pe_reason
        confirmed,count,started=self._confirm_entry(index,best,reason,now)
        target=ce if best=="CE" else pe
        if started:
            logger.info("TRI_WAVE_V2_ENTRY_CONFIRM_START | index=%s | side=%s | phase=%s | support=%.2f | risk=%.2f | edge=%.2f | last5=%.2f | flow=%.2f | ofi=%.2f",index,best,target.phase,target.stats.get("dynamic_support_score",0.0),target.stats.get("dynamic_risk_score",0.0),target.stats.get("dynamic_edge",0.0),target.stats.get("last_5_delta",0.0),target.stats.get("flow",0.0),target.stats.get("ofi",0.0))
        if not confirmed:
            logger.info("TRI_WAVE_V2_ENTRY_CONFIRM_WAIT | index=%s | side=%s | count=%s | required=%s | reason=%s | phase=%s | pos=%.2f | rec=%.2f | clean=%.2f | flow=%.2f | ofi=%.2f",index,best,count,self.ENTRY_CONFIRM_TICKS,reason,target.phase,target.stats["position_in_range"],target.stats["recovery_score"],target.stats["clean_trade_score"],target.stats["flow"],target.stats["ofi"])
            return TriWaveV2Signal()
        return TriWaveV2Signal(action=f"BUY_{best}",side=best,reason=f"TRI_WAVE_V2_ENTRY:{best}_WAVE_RECOVERY",confidence=0.78)
