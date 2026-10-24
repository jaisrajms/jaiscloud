#!/usr/bin/env python3
"""crypto-medallion leaderboard — Cloud Run page + JSON API (design §5.4).

Reads the per-symbol Firestore documents the publisher writes and serves a live
leaderboard. The page polls `/api/leaderboard` twice a second and updates the
rows in place — tweened numbers, animated volume bars, a smooth reorder and a
flash when a value actually changes — so the recording always has motion even
between data windows (design §9).
"""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

EMULATOR = os.environ.get("EMULATOR", "http://jaiscloud-gcp.jaiscloud.svc.cluster.local:8080")
PROJECT = os.environ.get("PROJECT", "crypto-medallion")
COLLECTION = os.environ.get("FIRESTORE_COLLECTION", "leaderboard")
PORT = int(os.environ.get("PORT", "8080"))

PAGE = """<!doctype html><html><head><meta charset="utf-8">
<title>crypto medallion · live leaderboard</title>
<style>
 :root{--row:64px}
 body{margin:0;font-family:system-ui,-apple-system,sans-serif;background:#0b1220;color:#e6edf3;overflow:hidden}
 header{padding:18px 24px;border-bottom:1px solid #1e2a44;display:flex;align-items:baseline;gap:12px}
 h1{font-size:19px;margin:0;font-weight:650}
 .tag{font-size:12px;color:#7d8db1}
 .dot{width:9px;height:9px;border-radius:50%;background:#2ecc71;display:inline-block;margin-right:6px;
      box-shadow:0 0 0 0 rgba(46,204,113,.7);animation:pulse 1.1s infinite}
 @keyframes pulse{0%{box-shadow:0 0 0 0 rgba(46,204,113,.7);opacity:1}
                  70%{box-shadow:0 0 0 9px rgba(46,204,113,0);opacity:.9}
                  100%{box-shadow:0 0 0 0 rgba(46,204,113,0);opacity:1}}
 .grid{display:grid;grid-template-columns:120px 1fr 140px 130px 90px 160px;align-items:center}
 .head{padding:10px 24px;color:#7d8db1;font-size:11px;letter-spacing:.08em;text-transform:uppercase}
 .head div:nth-child(n+3){text-align:right}
 #board{position:relative;margin:2px 0}
 .row{position:absolute;left:0;right:0;height:var(--row);padding:0 24px;box-sizing:border-box;
      display:grid;grid-template-columns:120px 1fr 140px 130px 90px 160px;align-items:center;
      border-bottom:1px solid #16203a;background:#0b1220;
      transition:transform .65s cubic-bezier(.2,.8,.2,1),background .5s ease}
 .row.flash{background:#12233f}
 .sym{font-weight:650;letter-spacing:.02em}
 .barwrap{height:8px;background:#16203a;border-radius:6px;overflow:hidden;margin-right:22px}
 .bar{height:100%;width:0;background:linear-gradient(90deg,#2563eb,#22d3ee);
      transition:width .7s cubic-bezier(.2,.8,.2,1)}
 .num{text-align:right;font-variant-numeric:tabular-nums}
 .vwap{text-align:right;font-variant-numeric:tabular-nums;font-weight:600}
 .tr{text-align:right}
 .delta{font-size:11px;margin-left:6px}
 .up{color:#2ecc71}.down{color:#f87171}
 .we{text-align:right;color:#7d8db1;font-size:12px}
 footer{padding:12px 24px;color:#7d8db1;font-size:12px}
</style></head>
<body>
<header><span class="dot"></span><h1>Live crypto leaderboard</h1>
<span class="tag">jaiscloud-gcp · Firestore read model · Eventarc-driven</span></header>
<div class="grid head"><div>Symbol</div><div>Volume</div><div>VWAP</div><div>Volume</div><div>Trades</div><div>Window end</div></div>
<div id="board"></div>
<footer id="stamp">live · waiting for data…</footer>
<script>
const ROW=64, board=document.getElementById('board'), state={};
let lastData=0, seen=0;
const el=(t,c,x)=>{const e=document.createElement(t); if(c)e.className=c; if(x!=null)e.textContent=x; return e;};
function makeRow(sym){
  const r=el('div','row'); r.style.transform='translateY(-200px)';
  r.appendChild(el('div','sym',sym));
  const bw=el('div','barwrap'), b=el('div','bar'); bw.appendChild(b); r.appendChild(bw);
  const vwap=el('div','vwap','—'); r.appendChild(vwap);
  const vol=el('div','num','—'), d=el('span','delta',''); vol.appendChild(d); r.appendChild(vol);
  const tr=el('div','tr','—'); r.appendChild(tr);
  const we=el('div','we','—'); r.appendChild(we);
  board.appendChild(r);
  state[sym]={el:r,bar:b,vwap,vwapVal:0,vol,volVal:0,d,tr,we,prev:null};
}
function tween(st,key,to,apply){
  const from=st[key]; st[key]=to;
  if(from===to){apply(to);return;}
  const t0=performance.now();
  (function step(t){const k=Math.min(1,(t-t0)/700), e=k*(2-k);
    apply(from+(to-from)*e); if(k<1)requestAnimationFrame(step);})(t0);
}
async function tick(){
  try{
    const rows=await (await fetch('/api/leaderboard',{cache:'no-store'})).json();
    rows.sort((a,b)=>b.volume-a.volume);
    const maxVol=Math.max(1,...rows.map(x=>x.volume));
    rows.forEach((x,i)=>{
      if(!state[x.symbol]) makeRow(x.symbol);
      const st=state[x.symbol];
      st.el.style.transform=`translateY(${i*ROW}px)`;
      st.bar.style.width=Math.max(2,100*x.volume/maxVol)+'%';
      tween(st,'vwapVal',Number(x.vwap), v=>st.vwap.textContent=v.toLocaleString(undefined,{maximumFractionDigits:4}));
      tween(st,'volVal',Number(x.volume), v=>st.vol.firstChild.nodeValue=v.toLocaleString(undefined,{maximumFractionDigits:4}));
      st.tr.textContent=x.trades;
      st.we.textContent=(x.window_end||'').replace('T',' ').slice(0,19);
      if(st.prev!==null && Number(x.volume)!==st.prev){
        st.el.classList.add('flash'); setTimeout(()=>st.el.classList.remove('flash'),600);
        st.d.textContent=Number(x.volume)>st.prev?'▲':'▼';
        st.d.className='delta '+(Number(x.volume)>st.prev?'up':'down');
      }
      st.prev=Number(x.volume);
    });
    board.style.height=(rows.length*ROW)+'px';
    if(rows.length){lastData=Date.now(); seen=rows.length;}
  }catch(e){document.getElementById('stamp').textContent='feed error: '+e;}
}
setInterval(tick,500); tick();
setInterval(()=>{
  const s=lastData?Math.round((Date.now()-lastData)/1000):null;
  document.getElementById('stamp').textContent='live · '+seen+' symbols · last data '+
    (s===null?'—':s+'s ago')+' · '+new Date().toLocaleTimeString();
},250);
</script></body></html>"""


def http_get(path: str):
    req = urllib.request.Request(EMULATOR + path)
    req.add_header("Authorization", "Bearer demo-token")
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read() or b"{}")


def value(field: dict):
    for key in ("stringValue", "doubleValue", "integerValue", "timestampValue",
                "booleanValue"):
        if key in field:
            v = field[key]
            if key == "integerValue":
                return int(v)
            return v
    return None


def leaderboard_rows() -> list[dict]:
    path = f"/v1/projects/{PROJECT}/databases/(default)/documents/{COLLECTION}"
    data = http_get(path)
    rows = []
    for doc in data.get("documents", []):
        rows.append({k: value(v) for k, v in doc.get("fields", {}).items()})
    rows.sort(key=lambda r: float(r.get("volume") or 0), reverse=True)
    return rows


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        return

    def _send(self, code, body: bytes, ctype="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/api/leaderboard"):
            try:
                body = json.dumps(leaderboard_rows()).encode()
            except Exception as exc:
                body = json.dumps({"error": str(exc)}).encode()
            self._send(200, body)
        elif self.path.startswith("/health"):
            self._send(200, b'{"status":"ok"}')
        else:
            self._send(200, PAGE.encode(), "text/html; charset=utf-8")


def main() -> None:
    print(f"[leaderboard] listening on :{PORT}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
