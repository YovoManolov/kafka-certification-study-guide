(() => {
  const root = document.querySelector('.exam-dashboard');
  if (!root) return;
  const exams = [
    ['mock-1','Mock Exam A — Developer Fundamentals','CCDAK','50'],
    ['mock-2','Mock Exam B — Producer / Consumer','CCDAK','50'],
    ['mock-3','Mock Exam C — Schema / Connect / Streams','CCDAK','50'],
    ['mock-4','Mock Exam D — Administrator','CCAAK','50'],
    ['mock-5','Mock Exam E — Security / Networking','CCAAK','50'],
    ['mock-6','Mock Exam F — Operations / Troubleshooting','CCAAK','50'],
    ['mock-7','Mock Exam G — Full CCDAK Simulation','CCDAK','60'],
    ['mock-8','Mock Exam H — Full CCAAK Simulation','CCAAK','60']
  ];
  const esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[c]));
  const getAttempts=()=>{try{return JSON.parse(localStorage.getItem('kafka-cert-history')||'[]')}catch{return[]}};
  const attempts=getAttempts();
  const latestByExam={};
  attempts.forEach(a=>{if(!latestByExam[a.examId] || a.timestamp>latestByExam[a.examId].timestamp) latestByExam[a.examId]=a});
  const scores=Object.values(latestByExam).map(a=>a.percent);
  const avg=scores.length?Math.round(scores.reduce((a,b)=>a+b,0)/scores.length):0;
  const best=scores.length?Math.max(...scores):0;
  const attempted=Object.keys(latestByExam).length;
  const weak=[];
  attempts.forEach(a=>Object.entries(a.wrongByQuestion||{}).forEach(([q,n])=>weak.push({exam:a.title,q,count:n})));
  const grouped={}; weak.forEach(x=>{const k=x.exam+' · Q'+x.q; grouped[k]=(grouped[k]||0)+x.count});
  const topWeak=Object.entries(grouped).sort((a,b)=>b[1]-a[1]).slice(0,8);
  root.innerHTML=`<section class="dash-hero"><div><div class="exam-eyebrow">CERTIFICATION TRAINING PLATFORM</div><h2>Progress Dashboard</h2><p>Practice results stay in this browser. Nothing is uploaded to a server.</p></div><button class="secondary" id="clear-history">Reset local progress</button></section>
  <section class="dash-stats"><div><b>${attempted}/8</b><span>Exams attempted</span></div><div><b>${avg||'—'}%</b><span>Latest-attempt average</span></div><div><b>${best||'—'}%</b><span>Best latest score</span></div><div><b>${attempts.length}</b><span>Total attempts</span></div></section>
  <section class="dash-grid"><div class="dash-panel"><h3>Exam progress</h3>${exams.map(([id,title,cert,count])=>{const a=latestByExam[id];return `<div class="dash-exam"><div><strong>${esc(title)}</strong><small>${cert} · ${count} questions</small></div><div class="dash-score ${a?(a.percent>=85?'good':a.percent>=70?'mid':'low'):''}">${a?a.percent+'%':'Not attempted'}</div></div>`}).join('')}</div>
  <div class="dash-panel"><h3>Repeated weak questions</h3>${topWeak.length?topWeak.map(([k,n])=>`<div class="weak-item"><span>${esc(k)}</span><b>${n}× wrong</b></div>`).join(''):'<p class="muted">Complete an exam to build your weak-area history.</p>'}</div></section>
  <section class="dash-panel"><h3>Recommended thresholds</h3><div class="thresholds"><span><b>70%</b> first-pass target</span><span><b>80%</b> strong practice target</span><span><b>85%+</b> simulation target</span></div></section>`;
  root.querySelector('#clear-history').onclick=()=>{if(confirm('Reset all local exam history and saved attempts?')){Object.keys(localStorage).filter(k=>k.startsWith('kafka-cert-')).forEach(k=>localStorage.removeItem(k));location.reload();}};
})();