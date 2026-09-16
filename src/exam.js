(() => {
  const root = document.querySelector('.exam-app');
  const dataEl = document.querySelector('.exam-data');
  if (!root || !dataEl) return;
  const exam = JSON.parse(dataEl.textContent);
  const key = `kafka-cert-${root.dataset.examId}`;
  const count = exam.questions.length;
  const defaultMinutes = count >= 60 ? 90 : 60;
  let state = load() || {
    mode: 'intro', current: 0, answers: {}, flagged: {},
    startedAt: null, remaining: defaultMinutes * 60, submitted: false
  };
  let timer = null;

  function save() { localStorage.setItem(key, JSON.stringify(state)); }
  function load() { try { return JSON.parse(localStorage.getItem(key)); } catch { return null; } }
  function clear() { localStorage.removeItem(key); }
  function esc(s) { return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[c])); }
  function answeredCount() { return Object.keys(state.answers).length; }
  function flaggedCount() { return Object.values(state.flagged).filter(Boolean).length; }
  function score() { return exam.questions.reduce((n,q) => n + (state.answers[q.id] === q.answer ? 1 : 0), 0); }
  function recordHistory() { const wrongByQuestion={}; exam.questions.forEach(q=>{ if(state.answers[q.id] !== q.answer) wrongByQuestion[q.id]=(wrongByQuestion[q.id]||0)+1; }); let h=[]; try{h=JSON.parse(localStorage.getItem('kafka-cert-history')||'[]')}catch{} h.push({examId:root.dataset.examId,title:exam.title,percent:pct(),score:score(),count,timestamp:Date.now(),wrongByQuestion}); localStorage.setItem('kafka-cert-history',JSON.stringify(h.slice(-100))); }
  function pct() { return Math.round(score() / count * 100); }
  function formatTime(s) { s=Math.max(0,s); const h=Math.floor(s/3600), m=Math.floor((s%3600)/60), sec=s%60; return h ? `${h}:${String(m).padStart(2,'0')}:${String(sec).padStart(2,'0')}` : `${m}:${String(sec).padStart(2,'0')}`; }

  function intro() {
    const mins = defaultMinutes;
    root.innerHTML = `
      <section class="exam-hero">
        <div class="exam-eyebrow">KAFKA CERTIFICATION PRACTICE</div>
        <h2>${esc(exam.title)}</h2>
        <p class="exam-lead">${count} questions · ${mins} minute practice timer</p>
        <div class="exam-rules">
          <div><b>Exam mode</b><span>One question at a time</span></div>
          <div><b>Navigation</b><span>Previous, next, question grid</span></div>
          <div><b>Review</b><span>Flag uncertain questions</span></div>
          <div><b>Results</b><span>Answers revealed after submission</span></div>
        </div>
        ${state && (answeredCount() || state.current || state.startedAt) ? `<div class="resume-box"><b>Saved attempt found</b><span>${answeredCount()}/${count} answered${flaggedCount() ? ` · ${flaggedCount()} flagged` : ''}</span><button class="secondary" id="resume">Resume attempt</button><button class="ghost" id="restart">Start over</button></div>` : `<button class="primary start" id="start">Start exam</button>`}
      </section>`;
    root.querySelector('#start')?.addEventListener('click', start);
    root.querySelector('#resume')?.addEventListener('click', () => { state.mode='exam'; startTimer(); render(); });
    root.querySelector('#restart')?.addEventListener('click', () => { clear(); state={mode:'intro',current:0,answers:{},flagged:{},startedAt:null,remaining:mins*60,submitted:false}; intro(); });
  }

  function start() {
    state={mode:'exam',current:0,answers:{},flagged:{},startedAt:Date.now(),remaining:defaultMinutes*60,submitted:false};
    save(); startTimer(); render();
  }
  function startTimer() {
    clearInterval(timer);
    timer=setInterval(() => { if (state.mode !== 'exam') return; state.remaining--; save(); updateTimer(); if(state.remaining<=0){ state.remaining=0; submit(true); } },1000);
  }
  function updateTimer() { const el=root.querySelector('.timer'); if(el) el.textContent=formatTime(state.remaining); }
  function render() {
    const q=exam.questions[state.current];
    const answered=answeredCount();
    const opts=q.options.map((o,i)=>{ const letter='ABCD'[i], selected=state.answers[q.id]===letter; return `<button class="option ${selected?'selected':''}" data-letter="${letter}"><span class="letter">${letter}</span><span>${esc(o)}</span></button>`; }).join('');
    const grid=exam.questions.map((x,i)=>`<button class="qdot ${i===state.current?'current':''} ${state.answers[x.id]?'answered':''} ${state.flagged[x.id]?'flagged':''}" data-q="${i}" title="Question ${x.id}">${x.id}</button>`).join('');
    root.innerHTML=`
      <div class="exam-shell">
        <header class="exam-top">
          <div><div class="exam-eyebrow">${esc(exam.title)}</div><strong>Question ${q.id} of ${count}</strong></div>
          <div class="exam-status"><span>${answered}/${count} answered</span><span class="timer">${formatTime(state.remaining)}</span></div>
        </header>
        <div class="progress"><div style="width:${((state.current+1)/count)*100}%"></div></div>
        <div class="exam-layout">
          <main class="question-card">
            <div class="question-meta"><span>Question ${q.id}</span><button class="flag ${state.flagged[q.id]?'on':''}" id="flag">${state.flagged[q.id]?'⚑ Flagged':'⚐ Flag for review'}</button></div>
            <h2>${esc(q.question).replace(/\n/g,'<br>')}</h2>
            <div class="options">${opts}</div>
            <div class="nav-row">
              <button class="secondary" id="prev" ${state.current===0?'disabled':''}>← Previous</button>
              <button class="secondary" id="next">${state.current===count-1?'Review':'Next →'}</button>
            </div>
          </main>
          <aside class="exam-sidebar">
            <div class="side-card"><h3>Question navigator</h3><div class="qgrid">${grid}</div><div class="legend"><span><i class="answered-dot"></i>Answered</span><span><i class="flag-dot"></i>Flagged</span></div></div>
            <div class="side-card"><h3>Before submitting</h3><p>Review every flagged or unanswered question. You can change answers until submission.</p><button class="submit" id="submit">Submit exam</button></div>
          </aside>
        </div>
      </div>`;
    root.querySelectorAll('.option').forEach(b=>b.addEventListener('click',()=>{state.answers[q.id]=b.dataset.letter; save(); render();}));
    root.querySelector('#flag').addEventListener('click',()=>{state.flagged[q.id]=!state.flagged[q.id]; save(); render();});
    root.querySelector('#prev').addEventListener('click',()=>{state.current--; save(); render();});
    root.querySelector('#next').addEventListener('click',()=>{ if(state.current===count-1){ review(); } else {state.current++; save(); render();} });
    root.querySelectorAll('.qdot').forEach(b=>b.addEventListener('click',()=>{state.current=Number(b.dataset.q); save(); render();}));
    root.querySelector('#submit').addEventListener('click',()=>review());
    updateTimer();
  }
  function review() {
    const unanswered=exam.questions.filter(q=>!state.answers[q.id]).map(q=>q.id);
    const flagged=exam.questions.filter(q=>state.flagged[q.id]).map(q=>q.id);
    root.innerHTML=`<section class="review-card"><div class="exam-eyebrow">FINAL REVIEW</div><h2>Ready to submit?</h2><div class="review-stats"><div><b>${answeredCount()}</b><span>Answered</span></div><div><b>${unanswered.length}</b><span>Unanswered</span></div><div><b>${flagged.length}</b><span>Flagged</span></div><div><b>${formatTime(state.remaining)}</b><span>Time left</span></div></div>${unanswered.length?`<div class="warning"><b>Unanswered:</b> ${unanswered.join(', ')}</div>`:''}${flagged.length?`<div class="warning"><b>Flagged:</b> ${flagged.join(', ')}</div>`:''}<div class="review-actions"><button class="secondary" id="back">Back to exam</button><button class="submit" id="confirm">Submit and reveal score</button></div></section>`;
    root.querySelector('#back').addEventListener('click',render); root.querySelector('#confirm').addEventListener('click',()=>submit(false));
  }
  function submit(auto) {
    clearInterval(timer); state.mode='results'; state.submitted=true; recordHistory(); save(); results(auto);
  }
  function results(auto) {
    const s=score(), p=pct();
    const rows=exam.questions.map(q=>{const chosen=state.answers[q.id]||'—'; const ok=chosen===q.answer; return `<details class="result-row ${ok?'correct':'wrong'}"><summary><span class="result-icon">${ok?'✓':'✗'}</span><span><b>Q${q.id}</b> ${esc(q.question)}</span><span class="result-answer">${chosen} / ${q.answer}</span></summary><div class="result-body"><p><b>Your answer:</b> ${chosen==='—'?'Not answered':esc(q.options['ABCD'.indexOf(chosen)]||'')}</p><p><b>Correct answer:</b> ${q.answer}. ${esc(q.options['ABCD'.indexOf(q.answer)]||'')}</p></div></details>`;}).join('');
    root.innerHTML=`<section class="results-card"><div class="exam-eyebrow">EXAM COMPLETE${auto?' · TIME EXPIRED':''}</div><h2>Practice result</h2><div class="score-ring"><strong>${p}%</strong><span>${s} / ${count}</span></div><p class="result-note">Answers are now revealed for review. This score is for this practice attempt only.</p><div class="result-actions"><button class="primary" id="retake">Retake exam</button><button class="secondary" id="review-missed">Show missed only</button></div><h3>Question review</h3><div id="results-list">${rows}</div></section>`;
    root.querySelector('#retake').addEventListener('click',()=>{clear();state={mode:'intro',current:0,answers:{},flagged:{},startedAt:null,remaining:defaultMinutes*60,submitted:false};intro();});
    root.querySelector('#review-missed').addEventListener('click',()=>{const btn=root.querySelector('#review-missed'); const showingMissed=btn.dataset.mode!=='missed'; root.querySelectorAll('.result-row.correct').forEach(x=>x.hidden=showingMissed); btn.dataset.mode=showingMissed?'missed':'all'; btn.textContent=showingMissed?'Show all questions':'Show missed only';});
  }
  if(state.mode==='exam' && state.startedAt){ startTimer(); render(); } else if(state.mode==='results'){ results(false); } else intro();
})();
