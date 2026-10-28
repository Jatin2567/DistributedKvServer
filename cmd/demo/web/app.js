const $ = id => document.getElementById(id);
const text = (tag, value, cls) => {const el=document.createElement(tag);el.textContent=value;if(cls)el.className=cls;return el;};
async function refresh(){
 try{
  const response=await fetch('/api/cluster');if(!response.ok)throw Error('Unavailable');const nodes=await response.json();
  $('nodes').replaceChildren(...nodes.map(node=>{const card=text('article','','node'+(node.role==='Leader'?' leader':''));const head=text('div','','nodehead');head.append(text('strong',node.id),text('span',node.role,'role'));const metrics=text('div','','metrics');for(const [label,value] of [['TERM',node.term],['LOG ENTRIES',node.log_length],['COMMITTED',node.commit_index+1]]){const metric=text('div','');metric.append(text('small',label),text('b',value));metrics.append(metric)}card.append(head,metrics,text('div',`${node.applied+1} entries applied · in-memory storage`,'nodefoot'));return card}));
  $('connection').textContent=nodes.some(n=>n.role==='Leader')?'Cluster online · 3 nodes':'Electing a leader…';
 }catch{$('connection').textContent='Connection lost · retrying…'}
}
async function operate(method){
 if(!$('key').reportValidity())return;const key=$('key').value;
 document.querySelectorAll('button').forEach(b=>b.disabled=true);$('result').textContent='Waiting for cluster…';const started=performance.now();
 try{const response=await fetch('/api/kv'+(method==='GET'?'?key='+encodeURIComponent(key):''),{method,headers:{'Content-Type':'application/json'},...(method==='GET'?{}:{body:JSON.stringify({key,value:$('value').value})})});if(!response.ok)throw Error(await response.text());const result=await response.json();$('result').textContent=JSON.stringify(result,null,2);if($('activity').querySelector('.empty'))$('activity').replaceChildren();$('activity').prepend(text('li',`${new Date().toLocaleTimeString()} · ${method} ${key} · ${Math.round(performance.now()-started)} ms`));while($('activity').children.length>20)$('activity').lastChild.remove();await refresh();}
 catch(error){$('result').textContent=error.message;}finally{document.querySelectorAll('button').forEach(b=>b.disabled=false)}
}
$('kvform').addEventListener('submit',event=>{event.preventDefault();operate('PUT')});$('read').addEventListener('click',()=>operate('GET'));$('delete').addEventListener('click',()=>operate('DELETE'));
async function poll(){await refresh();setTimeout(poll,1000)}poll();
