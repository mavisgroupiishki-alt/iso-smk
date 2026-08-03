const fs = require('fs');
const vm = require('vm');
const html = fs.readFileSync(require('path').join(__dirname, '..', 'index.html'),'utf8');
const scripts = [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/gi)].map(m=>m[1]).join('\n');
const localMap = new Map();
const serverMap = new Map();
const localStorage = {
  get length(){return localMap.size;}, key(i){return [...localMap.keys()][i] ?? null;},
  getItem(k){return localMap.has(k)?localMap.get(k):null;}, setItem(k,v){localMap.set(String(k),String(v));}, removeItem(k){localMap.delete(k);}
};
function resp(obj, ok=true, status=200){return {ok,status,json:async()=>obj,text:async()=>JSON.stringify(obj)};}
async function fetch(url, opts={}){
  if(url.startsWith('/api/kv/set')){const b=JSON.parse(opts.body);serverMap.set(b.key,b.value);return resp({success:true,key:b.key});}
  if(url.startsWith('/api/kv/get')){const key=decodeURIComponent(url.split('key=')[1]||'');return resp({key,value:serverMap.has(key)?serverMap.get(key):null});}
  if(url.startsWith('/api/kv/list')){const prefix=decodeURIComponent(url.split('prefix=')[1]||'');return resp({keys:[...serverMap.keys()].filter(k=>k.startsWith(prefix))});}
  if(url.startsWith('/api/kv/delete')){const b=JSON.parse(opts.body);serverMap.delete(b.key);return resp({success:true});}
  return resp({});
}
const dummy = {style:{},classList:{add(){},remove(){}},addEventListener(){},insertAdjacentHTML(){},innerHTML:'',textContent:'',value:'',disabled:false};
const document = {addEventListener(){},getElementById(){return dummy;},querySelector(){return dummy;},querySelectorAll(){return [];},createElement(){return {...dummy,remove(){},appendChild(){}};},body:{appendChild(){}},visibilityState:'visible'};
const context = {console,localStorage,fetch,document,window:{addEventListener(){}},globalThis:null,crypto:require('crypto').webcrypto,setInterval(){return 1;},clearInterval(){},setTimeout,clearTimeout,Blob,atob,btoa,URL:{createObjectURL(){return''},revokeObjectURL(){}},confirm(){return true},alert(){}};
context.globalThis=context; vm.createContext(context); vm.runInContext(scripts,context);
(async()=>{
  vm.runInContext(`aiCurrentData={company:{form:'ООО',name:'Тест',unp:'123',bank_name:'Банк'},staff:[{fio:'Иванов',position:'Прораб'}],objects:[{name:'Объект'}],suppliers:[{name:'Поставщик'}],spk:{measurement_tools:[{name:'Рулетка'}],ttk:[{code:'ТТК-1'}]},certification:{standard:'spk_stroy'}};aiHistory=[{role:'user',content:'test'}];`,context);
  await vm.runInContext('aiSaveCurrentCompany()',context);
  const key=vm.runInContext('aiCurrentCompanyKey',context);
  const saved=JSON.parse(localStorage.getItem(key));
  if(saved.data.spk.measurement_tools[0].name!=='Рулетка') throw new Error('SPK data lost');
  // Partial update must not delete existing staff/objects/SPK.
  vm.runInContext(`aiRenderCard({company:{phone:'+375'},staff:[{fio:'Иванов',ot_certificate:true}],objects:[{name:'Второй объект'}],spk:{ttk:[{code:'ТТК-2'}]}})`,context);
  const data=vm.runInContext('aiCurrentData',context);
  if(data.staff.length!==1 || !data.staff[0].ot_certificate) throw new Error('staff merge failed');
  if(data.objects.length!==2) throw new Error('objects merge failed');
  if(data.spk.measurement_tools.length!==1 || data.spk.ttk.length!==1 || data.spk.ttk[0].code!=='ТТК-2') throw new Error('deep SPK merge failed');
  vm.runInContext('aiSaveCurrentCompanyLocal()',context);
  vm.runInContext('aiCurrentData={};aiHistory=[];aiCurrentCompanyKey=null;',context);
  if(!(await vm.runInContext(`aiLoadCompany(${JSON.stringify(key)})`,context))) throw new Error('reload failed');
  if(vm.runInContext('aiCurrentData.company.phone',context)!=='+375') throw new Error('company requisites not restored');
  console.log('V13 FRONTEND PERSISTENCE TEST PASSED');
})().catch(e=>{console.error(e);process.exit(1)});
