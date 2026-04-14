#!/usr/bin/env python3
"""ИСО/СМК Генератор. Запуск: python server.py  →  http://localhost:8766"""
import sys,json,os,shutil,tempfile,base64,zipfile,re
import http.server,socketserver
from pathlib import Path
from datetime import datetime,timedelta

BASE_DIR    = Path(__file__).parent.resolve()
TPL_DIR     = BASE_DIR/'templates'/'ISO_shablon'/'ИСО ЭнергоМагистраль'
# On cloud (Render) use persistent disk, locally use app folder
_DATA = Path('/data') if Path('/data').exists() else BASE_DIR
JOURNAL_DIR = _DATA/'journal'
CO_DIR      = _DATA/'companies'
OUT_DIR     = _DATA/'output'
PORT=int(os.environ.get("PORT", 8766))

for d in [JOURNAL_DIR,CO_DIR,OUT_DIR]: d.mkdir(parents=True,exist_ok=True)

def date_dot(s):
    s=str(s).strip()
    if '-' in s:
        p=s.split('-'); return f"{int(p[2]):02d}.{int(p[1]):02d}.{p[0]}"
    return s

def parse_date(s):
    s=date_dot(s); p=s.split('.'); return int(p[0]),int(p[1]),int(p[2])

def date_minus(s,days):
    d,m,y=parse_date(s)
    dt=datetime(y,m,d)-timedelta(days=days)
    return dt.strftime('%d.%m.%Y')

def year_of(s):
    _,_,y=parse_date(s); return str(y)


def merge_date_runs(xml):
    """Merge split dates: DD + .MM.YYYY across XML runs"""
    import re as _re
    pattern = (
        r'(<w:t[^>]*>)(\d{2})(</w:t></w:r>)'
        r'(?:<w:bookmarkStart[^/]*/>\s*<w:bookmarkEnd[^/]*/>\s*)?'
        r'(?:<w:proofErr[^/]*/>\s*)*'
        r'(<w:r[^>]*>(?:<w:rPr>.*?</w:rPr>)?<w:t[^>]*>)'
        r'(\.\d{2}\.\d{4})'
    )
    return _re.sub(pattern,
        lambda m: m.group(1)+m.group(2)+m.group(5)+m.group(3)+m.group(4),
        xml, flags=_re.DOTALL)

def merge_name_runs(xml):
    """Merge split text runs (single letter + rest, or word + initials split by proofErr)"""
    import re as _re
    # Fix 1: single letter + rest of word (e.g. "Д" + "иректора")
    # Only merge if result would be a 4+ char Cyrillic word (not initials)
    p1 = (
        r'(<w:t[^>]*>)([А-ЯA-Z])(</w:t></w:r>)'
        r'(?:<w:bookmarkStart[^/]*/>\s*<w:bookmarkEnd[^/]*/>\s*)?'
        r'(<w:r[^>]*>(?:<w:rPr>.*?</w:rPr>)?<w:t[^>]*>)'
        r'([а-яё]{3,})'
    )
    xml = _re.sub(p1, lambda m: m.group(1)+m.group(2)+m.group(5)+m.group(3)+m.group(4),
        xml, flags=_re.DOTALL)
    # Fix 2: standalone surname run + proofErr + initials run
    # The surname must be the ENTIRE content of its <w:t> tag
    p2 = (
        r'(>[А-Я][а-яё\-]{2,})(</w:t></w:r>)'
        r'(?:<w:proofErr[^/]*/>\s*)+'
        r'(<w:r[^>]*>(?:<w:rPr>.*?</w:rPr>)?<w:t[^>]*>)'
        r'( [А-ЯA-Z]\.[А-ЯA-Z]\.)'
    )
    xml = _re.sub(p2,
        lambda m: m.group(1)+m.group(4)+m.group(2)+m.group(3),
        xml, flags=_re.DOTALL)
    # Fix 3: partial word + 1-3 char suffix in adjacent runs (e.g. "Нестерён"+"ка")
    # Safe: only joins if combined result is a valid word continuation
    p3 = (
        r'(>[А-Яа-яё\-]{4,})(</w:t></w:r>)'
        r'(?:<w:bookmarkStart[^/]*/>\s*<w:bookmarkEnd[^/]*/>\s*)?'
        r'(<w:r[^>]*>(?:<w:rPr>.*?</w:rPr>)?<w:t[^>]*>)'
        r'([а-яё]{1,3}</w:t>)'
    )
    def _join_p3(m):
        suffix = m.group(4).replace('</w:t>','')
        return m.group(1)+suffix+m.group(2)+m.group(3)+'</w:t>'
    xml = _re.sub(p3, _join_p3, xml, flags=_re.DOTALL)
    return xml

def replace_in_docx(src,dst,reps):
    with tempfile.TemporaryDirectory() as td:
        tmp=os.path.join(td,'s.docx'); shutil.copy2(str(src),tmp)
        up=os.path.join(td,'up'); os.makedirs(up)
        with zipfile.ZipFile(tmp,'r') as z: z.extractall(up)
        for root,dirs,files in os.walk(up):
            for fn in files:
                if not fn.endswith('.xml'): continue
                fp=os.path.join(root,fn)
                try:
                    with open(fp,'r',encoding='utf-8') as f: c=f.read()
                    c=merge_date_runs(c)
                    c=merge_name_runs(c)
                    ch=False
                    for o,n in reps:
                        if o and n is not None and o in c: c=c.replace(o,n); ch=True
                    if ch:
                        with open(fp,'w',encoding='utf-8') as f: f.write(c)
                except: pass
        Path(dst).parent.mkdir(parents=True,exist_ok=True)
        with zipfile.ZipFile(str(dst),'w',zipfile.ZIP_DEFLATED) as zo:
            for root,dirs,files in os.walk(up):
                for fn in files:
                    fp=os.path.join(root,fn)
                    zo.write(fp,os.path.relpath(fp,up))

def build_reps(data):
    org =data['orgName']; form=data.get('orgForm','ООО'); city=data.get('city','Минск')
    scope=data.get('scope','производства строительно-монтажных работ')
    ds=data['dirSurname']; di=data['dirInitials']
    a1p=data.get('aud1Post','директор'); a1s=data.get('aud1Surname',ds); a1i=data.get('aud1Initials',di)
    a2p=data.get('aud2Post',''); a2s=data.get('aud2Surname',''); a2i=data.get('aud2Initials','')
    a3p=data.get('aud3Post',''); a3s=data.get('aud3Surname',''); a3i=data.get('aud3Initials','')
    sp=data.get('secPost',a2p); ss=data.get('secSurname',a2s); si=data.get('secInitials',a2i)
    impl=date_dot(data.get('implDate','')); start=date_dot(data.get('startDate',impl))
    end=date_dot(data.get('endDate',impl)); ord1=date_minus(impl,4); yr=year_of(impl)
    def cap(s): return s[0].upper()+s[1:] if s else s
    def gen(s):
        if s.endswith(('ов','ев','ин','ын')): return s+'а'
        return s+'а'
    r=[
        ('ЭнергоМагистраль',org), ('«ЭнергоМагистраль»',f'«{org}»'),
        (f'ООО «ЭнергоМагистраль»',f'{form} «{org}»'),
        ('А.А. Шакуро',f'{di} {ds}'), ('Шакуро А.А.',f'{ds} {di}'),
        ('директора А.А. Шакуро',f'директора {di} {ds}'),
        ('директора Шакуро А.А.',f'директора {ds} {di}'),
        ('Директора А.А. Шакуро',f'Директора {di} {ds}'),
        ('Директора Шакуро А.А.',f'Директора {ds} {di}'),
        ('Директору А.А. Шакуро',f'Директору {di} {ds}'),
        ('В.В. Семенчуков',f'{a2i} {a2s}'),
        ('Семенчуков В.В.',f'{a2s} {a2i}'),
        ('Семенчукова В.В.',f'{gen(a2s)} {a2i}'),
        ('главного инженера Семенчукова В.В.',f'{a2p} {gen(a2s)} {a2i}'),
        ('Главного инженера Семенчукова В.В.',f'{cap(a2p)} {gen(a2s)} {a2i}'),
        ('Главного инженера В.В. Семенчукова',f'{cap(a2p)} {a2i} {gen(a2s)}'),
        ('Главный инженер В.В. Семенчуков',f'{cap(a2p)} {a2i} {a2s}'),
        ('С.Д. Нестерёнок',f'{a3i} {a3s}' if a3s else None),
        ('Нестерёнок С.Д.',f'{a3s} {a3i}' if a3s else None),
        ('производителя работ Нестерёнок С.Д.',f'{a3p} {a3s} {a3i}' if a3s else None),
        # Full dative patterns for the ТО СИ order
        ('Производителю работ С.Д. Нестерёнку', f'{cap(a3p)}у работ {a3i} {a3s}' if a3s else None),
        ('Производителю работ Нестерёнку', f'{cap(a3p)}у работ {a3s}' if a3s else None),
        ('производителю работ С.Д. Нестерёнку', f'{a3p}у работ {a3i} {a3s}' if a3s else None),
        ('производителя работ С.Д. Нестерёнок',f'{a3p} {a3i} {a3s}' if a3s else None),
        ('Производитель работ С.Д. Нестерёнок',f'{cap(a3p)} {a3i} {a3s}' if a3s else None),
        ('Председатель КС: Директор А.А. Шакуро',f'Председатель КС: {cap(a1p)} {a1i} {a1s}'),
        ('Члены КС: Главный инженер В.В. Семенчуков',f'Члены КС: {cap(a2p)} {a2i} {a2s}'),
        ('        Производитель работ С.Д. Нестерёнок',
            f'        {cap(a3p)} {a3i} {a3s}' if a3s else f'        {cap(a3p)}'),
        ('директора Шакуро А.А., главного инженера Семенчукова В.В., производителя работ С.Д. Нестерёнок',
            f'{a1p} {a1s} {a1i}, {a2p} {a2s} {a2i}'+(f', {a3p} {a3s} {a3i}' if a3s else '')),
        ('Назначить Главного инженера Семенчукова В.В.  ответственным за управление фондом ТТК, ТНПА, НПА.',
            f'Назначить {cap(sp)} {ss} {si} ответственным за управление фондом ТТК, ТНПА, НПА.'),
        ('выполнения функций генерального подрядчика, производства строительно-монтажных работ',scope),
        ('выполнение функций генерального подрядчика, производство строительно-монтажных работ',scope),
        ('16.02.2026 г. № 1',f'{ord1} г. № 1'),('09.02.2026 г. № 1',f'{ord1} г. № 1'),
        ('04.02.2026 г. № 1',f'{ord1} г. № 1'),
        ('16.02.2026',impl),('09.02.2026',ord1),('04.02.2026',ord1),
        ('09.03.2026',end),('02.03.2026',end),('12.01.2026',start),
        ('2026г.',f'{yr}г.'),('2026 г.',f'{yr} г.'),
        ('на 2026 год',f'на {yr} год'),('на 2026 г',f'на {yr} г'),
        (' 2026год',f' {yr}год'),('2026год',f'{yr}год'),
        ('г. Минск',f'г. {city}'),
        # Standalone surnames (catch remaining split runs where only surname is in the run)
        ('Шакуро', ds),
        ('Нестерёнок', a3s if a3s else None),
        # "Нестерёнк" split form (before ка/ку suffix gets appended)
        ('Нестерёнк', a3s[:-2]+'к' if (a3s and a3s.endswith('ок')) else a3s if a3s else None),
        ('Нестерёнка', (a3s[:-2]+'ка' if a3s.endswith('ок') else a3s+'а') if a3s else None),
        ('Нестерёнку', (a3s[:-2]+'ку' if a3s.endswith('ок') else a3s+'у') if a3s else None),
        ('Семенчукова', (a2s[:-2]+'ича' if a2s.endswith('ич') else a2s+'а') if a2s else None),
        ('Семенчуков', a2s if a2s else None),
        ('Никитича', (a2s+'а') if a2s else None),
        ('Никитич', a2s if a2s else None),
    ]
    return [(o,n) for o,n in r if o and n is not None]


def replace_itr_table(src, dst, itr_list, impl_date):
    """Replace ITR table in 2.2 лист ознакомления с целями"""
    import shutil, tempfile as _tmp
    with _tmp.TemporaryDirectory() as td:
        import os as _os
        tmp = _os.path.join(td,'s.docx')
        shutil.copy2(str(src), tmp)
        up = _os.path.join(td,'up'); _os.makedirs(up)
        with zipfile.ZipFile(tmp,'r') as z: z.extractall(up)
        fp = _os.path.join(up,'word','document.xml')
        with open(fp,'r',encoding='utf-8') as f: xml = f.read()
        # Find all table rows
        row_matches = list(re.finditer(r'<w:tr[ >].*?</w:tr>', xml, re.DOTALL))
        if len(row_matches) > 1:
            # Get first data row as template
            tmpl = row_matches[1].group(0)
            # Build new rows for each ITR person
            new_rows = ''
            for person in itr_list:
                fio = person.get('fio','').strip()
                if not fio: continue
                row = tmpl
                # Replace FIO (first w:t content)
                row = re.sub(r'(<w:t[^>]*>)[^<]*(</w:t>)',
                    lambda m, v=fio: m.group(1)+v+m.group(2), row, count=1)
                # Replace date
                row = re.sub(r'(<w:t[^>]*>)\d{2}\.\d{2}\.\d{4}(</w:t>)',
                    lambda m, d=impl_date: m.group(1)+d+m.group(2), row)
                # Clear signature cell
                new_rows += row
            # Replace all data rows with new content
            start = row_matches[1].start()
            end   = row_matches[-1].end()
            xml = xml[:start] + new_rows + xml[end:]
        with open(fp,'w',encoding='utf-8') as f: f.write(xml)
        Path(dst).parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(str(dst),'w',zipfile.ZIP_DEFLATED) as zo:
            for root,_,files in _os.walk(up):
                for fn in files:
                    fpath = _os.path.join(root,fn)
                    zo.write(fpath, _os.path.relpath(fpath,up))

def generate_all(data,out_dir):
    reps=build_reps(data); org=data['orgName']
    impl=date_dot(data.get('implDate',''))
    # Parse ITR list from form (list of {fio, post} dicts or newline-separated string)
    itr_raw = data.get('itrList','')
    itr_list = []
    if isinstance(itr_raw, list):
        itr_list = itr_raw
    elif itr_raw:
        for line in itr_raw.strip().split('\n'):
            line = line.strip()
            if line: itr_list.append({'fio': line})
    Path(out_dir).mkdir(parents=True,exist_ok=True); done=[]
    for src in TPL_DIR.rglob('*'):
        if src.is_dir(): continue
        if not src.name.endswith(('.docx','.doc')): continue
        parts=list(src.relative_to(TPL_DIR).parts)
        parts[-1]=parts[-1].replace('ЭнергоМагистраль',org)
        rel=os.path.join(*parts); dst=Path(out_dir)/rel
        try:
            is_22 = '2.2' in src.name and 'ознакомл' in src.name.lower()
            if src.name.endswith('.docx') and is_22 and itr_list:
                replace_itr_table(src, dst, itr_list, impl)
                # Also apply standard replacements on top
                replace_in_docx(dst, dst, reps)
            elif src.name.endswith('.docx'): replace_in_docx(src,dst,reps)
            else: dst.parent.mkdir(parents=True,exist_ok=True); shutil.copy2(src,dst)
            done.append({'name':parts[-1],'path':str(dst),'rel':rel})
        except Exception as e: print(f'  ERR {src.name}: {e}')
    return done

def get_companies():
    return [json.loads(f.read_text('utf-8')) for f in sorted(CO_DIR.glob('*.json'))
            if not f.name.startswith('.')]

def save_company(data):
    cid=data.get('id') or f"c{int(datetime.now().timestamp()*1000)}"
    data['id']=cid
    (CO_DIR/f'{cid}.json').write_text(json.dumps(data,ensure_ascii=False,indent=2),'utf-8')
    return cid

def get_journal():
    return [json.loads(f.read_text('utf-8')) for f in sorted(JOURNAL_DIR.glob('*.json'),reverse=True)]

def save_journal(entry):
    eid=f"j{int(datetime.now().timestamp()*1000)}"
    entry.update({'id':eid,'created':datetime.now().strftime('%d.%m.%Y %H:%M')})
    (JOURNAL_DIR/f'{eid}.json').write_text(json.dumps(entry,ensure_ascii=False,indent=2),'utf-8')
    return eid

def get_zip(eid):
    for f in JOURNAL_DIR.glob('*.json'):
        try:
            e=json.loads(f.read_text('utf-8'))
            if e.get('id')==eid:
                zp=e.get('zipPath'); return zp if zp and os.path.exists(zp) else None
        except: pass

INDEX=(BASE_DIR/'index.html').read_text('utf-8')

class H(http.server.BaseHTTPRequestHandler):
    def log_message(self,*a): pass
    def do_GET(self):
        p=self.path.split('?')[0]
        if p in('/','//index.html'):           self._html(INDEX)
        elif p=='/api/companies':              self._json(get_companies())
        elif p=='/api/journal':                self._json(get_journal())
        elif p.startswith('/api/download/'):
            zp=get_zip(p.split('/')[-1])
            if zp:
                d=open(zp,'rb').read()
                self.send_response(200)
                self.send_header('Content-Type','application/zip')
                self.send_header('Content-Disposition','attachment; filename="ISO_docs.zip"')
                self.send_header('Content-Length',str(len(d)))
                self.end_headers(); self.wfile.write(d)
            else: self.send_response(404); self.end_headers()
        else: self.send_response(404); self.end_headers()

    def do_POST(self):
        body=self.rfile.read(int(self.headers.get('Content-Length',0)))
        p=self.path.split('?')[0]
        try:
            if p=='/api/companies/save':
                d=json.loads(body); self._json({'success':True,'id':save_company(d)})
            elif p=='/api/companies/delete':
                cid=json.loads(body)['id']; f=CO_DIR/f'{cid}.json'
                if f.exists(): f.unlink()
                self._json({'success':True})
            elif p=='/api/generate':
                data=json.loads(body)
                org=re.sub(r'[^\w\-]','_',data.get('orgName','org'))
                ts=datetime.now().strftime('%Y%m%d_%H%M%S')
                out=OUT_DIR/f'{org}_{ts}'
                done=generate_all(data,out)
                zp=str(out)+'.zip'
                with zipfile.ZipFile(zp,'w',zipfile.ZIP_DEFLATED) as zf:
                    for item in done:
                        if os.path.exists(item['path']): zf.write(item['path'],item['rel'])
                zb=base64.b64encode(open(zp,'rb').read()).decode()
                eid=save_journal({'orgName':data.get('orgName',''),'implDate':data.get('implDate',''),
                                  'fileCount':len(done),'zipPath':zp})
                self._json({'success':True,'fileCount':len(done),'journalId':eid,'zip':zb})
            elif p=='/api/journal/delete':
                eid=json.loads(body)['id']
                for f in JOURNAL_DIR.glob('*.json'):
                    try:
                        e=json.loads(f.read_text('utf-8'))
                        if e.get('id')==eid:
                            zp=e.get('zipPath')
                            if zp and os.path.exists(zp): os.remove(zp)
                            f.unlink(); break
                    except: pass
                self._json({'success':True})
            else: self.send_response(404); self.end_headers()
        except Exception as e:
            import traceback
            self._json({'success':False,'error':str(e),'trace':traceback.format_exc()},500)

    def _json(self,d,code=200):
        b=json.dumps(d,ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type','application/json; charset=utf-8')
        self.send_header('Content-Length',str(len(b)))
        self.end_headers(); self.wfile.write(b)

    def _html(self,h):
        b=h.encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type','text/html; charset=utf-8')
        self.send_header('Content-Length',str(len(b)))
        self.end_headers(); self.wfile.write(b)

    def do_OPTIONS(self): self.send_response(200); self.end_headers()

if __name__=='__main__':
    print(f'\n✅  ИСО/СМК Генератор: http://localhost:{PORT}')
    print(f'   Откройте в браузере | Ctrl+C для остановки\n')
    class ThreadedServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
        allow_reuse_address = True
    with ThreadedServer(('', PORT), H) as s:
        try: s.serve_forever()
        except KeyboardInterrupt: print('\n⏹  Остановлен')
