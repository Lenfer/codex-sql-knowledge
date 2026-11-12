"""Build self-contained README figure sources. Python standard library only."""
from pathlib import Path
from html import escape

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'docs/figures'
PAPER, INK, MUTED, RULE, ACCENT, TINT = '#f7f4ed', '#30332e', '#656b60', '#d6d8cf', '#a75035', '#f0e1d6'

def text(x, y, value, kind='body'):
    return f'<text x="{x}" y="{y}" class="{kind}">{escape(value)}</text>'

def rect(x, y, w, h, focal=False):
    return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="6" fill="{TINT if focal else "#fffefa"}" stroke="{ACCENT if focal else RULE}" stroke-width="1.5"/>'

def line(path):
    return f'<path d="{path}" fill="none" stroke="{MUTED}" stroke-width="1.5" marker-end="url(#arrow)"/>'

def svg(slug, title, desc, body):
    return f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 960 600" role="img" aria-labelledby="{slug}-title {slug}-desc">
<title id="{slug}-title">{escape(title)}</title><desc id="{slug}-desc">{escape(desc)}</desc>
<defs><marker id="arrow" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto"><path d="M0 0 L8 3 L0 6 Z" fill="{MUTED}"/></marker></defs>
<rect width="960" height="600" fill="{PAPER}"/>{body}</svg>'''

STYLE = f'''body{{margin:0;background:{PAPER};color:{INK}}}svg{{display:block;width:960px;height:600px}}
text{{font-family:Arial,Helvetica,sans-serif;fill:{INK}}}.title{{font:38px Georgia,serif;letter-spacing:-.6px}}.heading{{font-size:22px;font-weight:600}}.body{{font-size:18px}}.small{{font-size:16px;fill:{MUTED}}}.code{{font:15px Menlo,Consolas,monospace;fill:{MUTED}}}.eyebrow{{font:12px Menlo,monospace;letter-spacing:2px;fill:{MUTED}}}'''

def save(slug, title, graphic):
    (OUT / f'{slug}.html').write_text(f'<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>{title}</title><style>{STYLE}</style><main>{graphic}</main></html>')

def architecture():
    b = text(40,48,'SQL KNOWLEDGE / PROJECT MEMORY','eyebrow')
    b += text(40,104,'One project. A memory you can inspect.','title')
    b += text(40,144,'Shared runtime; explicit project root; records with sources.','body')
    # Routing precedes nodes. Separate vertical ports keep the client paths apart.
    b += line('M236 252 H276 Q284 252 284 260 V296 Q284 304 292 304 H320')
    b += line('M236 340 H320')
    b += line('M236 428 H284 Q292 428 292 420 V384 Q292 376 300 376 H320')
    b += line('M560 340 H640')
    b += text(578,316,'commit','code')
    for y,title,sub in [(216,'Agent chat','stdio MCP'),(304,'Another chat','stdio MCP'),(392,'CLI','explicit --project')]:
        b += rect(40,y,196,72)+text(56,y+28,title,'heading')+text(56,y+52,sub,'code')
    b += rect(320,272,240,136,True)+text(340,308,'Project writer','heading')+text(340,340,'Revision checks','body')+text(340,368,'Idempotent retries','body')+text(340,392,'Local Unix socket','code')
    b += rect(640,248,280,184)+text(660,284,'knowledge.sqlite','heading')+text(660,320,'Records + evidence','body')+text(660,350,'History + receipts','body')+text(660,396,'SQLite WAL / FULL','code')
    b += f'<path d="M40 500 H920" stroke="{RULE}"/>'
    b += text(40,536,'Reads use independent connections to the project database.','body')
    b += text(40,568,'Each MCP server binds to one explicitly chosen project root.','small')
    save('architecture','SQL Knowledge architecture',svg('architecture','SQL Knowledge architecture','Three local clients send writes through one coordinator to a project database; reads use independent connections.',b))

def cycle(active=None):
    b=text(40,48,'SQL KNOWLEDGE / A WRITE TO DISK','eyebrow')
    b+=text(40,104,'Keep the record and the reason.','title')
    b+=text(40,144,'A success receipt follows the transaction commit.','body')
    xs=[40,276,512,748]
    for x in xs[:-1]: b+=line(f'M{x+176} 288 H{x+232}')
    labels=[('Request','Evidence + source',['actor','reason','unique key']),('Check','Current revision',['schema','identity','revision']),('Commit','One transaction',['record','history','receipt']),('Reply','Saved receipt',['success','after commit','same-key replay'])]
    for i,(x,(name,sub,details)) in enumerate(zip(xs,labels)):
        b+=text(x,216,f'0{i+1}','code')+rect(x,240,176,96,i==2)
        b+=text(x+16,280,name,'heading')+text(x+16,308,details[0],'code')
        b+=text(x,376,sub,'small')
        for j,s in enumerate(details[1:]): b+=text(x,408+j*26,s,'code')
        if active==i:
            b+=f'<rect x="{x-4}" y="236" width="184" height="104" rx="8" fill="none" stroke="{ACCENT}" stroke-width="3"/>'
            b+=f'<circle cx="{x+8}" cy="472" r="4" fill="{ACCENT}"/>'+text(x+20,478,'Current step','small')
    b+=f'<path d="M40 512 H920" stroke="{RULE}"/>'
    b+=text(40,552,'Lost response? Repeat the original request with its original key.','body')
    return svg('write-cycle','From request to durable receipt','The writer validates a sourced request, commits the record and history, and returns an idempotent receipt.',b)

if __name__=='__main__':
    OUT.mkdir(parents=True,exist_ok=True)
    architecture();save('write-cycle','From request to receipt',cycle())
    for i in range(4): save(f'write-step-{i+1}',f'Write step {i+1}',cycle(i))
