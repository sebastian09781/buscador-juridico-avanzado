"""
JurisBot Relatoría — Fusión
============================
LÓGICA: SQLite FTS5 · ThreadPoolExecutor · operadores avanzados · favoritos ·
        etiquetas · historial · vista texto+render · paginación · árbol carpetas
INTERFAZ: 3 columnas (resultados / preview / metadatos) · topbar compacta ·
          placeholder · Listbox · panel de metadatos lateral
"""

# ══════════════════════════════════════════════════════════════════════════════
#  BOOTSTRAP — instala dependencias la primera vez
# ══════════════════════════════════════════════════════════════════════════════
import sys, os, subprocess, importlib, threading
import tkinter as tk
from tkinter import ttk, messagebox

DEPS = [("fitz", "PyMuPDF"), ("docx", "python-docx"), ("PIL", "Pillow")]

def _ok(m):
    try: importlib.import_module(m); return True
    except ImportError: return False

def bootstrap():
    faltan = [(m, p) for m, p in DEPS if not _ok(m)]
    if not faltan: return True
    root = tk.Tk(); root.title("JurisBot — Primera configuración")
    root.geometry("520x240"); root.configure(bg="#1c2333"); root.resizable(False, False)
    root.lift(); root.focus_force()
    tk.Label(root, text="JURISBOT RELATORÍA", fg="#e6b450", bg="#1c2333",
             font=("Georgia", 13, "bold")).pack(pady=(22, 3))
    tk.Label(root, text="Instalando componentes necesarios — solo ocurre una vez",
             fg="#8b9ab0", bg="#1c2333", font=("Segoe UI", 9)).pack()
    tk.Label(root, text=f"Paquetes: {', '.join(p for _, p in faltan)}",
             fg="#e6b450", bg="#1c2333", font=("Courier New", 9)).pack(pady=3)
    lbl    = tk.Label(root, text="", fg="#cdd9e5", bg="#1c2333", font=("Courier New", 9)); lbl.pack(pady=2)
    pb     = ttk.Progressbar(root, mode="indeterminate", length=440); pb.pack(pady=6); pb.start(10)
    lbl_ok = tk.Label(root, text="", fg="#3fb950", bg="#1c2333", font=("Courier New", 10, "bold")); lbl_ok.pack()
    errores = []
    def _hilo():
        for m, p in faltan:
            lbl.config(text=f"Instalando {p}...")
            r = subprocess.run([sys.executable, "-m", "pip", "install", p, "--quiet"],
                               capture_output=True, text=True)
            if r.returncode != 0: errores.append(f"{p}: {r.stderr[-150:]}")
        pb.stop()
        if errores:
            messagebox.showerror("Error de instalación",
                "\n".join(errores) + "\n\nVerifique su conexión a internet.", parent=root)
            root.destroy()
        else:
            lbl_ok.config(text="✓ Listo. Iniciando JurisBot..."); root.after(1300, root.destroy)
    threading.Thread(target=_hilo, daemon=True).start()
    root.mainloop()
    return len(errores) == 0

if not bootstrap(): sys.exit(1)

# ══════════════════════════════════════════════════════════════════════════════
#  IMPORTS
# ══════════════════════════════════════════════════════════════════════════════
import sqlite3, re, time, queue, hashlib, io
from datetime import datetime
from pathlib import Path
from tkinter import filedialog
from concurrent.futures import ThreadPoolExecutor, as_completed

try:    import fitz;                        PDF_OK  = True
except: PDF_OK  = False

try:    from docx import Document as DocxDoc; DOCX_OK = True
except: DOCX_OK = False

try:    from PIL import Image, ImageTk;     PIL_OK  = True
except: PIL_OK  = False

# ══════════════════════════════════════════════════════════════════════════════
#  PALETA  (interfaz del 10-copia, extendida con los extras del 10)
# ══════════════════════════════════════════════════════════════════════════════
C = {
    "bg":        "#121826",
    "panel":     "#1B2333",
    "border":    "#2A3447",
    "text":      "#E6EAF2",
    "text2":     "#AAB2C3",
    "accent":    "#F4B400",
    "accent2":   "#3A86FF",
    "danger":    "#E05252",
    "hover":     "#253044",
    "sel":       "#1E3A5F",
    "font":      ("Segoe UI", 10),
    "font_sm":   ("Segoe UI", 9),
    "font_lg":   ("Segoe UI", 12),
    "font_mono": ("Consolas", 10),
    # extras del 10
    "green":     "#3fb950",
    "cyan":      "#39c5cf",
    "hl_bg":     "#7d5a00",
    "hl_fg":     "#ffffff",
    "hl_act":    "#ff6b35",
    "warn":      "#e6b450",
}

# ══════════════════════════════════════════════════════════════════════════════
#  CONSTANTES GLOBALES
# ══════════════════════════════════════════════════════════════════════════════
DB_FILENAME  = "Jurisbot_Relatoria.db"
EXTS         = {".pdf", ".docx"}
NUM_WORKERS  = max(2, (os.cpu_count() or 2))
PAGE_SIZE    = 50
MAX_RESULTS  = 500
PREVIEW_MAX  = 50_000

CFG_DIR  = os.path.join(os.path.expanduser("~"), ".jurisbot")
CFG_FILE = os.path.join(CFG_DIR, "config.json")

OPERADORES = [
    ("+",   "Y también",    "contrato + laboral",     "Busca documentos que contengan TODAS las palabras"),
    ("-",   "Excluir",      "contrato - civil",       "Excluye documentos que contengan esa palabra"),
    ('"…"', "Frase exacta", '"acción de tutela"',     "Busca la frase exactamente como está escrita"),
    (",",   "O cualquiera", "tutela, acción popular", "Busca documentos que tengan UNA u OTRA palabra"),
    ("*",   "Comodín",      "contrat*",               "Busca palabras que empiecen así: contrato, contratar..."),
]

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG GLOBAL
# ══════════════════════════════════════════════════════════════════════════════
def cfg_global_get(k, default=None):
    try:
        os.makedirs(CFG_DIR, exist_ok=True)
        if os.path.exists(CFG_FILE):
            import json
            with open(CFG_FILE, "r", encoding="utf-8") as f:
                return json.load(f).get(k, default)
    except: pass
    return default

def cfg_global_set(k, v):
    try:
        import json
        os.makedirs(CFG_DIR, exist_ok=True)
        data = {}
        if os.path.exists(CFG_FILE):
            with open(CFG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        data[k] = v
        with open(CFG_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except: pass

# ══════════════════════════════════════════════════════════════════════════════
#  BASE DE DATOS  (vive dentro de la carpeta analizada)
# ══════════════════════════════════════════════════════════════════════════════
def db_path_para(carpeta):
    return os.path.join(carpeta, DB_FILENAME)

def get_db(carpeta):
    path = db_path_para(carpeta)
    con  = sqlite3.connect(path, check_same_thread=False, timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA cache_size=-32000")
    con.execute("PRAGMA temp_store=MEMORY")
    con.execute("PRAGMA busy_timeout=8000")   # espera hasta 8s si hay lock
    con.execute("PRAGMA mmap_size=268435456") # 256MB mmap — lecturas más rápidas
    return con

def init_db(carpeta):
    con = get_db(carpeta)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS documentos (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre             TEXT NOT NULL,
            ruta               TEXT NOT NULL UNIQUE,
            fecha_modificacion TEXT,
            tipo               TEXT DEFAULT '',
            tamanio            INTEGER DEFAULT 0,
            anio               INTEGER DEFAULT 0,
            hash               TEXT
        );
        CREATE VIRTUAL TABLE IF NOT EXISTS fts_contenido USING fts5(
            contenido,
            nombre,
            documento_id UNINDEXED,
            tokenize = 'unicode61 remove_diacritics 1'
        );
        CREATE TABLE IF NOT EXISTS etiquetas (
            id    INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL UNIQUE,
            color  TEXT DEFAULT '#e6b450'
        );
        CREATE TABLE IF NOT EXISTS doc_etiquetas (
            documento_id INTEGER NOT NULL,
            etiqueta_id  INTEGER NOT NULL,
            PRIMARY KEY (documento_id, etiqueta_id)
        );
        CREATE TABLE IF NOT EXISTS favoritos (
            documento_id INTEGER PRIMARY KEY,
            fecha        TEXT
        );
        CREATE TABLE IF NOT EXISTS historial (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            query   TEXT NOT NULL,
            fecha   TEXT NOT NULL,
            results INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS configuracion (
            clave TEXT PRIMARY KEY,
            valor TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_doc_ruta ON documentos(ruta);
        CREATE INDEX IF NOT EXISTS idx_doc_tipo ON documentos(tipo);
        CREATE INDEX IF NOT EXISTS idx_doc_anio ON documentos(anio);
    """)
    con.commit()
    return con

def hay_indice(carpeta):
    p = db_path_para(carpeta)
    if not os.path.exists(p): return False, None
    try:
        con = sqlite3.connect(p)
        row = con.execute("SELECT valor FROM configuracion WHERE clave='ultima_indexacion'").fetchone()
        n   = con.execute("SELECT COUNT(*) FROM documentos").fetchone()[0]
        con.close()
        return n > 0, (row[0] if row else None)
    except:
        return False, None

def verificar_nuevos(carpeta):
    p = db_path_para(carpeta)
    if not os.path.exists(p): return 0, 0
    try:
        con = sqlite3.connect(p)
        existentes = {r[0]: r[1] for r in con.execute("SELECT ruta, hash FROM documentos")}
        con.close()
    except: return 0, 0
    nuevos = modificados = 0
    for raiz, dirs, files in os.walk(carpeta):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for f in files:
            if Path(f).suffix.lower() not in EXTS: continue
            ruta = os.path.join(raiz, f)
            h    = _hash_rapido(ruta)
            if ruta not in existentes: nuevos += 1
            elif existentes[ruta] != h: modificados += 1
    return nuevos, modificados

def _hash_rapido(ruta):
    try:
        s = os.stat(ruta)
        return f"{s.st_size}_{s.st_mtime:.0f}"
    except: return ""

def reconstruir_indice(carpeta):
    p = db_path_para(carpeta)
    if os.path.exists(p): os.remove(p)

# ══════════════════════════════════════════════════════════════════════════════
#  EXTRACCIÓN DE TEXTO
# ══════════════════════════════════════════════════════════════════════════════
def _limpiar_texto(texto):
    """Normaliza caracteres raros comunes en PDFs judiciales colombianos."""
    if not texto: return ""
    # Remplazos de codificación frecuentes en PDFs escaneados
    fixes = {
        "\ufffd": "?",        # replacement char genérico
        "\u2019": "'",        # comilla tipográfica derecha
        "\u2018": "'",        # comilla tipográfica izquierda
        "\u201c": '"',        # comilla doble izquierda
        "\u201d": '"',        # comilla doble derecha
        "\u2013": "-",        # guión medio
        "\u2014": "-",        # guión largo
        "\u00ad": "",         # guión suave (invisible)
        "\u00a0": " ",        # espacio no separable
        "\u000c": "\n",      # form feed → salto de línea
    }
    for bad, good in fixes.items():
        texto = texto.replace(bad, good)
    # Eliminar caracteres de control excepto newline/tab
    texto = "".join(c if c >= " " or c in "\n\t" else " " for c in texto)
    # Colapsar espacios múltiples en línea (pero no newlines)
    lines = [" ".join(l.split()) for l in texto.split("\n")]
    return "\n".join(lines)

def extraer_pdf(ruta):
    try:
        doc   = fitz.open(ruta)
        paginas = []
        for p in doc:
            t = p.get_text("text")
            if not t.strip():
                # Página posiblemente escaneada — intentar con dict para mejor extracción
                t = p.get_text("dict")
                if isinstance(t, dict):
                    bloques = []
                    for b in t.get("blocks", []):
                        for l in b.get("lines", []):
                            for s in l.get("spans", []):
                                bloques.append(s.get("text", ""))
                    t = " ".join(bloques)
            paginas.append(t)
        doc.close()
        return _limpiar_texto("\n".join(paginas))
    except: return ""

def extraer_docx(ruta):
    try:
        doc   = DocxDoc(ruta)
        partes = [p.text for p in doc.paragraphs if p.text.strip()]
        for t in doc.tables:
            for row in t.rows:
                for c in row.cells:
                    if c.text.strip(): partes.append(c.text)
        return _limpiar_texto("\n".join(partes))
    except: return ""

def render_docx_en_widget(ruta, txt_widget, terminos=None):
    """
    Renderiza un DOCX directamente en un tk.Text con formato básico.
    No requiere LibreOffice. Usa python-docx.
    Estilos aplicados: heading (dorado+negrita), bold, tabla, normal.
    """
    if not DOCX_OK:
        txt_widget.config(state="normal")
        txt_widget.delete("1.0", "end")
        txt_widget.insert("end", "⚠  python-docx no disponible.\nReinicie la aplicación.", "cargando")
        txt_widget.config(state="disabled")
        return

    try:
        doc = DocxDoc(ruta)
    except Exception as e:
        txt_widget.config(state="normal")
        txt_widget.delete("1.0", "end")
        txt_widget.insert("end", f"⚠  No se pudo abrir el archivo:\n{e}", "cargando")
        txt_widget.config(state="disabled")
        return

    txt_widget.config(state="normal")
    txt_widget.delete("1.0", "end")

    limpios = [t for t in (terminos or []) if t and len(t) > 1]
    patron  = re.compile("(" + "|".join(re.escape(t) for t in limpios) + ")", re.IGNORECASE) if limpios else None

    def _insertar(texto, tag_base):
        if not texto: return
        if patron:
            for i, parte in enumerate(patron.split(texto)):
                if parte:
                    txt_widget.insert("end", parte, "resaltado" if i % 2 == 1 else tag_base)
        else:
            txt_widget.insert("end", texto, tag_base)

    chars = 0
    for elem in doc.element.body:
        from docx.oxml.ns import qn
        tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

        if tag == "p":
            from docx.text.paragraph import Paragraph
            try:
                p = Paragraph(elem, doc)
                texto = p.text
                if not texto.strip():
                    txt_widget.insert("end", "\n")
                    continue
                estilo = (p.style.name or "").lower()
                if "heading" in estilo or "título" in estilo or "titulo" in estilo:
                    _insertar(texto + "\n", "heading")
                elif any(run.bold for run in p.runs if run.text.strip()):
                    _insertar(texto + "\n", "bold_txt")
                else:
                    _insertar(texto + "\n", "normal")
                chars += len(texto)
            except: pass

        elif tag == "tbl":
            from docx.table import Table
            try:
                t = Table(elem, doc)
                for row in t.rows:
                    celdas = [c.text.replace("\n", " ").strip() for c in row.cells]
                    linea  = "  │  ".join(celdas)
                    _insertar(linea + "\n", "tabla")
                txt_widget.insert("end", "\n")
                chars += 100
            except: pass

        if chars > 60_000:
            txt_widget.insert("end", "\n[… documento truncado para preview …]", "cargando")
            break

    txt_widget.config(state="disabled")

def extraer(ruta):
    ext = Path(ruta).suffix.lower()
    if ext == ".pdf":  return extraer_pdf(ruta)
    if ext == ".docx": return extraer_docx(ruta)
    return ""

def texto_desde_db(carpeta, doc_id):
    try:
        con = get_db(carpeta)
        row = con.execute(
            "SELECT contenido FROM fts_contenido WHERE documento_id=?", (doc_id,)
        ).fetchone()
        con.close()
        return row["contenido"] if row else None
    except:
        return None

def hash_completo(ruta):
    try:
        s = os.stat(ruta)
        h = hashlib.md5()
        h.update(str(s.st_size).encode())
        with open(ruta, "rb") as f: h.update(f.read(8192))
        return h.hexdigest()
    except: return ""

def tamanio_fmt(b):
    if b < 1024:     return f"{b} B"
    if b < 1048576:  return f"{b/1024:.0f} KB"
    return f"{b/1048576:.1f} MB"

# ══════════════════════════════════════════════════════════════════════════════
#  WORKER DE INDEXACIÓN
# ══════════════════════════════════════════════════════════════════════════════
def _worker_indexar(ruta):
    try:
        nombre = os.path.basename(ruta)
        texto  = extraer(ruta)
        stat   = os.stat(ruta)
        fecha  = datetime.fromtimestamp(stat.st_mtime).strftime("%d/%m/%Y %H:%M")
        anio   = datetime.fromtimestamp(stat.st_mtime).year
        tam    = stat.st_size
        tipo   = Path(ruta).suffix.lower().lstrip(".")
        h      = hash_completo(ruta)
        return {"ok": True, "ruta": ruta, "nombre": nombre, "texto": texto,
                "fecha": fecha, "anio": anio, "tam": tam, "tipo": tipo, "hash": h}
    except Exception as e:
        return {"ok": False, "ruta": ruta, "error": str(e)}

# ══════════════════════════════════════════════════════════════════════════════
#  INDEXACIÓN PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════
def indexar(carpeta, cola, stop):
    con = init_db(carpeta)
    lista = []
    for raiz, dirs, files in os.walk(carpeta):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for f in files:
            ruta_f = os.path.join(raiz, f)
            if Path(f).suffix.lower() in EXTS and ruta_f != db_path_para(carpeta):
                lista.append(ruta_f)

    total = len(lista)
    cola.put(("total", total))

    if total == 0:
        cola.put(("fin", 0, 0, 0)); con.close(); return

    existentes = {}
    for r in con.execute("SELECT id, ruta, hash FROM documentos"):
        existentes[r["ruta"]] = {"id": r["id"], "hash": r["hash"]}

    pendientes = []
    omitidos   = 0
    for ruta in lista:
        info = existentes.get(ruta)
        if info and info["hash"] and info["hash"] == hash_completo(ruta):
            omitidos += 1
        else:
            pendientes.append(ruta)

    cola.put(("resumen", len(pendientes), omitidos, total))

    if not pendientes:
        _cfg_set_local(con, carpeta)
        con.close()
        cola.put(("fin", 0, 0, omitidos))
        return

    nuevos = actualizados = 0
    procesados = 0
    BATCH = 40
    batch_new = []
    batch_upd = []

    def flush():
        nonlocal nuevos, actualizados
        if not batch_new and not batch_upd: return
        try:
            con.execute("BEGIN IMMEDIATE")
            for d in batch_new:
                con.execute(
                    "INSERT OR IGNORE INTO documentos(nombre,ruta,fecha_modificacion,tipo,tamanio,anio,hash) "
                    "VALUES(:nombre,:ruta,:fecha,:tipo,:tam,:anio,:hash)", d)
                row = con.execute("SELECT id FROM documentos WHERE ruta=?", (d["ruta"],)).fetchone()
                if row:
                    con.execute("INSERT INTO fts_contenido(contenido,nombre,documento_id) VALUES(?,?,?)",
                                (d["texto"], d["nombre"], row["id"]))
            nuevos += len(batch_new); batch_new.clear()

            for d in batch_upd:
                doc_id = existentes[d["ruta"]]["id"]
                con.execute(
                    "UPDATE documentos SET nombre=?,fecha_modificacion=?,tipo=?,tamanio=?,anio=?,hash=? WHERE id=?",
                    (d["nombre"], d["fecha"], d["tipo"], d["tam"], d["anio"], d["hash"], doc_id))
                con.execute("DELETE FROM fts_contenido WHERE documento_id=?", (doc_id,))
                con.execute("INSERT INTO fts_contenido(contenido,nombre,documento_id) VALUES(?,?,?)",
                            (d["texto"], d["nombre"], doc_id))
            actualizados += len(batch_upd); batch_upd.clear()
            con.execute("COMMIT")
        except Exception as e:
            try: con.execute("ROLLBACK")
            except: pass
            cola.put(("warn", f"Error guardando lote: {e}"))

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as ex:
        futuros = {ex.submit(_worker_indexar, r): r for r in pendientes}
        for fut in as_completed(futuros):
            if stop.is_set():
                ex.shutdown(wait=False, cancel_futures=True); break
            procesados += 1
            try:
                res = fut.result(timeout=120)
            except Exception as e:
                cola.put(("warn", str(e))); continue

            if not res["ok"]:
                cola.put(("warn", res.get("error", ""))); continue

            if res["ruta"] in existentes: batch_upd.append(res)
            else:                         batch_new.append(res)

            if (len(batch_new) + len(batch_upd)) >= BATCH: flush()
            cola.put(("prog", procesados, len(pendientes), omitidos, total, res["nombre"]))

    flush()
    cola.put(("optimizando",))
    try:
        con.execute("INSERT INTO fts_contenido(fts_contenido) VALUES('optimize')")
        con.commit()
    except: pass

    _cfg_set_local(con, carpeta)
    con.close()
    cola.put(("fin", nuevos, actualizados, omitidos))

def _cfg_set_local(con, carpeta):
    ts = datetime.now().strftime("%d/%m/%Y %H:%M")
    con.execute("INSERT OR REPLACE INTO configuracion(clave,valor) VALUES(?,?)",
                ("ultima_indexacion", ts))
    con.execute("INSERT OR REPLACE INTO configuracion(clave,valor) VALUES(?,?)",
                ("carpeta", carpeta))
    con.commit()
    cfg_global_set("ultima_carpeta", carpeta)

# ══════════════════════════════════════════════════════════════════════════════
#  CONVERSIÓN OPERADORES ESPAÑOL → FTS5
# ══════════════════════════════════════════════════════════════════════════════
def convertir_query(q_raw):
    q = q_raw.strip()
    if not q: return q
    if re.match(r'^"[^"]*"$', q): return q
    if " + " in q:
        partes = [p.strip() for p in q.split("+") if p.strip()]
        return " AND ".join(f'"{p}"' if " " in p else p for p in partes)
    if " - " in q:
        partes = re.split(r'\s+-\s+', q, maxsplit=1)
        inc = partes[0].strip(); exc = partes[1].strip() if len(partes) > 1 else ""
        fi  = f'"{inc}"' if " " in inc else inc
        fe  = f'"{exc}"' if " " in exc else exc
        return f"{fi} NOT {fe}" if exc else fi
    if "," in q:
        partes = [p.strip() for p in q.split(",") if p.strip()]
        return " OR ".join(f'"{p}"' if " " in p else p for p in partes)
    if "*" in q and " " not in q: return q
    if "*" in q: return " AND ".join(q.split())
    if " " in q: return " AND ".join(q.split())
    return q

def terminos_resaltar(query):
    q = query.replace('"','').replace('+', ' ').replace('-', ' ').replace(',', ' ')
    return [t.strip().lower().rstrip('*') for t in q.split() if t.strip() and len(t.strip()) > 1]

# ══════════════════════════════════════════════════════════════════════════════
#  BÚSQUEDA
# ══════════════════════════════════════════════════════════════════════════════
def buscar(carpeta, query, tipos=None, anios=None, carpetas_filtro=None,
           solo_favoritos=False, etiqueta_id=None, offset=0, limite=PAGE_SIZE):
    p = db_path_para(carpeta)
    if not os.path.exists(p): return [], 0
    q = query.strip()
    if not q: return [], 0

    con    = get_db(carpeta)
    fts_q  = convertir_query(q)
    where  = []
    params = []

    if tipos:
        where.append("d.tipo IN (%s)" % ",".join("?" * len(tipos)))
        params.extend(tipos)
    if anios:
        where.append("d.anio IN (%s)" % ",".join("?" * len(anios)))
        params.extend(anios)
    if carpetas_filtro:
        sub = " OR ".join("d.ruta LIKE ?" for _ in carpetas_filtro)
        where.append(f"({sub})")
        for c in carpetas_filtro: params.append(c.rstrip("\\/") + "%")
    if solo_favoritos:
        where.append("EXISTS(SELECT 1 FROM favoritos fav WHERE fav.documento_id=d.id)")
    if etiqueta_id:
        where.append("EXISTS(SELECT 1 FROM doc_etiquetas de WHERE de.documento_id=d.id AND de.etiqueta_id=?)")
        params.append(etiqueta_id)

    w = ("AND " + " AND ".join(where)) if where else ""

    sql_count = f"SELECT COUNT(*) as cnt FROM fts_contenido f JOIN documentos d ON d.id=f.documento_id WHERE fts_contenido MATCH ? {w}"
    sql = f"""
        SELECT d.id, d.nombre, d.ruta, d.fecha_modificacion,
               d.tipo, d.tamanio, d.anio, rank AS score,
               EXISTS(SELECT 1 FROM favoritos fav WHERE fav.documento_id=d.id) as es_favorito,
               snippet(fts_contenido, 0, '<HIT>', '</HIT>', '…', 20) as snip
        FROM fts_contenido f
        JOIN documentos d ON d.id=f.documento_id
        WHERE fts_contenido MATCH ? {w}
        ORDER BY rank
        LIMIT {limite} OFFSET {offset}
    """
    try:
        total = con.execute(sql_count, [fts_q] + params).fetchone()["cnt"]
        rows  = con.execute(sql, [fts_q] + params).fetchall()
    except sqlite3.OperationalError:
        con.close()
        return _buscar_fallback(carpeta, query, tipos, offset, limite)

    resultados = []
    for r in rows:
        etqs = con.execute(
            "SELECT e.nombre,e.color FROM etiquetas e "
            "JOIN doc_etiquetas de ON de.etiqueta_id=e.id WHERE de.documento_id=?",
            (r["id"],)).fetchall()
        resultados.append({
            "id":       r["id"],      "nombre":   r["nombre"],
            "ruta":     r["ruta"],    "fecha":    r["fecha_modificacion"] or "",
            "tipo":     r["tipo"].upper(), "tam": r["tamanio"], "anio": r["anio"],
            "score":    abs(r["score"]) if r["score"] else 1,
            "favorito": bool(r["es_favorito"]),
            "snippet":  (r["snip"] or "").replace("<HIT>", "«").replace("</HIT>", "»"),
            "etiquetas":[(e["nombre"], e["color"]) for e in etqs],
        })

    try:
        con.execute("INSERT INTO historial(query,fecha,results) VALUES(?,?,?)",
                    (query, datetime.now().strftime("%d/%m/%Y %H:%M"), total))
        con.execute("DELETE FROM historial WHERE id NOT IN "
                    "(SELECT id FROM historial ORDER BY id DESC LIMIT 50)")
        con.commit()
    except: pass
    con.close()
    return resultados, total

def _buscar_fallback(carpeta, query, tipos, offset, limite):
    p = db_path_para(carpeta)
    if not os.path.exists(p): return [], 0
    con      = get_db(carpeta)
    terminos = [t.strip().lower() for t in re.split(r'\s+', query) if t.strip()]
    if not terminos: con.close(); return [], 0
    where  = ["(LOWER(f.contenido) LIKE ? OR LOWER(f.nombre) LIKE ?)" for _ in terminos]
    params = []
    for t in terminos: params.extend([f"%{t}%", f"%{t}%"])
    sql = f"""
        SELECT d.id,d.nombre,d.ruta,d.fecha_modificacion,d.tipo,d.tamanio,d.anio,
               1 AS score, 0 as es_favorito, '' as snip
        FROM fts_contenido f JOIN documentos d ON d.id=f.documento_id
        WHERE {' AND '.join(where)}
        LIMIT {limite} OFFSET {offset}
    """
    try:
        rows  = con.execute(sql, params).fetchall()
        total = len(rows)
    except: con.close(); return [], 0
    res = [{"id":r["id"],"nombre":r["nombre"],"ruta":r["ruta"],"fecha":r["fecha_modificacion"] or "",
            "tipo":r["tipo"].upper(),"tam":r["tamanio"],"anio":r["anio"],"score":1,
            "favorito":False,"snippet":"","etiquetas":[]} for r in rows]
    con.close()
    return res, total

# ══════════════════════════════════════════════════════════════════════════════
#  RENDERIZADO DOCX → PDF (via LibreOffice)
# ══════════════════════════════════════════════════════════════════════════════
_libreoffice_path = None

def _encontrar_libreoffice():
    global _libreoffice_path
    if _libreoffice_path: return _libreoffice_path
    candidatos = [
        r"C:\Program Files\LibreOffice\program\soffice.exe",
        r"C:\Program Files (x86)\LibreOffice\program\soffice.exe",
        "/usr/bin/libreoffice", "/usr/bin/soffice",
        "/Applications/LibreOffice.app/Contents/MacOS/soffice",
    ]
    for c in candidatos:
        if os.path.exists(c):
            _libreoffice_path = c; return c
    return None

_docx_pdf_cache = {}

def docx_a_pdf_temporal(ruta_docx):
    if ruta_docx in _docx_pdf_cache:
        pdf = _docx_pdf_cache[ruta_docx]
        if os.path.exists(pdf): return pdf
    lo = _encontrar_libreoffice()
    if not lo: return None
    import tempfile
    tmpdir = tempfile.mkdtemp(prefix="jurisbot_")
    try:
        r = subprocess.run(
            [lo, "--headless", "--convert-to", "pdf", "--outdir", tmpdir, ruta_docx],
            capture_output=True, timeout=60
        )
        nombre_pdf = Path(ruta_docx).stem + ".pdf"
        ruta_pdf   = os.path.join(tmpdir, nombre_pdf)
        if os.path.exists(ruta_pdf):
            _docx_pdf_cache[ruta_docx] = ruta_pdf
            return ruta_pdf
    except: pass
    return None

# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS UI
# ══════════════════════════════════════════════════════════════════════════════
def _mk_btn(parent, text, cmd, bg=None, fg=None, font=None, tooltip=None, **kw):
    bg = bg or C["accent"]; fg = fg or "#1a1a1a"
    b = tk.Button(parent, text=text, command=cmd,
                  bg=bg, fg=fg,
                  activebackground=C["hover"], activeforeground=C["text"],
                  relief="flat", font=font or C["font"],
                  padx=10, pady=4, cursor="hand2", bd=0, **kw)
    b.bind("<Enter>", lambda e, _bg=bg: b.configure(bg=C["hover"]))
    b.bind("<Leave>", lambda e, _bg=bg: b.configure(bg=_bg))
    if tooltip: _Tooltip(b, tooltip)
    return b

class _Tooltip:
    def __init__(self, widget, text):
        self.widget = widget; self.text = text; self.tip = None
        widget.bind("<Enter>", self._show); widget.bind("<Leave>", self._hide)
        widget.bind("<Button>", self._hide)

    def _show(self, _=None):
        if self.tip: return
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 4
        self.tip = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True); tw.wm_geometry(f"+{x}+{y}"); tw.configure(bg=C["panel"])
        tk.Frame(tw, bg=C["accent"], height=1).pack(fill="x")
        tk.Label(tw, text=self.text, justify="left", bg=C["panel"], fg=C["text"],
                 font=C["font_sm"], padx=10, pady=6, wraplength=340).pack()
        tk.Frame(tw, bg=C["accent"], height=1).pack(fill="x")

    def _hide(self, _=None):
        if self.tip: self.tip.destroy(); self.tip = None

def _sep(p, color=None, pady=0):
    color = color or C["border"]
    f = tk.Frame(p, bg=color, height=1); f.pack(fill="x", pady=pady); return f

# ══════════════════════════════════════════════════════════════════════════════
#  MODAL DE INDEXACIÓN  (estilo del 10-copia, lógica completa del 10)
# ══════════════════════════════════════════════════════════════════════════════
class IndexProgressDialog(tk.Toplevel):

    def __init__(self, master, carpeta, on_done=None):
        super().__init__(master)
        self.carpeta  = carpeta
        self.on_done  = on_done
        self._cola    = queue.Queue()
        self._stop    = threading.Event()
        self._t_ini   = None
        self._pendientes = 0
        self._proc_prev  = 0
        self._t_vel   = time.time()

        self.title("Indexando…")
        self.geometry("500x300")
        self.resizable(False, False)
        self.configure(bg=C["bg"])
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self._cancel_op)

        tk.Label(self, text="📂 Construyendo índice…",
                 bg=C["bg"], fg=C["text"],
                 font=("Segoe UI", 11, "bold")).pack(pady=(20, 4))
        tk.Label(self, text=carpeta, bg=C["bg"], fg=C["text2"],
                 font=C["font_sm"], wraplength=460,
                 justify="center").pack()

        self._file_lbl = tk.Label(self, text="Preparando…",
                                  bg=C["bg"], fg=C["accent"],
                                  font=C["font_sm"], wraplength=460)
        self._file_lbl.pack(pady=4)

        self._pv = tk.DoubleVar()
        ttk.Progressbar(self, variable=self._pv,
                        maximum=100, length=450,
                        mode="determinate").pack(pady=6)

        fi = tk.Frame(self, bg=C["bg"]); fi.pack()
        self._count_lbl = tk.Label(fi, text="0 / ?",
                                   bg=C["bg"], fg=C["text2"],
                                   font=C["font_sm"])
        self._count_lbl.pack(side="left", padx=12)
        self._vel_lbl = tk.Label(fi, text="",
                                 bg=C["bg"], fg=C["cyan"] if "cyan" in C else C["accent2"],
                                 font=C["font_sm"])
        self._vel_lbl.pack(side="left", padx=12)
        self._eta_lbl = tk.Label(fi, text="",
                                 bg=C["bg"], fg=C["accent"],
                                 font=C["font_sm"])
        self._eta_lbl.pack(side="left", padx=12)

        self._status_lbl = tk.Label(self, text="",
                                    bg=C["bg"], fg=C["text2"],
                                    font=C["font_sm"])
        self._status_lbl.pack(pady=2)

        fb = tk.Frame(self, bg=C["bg"]); fb.pack(pady=10)
        self._btn_cancel = tk.Button(fb, text="Cancelar", command=self._cancel_op,
                                     bg=C["danger"], fg="white", relief="flat",
                                     font=C["font"], padx=12, pady=4)
        self._btn_cancel.pack(side="left", padx=6)
        self._btn_close = tk.Button(fb, text="Cerrar", command=self.destroy,
                                    bg=C["border"], fg=C["text2"], relief="flat",
                                    font=C["font"], padx=12, pady=4, state="disabled")
        self._btn_close.pack(side="left", padx=6)

        self._t_ini = time.time()
        threading.Thread(target=indexar,
                         args=(self.carpeta, self._cola, self._stop),
                         daemon=True).start()
        self.after(120, self._poll)

    def _cancel_op(self):
        self._stop.set()
        self._status_lbl.config(text="Deteniendo…", fg=C["warn"] if "warn" in C else C["accent"])
        self._btn_cancel.config(state="disabled")

    def _poll(self):
        try:
            while True:
                msg = self._cola.get_nowait(); t = msg[0]

                if t == "total":
                    pass

                elif t == "resumen":
                    _, pend, omit, total = msg
                    self._pendientes = pend
                    self._pv.set(0)
                    self._count_lbl.config(text=f"0 / {pend:,}")
                    self._status_lbl.config(
                        text=f"Total: {total:,}  ·  Nuevos/modificados: {pend:,}  ·  Sin cambios: {omit:,}",
                        fg=C["text2"])

                elif t == "prog":
                    _, proc, pend, omit, total, nombre = msg
                    pct = proc / pend * 100 if pend else 100
                    self._pv.set(pct)
                    self._count_lbl.config(text=f"{proc:,} / {pend:,}")
                    self._file_lbl.config(text=nombre[:70])

                    ahora = time.time()
                    dt    = ahora - self._t_vel
                    if dt >= 1.0:
                        vel = (proc - self._proc_prev) / dt
                        self._proc_prev = proc; self._t_vel = ahora
                        if vel > 0:
                            self._vel_lbl.config(text=f"⚡ {vel:.1f} arch/s")
                            restantes = max(0, pend - proc)
                            eta_s = int(restantes / vel)
                            m, s  = divmod(eta_s, 60)
                            h, m  = divmod(m, 60)
                            if h:   eta_txt = f"~{h}h {m}m"
                            elif m: eta_txt = f"~{m}m {s}s"
                            else:   eta_txt = f"~{s}s"
                            self._eta_lbl.config(text=eta_txt)

                elif t == "optimizando":
                    self._file_lbl.config(text="Optimizando índice FTS5…")

                elif t == "fin":
                    _, nv, ac, om = msg
                    elapsed = time.time() - (self._t_ini or time.time())
                    self._pv.set(100)
                    m, s = divmod(int(elapsed), 60)
                    tiempo_txt = f"{m}m {s}s" if m else f"{s}s"
                    self._file_lbl.config(text="")
                    self._status_lbl.config(
                        text=f"✓  {nv} nuevos · {ac} actualizados · {om} sin cambios  ({tiempo_txt})",
                        fg=C["green"] if "green" in C else C["accent"])
                    self._eta_lbl.config(text="")
                    self._btn_cancel.config(state="disabled")
                    self._btn_close.config(state="normal")
                    if self.on_done: self.on_done()

        except queue.Empty: pass
        self.after(100, self._poll)

# ══════════════════════════════════════════════════════════════════════════════
#  VENTANA DE ETIQUETAS Y FAVORITOS
# ══════════════════════════════════════════════════════════════════════════════
COLORES_ETQ = ["#e6b450","#3fb950","#58a6ff","#f85149","#bc8cff",
                "#39c5cf","#ff9580","#ffa500","#ff69b4","#90ee90"]

class WinEtiquetas(tk.Toplevel):
    def __init__(self, parent, carpeta, doc_id, doc_nombre, callback=None):
        super().__init__(parent)
        self.title("Etiquetas y favorito — JurisBot")
        self.geometry("420x500"); self.configure(bg=C["panel"])
        self.resizable(False, False); self.grab_set()
        self.carpeta    = carpeta
        self.doc_id     = doc_id
        self.doc_nombre = doc_nombre
        self.callback   = callback
        self._ci        = 0
        self._ui(); self._cargar()

    def _ui(self):
        tk.Frame(self, bg=C["accent"], height=3).pack(fill="x")
        tk.Label(self, text="ETIQUETAS Y FAVORITO", fg=C["accent"], bg=C["panel"],
                 font=("Segoe UI", 11, "bold")).pack(pady=(12, 2))
        tk.Label(self, text=self.doc_nombre[:60], fg=C["text2"], bg=C["panel"],
                 font=C["font_sm"], wraplength=380).pack()
        _sep(self, pady=8)

        ff = tk.Frame(self, bg=C["panel"], padx=20); ff.pack(fill="x", pady=4)
        self.var_fav = tk.BooleanVar()
        cb = tk.Checkbutton(ff, text="  ⭐  Marcar como favorito",
                             variable=self.var_fav, fg=C["accent"], bg=C["panel"],
                             selectcolor=C["bg"], activebackground=C["panel"],
                             font=("Segoe UI", 10, "bold"))
        cb.pack(side="left")
        _sep(self, pady=6)

        tk.Label(self, text="Etiquetas asignadas:", fg=C["text2"], bg=C["panel"],
                 font=("Segoe UI", 9, "bold")).pack(anchor="w", padx=20, pady=(4, 2))
        self.frame_etqs = tk.Frame(self, bg=C["panel"], padx=20); self.frame_etqs.pack(fill="x")

        _sep(self, pady=8)
        tk.Label(self, text="Crear nueva etiqueta:", fg=C["text2"], bg=C["panel"],
                 font=("Segoe UI", 9, "bold")).pack(anchor="w", padx=20)
        fn = tk.Frame(self, bg=C["panel"], padx=20); fn.pack(fill="x", pady=4)
        self.var_nueva = tk.StringVar()
        tk.Entry(fn, textvariable=self.var_nueva, width=22, bg=C["bg"], fg=C["text"],
                 insertbackground=C["accent"], relief="flat", bd=0,
                 highlightthickness=1, highlightcolor=C["accent"],
                 highlightbackground=C["border"], font=C["font_sm"]).pack(
            side="left", ipady=4, padx=(0, 6))
        self.btn_color = tk.Button(fn, bg=COLORES_ETQ[0], width=3, relief="flat",
                                    cursor="hand2", command=self._ciclar)
        self.btn_color.pack(side="left", padx=4)
        _mk_btn(fn, "+ Crear", self._crear, bg=C["border"], fg=C["accent"],
                font=C["font_sm"]).pack(side="left", padx=4)

        _sep(self, pady=8)
        fb = tk.Frame(self, bg=C["panel"]); fb.pack(pady=6)
        _mk_btn(fb, "✓  Guardar", self._guardar).pack(side="left", padx=6)
        _mk_btn(fb, "Cancelar", self.destroy, bg=C["border"], fg=C["text2"]).pack(side="left", padx=6)

    def _ciclar(self):
        self._ci = (self._ci + 1) % len(COLORES_ETQ)
        self.btn_color.config(bg=COLORES_ETQ[self._ci])

    def _cargar(self):
        con      = get_db(self.carpeta)
        fav      = con.execute("SELECT 1 FROM favoritos WHERE documento_id=?", (self.doc_id,)).fetchone()
        self.var_fav.set(bool(fav))
        todas    = con.execute("SELECT id,nombre,color FROM etiquetas ORDER BY nombre").fetchall()
        asignadas = {r["etiqueta_id"] for r in
                     con.execute("SELECT etiqueta_id FROM doc_etiquetas WHERE documento_id=?",
                                 (self.doc_id,)).fetchall()}
        con.close()
        for w in self.frame_etqs.winfo_children(): w.destroy()
        self._vars_etq = {}
        for e in todas:
            var = tk.BooleanVar(value=e["id"] in asignadas)
            self._vars_etq[e["id"]] = var
            rf = tk.Frame(self.frame_etqs, bg=C["panel"]); rf.pack(anchor="w", pady=1)
            tk.Label(rf, text="●", fg=e["color"], bg=C["panel"], font=C["font"]).pack(side="left")
            tk.Checkbutton(rf, text=f"  {e['nombre']}", variable=var,
                            fg=C["text"], bg=C["panel"], selectcolor=C["bg"],
                            activebackground=C["panel"], font=C["font_sm"]).pack(side="left")

    def _crear(self):
        nombre = self.var_nueva.get().strip()
        if not nombre: return
        con = get_db(self.carpeta)
        try:
            con.execute("INSERT OR IGNORE INTO etiquetas(nombre,color) VALUES(?,?)",
                        (nombre, COLORES_ETQ[self._ci])); con.commit()
        except: pass
        con.close(); self.var_nueva.set(""); self._cargar()

    def _guardar(self):
        con = get_db(self.carpeta)
        if self.var_fav.get():
            con.execute("INSERT OR IGNORE INTO favoritos(documento_id,fecha) VALUES(?,?)",
                        (self.doc_id, datetime.now().strftime("%d/%m/%Y")))
        else:
            con.execute("DELETE FROM favoritos WHERE documento_id=?", (self.doc_id,))
        con.execute("DELETE FROM doc_etiquetas WHERE documento_id=?", (self.doc_id,))
        for eid, var in self._vars_etq.items():
            if var.get():
                con.execute("INSERT OR IGNORE INTO doc_etiquetas VALUES(?,?)", (self.doc_id, eid))
        con.commit(); con.close()
        if self.callback: self.callback()
        self.destroy()

# ══════════════════════════════════════════════════════════════════════════════
#  VENTANA GUÍA RÁPIDA
# ══════════════════════════════════════════════════════════════════════════════
class WinGuia(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("¿Cómo buscar? — Guía rápida")
        self.geometry("620x560"); self.configure(bg=C["panel"])
        self.resizable(False, False); self._ui()

    def _ui(self):
        tk.Frame(self, bg=C["accent"], height=3).pack(fill="x")
        tk.Label(self, text="¿CÓMO BUSCAR?", fg=C["accent"], bg=C["panel"],
                 font=("Georgia", 14, "bold")).pack(pady=(16, 2))
        tk.Label(self, text="Use estos símbolos para hacer búsquedas más precisas:",
                 fg=C["text2"], bg=C["panel"], font=C["font_sm"]).pack()
        _sep(self, pady=8)

        sf = tk.Frame(self, bg=C["panel"], padx=24); sf.pack(fill="both", expand=True)
        FILAS = [
            ("Búsqueda simple",           "contrato laboral",
             "Escribe las palabras y el sistema busca documentos que contengan TODAS.", C["green"] if "green" in C else C["accent"]),
            ("Símbolo  +  (Y también)",   "contrato + laboral + 2023",
             "Busca documentos que tengan TODAS las palabras que escribas.", C["accent"]),
            ("Símbolo  -  (Excluir)",     "contrato - civil",
             "Busca 'contrato' pero EXCLUYE los que también digan 'civil'.", C["danger"]),
            ('Comillas  "…"  (Frase exacta)', '"acción de tutela"',
             "Busca esa frase EXACTAMENTE como está escrita, en ese mismo orden.", C["accent2"]),
            ("Coma  ,  (O cualquiera)",   "tutela, acción popular, nulidad",
             "Busca documentos que tengan CUALQUIERA de esas palabras.", C["cyan"] if "cyan" in C else C["accent2"]),
            ("Asterisco  *  (Comodín)",   "contrat*",
             "Busca palabras que EMPIECEN así: contrato, contratar, contratación…", "#bc8cff"),
        ]
        for titulo, ejemplo, desc, color in FILAS:
            fila = tk.Frame(sf, bg=C["bg"], padx=14, pady=8); fila.pack(fill="x", pady=2)
            tk.Label(fila, text=titulo, fg=color, bg=C["bg"],
                     font=("Segoe UI", 9, "bold"), anchor="w").pack(anchor="w")
            tk.Label(fila, text=f"  Ejemplo:  {ejemplo}", fg=C["accent"], bg=C["bg"],
                     font=C["font_mono"], anchor="w").pack(anchor="w")
            tk.Label(fila, text=f"  {desc}", fg=C["text2"], bg=C["bg"],
                     font=C["font_sm"], anchor="w", wraplength=540).pack(anchor="w")

        _sep(self, pady=8)
        _mk_btn(self, "Entendido, cerrar", self.destroy).pack(pady=6)

# ══════════════════════════════════════════════════════════════════════════════
#  APLICACIÓN PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════
class App(tk.Tk):

    def __init__(self):
        super().__init__()
        self.title("JurisBot Relatoría")
        self.geometry("1400x820")
        self.minsize(960, 600)
        self.configure(bg=C["bg"])

        # Estado
        self._carpeta        = None
        self._resultados     = []
        self._total_res      = 0
        self._offset         = 0
        self._query_actual   = ""
        self._cola_doc       = queue.Queue()
        self._cache_doc      = {}
        self._hits_pos       = []
        self._hit_actual     = 0
        self._ruta_sel       = None
        self._id_sel         = None
        self._terminos       = []
        self._debounce_id    = None
        self._placeholder_on = True
        self._pdf_doc        = None
        self._pdf_page_idx   = 0
        self._modo_vista     = "texto"
        self._carpetas_check = {}
        self._etq_map        = {"— Todas —": None}
        self._orden_asc      = {}

        self._apply_style()
        self._build_ui()
        self.bind("<F1>", lambda e: WinGuia(self))
        self.bind("<Control-f>", lambda e: (self._entry.focus(), self._ph_clear()))
        self.bind("<Control-i>", lambda e: self._actualizar_idx())
        self.bind("<Escape>",    lambda e: self._limpiar())
        self.after(300, self._inicio)
        self.after(200, self._poll_doc)

    # ── ESTILOS ───────────────────────────────────────────────────────────────
    def _apply_style(self):
        s = ttk.Style(self); s.theme_use("clam")
        s.configure("Treeview", background=C["bg"], foreground=C["text"],
                    fieldbackground=C["bg"], rowheight=26, font=C["font"], borderwidth=0)
        s.configure("Treeview.Heading", background=C["panel"], foreground=C["accent"],
                    font=("Segoe UI", 8, "bold"), relief="flat", borderwidth=0)
        s.map("Treeview", background=[("selected", C["sel"])], foreground=[("selected", C["text"])])
        s.configure("TScrollbar", background=C["border"], troughcolor=C["bg"],
                    borderwidth=0, arrowcolor=C["text2"], relief="flat")
        s.configure("TProgressbar", troughcolor=C["border"], background=C["accent"], borderwidth=0)
        s.configure("TCombobox", fieldbackground=C["bg"], background=C["bg"],
                    foreground=C["text"], selectbackground=C["sel"], arrowcolor=C["text2"])
        s.map("TCombobox", fieldbackground=[("readonly", C["bg"])],
              foreground=[("readonly", C["text"])])

    # ── UI PRINCIPAL ──────────────────────────────────────────────────────────
    def _build_ui(self):
        self._build_topbar()
        self._build_body()

    def _build_topbar(self):
        bar = tk.Frame(self, bg=C["panel"], height=56)
        bar.pack(fill="x")
        bar.pack_propagate(False)

        # Línea de acento superior
        tk.Frame(self, bg=C["accent"], height=3).pack(fill="x", before=bar)

        # Logo
        tk.Label(bar, text="⚖ JurisBot Relatoría",
                 bg=C["panel"], fg=C["accent"],
                 font=("Georgia", 12, "bold")).pack(side="left", padx=(14, 8), pady=10)
        tk.Frame(bar, bg=C["border"], width=1).pack(side="left", fill="y", pady=8)

        # Carpeta activa
        self.lbl_carpeta_hdr = tk.Label(bar, text="Sin carpeta", fg=C["text2"],
                                         bg=C["panel"], font=C["font_sm"])
        self.lbl_carpeta_hdr.pack(side="left", padx=(8, 0))
        tk.Frame(bar, bg=C["border"], width=1).pack(side="left", fill="y", pady=8, padx=8)

        # Campo de búsqueda
        self._search_var = tk.StringVar()
        self._entry = tk.Entry(
            bar, textvariable=self._search_var,
            bg=C["bg"], fg=C["text2"],
            insertbackground=C["text"], relief="flat",
            font=C["font_lg"], highlightthickness=1,
            highlightcolor=C["accent"],
            highlightbackground=C["border"])
        self._entry.pack(side="left", fill="x", expand=True, ipady=6, padx=4)
        self._PH = "Buscar por texto, radicado, año, palabra clave…"
        self._entry.insert(0, self._PH)
        self._entry.bind("<Return>",    lambda e: self._buscar_desde_cero())
        self._entry.bind("<KeyRelease>",self._debounce)
        self._entry.bind("<FocusIn>",   self._ph_clear)
        self._entry.bind("<FocusOut>",  self._ph_restore)
        _Tooltip(self._entry,
                 "Escriba palabras para buscar.  Pulse F1 para la guía de operadores.\n"
                 "Ctrl+F: enfocar búsqueda  ·  Ctrl+I: actualizar índice")

        # Filtros rápidos
        ff = tk.Frame(bar, bg=C["panel"]); ff.pack(side="left", padx=6)
        tk.Label(ff, text="Año:", bg=C["panel"], fg=C["text2"],
                 font=C["font_sm"]).pack(side="left")
        self._year_var = tk.StringVar()
        tk.Entry(ff, textvariable=self._year_var, width=5,
                 bg=C["bg"], fg=C["text"],
                 insertbackground=C["text"], relief="flat", font=C["font_sm"],
                 highlightthickness=1, highlightcolor=C["accent"],
                 highlightbackground=C["border"]).pack(side="left", padx=(2, 8))
        tk.Label(ff, text="Tipo:", bg=C["panel"], fg=C["text2"],
                 font=C["font_sm"]).pack(side="left")
        self._ext_var = tk.StringVar()
        ttk.Combobox(ff, textvariable=self._ext_var,
                     values=["", ".pdf", ".docx"],
                     width=7, state="readonly").pack(side="left", padx=2)

        # Botones de acción
        _mk_btn(bar, "🔍 Buscar", self._buscar_desde_cero,
                tooltip="Ejecutar búsqueda").pack(side="left", padx=3, pady=8)
        _mk_btn(bar, "✕", self._limpiar, bg=C["border"], fg=C["text2"],
                font=C["font"], tooltip="Limpiar búsqueda").pack(side="left", padx=2, pady=8)
        _mk_btn(bar, "?", lambda: WinGuia(self), bg=C["border"], fg=C["accent"],
                font=("Segoe UI", 10, "bold"), tooltip="Guía de búsqueda (F1)").pack(side="left", padx=2, pady=8)
        self._btn_avanzado = _mk_btn(bar, "▼ Avanzado", self._toggle_avanzado,
                bg=C["border"], fg=C["text2"], font=C["font_sm"],
                tooltip="Mostrar/ocultar filtros avanzados").pack(side="left", padx=2, pady=8)

        tk.Frame(bar, bg=C["border"], width=1).pack(side="left", fill="y", pady=8, padx=4)

        _mk_btn(bar, "▶ Actualizar índice", self._actualizar_idx,
                bg=C["accent"], fg="#1a1a1a",
                font=("Segoe UI", 9, "bold"),
                tooltip="Indexa archivos nuevos o modificados").pack(side="left", padx=3, pady=8)
        _mk_btn(bar, "⚙", self._abrir_config, bg=C["border"], fg=C["text2"],
                font=C["font"], tooltip="Configuración: cambiar carpeta, reconstruir índice").pack(
            side="left", padx=2, pady=8)

        # Indicador de estado
        self.lbl_badge  = tk.Label(bar, text="● CARGANDO", fg=C["warn"] if "warn" in C else C["accent"],
                                    bg=C["panel"], font=("Segoe UI", 8, "bold"))
        self.lbl_badge.pack(side="right", padx=(0, 12))
        self.lbl_ultima = tk.Label(bar, text="", fg=C["text2"], bg=C["panel"],
                                    font=("Segoe UI", 7))
        self.lbl_ultima.pack(side="right", padx=4)

        # Chips de operadores (segunda fila debajo del topbar)
        chips_row = tk.Frame(self, bg=C["panel"], padx=14, pady=4)
        chips_row.pack(fill="x")
        tk.Label(chips_row, text="Operadores:", fg=C["text2"], bg=C["panel"],
                 font=C["font_sm"]).pack(side="left", padx=(0, 6))
        for sym, nombre, ej, desc in OPERADORES:
            f = tk.Frame(chips_row, bg=C["bg"], padx=8, pady=2, cursor="hand2")
            f.pack(side="left", padx=2)
            tk.Label(f, text=sym,      fg=C["accent"], bg=C["bg"], font=C["font_mono"]).pack(side="left")
            tk.Label(f, text=f" {nombre}", fg=C["text2"], bg=C["bg"], font=C["font_sm"]).pack(side="left")
            _Tooltip(f, f"{desc}\n\nEjemplo:  {ej}\n\nHaga clic para insertar.")
            f.bind("<Button-1>", lambda e, s=sym: self._insertar_op(s))
            for w in f.winfo_children():
                w.bind("<Button-1>", lambda e, s=sym: self._insertar_op(s))

        tk.Frame(self, bg=C["border"], height=1).pack(fill="x")

        # Panel avanzado (oculto por defecto)
        self._panel_avanzado = tk.Frame(self, bg="#0d1520", padx=12, pady=8)
        self._avanzado_visible = False
        self._build_panel_avanzado(self._panel_avanzado)

    def _build_panel_avanzado(self, p):
        """Panel colapsable de filtros avanzados bajo el topbar."""
        tk.Label(p, text="FILTROS AVANZADOS", fg=C["accent"], bg="#0d1520",
                 font=("Segoe UI", 8, "bold")).grid(row=0, column=0, columnspan=8,
                 sticky="w", pady=(0, 4))

        # Año desde/hasta
        tk.Label(p, text="Año desde:", fg=C["text2"], bg="#0d1520",
                 font=C["font_sm"]).grid(row=1, column=0, sticky="w", padx=(0, 4))
        self._adv_desde = tk.StringVar()
        tk.Entry(p, textvariable=self._adv_desde, width=6, bg=C["bg"], fg=C["text"],
                 insertbackground=C["accent"], relief="flat", font=C["font_sm"],
                 highlightthickness=1, highlightcolor=C["accent"],
                 highlightbackground=C["border"]).grid(row=1, column=1, sticky="w", ipady=2, padx=(0,8))
        tk.Label(p, text="hasta:", fg=C["text2"], bg="#0d1520",
                 font=C["font_sm"]).grid(row=1, column=2, sticky="w", padx=(0, 4))
        self._adv_hasta = tk.StringVar()
        tk.Entry(p, textvariable=self._adv_hasta, width=6, bg=C["bg"], fg=C["text"],
                 insertbackground=C["accent"], relief="flat", font=C["font_sm"],
                 highlightthickness=1, highlightcolor=C["accent"],
                 highlightbackground=C["border"]).grid(row=1, column=3, sticky="w", ipady=2, padx=(0,16))

        # Solo nombre
        self._adv_solo_nombre = tk.BooleanVar(value=False)
        tk.Checkbutton(p, text="Solo buscar en nombre del archivo",
                       variable=self._adv_solo_nombre,
                       fg=C["text2"], bg="#0d1520", selectcolor=C["bg"],
                       activebackground="#0d1520", font=C["font_sm"]).grid(
            row=1, column=4, sticky="w", padx=(0,16))

        # Ordenar por
        tk.Label(p, text="Ordenar por:", fg=C["text2"], bg="#0d1520",
                 font=C["font_sm"]).grid(row=1, column=5, sticky="w", padx=(0,4))
        self._adv_orden = tk.StringVar(value="Relevancia")
        ttk.Combobox(p, textvariable=self._adv_orden,
                     values=["Relevancia", "Fecha ↓", "Fecha ↑", "Nombre A-Z", "Nombre Z-A", "Tamaño ↓"],
                     state="readonly", width=12, font=C["font_sm"]).grid(
            row=1, column=6, sticky="w", padx=(0,8))

        bf = tk.Frame(p, bg="#0d1520"); bf.grid(row=1, column=7, sticky="w", padx=8)
        _mk_btn(bf, "Aplicar", self._buscar_desde_cero,
                tooltip="Aplicar filtros avanzados").pack(side="left", padx=2)
        _mk_btn(bf, "Limpiar", self._limpiar_avanzado, bg=C["border"], fg=C["text2"],
                font=C["font_sm"]).pack(side="left", padx=2)

    def _toggle_avanzado(self):
        if self._avanzado_visible:
            self._panel_avanzado.pack_forget()
            self._avanzado_visible = False
        else:
            self._panel_avanzado.pack(fill="x", before=self._body_frame
                                       if hasattr(self, "_body_frame") else None)
            self._avanzado_visible = True

    def _limpiar_avanzado(self):
        self._adv_desde.set(""); self._adv_hasta.set("")
        self._adv_solo_nombre.set(False); self._adv_orden.set("Relevancia")

    def _build_body(self):
        body = tk.Frame(self, bg=C["bg"])
        self._body_frame = body
        body.pack(fill="both", expand=True)
        body.rowconfigure(0, weight=1)
        # Sidebar arranca colapsado (weight=0), lista razonable, preview máximo
        body.columnconfigure(0, weight=0, minsize=0)   # sidebar (colapsado)
        body.columnconfigure(1, weight=2, minsize=220) # resultados
        body.columnconfigure(2, weight=7)              # preview — prioridad
        body.columnconfigure(3, weight=0, minsize=0)   # meta (colapsado)

        self._build_sidebar(body)
        self._build_results_panel(body)
        self._build_preview_panel(body)
        self._build_meta_panel(body)

        # Overlay de búsqueda (invisible hasta que se activa)
        self._overlay = tk.Frame(body, bg="#0a0f1a")
        self._overlay_lbl = tk.Label(self._overlay,
            text="", fg=C["accent"], bg="#0a0f1a",
            font=("Segoe UI", 16, "bold"))
        self._overlay_sub = tk.Label(self._overlay,
            text="", fg=C["text2"], bg="#0a0f1a",
            font=("Segoe UI", 9))
        self._overlay_lbl.place(relx=0.5, rely=0.44, anchor="center")
        self._overlay_sub.place(relx=0.5, rely=0.52, anchor="center")
        # El overlay arranca oculto
        self._overlay_visible = False

        # lbl_status: label oculto para no romper referencias (no se muestra en UI)
        self.lbl_status = tk.Label(self, text="", fg=C["text2"], bg=C["bg"],
                                    font=C["font_sm"])

        # Colapsar sidebar al inicio para dar protagonismo al preview
        self.after(100, self._colapsar_sidebar_inicial)

    def _colapsar_sidebar_inicial(self):
        """Colapsa el sidebar al inicio sin mostrar botón de restaurar aún."""
        try:
            self._sidebar_frame.grid_remove()
            if not hasattr(self, "_btn_restaurar_sidebar"):
                self._btn_restaurar_sidebar = _mk_btn(
                    self._body_frame, "▶", self._restaurar_sidebar,
                    bg=C["border"], fg=C["accent"], font=("Segoe UI", 9),
                    tooltip="Mostrar filtros")
            self._btn_restaurar_sidebar.grid(row=0, column=0, sticky="ns", padx=(4,0), pady=8)
        except: pass

    # ── SIDEBAR ───────────────────────────────────────────────────────────────
    def _build_sidebar(self, parent):
        self._sidebar_frame = tk.Frame(parent, bg=C["panel"])
        self._sidebar_frame.grid(row=0, column=0, sticky="nsew", padx=(8, 4), pady=8)
        self._sidebar_frame.columnconfigure(0, weight=1)
        f = self._sidebar_frame

        # Botón colapsar sidebar completo
        hdr = tk.Frame(f, bg=C["panel"]); hdr.pack(fill="x", padx=6, pady=(6,2))
        tk.Label(hdr, text="Filtros", fg=C["accent"], bg=C["panel"],
                 font=("Segoe UI", 9, "bold")).pack(side="left")
        _mk_btn(hdr, "◀", self._colapsar_sidebar, bg=C["border"], fg=C["text2"],
                font=("Segoe UI", 8), tooltip="Ocultar panel lateral").pack(side="right")
        _sep(f)

        # ── Sección: Tipo ─────────────────────────────────────────────────────
        self._sec_tipo = self._seccion(f, "Tipo de documento")
        self.var_pdf  = tk.BooleanVar(value=True)
        self.var_docx = tk.BooleanVar(value=True)
        for var, txt, color in [(self.var_pdf, "■ PDF", "#FF7B72"), (self.var_docx, "■ DOCX", "#79C0FF")]:
            frow = tk.Frame(self._sec_tipo["body"], bg=C["panel"]); frow.pack(anchor="w", padx=6)
            tk.Checkbutton(frow, text=f"  {txt}", variable=var, fg=color,
                           bg=C["panel"], selectcolor=C["bg"], activebackground=C["panel"],
                           font=C["font_sm"], command=self._buscar_desde_cero).pack(side="left")

        # ── Sección: Favoritos y etiquetas ────────────────────────────────────
        self._sec_etq = self._seccion(f, "Favoritos y etiquetas")
        self.var_solo_fav = tk.BooleanVar(value=False)
        fav_f = tk.Frame(self._sec_etq["body"], bg=C["panel"], padx=6); fav_f.pack(fill="x")
        tk.Checkbutton(fav_f, text="  ⭐ Solo favoritos",
                        variable=self.var_solo_fav, fg=C["accent"], bg=C["panel"],
                        selectcolor=C["bg"], activebackground=C["panel"], font=C["font_sm"],
                        command=self._buscar_desde_cero).pack(side="left")
        self.var_etq = tk.StringVar(value="— Todas —")
        self.cb_etq  = ttk.Combobox(self._sec_etq["body"], textvariable=self.var_etq,
                                     state="readonly", font=C["font_sm"], width=20)
        self.cb_etq.pack(padx=8, pady=2)
        self.cb_etq.bind("<<ComboboxSelected>>", lambda e: self._buscar_desde_cero())

        # ── Sección: Historial ────────────────────────────────────────────────
        self._sec_hist = self._seccion(f, "Recientes")
        hf = tk.Frame(self._sec_hist["body"], bg=C["panel"]); hf.pack(fill="x", padx=4)
        self.tree_hist = ttk.Treeview(hf, show="tree", selectmode="browse", height=4)
        vsb_h = ttk.Scrollbar(hf, orient="vertical", command=self.tree_hist.yview)
        self.tree_hist.configure(yscrollcommand=vsb_h.set)
        vsb_h.pack(side="right", fill="y"); self.tree_hist.pack(fill="x")
        self.tree_hist.bind("<Double-1>", self._repetir_hist)

        # ── Sección: Carpetas ─────────────────────────────────────────────────
        self._sec_carp = self._seccion(f, "Subcarpetas", expand=True)
        cf = tk.Frame(self._sec_carp["body"], bg=C["panel"]); cf.pack(fill="both", expand=True, padx=4)
        cf.rowconfigure(0, weight=1); cf.columnconfigure(0, weight=1)
        self._tree_carpetas = ttk.Treeview(cf, show="tree", selectmode="none", height=8)
        vsb_c = ttk.Scrollbar(cf, orient="vertical", command=self._tree_carpetas.yview)
        self._tree_carpetas.configure(yscrollcommand=vsb_c.set)
        vsb_c.grid(row=0, column=1, sticky="ns")
        self._tree_carpetas.grid(row=0, column=0, sticky="nsew")
        self._tree_carpetas.bind("<Button-1>", self._on_click_carpeta)
        self._tree_carpetas.tag_configure("checked",   foreground=C["accent"])
        self._tree_carpetas.tag_configure("unchecked", foreground=C["text2"])
        self._tree_carpetas.tag_configure("root",      foreground=C["accent"],
                                          font=("Segoe UI", 8, "bold"))

        fb = tk.Frame(self._sec_carp["body"], bg=C["panel"]); fb.pack(fill="x", padx=4, pady=2)
        _mk_btn(fb, "✓ Todas", self._marcar_todos_arbol,
                bg=C["border"], fg=C["green"] if "green" in C else C["accent"],
                font=("Segoe UI", 7)).pack(side="left", padx=2)
        _mk_btn(fb, "✗ Ninguna", self._desmarcar_todos_arbol,
                bg=C["border"], fg=C["danger"], font=("Segoe UI", 7)).pack(side="left", padx=2)
        _mk_btn(fb, "Limpiar", self._limpiar_filtros,
                bg=C["border"], fg=C["text2"], font=("Segoe UI", 7)).pack(side="right", padx=2)

    def _seccion(self, parent, titulo, expand=False):
        """Crea una sección colapsable con header clickeable."""
        estado = {"abierto": True}
        outer  = tk.Frame(parent, bg=C["panel"]); outer.pack(fill="x" if not expand else "both",
                                                               expand=expand, pady=1)
        # Header de sección
        hdr = tk.Frame(outer, bg=C["bg"], cursor="hand2"); hdr.pack(fill="x")
        lbl_tri = tk.Label(hdr, text="▾", fg=C["text2"], bg=C["bg"],
                           font=("Segoe UI", 8)); lbl_tri.pack(side="left", padx=(6,2))
        tk.Label(hdr, text=titulo.upper(), fg=C["text2"], bg=C["bg"],
                 font=("Segoe UI", 7, "bold")).pack(side="left", pady=3)
        # Body
        body = tk.Frame(outer, bg=C["panel"]); body.pack(fill="both" if expand else "x",
                                                          expand=expand, pady=2)

        def _toggle(_e=None):
            if estado["abierto"]:
                body.pack_forget(); lbl_tri.config(text="▸"); estado["abierto"] = False
            else:
                body.pack(fill="both" if expand else "x", expand=expand, pady=2)
                lbl_tri.config(text="▾"); estado["abierto"] = True

        hdr.bind("<Button-1>", _toggle)
        for w in hdr.winfo_children(): w.bind("<Button-1>", _toggle)
        _sep(outer, color=C["border"])
        return {"body": body, "toggle": _toggle, "estado": estado}

    def _colapsar_sidebar(self):
        """Oculta el sidebar y muestra un botón para restaurarlo."""
        self._sidebar_frame.grid_remove()
        # Crear botón flotante para restaurar
        if not hasattr(self, "_btn_restaurar_sidebar"):
            self._btn_restaurar_sidebar = _mk_btn(
                self._body_frame, "▶", self._restaurar_sidebar,
                bg=C["border"], fg=C["accent"], font=("Segoe UI", 9),
                tooltip="Mostrar panel lateral")
        self._btn_restaurar_sidebar.grid(row=0, column=0, sticky="ns", padx=2, pady=8)

    def _restaurar_sidebar(self):
        if hasattr(self, "_btn_restaurar_sidebar"):
            self._btn_restaurar_sidebar.grid_remove()
        self._sidebar_frame.grid()
    # ── PANEL RESULTADOS (Listbox + paginación) ───────────────────────────────
    def _build_results_panel(self, parent):
        f = tk.Frame(parent, bg=C["panel"])
        f.grid(row=0, column=1, sticky="nsew", padx=4, pady=8)
        f.rowconfigure(2, weight=1)
        f.columnconfigure(0, weight=1)

        hdr = tk.Frame(f, bg=C["panel"]); hdr.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 0))
        self._results_lbl = tk.Label(hdr, text="Resultados",
                                      bg=C["panel"], fg=C["text"],
                                      font=("Segoe UI", 10, "bold"))
        self._results_lbl.pack(side="left")
        self.lbl_pag = tk.Label(hdr, text="", fg=C["text2"], bg=C["panel"], font=C["font_sm"])
        self.lbl_pag.pack(side="left", padx=8)

        # Paginación
        pf = tk.Frame(f, bg=C["panel"]); pf.grid(row=1, column=0, sticky="ew", padx=8, pady=2)
        self.btn_ant = _mk_btn(pf, "◀ Anterior", self._pag_ant,
                                bg=C["border"], fg=C["text2"], font=C["font_sm"],
                                tooltip="Página anterior")
        self.btn_ant.pack(side="left", padx=2)
        self.btn_sig = _mk_btn(pf, "Siguiente ▶", self._pag_sig,
                                bg=C["border"], fg=C["text2"], font=C["font_sm"],
                                tooltip="Página siguiente")
        self.btn_sig.pack(side="left", padx=2)

        lf = tk.Frame(f, bg=C["panel"]); lf.grid(row=2, column=0, sticky="nsew", padx=4, pady=4)
        lf.rowconfigure(0, weight=1); lf.columnconfigure(0, weight=1)

        self._listbox = tk.Listbox(
            lf, bg=C["bg"], fg=C["text"],
            selectbackground=C["sel"], selectforeground=C["text"],
            font=C["font"], relief="flat", bd=0,
            activestyle="none", highlightthickness=0)
        self._listbox.grid(row=0, column=0, sticky="nsew")
        self._listbox.bind("<<ListboxSelect>>", self._on_list_select)
        self._listbox.bind("<Double-1>", lambda e: self._abrir_original())
        self._listbox.bind("<Button-3>", self._menu_ctx_list)
        _Tooltip(self._listbox,
                 "Clic → ver documento\n"
                 "Doble clic → abrir con programa predeterminado\n"
                 "Clic derecho → opciones")

        sb = ttk.Scrollbar(lf, orient="vertical", command=self._listbox.yview)
        sb.grid(row=0, column=1, sticky="ns")
        self._listbox.configure(yscrollcommand=sb.set)

    # ── PANEL PREVIEW ─────────────────────────────────────────────────────────
    def _build_preview_panel(self, parent):
        f = tk.Frame(parent, bg=C["panel"])
        f.grid(row=0, column=2, sticky="nsew", padx=4, pady=8)
        f.rowconfigure(2, weight=1)
        f.columnconfigure(0, weight=1)

        # Header del preview
        dh = tk.Frame(f, bg=C["panel"], pady=5, padx=10); dh.grid(row=0, column=0, sticky="ew")
        self._preview_lbl = tk.Label(dh,
                                      text="— Seleccione un resultado —",
                                      bg=C["panel"], fg=C["accent"],
                                      font=("Segoe UI", 9, "bold"), anchor="w")
        self._preview_lbl.pack(side="left")

        nav = tk.Frame(dh, bg=C["panel"]); nav.pack(side="right")
        self.lbl_hits = tk.Label(nav, text="", fg=C["text2"], bg=C["panel"], font=C["font_sm"])
        self.lbl_hits.pack(side="left", padx=6)
        _mk_btn(nav, "▲", self._hit_prev, bg=C["border"], fg=C["accent"],
                font=C["font_sm"], tooltip="Coincidencia anterior").pack(side="left", padx=1)
        _mk_btn(nav, "▼", self._hit_next, bg=C["border"], fg=C["accent"],
                font=C["font_sm"], tooltip="Coincidencia siguiente").pack(side="left", padx=1)

        self.btn_vista_txt = _mk_btn(nav, "📄 Texto", self._ver_texto,
                                      bg=C["accent"], fg="#1a1a1a",
                                      font=C["font_sm"], tooltip="Ver texto con términos resaltados")
        self.btn_vista_txt.pack(side="left", padx=(8, 1))
        self.btn_vista_doc = _mk_btn(nav, "🖼 Vista real", self._ver_doc_renderizado,
                                      bg=C["border"], fg=C["accent2"],
                                      font=C["font_sm"],
                                      tooltip="Ver el documento renderizado\n(PDF: directo · DOCX: requiere LibreOffice)")
        self.btn_vista_doc.pack(side="left", padx=1)

        self.btn_abrir = _mk_btn(nav, "↗ Abrir", self._abrir_original,
                                  bg=C["border"], fg=C["green"] if "green" in C else C["accent"],
                                  font=C["font_sm"],
                                  tooltip="Abrir con el programa predeterminado")
        self.btn_abrir.pack(side="left", padx=(8, 1))
        self.btn_abrir.config(state="disabled")
        _mk_btn(nav, "📋", self._copiar_ruta, bg=C["border"], fg=C["text2"],
                font=C["font_sm"], tooltip="Copiar ruta al portapapeles").pack(side="left", padx=1)
        _mk_btn(nav, "⭐", self._abrir_etiquetas, bg=C["border"], fg=C["accent"],
                font=C["font_sm"], tooltip="Favoritos y etiquetas").pack(side="left", padx=1)

        # Meta debajo del header
        self.lbl_doc_meta = tk.Label(f, text="", fg=C["text2"], bg=C["panel"],
                                      font=("Segoe UI", 7), anchor="w", padx=10)
        self.lbl_doc_meta.grid(row=1, column=0, sticky="ew")

        # Navegación páginas renderizadas
        self.frame_pag_nav = tk.Frame(f, bg=C["panel"])
        _mk_btn(self.frame_pag_nav, "◀", self._pag_doc_ant, bg=C["border"], fg=C["text"],
                font=C["font_sm"]).pack(side="left", padx=6)
        self.lbl_pag_doc = tk.Label(self.frame_pag_nav, text="", fg=C["text2"], bg=C["panel"],
                                     font=C["font_sm"])
        self.lbl_pag_doc.pack(side="left", padx=8)
        _mk_btn(self.frame_pag_nav, "▶", self._pag_doc_sig, bg=C["border"], fg=C["text"],
                font=C["font_sm"]).pack(side="left", padx=6)

        # Contenido
        self.frame_contenido = tk.Frame(f, bg=C["bg"])
        self.frame_contenido.grid(row=2, column=0, sticky="nsew")
        self.frame_contenido.rowconfigure(0, weight=1)
        self.frame_contenido.columnconfigure(0, weight=1)

        # Vista texto
        self.frame_texto = tk.Frame(self.frame_contenido, bg=C["bg"])
        self.frame_texto.grid(row=0, column=0, sticky="nsew")
        self.frame_texto.rowconfigure(0, weight=1); self.frame_texto.columnconfigure(0, weight=1)
        tf2 = tk.Frame(self.frame_texto, bg=C["bg"]); tf2.grid(row=0, column=0, sticky="nsew")
        tf2.rowconfigure(0, weight=1); tf2.columnconfigure(0, weight=1)
        self.txt = tk.Text(tf2, font=("Georgia", 11), bg=C["bg"], fg=C["text"], relief="flat", bd=0,
                            wrap="word", state="disabled", highlightthickness=0,
                            spacing1=2, spacing3=4, padx=20, pady=16,
                            selectbackground=C["sel"])
        vsb_t = ttk.Scrollbar(tf2, orient="vertical", command=self.txt.yview)
        self.txt.configure(yscrollcommand=vsb_t.set)
        vsb_t.grid(row=0, column=1, sticky="ns"); self.txt.grid(row=0, column=0, sticky="nsew")
        self.txt.tag_configure("normal",    foreground=C["text"])
        self.txt.tag_configure("heading",   foreground=C["accent"], font=("Georgia", 12, "bold"))
        self.txt.tag_configure("bold_txt",  foreground=C["text"],   font=("Georgia", 11, "bold"))
        self.txt.tag_configure("tabla",     foreground=C["text2"],  font=("Consolas", 10),
                                background="#0d1520")
        self.txt.tag_configure("resaltado", foreground=C["hl_fg"] if "hl_fg" in C else "#ffffff",
                                background=C["hl_bg"] if "hl_bg" in C else "#7d5a00",
                                font=("Georgia", 11, "bold"))
        self.txt.tag_configure("activo",    foreground="#000000",
                                background=C["hl_act"] if "hl_act" in C else "#ff6b35",
                                font=("Georgia", 11, "bold"))
        self.txt.tag_configure("cargando",  foreground=C["text2"], font=("Georgia", 10, "italic"))

        # Vista renderizada
        self.frame_render = tk.Frame(self.frame_contenido, bg=C["bg"])
        rf = tk.Frame(self.frame_render, bg=C["bg"]); rf.pack(fill="both", expand=True)
        self.canvas_doc = tk.Canvas(rf, bg=C["bg"], highlightthickness=0)
        vsb_r = ttk.Scrollbar(rf, orient="vertical",   command=self.canvas_doc.yview)
        hsb_r = ttk.Scrollbar(rf, orient="horizontal", command=self.canvas_doc.xview)
        self.canvas_doc.configure(yscrollcommand=vsb_r.set, xscrollcommand=hsb_r.set)
        vsb_r.pack(side="right", fill="y"); hsb_r.pack(side="bottom", fill="x")
        self.canvas_doc.pack(fill="both", expand=True)
        self.canvas_doc.bind("<MouseWheel>", lambda e: self.canvas_doc.yview_scroll(
            -1 * (e.delta // 120), "units"))
        self._img_ref = None

    # ── PANEL METADATOS ───────────────────────────────────────────────────────
    def _build_meta_panel(self, parent):
        f = tk.Frame(parent, bg=C["panel"])
        f.grid(row=0, column=3, sticky="nsew", padx=(4, 8), pady=8)
        f.columnconfigure(0, weight=1)

        # Header con botón colapsar
        hdr = tk.Frame(f, bg=C["panel"]); hdr.grid(row=0, column=0, sticky="ew", padx=6, pady=(6,2))
        tk.Label(hdr, text="Info", fg=C["accent"], bg=C["panel"],
                 font=("Segoe UI", 9, "bold")).pack(side="left")
        self._meta_visible = True
        self._btn_colapsar_meta = _mk_btn(hdr, "▶", self._toggle_meta,
                bg=C["border"], fg=C["text2"], font=("Segoe UI", 8),
                tooltip="Colapsar panel de metadatos")
        self._btn_colapsar_meta.pack(side="right")

        # Cuerpo colapsable
        self._meta_body = tk.Frame(f, bg=C["panel"]); self._meta_body.grid(
            row=1, column=0, sticky="nsew")
        f.rowconfigure(1, weight=1)
        mb = self._meta_body; mb.columnconfigure(0, weight=1)

        # Campos compactos
        self._meta_vars = {}
        fields = [("Nombre","nombre"),("Tipo","tipo"),("Fecha","fecha"),("Ruta","ruta")]
        for i, (lbl, key) in enumerate(fields):
            tk.Label(mb, text=lbl, bg=C["panel"], fg=C["text2"],
                     font=("Segoe UI", 7, "bold"), anchor="w").grid(
                row=i*2, column=0, sticky="w", padx=8, pady=(4,0))
            var = tk.StringVar()
            self._meta_vars[key] = var
            tk.Label(mb, textvariable=var, bg=C["panel"], fg=C["text"],
                     font=("Segoe UI", 8), wraplength=160,
                     justify="left", anchor="w").grid(
                row=i*2+1, column=0, sticky="w", padx=8, pady=(0,2))

        # Indicador de tipo con color
        self._tipo_badge = tk.Label(mb, text="", bg=C["panel"],
                                     font=("Segoe UI", 9, "bold"))
        self._tipo_badge.grid(row=0, column=0, sticky="e", padx=8)

        tk.Frame(mb, bg=C["border"], height=1).grid(row=9, column=0, sticky="ew", pady=6, padx=8)

        # Etiquetas del doc
        self.frame_etqs_meta = tk.Frame(mb, bg=C["panel"])
        self.frame_etqs_meta.grid(row=10, column=0, sticky="w", padx=8, pady=2)

        tk.Frame(mb, bg=C["border"], height=1).grid(row=11, column=0, sticky="ew", pady=4, padx=8)

        # Botones de acción compactos
        bf = tk.Frame(mb, bg=C["panel"]); bf.grid(row=12, column=0, sticky="ew", padx=6)
        _mk_btn(bf, "↗ Abrir", self._abrir_original,
                bg=C["accent2"], fg=C["text"], font=C["font_sm"]).pack(fill="x", pady=1)
        _mk_btn(bf, "📁 Ubicación", self._abrir_carpeta,
                bg=C["border"], fg=C["text"], font=C["font_sm"]).pack(fill="x", pady=1)
        _mk_btn(bf, "⭐ Etiquetas", self._abrir_etiquetas,
                bg=C["border"], fg=C["text"], font=C["font_sm"]).pack(fill="x", pady=1)

        tk.Frame(mb, bg=C["border"], height=1).grid(row=13, column=0, sticky="ew", pady=4, padx=8)
        self._index_info_var = tk.StringVar(value="Sin índice.")
        tk.Label(mb, textvariable=self._index_info_var, bg=C["panel"], fg=C["text2"],
                 font=("Segoe UI", 7), wraplength=160, justify="left").grid(
            row=14, column=0, sticky="w", padx=8)

    def _toggle_meta(self):
        if self._meta_visible:
            self._meta_body.grid_remove()
            self._btn_colapsar_meta.config(text="◀")
            self._meta_visible = False
        else:
            self._meta_body.grid()
            self._btn_colapsar_meta.config(text="▶")
            self._meta_visible = True
    # ── PLACEHOLDER ───────────────────────────────────────────────────────────
    def _ph_clear(self, e=None):
        if self._placeholder_on:
            self._entry.delete(0, "end")
            self._entry.configure(fg=C["text"])
            self._placeholder_on = False

    def _ph_restore(self, e=None):
        if not self._search_var.get():
            self._entry.insert(0, self._PH)
            self._entry.configure(fg=C["text2"])
            self._placeholder_on = True

    # ── INICIO ────────────────────────────────────────────────────────────────
    def _inicio(self):
        ultima = cfg_global_get("ultima_carpeta", "")
        if ultima and os.path.isdir(ultima):
            self._cargar_carpeta(ultima)
        else:
            self._pedir_carpeta()

    def _pedir_carpeta(self):
        c = filedialog.askdirectory(title="Seleccionar carpeta de documentos")
        if c: self._cargar_carpeta(c)

    def _cargar_carpeta(self, carpeta):
        self._carpeta = carpeta
        cfg_global_set("ultima_carpeta", carpeta)
        nombre_corto = os.path.basename(carpeta) or carpeta
        self.lbl_carpeta_hdr.config(text=f"📂 {nombre_corto}")

        tiene, ultima_idx = hay_indice(carpeta)
        if tiene:
            self.lbl_badge.config(text="● ÍNDICE ACTIVO", fg=C["green"] if "green" in C else C["accent"])
            self.lbl_ultima.config(text=f"Indexado: {ultima_idx}")
            self._poblar_arbol_check(carpeta)
            self._recargar_etiquetas()
            self._cargar_historial()
            self.lbl_status.config(text="Listo — escriba una palabra para buscar.", fg=C["text2"])
            n = self._contar_documentos()
            self._index_info_var.set(
                f"Documentos: {n:,}\nCarpeta:\n{carpeta[:40]}…"
                if len(carpeta) > 40 else f"Documentos: {n:,}\nCarpeta: {carpeta}")
            threading.Thread(target=self._verificar_nuevos_bg, daemon=True).start()
        else:
            self.lbl_badge.config(text="● SIN ÍNDICE", fg=C["danger"])
            self.lbl_status.config(text="Esta carpeta no tiene índice. Iniciando indexación…",
                                   fg=C["warn"] if "warn" in C else C["accent"])
            self.after(600, lambda: IndexProgressDialog(
                self, carpeta, on_done=lambda: self._cargar_carpeta(carpeta)))

    def _contar_documentos(self):
        if not self._carpeta: return 0
        try:
            con = get_db(self._carpeta)
            n = con.execute("SELECT COUNT(*) FROM documentos").fetchone()[0]
            con.close(); return n
        except: return 0

    def _verificar_nuevos_bg(self):
        try:
            nuevos, modificados = verificar_nuevos(self._carpeta)
            if nuevos > 0 or modificados > 0:
                self.after(0, lambda: self._avisar_nuevos(nuevos, modificados))
        except: pass

    def _avisar_nuevos(self, nuevos, modificados):
        partes = []
        if nuevos:      partes.append(f"{nuevos} nuevo(s)")
        if modificados: partes.append(f"{modificados} modificado(s)")
        msg = " y ".join(partes)
        self.lbl_status.config(
            text=f"⚠  Archivos: {msg} — pulse 'Actualizar índice' para indexarlos.",
            fg=C["warn"] if "warn" in C else C["accent"])
        self.lbl_badge.config(text="● ÍNDICE DESACTUALIZADO",
                               fg=C["warn"] if "warn" in C else C["accent"])

    def _actualizar_idx(self):
        if not self._carpeta:
            c = filedialog.askdirectory(title="Seleccionar carpeta de documentos")
            if c: self._cargar_carpeta(c)
            return
        IndexProgressDialog(self, self._carpeta,
                            on_done=lambda: self._cargar_carpeta(self._carpeta))

    def _abrir_config(self):
        if not self._carpeta: return
        menu = tk.Menu(self, tearoff=0, bg=C["panel"], fg=C["text"],
                       activebackground=C["sel"], activeforeground=C["accent"],
                       font=C["font_sm"], relief="flat", bd=0)
        menu.add_command(label="📂  Cambiar carpeta", command=self._pedir_carpeta)
        menu.add_command(label="📂  Abrir carpeta del índice",
                          command=lambda: os.startfile(self._carpeta) if os.path.isdir(self._carpeta) else None)
        menu.add_separator()
        menu.add_command(label="🗑  Reconstruir índice desde cero", command=self._reconstruir_indice)
        try: menu.tk_popup(self.winfo_pointerx(), self.winfo_pointery())
        finally: menu.grab_release()

    def _reconstruir_indice(self):
        if not messagebox.askyesno("Reconstruir índice",
            f"Esto eliminará el índice en:\n{db_path_para(self._carpeta)}\n\n"
            "¿Está seguro?"):
            return
        try:
            reconstruir_indice(self._carpeta)
            messagebox.showinfo("Índice eliminado", "El índice fue eliminado. Se re-indexará ahora.")
            IndexProgressDialog(self, self._carpeta,
                                on_done=lambda: self._cargar_carpeta(self._carpeta))
        except Exception as e:
            messagebox.showerror("Error", str(e))

    # ── CARPETAS SUPERFICIALES (solo nivel 1, sin recursion) ────────────────
    def _poblar_arbol_check(self, raiz):
        """Lee SOLO las subcarpetas directas (nivel 1). Rapido y estable."""
        self._carpetas_check = {}
        self._tree_carpetas.delete(*self._tree_carpetas.get_children())
        if not raiz or not os.path.isdir(raiz): return

        nombre_raiz = os.path.basename(raiz) or raiz
        self._tree_carpetas.insert("", "end", iid="__root__",
                                   text=f"\u2302  {nombre_raiz}  (todo)",
                                   tags=("root",))
        try:
            subs = sorted(
                d for d in os.listdir(raiz)
                if os.path.isdir(os.path.join(raiz, d)) and not d.startswith(".")
            )
        except OSError:
            subs = []

        for d in subs:
            ruta_sub = os.path.join(raiz, d)
            self._carpetas_check[ruta_sub] = True
            self._tree_carpetas.insert("", "end", iid=ruta_sub,
                                       text=f"\u2611  {d}",
                                       tags=("checked",))

    def _on_click_carpeta(self, event):
        item = self._tree_carpetas.identify_row(event.y)
        if not item or item == "__root__": return
        if item not in self._carpetas_check: return
        marcada = not self._carpetas_check[item]
        self._carpetas_check[item] = marcada
        nombre = os.path.basename(item)
        if marcada:
            self._tree_carpetas.item(item, text=f"\u2611  {nombre}", tags=("checked",))
        else:
            self._tree_carpetas.item(item, text=f"\u2610  {nombre}", tags=("unchecked",))
        self._buscar_desde_cero()

    def _marcar_todos_arbol(self):
        for ruta in self._carpetas_check:
            self._carpetas_check[ruta] = True
            self._tree_carpetas.item(ruta, text=f"\u2611  {os.path.basename(ruta)}", tags=("checked",))
        self._buscar_desde_cero()

    def _desmarcar_todos_arbol(self):
        for ruta in self._carpetas_check:
            self._carpetas_check[ruta] = False
            self._tree_carpetas.item(ruta, text=f"\u2610  {os.path.basename(ruta)}", tags=("unchecked",))
        self._buscar_desde_cero()

    def _get_carpetas_activas(self):
        if not self._carpetas_check: return None
        marcadas = [r for r, v in self._carpetas_check.items() if v]
        if len(marcadas) == len(self._carpetas_check): return None
        return marcadas if marcadas else []

    # ── BÚSQUEDA ─────────────────────────────────────────────────────────────
    def _get_filtros(self):
        tipos = []
        if self.var_pdf.get():  tipos.append("pdf")
        if self.var_docx.get(): tipos.append("docx")

        # filtro de extensión del topbar
        ext = self._ext_var.get().strip()
        if ext:
            tipos = [ext.lstrip(".").lower()]

        # Rango de años (avanzado tiene prioridad sobre campo simple)
        anios = None
        desde = getattr(self, "_adv_desde", None)
        hasta = getattr(self, "_adv_hasta", None)
        year  = self._year_var.get().strip()
        try:
            d = int(desde.get()) if desde and desde.get().strip() else None
            h = int(hasta.get()) if hasta and hasta.get().strip() else None
            if d and h:   anios = list(range(d, h + 1))
            elif d:       anios = list(range(d, datetime.now().year + 1))
            elif h:       anios = list(range(1990, h + 1))
            elif year and year.isdigit(): anios = [int(year)]
        except: pass

        solo_fav   = self.var_solo_fav.get()
        etq_nombre = self.var_etq.get()
        etq_id     = self._etq_map.get(etq_nombre) if hasattr(self, "_etq_map") else None
        carpetas   = self._get_carpetas_activas()

        return tipos if tipos else None, anios, carpetas, solo_fav, etq_id

    def _ordenar_resultados(self, resultados):
        """Aplica el orden seleccionado en el panel avanzado."""
        orden = getattr(self, "_adv_orden", None)
        if not orden: return resultados
        v = orden.get()
        if v == "Fecha ↓":       return sorted(resultados, key=lambda r: r["fecha"], reverse=True)
        if v == "Fecha ↑":       return sorted(resultados, key=lambda r: r["fecha"])
        if v == "Nombre A-Z":    return sorted(resultados, key=lambda r: r["nombre"].lower())
        if v == "Nombre Z-A":    return sorted(resultados, key=lambda r: r["nombre"].lower(), reverse=True)
        if v == "Tamaño ↓":      return sorted(resultados, key=lambda r: r["tam"], reverse=True)
        return resultados  # Relevancia: sin cambios

    def _buscar_desde_cero(self):
        self._offset = 0; self._ejecutar_busqueda()

    def _ejecutar_busqueda(self):
        if not self._carpeta: return
        if self._placeholder_on: return
        q = self._search_var.get().strip()
        if not q: return
        if not os.path.exists(db_path_para(self._carpeta)):
            messagebox.showwarning("Sin índice",
                "Esta carpeta no tiene índice.\nUse 'Actualizar índice' para crearlo.")
            return

        # Número de secuencia — descarta resultados de búsquedas obsoletas
        self._busqueda_seq = getattr(self, "_busqueda_seq", 0) + 1
        mi_seq  = self._busqueda_seq
        tipos, anios, carpetas, solo_fav, etq_id = self._get_filtros()
        offset  = self._offset

        # Mostrar overlay ANTES de lanzar el hilo — UI responde siempre
        self._mostrar_overlay(q)

        def _hilo():
            t0 = time.time()
            try:
                res, total = buscar(self._carpeta, q, tipos, anios, carpetas,
                                     solo_fav, etq_id, offset, PAGE_SIZE)
                dt = time.time() - t0
                self._cola_doc.put(("busqueda_ok", mi_seq, q, res, total, dt, offset))
            except Exception as e:
                self._cola_doc.put(("busqueda_error", mi_seq, str(e)))

        threading.Thread(target=_hilo, daemon=True).start()

    # ── OVERLAY DE BÚSQUEDA ───────────────────────────────────────────────────
    def _mostrar_overlay(self, q):
        if not hasattr(self, "_overlay"): return
        self._overlay.grid(row=0, column=1, columnspan=2, sticky="nsew")
        self._overlay.lift()
        self._overlay_visible = True
        self._overlay_lbl.config(text="⏳  Buscando…")
        self._overlay_sub.config(text=f'« {q} »')
        self._animar_overlay(0)

    def _animar_overlay(self, tick):
        if not getattr(self, "_overlay_visible", False): return
        self._overlay_lbl.config(text=["⏳  Buscando…", "⌛  Buscando…"][tick % 2])
        self._overlay_anim_id = self.after(450, self._animar_overlay, tick + 1)

    def _ocultar_overlay(self):
        if not hasattr(self, "_overlay"): return
        self._overlay_visible = False
        try:
            if hasattr(self, "_overlay_anim_id"): self.after_cancel(self._overlay_anim_id)
        except: pass
        self._overlay.grid_remove()

    def _on_busqueda_ok(self, seq, q, res, total, dt, offset):
        self._ocultar_overlay()
        if seq != getattr(self, "_busqueda_seq", 0): return  # descarta obsoleta

        res = self._ordenar_resultados(res)
        if getattr(self, "_adv_solo_nombre", None) and self._adv_solo_nombre.get():
            q_lower = q.lower()
            terms   = [t.strip().lower() for t in re.split(r'[ +,]', q_lower) if t.strip()]
            res = [r for r in res if all(t in r["nombre"].lower() for t in terms)]
            total = len(res)

        self._resultados   = res
        self._total_res    = total
        self._query_actual = q
        self._terminos     = terminos_resaltar(q)
        self._mostrar_resultados(res)

        pag_actual = offset // PAGE_SIZE + 1
        pags_total = max(1, -(-total // PAGE_SIZE))
        sufijo     = f"  ·  {dt*1000:.0f} ms"
        if total == 0:
            self.lbl_pag.config(text=f"Sin resultados{sufijo}")
        else:
            self.lbl_pag.config(text=f"Pág {pag_actual}/{pags_total}{sufijo}")
        self.btn_ant.config(state="normal" if offset > 0 else "disabled")
        self.btn_sig.config(state="normal" if (offset + PAGE_SIZE) < total else "disabled")
        self._cargar_historial()

    def _debounce(self, _=None):
        if self._placeholder_on: return
        if self._debounce_id: self.after_cancel(self._debounce_id)
        if len(self._search_var.get().strip()) >= 3:
            self._debounce_id = self.after(400, self._buscar_desde_cero)

    def _pag_ant(self):
        if self._offset >= PAGE_SIZE:
            self._offset -= PAGE_SIZE; self._ejecutar_busqueda()

    def _pag_sig(self):
        if (self._offset + PAGE_SIZE) < self._total_res:
            self._offset += PAGE_SIZE; self._ejecutar_busqueda()

    def _mostrar_resultados(self, rs):
        self._listbox.delete(0, "end")
        self._limpiar_doc()
        self.btn_abrir.config(state="disabled")
        # Colores por tipo configurados en el Listbox
        self._listbox_tipos = []  # índice → tipo para colorear
        for r in rs:
            fav  = "⭐ " if r["favorito"] else ""
            tipo = r["tipo"]
            if tipo == "PDF":
                ico = "▐"   # marcador rojo
            elif tipo == "DOCX":
                ico = "▐"   # marcador azul
            else:
                ico = " "
            etq = f" [{', '.join(e[0] for e in r['etiquetas'])}]" if r["etiquetas"] else ""
            self._listbox.insert("end", f" {ico} {fav}{r['nombre']}{etq}")
            self._listbox_tipos.append(tipo)

        # Colorear por tipo con itemconfig
        for i, tipo in enumerate(self._listbox_tipos):
            if tipo == "PDF":
                self._listbox.itemconfig(i, fg="#FF7B72")    # rojo suave
            elif tipo == "DOCX":
                self._listbox.itemconfig(i, fg="#79C0FF")    # azul suave
            else:
                self._listbox.itemconfig(i, fg=C["text"])

        total = self._total_res
        self._results_lbl.config(text=f"Resultados ({total:,})" if total else "Resultados")

    # ── SELECCIÓN ─────────────────────────────────────────────────────────────
    def _on_list_select(self, event=None):
        sel = self._listbox.curselection()
        if not sel or sel[0] >= len(self._resultados): return
        r = self._resultados[sel[0]]
        self._ruta_sel = r["ruta"]; self._id_sel = r["id"]

        # Incrementar seq para cancelar renders obsoletos de selecciones anteriores
        self._preview_seq = getattr(self, "_preview_seq", 0) + 1
        mi_seq = self._preview_seq

        self.btn_abrir.config(state="normal")
        self._preview_lbl.config(text=r["nombre"])
        self.lbl_doc_meta.config(
            text=f"Tipo: {r['tipo']}  ·  Tamaño: {tamanio_fmt(r['tam'])}  ·  Modificado: {r['fecha']}")

        # Actualizar metadatos
        self._meta_vars["nombre"].set(r["nombre"])
        self._meta_vars["tipo"].set(r["tipo"] + f"  {tamanio_fmt(r['tam'])}")
        self._meta_vars["fecha"].set(r["fecha"])
        self._meta_vars["ruta"].set(r["ruta"])
        # Badge de tipo con color
        if r["tipo"] == "PDF":
            self._tipo_badge.config(text="● PDF", fg="#FF7B72")
        elif r["tipo"] == "DOCX":
            self._tipo_badge.config(text="● DOCX", fg="#79C0FF")
        else:
            self._tipo_badge.config(text=f"● {r['tipo']}", fg=C["text2"])

        # Etiquetas en panel meta
        for w in self.frame_etqs_meta.winfo_children(): w.destroy()
        for nombre_etq, color_etq in r["etiquetas"]:
            tk.Label(self.frame_etqs_meta, text=f"● {nombre_etq}",
                     fg=color_etq, bg=C["panel"], font=C["font_sm"]).pack(anchor="w")
        if not r["etiquetas"]:
            tk.Label(self.frame_etqs_meta, text="—", fg=C["text2"],
                     bg=C["panel"], font=C["font_sm"]).pack(anchor="w")

        # Cargar texto — siempre en hilo para no bloquear UI
        doc_id = r["id"]
        self.txt.config(state="normal"); self.txt.delete("1.0", "end")
        self.txt.config(state="disabled")
        threading.Thread(target=self._cargar_doc_hilo,
                         args=(doc_id, r["ruta"], self._terminos, mi_seq),
                         daemon=True).start()

        # Vista real: siempre actualizar al cambiar de documento
        if self._modo_vista == "render":
            self.canvas_doc.delete("all")
            self.canvas_doc.create_text(10, 10, anchor="nw",
                text="⏳  Cargando…", fill=C["text2"], font=C["font_sm"])
            threading.Thread(target=self._cargar_render_bg, args=(r["ruta"], mi_seq),
                             daemon=True).start()
        elif self._modo_vista == "docx_nativo":
            threading.Thread(target=self._cargar_docx_nativo_hilo,
                             args=(r["ruta"], self._terminos, mi_seq),
                             daemon=True).start()

    # Límite de caracteres mostrados en preview (suficiente para leer, rápido de renderizar)
    _PREVIEW_CHARS = 40_000

    def _cargar_doc_hilo(self, doc_id, ruta, terminos, seq):
        # 1) Caché en memoria (instantáneo)
        if doc_id in self._cache_doc:
            self._cola_doc.put(("texto", doc_id, self._cache_doc[doc_id], terminos, seq))
            return
        # 2) Leer desde DB (milisegundos, sin tocar disco)
        texto = texto_desde_db(self._carpeta, doc_id)
        if texto is None:
            # 3) Fallback: leer archivo físico (solo si no está en índice)
            texto = extraer(ruta)
        # Limitar para renderizado rápido
        if texto and len(texto) > self._PREVIEW_CHARS:
            texto = texto[:self._PREVIEW_CHARS] + "\n\n[… texto truncado para preview …]"
        self._cache_doc[doc_id] = texto
        self._cola_doc.put(("texto", doc_id, texto, terminos, seq))

    def _poll_doc(self):
        try:
            while True:
                msg = self._cola_doc.get_nowait(); t = msg[0]
                if t == "texto":
                    _, doc_id, texto, terminos, seq = msg
                    # Ignorar si el usuario ya seleccionó otro documento
                    if seq == getattr(self, "_preview_seq", 0):
                        self._renderizar_texto(texto, terminos)
                elif t == "render_listo":
                    _, doc, ruta, seq = msg
                    if seq == getattr(self, "_preview_seq", 0):
                        self._pdf_doc = doc; self._pdf_page_idx = 0
                        self._mostrar_pagina_render()
                elif t == "docx_nativo":
                    _, ruta, seq = msg
                    if seq == getattr(self, "_preview_seq", 0):
                        self._modo_vista = "docx_nativo"
                        self.frame_render.pack_forget()
                        self.frame_pag_nav.pack_forget()
                        self.frame_texto.grid(row=0, column=0, sticky="nsew")
                        render_docx_en_widget(ruta, self.txt, self._terminos)
                elif t == "render_error":
                    _, err, seq = msg
                    if seq == getattr(self, "_preview_seq", 0):
                        self.canvas_doc.delete("all")
                        self.canvas_doc.create_text(20, 20, anchor="nw",
                                                    text=f"⚠  {err}",
                                                    fill=C["warn"] if "warn" in C else C["accent"],
                                                    font=C["font"])
                elif t == "busqueda_ok":
                    _, seq, q, res, total, dt, offset = msg
                    self._on_busqueda_ok(seq, q, res, total, dt, offset)
                elif t == "busqueda_error":
                    _, seq, err = msg
                    self._ocultar_overlay()
                    messagebox.showerror("Error en búsqueda", err)
        except queue.Empty: pass
        self.after(80, self._poll_doc)   # 80ms — más ágil, sin costo perceptible

    # ── VISTA TEXTO / RENDER ──────────────────────────────────────────────────
    def _ver_texto(self):
        self._modo_vista = "texto"
        self.frame_render.pack_forget()
        self.frame_pag_nav.pack_forget()
        self.frame_texto.grid(row=0, column=0, sticky="nsew")
        self.btn_vista_txt.config(bg=C["accent"], fg="#1a1a1a")
        self.btn_vista_doc.config(bg=C["border"], fg=C["accent2"])

    def _ver_doc_renderizado(self):
        if not self._ruta_sel: return
        ext = Path(self._ruta_sel).suffix.lower()
        if ext == ".pdf" and not PDF_OK:
            messagebox.showwarning("No disponible",
                "La vista renderizada de PDF requiere PyMuPDF.\nReinicie la aplicación."); return
        if ext == ".docx" and not _encontrar_libreoffice():
            messagebox.showinfo("LibreOffice no encontrado",
                "Para ver DOCX renderizado se necesita LibreOffice instalado.\n"
                "https://www.libreoffice.org\n\nUse la vista de texto mientras tanto."); return

        self._modo_vista = "render"
        self.frame_texto.grid_forget()
        self.frame_render.pack(fill="both", expand=True)
        self.frame_pag_nav.pack(fill="x")
        self.btn_vista_doc.config(bg=C["accent"], fg="#1a1a1a")
        self.btn_vista_txt.config(bg=C["border"], fg=C["accent"])

        self.canvas_doc.delete("all")
        self.canvas_doc.create_text(20, 20, anchor="nw",
                                     text="⏳  Renderizando documento…",
                                     fill=C["text2"], font=("Segoe UI", 11, "italic"))
        threading.Thread(target=self._cargar_render_bg,
                         args=(self._ruta_sel,), daemon=True).start()

    def _cargar_render_bg(self, ruta, seq=None):
        try:
            ext = Path(ruta).suffix.lower()
            if ext == ".pdf":
                doc = fitz.open(ruta)
                self._cola_doc.put(("render_listo", doc, ruta, seq))
            elif ext == ".docx":
                # Intentar LibreOffice primero; si no, señalizar para render nativo
                lo = _encontrar_libreoffice()
                if lo:
                    pdf_tmp = docx_a_pdf_temporal(ruta)
                    if pdf_tmp:
                        doc = fitz.open(pdf_tmp)
                        self._cola_doc.put(("render_listo", doc, ruta, seq))
                        return
                # Sin LibreOffice → usar render nativo docx
                self._cola_doc.put(("docx_nativo", ruta, seq))
        except Exception as e:
            self._cola_doc.put(("render_error", str(e), seq))

    def _cargar_docx_nativo_hilo(self, ruta, terminos, seq=None):
        """Señaliza al hilo principal para que renderice el DOCX nativamente."""
        self._cola_doc.put(("docx_nativo", ruta, seq))

    def _mostrar_pagina_render(self):
        if not self._pdf_doc: return
        try:
            page = self._pdf_doc[self._pdf_page_idx]
            ancho_canvas = max(600, self.canvas_doc.winfo_width())
            rect   = page.rect
            escala = ancho_canvas / rect.width if rect.width > 0 else 1.5
            escala = max(1.0, min(escala, 2.5))
            mat    = fitz.Matrix(escala, escala)
            pix    = page.get_pixmap(matrix=mat, alpha=False)
            img    = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            self._img_ref = ImageTk.PhotoImage(img)
            self.canvas_doc.delete("all")
            self.canvas_doc.config(scrollregion=(0, 0, pix.width, pix.height))
            self.canvas_doc.create_image(0, 0, anchor="nw", image=self._img_ref)
            total = len(self._pdf_doc)
            self.lbl_pag_doc.config(text=f"Página {self._pdf_page_idx + 1} de {total}")
        except Exception as e:
            self.canvas_doc.create_text(20, 20, anchor="nw",
                                         text=f"Error al renderizar: {e}", fill=C["danger"], font=C["font"])

    def _pag_doc_ant(self):
        if self._pdf_doc and self._pdf_page_idx > 0:
            self._pdf_page_idx -= 1; self._mostrar_pagina_render()

    def _pag_doc_sig(self):
        if self._pdf_doc and self._pdf_page_idx < len(self._pdf_doc) - 1:
            self._pdf_page_idx += 1; self._mostrar_pagina_render()

    # ── RENDERIZADO DE TEXTO ──────────────────────────────────────────────────
    def _renderizar_texto(self, texto, terminos):
        self.txt.config(state="normal"); self.txt.delete("1.0", "end")
        if not texto:
            self.txt.insert("end",
                "⚠  No se pudo leer el contenido de este archivo.\n\n"
                "Intente abrirlo directamente con el botón 'Abrir'.", "cargando")
            self.txt.config(state="disabled"); self.lbl_hits.config(text=""); return

        limpios = [t for t in terminos if t and len(t) > 1]
        if not limpios:
            self.txt.insert("end", texto, "normal"); self.txt.config(state="disabled")
            self.lbl_hits.config(text=""); return

        patron = re.compile("(" + "|".join(re.escape(t) for t in limpios) + ")", re.IGNORECASE)
        partes = patron.split(texto)
        # Insertar en bloque único (más rápido que muchos inserts pequeños)
        self.txt.config(state="normal")
        for i, parte in enumerate(partes):
            if parte:
                self.txt.insert("end", parte, "resaltado" if i % 2 == 1 else "normal")
        self.txt.config(state="disabled")

        # Contar hits con search (rápido, solo busca posiciones)
        self._hits_pos = []; idx = "1.0"
        while True:
            idx = self.txt.search(patron.pattern, idx, stopindex="end", regexp=True, nocase=True)
            if not idx: break
            self._hits_pos.append(idx)
            line, col = idx.split(".")
            idx = f"{line}.{int(col)+1}"
            if len(self._hits_pos) > 500: break   # cap: evitar lag con miles de hits

        n = len(self._hits_pos); self._hit_actual = 0
        self.lbl_hits.config(
            text=f"{n}{'+ ' if n >= 500 else ' '}coincidencia(s)" if n else "Sin coincidencias",
            fg=C["accent"] if n else C["text2"])
        if self._hits_pos: self._saltar_hit(0)

    def _saltar_hit(self, idx):
        if not self._hits_pos: return
        idx = max(0, min(idx, len(self._hits_pos) - 1)); self._hit_actual = idx
        self.txt.tag_remove("activo", "1.0", "end")
        pos = self._hits_pos[idx]; line, col = pos.split(".")
        patron = re.compile("(" + "|".join(re.escape(t) for t in self._terminos if t) + ")", re.IGNORECASE)
        m = patron.match(self.txt.get(pos, f"{line}.{int(col)+100}"))
        fin = int(col) + (len(m.group(0)) if m else 1)
        self.txt.tag_add("activo", pos, f"{line}.{fin}")
        self.txt.see(pos); self.txt.mark_set("insert", pos)
        self.lbl_hits.config(text=f"{idx+1} / {len(self._hits_pos)} coincidencia(s)", fg=C["accent"])

    def _hit_next(self):
        if self._hits_pos: self._saltar_hit((self._hit_actual+1) % len(self._hits_pos))

    def _hit_prev(self):
        if self._hits_pos: self._saltar_hit((self._hit_actual-1) % len(self._hits_pos))

    def _limpiar_doc(self):
        self.txt.config(state="normal"); self.txt.delete("1.0", "end")
        self.txt.config(state="disabled")
        self._preview_lbl.config(text="— Seleccione un resultado —")
        self.lbl_doc_meta.config(text=""); self.lbl_hits.config(text="")
        self._hits_pos = []; self._hit_actual = 0
        self._pdf_doc  = None; self._pdf_page_idx = 0
        for key in self._meta_vars: self._meta_vars[key].set("")
        if hasattr(self, "_tipo_badge"): self._tipo_badge.config(text="")
        for w in self.frame_etqs_meta.winfo_children(): w.destroy()

    def _limpiar(self):
        self._search_var.set("")
        if not self._placeholder_on:
            self._ph_restore()
        self._listbox.delete(0, "end")
        self._limpiar_doc()
        self._results_lbl.config(text="Resultados")
        self.lbl_pag.config(text="")
        self.lbl_status.config(text="Listo — escriba una palabra para buscar.", fg=C["text2"])
        self._resultados = []; self._cache_doc = {}
        self._total_res = 0; self._offset = 0
        self.btn_abrir.config(state="disabled")
        self._year_var.set(""); self._ext_var.set("")

    def _limpiar_filtros(self):
        self.var_solo_fav.set(False); self.var_etq.set("— Todas —")
        self._marcar_todos_arbol()
        self._year_var.set(""); self._ext_var.set("")

    # ── HISTORIAL Y ETIQUETAS ─────────────────────────────────────────────────
    def _cargar_historial(self):
        if not self._carpeta: return
        try:
            con  = get_db(self._carpeta)
            rows = con.execute(
                "SELECT query,fecha,results FROM historial ORDER BY id DESC LIMIT 20"
            ).fetchall(); con.close()
            self.tree_hist.delete(*self.tree_hist.get_children())
            for r in rows:
                self.tree_hist.insert("", "end", text=f"{r['query']}  ({r['results']})",
                                       values=(r["query"],))
        except: pass

    def _repetir_hist(self, _=None):
        sel = self.tree_hist.selection()
        if not sel: return
        vals = self.tree_hist.item(sel[0], "values")
        if vals:
            self._ph_clear()
            self._search_var.set(vals[0])
            self._buscar_desde_cero()

    def _recargar_etiquetas(self):
        if not self._carpeta: return
        try:
            con  = get_db(self._carpeta)
            etqs = con.execute("SELECT id,nombre FROM etiquetas ORDER BY nombre").fetchall()
            con.close()
            vals = ["— Todas —"] + [e["nombre"] for e in etqs]
            self.cb_etq["values"] = vals
            self._etq_map = {"— Todas —": None} | {e["nombre"]: e["id"] for e in etqs}
        except: self._etq_map = {"— Todas —": None}

    def _insertar_op(self, sym):
        self._ph_clear()
        pos = self._entry.index(tk.INSERT)
        if sym == '"…"': self._entry.insert(pos, '""'); self._entry.icursor(pos+1)
        else: self._entry.insert(pos, f" {sym} ")
        self._entry.focus_set()

    # ── MENÚ CONTEXTUAL ───────────────────────────────────────────────────────
    def _menu_ctx_list(self, event):
        idx = self._listbox.nearest(event.y)
        if idx < 0 or idx >= len(self._resultados): return
        self._listbox.selection_clear(0, "end")
        self._listbox.selection_set(idx)
        self._on_list_select()
        r = self._resultados[idx]
        menu = tk.Menu(self, tearoff=0, bg=C["panel"], fg=C["text"],
                       activebackground=C["sel"], activeforeground=C["accent"],
                       font=C["font_sm"], relief="flat", bd=0)
        menu.add_command(label="↗  Abrir documento",   command=self._abrir_original)
        menu.add_command(label="📋  Copiar ruta",         command=self._copiar_ruta)
        menu.add_separator()
        fav_lbl = "★  Quitar de favoritos" if r["favorito"] else "⭐  Marcar como favorito"
        menu.add_command(label=fav_lbl, command=lambda: self._toggle_fav(r["id"]))
        menu.add_command(label="🏷  Gestionar etiquetas",  command=self._abrir_etiquetas)
        menu.add_separator()
        menu.add_command(label="📂  Abrir carpeta",
                          command=lambda: os.startfile(os.path.dirname(r["ruta"])))
        try: menu.tk_popup(event.x_root, event.y_root)
        finally: menu.grab_release()

    def _toggle_fav(self, doc_id):
        if not self._carpeta: return
        con = get_db(self._carpeta)
        existe = con.execute("SELECT 1 FROM favoritos WHERE documento_id=?", (doc_id,)).fetchone()
        if existe: con.execute("DELETE FROM favoritos WHERE documento_id=?", (doc_id,))
        else:
            con.execute("INSERT OR IGNORE INTO favoritos(documento_id,fecha) VALUES(?,?)",
                        (doc_id, datetime.now().strftime("%d/%m/%Y")))
        con.commit(); con.close(); self._buscar_desde_cero()

    def _abrir_etiquetas(self):
        if not self._id_sel or not self._carpeta: return
        r = next((x for x in self._resultados if x["id"] == self._id_sel), None)
        nombre = r["nombre"] if r else os.path.basename(self._ruta_sel or "")
        WinEtiquetas(self, self._carpeta, self._id_sel, nombre,
                     callback=lambda: (self._recargar_etiquetas(), self._buscar_desde_cero()))

    def _abrir_original(self):
        if self._ruta_sel:
            try: os.startfile(self._ruta_sel)
            except Exception as e: messagebox.showerror("Error al abrir", str(e))

    def _abrir_carpeta(self):
        if self._ruta_sel:
            try: os.startfile(os.path.dirname(self._ruta_sel))
            except Exception as e: messagebox.showerror("Error", str(e))

    def _copiar_ruta(self):
        if self._ruta_sel:
            self.clipboard_clear(); self.clipboard_append(self._ruta_sel)
            self.lbl_status.config(text=f"✓ Ruta copiada: {self._ruta_sel}", fg=C["accent"])


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    try:
        App().mainloop()
    except Exception:
        import traceback
        err = traceback.format_exc()
        LOG = os.path.join(os.path.expanduser("~"), "Desktop", "jurisbot_error.txt")
        try:
            with open(LOG, "w", encoding="utf-8") as f: f.write(err)
        except: pass
        try:
            root = tk.Tk(); root.withdraw()
            messagebox.showerror("Error al iniciar",
                f"Ocurrió un error inesperado.\nDetalle guardado en:\n{LOG}\n\n{err[-600:]}")
        except: pass
        sys.exit(1)
