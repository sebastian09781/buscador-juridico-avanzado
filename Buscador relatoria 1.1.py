"""
JURISBOT RELATORÍA v5.0
Motor: SQLite FTS5 · ThreadPoolExecutor
Vista instantánea: texto leído desde DB (sin tocar el archivo físico)
Índice guardado en la carpeta analizada como Jurisbot_Relatoria.db
Diseñado para operadores judiciales colombianos
"""
# ══════════════════════════════════════════════════════════════════════════════
#  BOOTSTRAP
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

try:    import fitz;                       PDF_OK  = True
except: PDF_OK  = False

try:    from docx import Document as DocxDoc; DOCX_OK = True
except: DOCX_OK = False

try:    from PIL import Image, ImageTk;    PIL_OK  = True
except: PIL_OK  = False

# ══════════════════════════════════════════════════════════════════════════════
#  PALETA
# ══════════════════════════════════════════════════════════════════════════════
BG      = "#1c2333"
BG2     = "#161b27"
BG3     = "#212d3b"
BG4     = "#0d1117"
BG5     = "#1a2235"
BGSEL   = "#1f3a5c"
GOLD    = "#e6b450"
GOLD2   = "#f0c96a"
HL_BG   = "#7d5a00"
HL_FG   = "#ffffff"
HL_ACT  = "#ff6b35"
GREEN   = "#3fb950"
RED     = "#f85149"
BLUE    = "#58a6ff"
CYAN    = "#39c5cf"
TEXT    = "#cdd9e5"
TEXT2   = "#768390"
BORDER  = "#2d3748"
SEP     = "#2a3444"
WARN    = "#e6b450"
FU      = "Segoe UI"
FM      = "Courier New"
FS      = "Georgia"

# ══════════════════════════════════════════════════════════════════════════════
#  CONSTANTES GLOBALES
# ══════════════════════════════════════════════════════════════════════════════
DB_FILENAME  = "Jurisbot_Relatoria.db"   # nombre del índice en la carpeta analizada
EXTS         = {".pdf", ".docx"}          # sin .txt
NUM_WORKERS  = max(2, (os.cpu_count() or 2))
PAGE_SIZE    = 50

# Ruta de configuración global (guarda qué carpeta fue la última usada)
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
#  CONFIG GLOBAL (última carpeta usada)
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
#  DB — la base de datos vive DENTRO de la carpeta analizada
# ══════════════════════════════════════════════════════════════════════════════
def db_path_para(carpeta):
    """Devuelve la ruta del .db dentro de la carpeta analizada."""
    return os.path.join(carpeta, DB_FILENAME)

def get_db(carpeta):
    path = db_path_para(carpeta)
    con  = sqlite3.connect(path, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA cache_size=-32000")
    con.execute("PRAGMA temp_store=MEMORY")
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

def db_cfg_get(con, k, default=None):
    r = con.execute("SELECT valor FROM configuracion WHERE clave=?", (k,)).fetchone()
    return r["valor"] if r else default

def db_cfg_set(con, k, v):
    con.execute("INSERT OR REPLACE INTO configuracion(clave,valor) VALUES(?,?)", (k, v))
    con.commit()

def hay_indice(carpeta):
    """True si existe el .db en la carpeta y tiene documentos indexados."""
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
    """
    Compara hashes de archivos en disco con los del índice.
    Retorna (nuevos, modificados) como conteo.
    Rápido porque solo verifica st_size + mtime sin leer contenido.
    """
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
            if ruta not in existentes:
                nuevos += 1
            elif existentes[ruta] != h:
                modificados += 1
    return nuevos, modificados

def _hash_rapido(ruta):
    """Hash rápido: tamaño + mtime (no lee el archivo, solo metadatos)."""
    try:
        s = os.stat(ruta)
        return f"{s.st_size}_{s.st_mtime:.0f}"
    except: return ""

def reconstruir_indice(carpeta):
    """Elimina el .db de la carpeta para forzar re-indexación total."""
    p = db_path_para(carpeta)
    if os.path.exists(p):
        os.remove(p)

# ══════════════════════════════════════════════════════════════════════════════
#  EXTRACCIÓN DE TEXTO
# ══════════════════════════════════════════════════════════════════════════════
def extraer_pdf(ruta):
    try:
        doc   = fitz.open(ruta)
        texto = "\n".join(p.get_text("text") for p in doc)
        doc.close()
        return texto
    except: return ""

def extraer_docx(ruta):
    try:
        doc   = DocxDoc(ruta)
        partes = [p.text for p in doc.paragraphs if p.text.strip()]
        for t in doc.tables:
            for row in t.rows:
                for c in row.cells:
                    if c.text.strip(): partes.append(c.text)
        return "\n".join(partes)
    except: return ""

def extraer(ruta):
    ext = Path(ruta).suffix.lower()
    if ext == ".pdf":  return extraer_pdf(ruta)
    if ext == ".docx": return extraer_docx(ruta)
    return ""

def texto_desde_db(carpeta, doc_id):
    """
    Lee el texto completo desde fts_contenido en la DB.
    Instantáneo — no toca el disco del archivo original.
    Fallback: retorna None si no está en DB (forzará lectura del archivo).
    """
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
    """Hash MD5 de primeros 8KB + tamaño para detección de cambios real."""
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
    """
    Recorre la carpeta, detecta archivos nuevos/modificados usando hash_rapido,
    los procesa en paralelo con ThreadPoolExecutor y los inserta en FTS5.
    NO hace contar_archivos() previo — descubre el total mientras indexa.
    """
    con = init_db(carpeta)

    # Recolectar lista de archivos (rápido, solo metadatos)
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

    # Cache de hashes existentes (hash rápido para comparar)
    existentes = {}
    for r in con.execute("SELECT id, ruta, hash FROM documentos"):
        existentes[r["ruta"]] = {"id": r["id"], "hash": r["hash"]}

    # Clasificar sin leer archivos (solo metadatos del SO)
    pendientes = []
    omitidos   = 0
    for ruta in lista:
        h_rapido = _hash_rapido(ruta)
        info     = existentes.get(ruta)
        if info and info["hash"] and info["hash"] == hash_completo(ruta):
            omitidos += 1
        else:
            pendientes.append(ruta)

    cola.put(("resumen", len(pendientes), omitidos, total))

    if not pendientes:
        cfg_set_local(con, carpeta)
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

    # Optimizar FTS5
    cola.put(("optimizando",))
    try:
        con.execute("INSERT INTO fts_contenido(fts_contenido) VALUES('optimize')")
        con.commit()
    except: pass

    cfg_set_local(con, carpeta)
    con.close()
    cola.put(("fin", nuevos, actualizados, omitidos))

def cfg_set_local(con, carpeta):
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

    # historial
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
#  RENDERIZADO DOCX COMO IMAGEN (usando LibreOffice o python-docx2pdf fallback)
#  Estrategia: convertir a PDF con LibreOffice silencioso, luego renderizar con fitz
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

_docx_pdf_cache = {}  # ruta_docx → ruta_pdf_temporal

def docx_a_pdf_temporal(ruta_docx):
    """
    Convierte el DOCX a PDF usando LibreOffice en background.
    Cachea el resultado. Devuelve ruta al PDF temporal o None.
    """
    if ruta_docx in _docx_pdf_cache:
        pdf = _docx_pdf_cache[ruta_docx]
        if os.path.exists(pdf): return pdf

    lo = _encontrar_libreoffice()
    if not lo: return None

    import tempfile
    tmpdir = tempfile.mkdtemp(prefix="jurisbot_")
    try:
        r = subprocess.run(
            [lo, "--headless", "--convert-to", "pdf",
             "--outdir", tmpdir, ruta_docx],
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
def _btn(p, txt, cmd, bg=GOLD, fg=BG, font=None, tooltip=None, **kw):
    b = tk.Button(p, text=txt, command=cmd, bg=bg, fg=fg,
                  font=font or (FU, 9, "bold"), relief="flat", bd=0,
                  cursor="hand2", activebackground=GOLD2, activeforeground=BG,
                  padx=10, pady=5, **kw)
    if tooltip: _Tooltip(b, tooltip)
    return b

def _sep(p, color=SEP, pady=0):
    f = tk.Frame(p, bg=color, height=1); f.pack(fill="x", pady=pady); return f

def _entry(p, var, width=20, tooltip=None, **kw):
    e = tk.Entry(p, textvariable=var, font=(FM, 9), bg=BG3, fg=TEXT,
                 insertbackground=GOLD, relief="flat", bd=0,
                 highlightthickness=1, highlightcolor=GOLD,
                 highlightbackground=BORDER, width=width, **kw)
    if tooltip: _Tooltip(e, tooltip)
    return e

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
        tw.wm_overrideredirect(True); tw.wm_geometry(f"+{x}+{y}"); tw.configure(bg=BG2)
        tk.Frame(tw, bg=GOLD, height=1).pack(fill="x")
        tk.Label(tw, text=self.text, justify="left", bg=BG2, fg=TEXT,
                 font=(FU, 8), padx=10, pady=6, wraplength=340).pack()
        tk.Frame(tw, bg=GOLD, height=1).pack(fill="x")

    def _hide(self, _=None):
        if self.tip: self.tip.destroy(); self.tip = None

# ══════════════════════════════════════════════════════════════════════════════
#  VENTANA — BIENVENIDA / SELECCIÓN DE CARPETA
# ══════════════════════════════════════════════════════════════════════════════
class WinBienvenida(tk.Toplevel):
    """
    Se muestra al iniciar si no hay carpeta configurada o si el índice no existe.
    Permite seleccionar carpeta. Si ya hay índice en esa carpeta, lo usa directo.
    """
    def __init__(self, parent, callback):
        super().__init__(parent)
        self.title("JurisBot — Seleccionar carpeta")
        self.geometry("580x380"); self.configure(bg=BG2)
        self.resizable(False, False); self.grab_set()
        self.callback = callback
        self._ui()

    def _ui(self):
        tk.Frame(self, bg=GOLD, height=4).pack(fill="x")
        tk.Label(self, text="⚖  JURISBOT RELATORÍA", fg=GOLD, bg=BG2,
                 font=(FS, 16, "bold")).pack(pady=(24, 4))
        tk.Label(self, text="Sistema de búsqueda de documentos judiciales",
                 fg=TEXT2, bg=BG2, font=(FU, 10)).pack()
        _sep(self, pady=16)

        body = tk.Frame(self, bg=BG2, padx=40); body.pack(fill="x")
        tk.Label(body,
                 text="Para comenzar, seleccione la carpeta donde están guardados\n"
                      "sus documentos (sentencias, autos, providencias, etc.).",
                 fg=TEXT, bg=BG2, font=(FU, 10), justify="center").pack(pady=(0, 16))

        # Última carpeta usada
        ultima = cfg_global_get("ultima_carpeta", "")
        if ultima and os.path.isdir(ultima):
            fult = tk.Frame(body, bg=BG3, padx=12, pady=10); fult.pack(fill="x", pady=(0, 12))
            tk.Label(fult, text="Última carpeta usada:", fg=TEXT2, bg=BG3,
                     font=(FU, 8)).pack(anchor="w")
            tk.Label(fult, text=ultima, fg=GOLD, bg=BG3,
                     font=(FM, 8), wraplength=460, anchor="w").pack(anchor="w")
            _btn(fult, "▶  Usar esta carpeta", lambda: self._usar(ultima),
                 tooltip="Cargar el índice existente en esta carpeta y comenzar a buscar").pack(
                anchor="e", pady=(6, 0))

        _btn(body, "📂  Seleccionar otra carpeta…", self._seleccionar, bg=BG3, fg=CYAN,
             font=(FU, 10),
             tooltip="Buscar una carpeta diferente en el explorador de archivos").pack(fill="x")

        _sep(self, pady=12)
        tk.Label(self, text="Si la carpeta ya tiene un índice (Jurisbot_Relatoria.db),\n"
                            "se cargará automáticamente sin necesidad de re-indexar.",
                 fg=TEXT2, bg=BG2, font=(FU, 8), justify="center").pack()

    def _seleccionar(self):
        c = filedialog.askdirectory(title="Seleccionar carpeta de documentos", parent=self)
        if c: self._usar(c)

    def _usar(self, carpeta):
        self.destroy()
        self.callback(carpeta)

# ══════════════════════════════════════════════════════════════════════════════
#  VENTANA — INDEXACIÓN
# ══════════════════════════════════════════════════════════════════════════════
class WinIndexar(tk.Toplevel):
    def __init__(self, parent, carpeta, callback=None, solo_nuevos=False):
        super().__init__(parent)
        self.title("Indexando documentos — JurisBot")
        self.geometry("700x500"); self.configure(bg=BG2)
        self.resizable(False, False); self.grab_set()
        self.carpeta     = carpeta
        self.callback    = callback
        self.solo_nuevos = solo_nuevos
        self._cola       = queue.Queue()
        self._stop       = threading.Event()
        self._total      = 0
        self._pendientes = 0
        self._t_ini      = None
        self._proc_prev  = 0
        self._t_vel      = time.time()
        self._pulso_id   = None
        self._ui()
        self.after(300, self._iniciar)

    def _ui(self):
        tk.Frame(self, bg=GOLD, height=4).pack(fill="x")
        tk.Label(self, text="INDEXANDO DOCUMENTOS", fg=GOLD, bg=BG2,
                 font=(FS, 13, "bold")).pack(pady=(14, 2))
        tk.Label(self, text=f"Carpeta:  {self.carpeta}",
                 fg=TEXT2, bg=BG2, font=(FM, 8), wraplength=660).pack()
        _sep(self, pady=8)

        # ── Contador grande ──────────────────────────────────────────────────
        self.frame_contador = tk.Frame(self, bg=BG2); self.frame_contador.pack(pady=6)

        self.lbl_num = tk.Label(self.frame_contador, text="0",
                                 fg=GOLD, bg=BG2, font=(FS, 48, "bold"))
        self.lbl_num.pack()
        self.lbl_de  = tk.Label(self.frame_contador, text="de 0 archivos",
                                 fg=TEXT2, bg=BG2, font=(FU, 12))
        self.lbl_de.pack()

        # Velocidad + ETA en la misma fila
        fve = tk.Frame(self, bg=BG2); fve.pack()
        self.lbl_vel = tk.Label(fve, text="", fg=CYAN, bg=BG2, font=(FU, 11, "bold"))
        self.lbl_vel.pack(side="left", padx=16)
        self.lbl_eta = tk.Label(fve, text="", fg=GOLD, bg=BG2, font=(FU, 11))
        self.lbl_eta.pack(side="left", padx=16)

        _sep(self, pady=8)

        # ── Barra de progreso ─────────────────────────────────────────────────
        fp = tk.Frame(self, bg=BG2, padx=30); fp.pack(fill="x")
        self.pb = ttk.Progressbar(fp, mode="determinate", length=630)
        self.pb.pack(pady=4)

        # ── Archivo actual (animado) ──────────────────────────────────────────
        self.lbl_archivo = tk.Label(fp, text="Preparando…", fg=TEXT2, bg=BG2,
                                     font=(FM, 8)); self.lbl_archivo.pack(anchor="w")

        # ── Estado general ────────────────────────────────────────────────────
        self.lbl_estado = tk.Label(self, text="", fg=GREEN, bg=BG2,
                                    font=(FU, 9, "bold")); self.lbl_estado.pack(pady=4)

        # ── Últimos archivos procesados ───────────────────────────────────────
        fl = tk.Frame(self, bg=BG2, padx=30); fl.pack(fill="x")
        tk.Label(fl, text="Últimos procesados:", fg=TEXT2, bg=BG2,
                 font=(FU, 8)).pack(anchor="w")
        self.lista_recientes = tk.Text(fl, height=4, bg=BG3, fg=TEXT2,
                                        font=(FM, 8), state="disabled",
                                        relief="flat", bd=0, highlightthickness=0)
        self.lista_recientes.pack(fill="x", pady=2)
        self._recientes = []

        _sep(self, pady=8)
        fb = tk.Frame(self, bg=BG2); fb.pack(pady=6)
        self.btn_det = _btn(fb, "■  Detener", self._detener, bg=BG3, fg=RED,
                             tooltip="Detiene la indexación. Los archivos ya procesados se conservan.")
        self.btn_det.pack(side="left", padx=6)
        self.btn_cerrar = _btn(fb, "Cerrar", self.destroy, bg=BG3, fg=TEXT2)
        self.btn_cerrar.pack(side="left", padx=6)
        self.btn_cerrar.config(state="disabled")

        self.after(120, self._poll)

    def _iniciar(self):
        self._t_ini    = time.time()
        self._stop     = threading.Event()
        self._cola     = queue.Queue()
        self._proc_prev = 0
        self._t_vel    = time.time()
        self._pulso_ini()
        threading.Thread(target=indexar, args=(self.carpeta, self._cola, self._stop),
                         daemon=True).start()

    def _pulso_ini(self):
        """Animación de 'latido' en el número para mostrar que está activo."""
        def _pulsar():
            colores = [GOLD, GOLD2, GOLD, "#c09030", GOLD]
            for c in colores:
                try: self.lbl_num.config(fg=c)
                except: return
                time.sleep(0.12)
        self._pulso_id = threading.Thread(target=_pulsar, daemon=True)
        self._pulso_id.start()

    def _detener(self):
        self._stop.set()
        self.lbl_estado.config(text="Deteniendo… espere.", fg=WARN)
        self.btn_det.config(state="disabled")

    def _agregar_reciente(self, nombre):
        self._recientes.append(nombre)
        if len(self._recientes) > 4: self._recientes.pop(0)
        self.lista_recientes.config(state="normal")
        self.lista_recientes.delete("1.0", "end")
        for n in reversed(self._recientes):
            self.lista_recientes.insert("end", f"  ✓  {n}\n")
        self.lista_recientes.config(state="disabled")

    def _poll(self):
        try:
            while True:
                msg = self._cola.get_nowait(); t = msg[0]

                if t == "total":
                    self._total = msg[1]
                    self.pb.config(maximum=max(1, self._total))

                elif t == "resumen":
                    _, pend, omit, total = msg
                    self._pendientes = pend
                    self._total      = total
                    self.pb.config(maximum=max(1, pend))
                    self.lbl_de.config(
                        text=f"de {pend:,} archivos a procesar  ({omit:,} sin cambios)")
                    self.lbl_estado.config(
                        text=f"Total en carpeta: {total:,}  ·  Procesando: {pend:,}  ·  Sin cambios: {omit:,}",
                        fg=CYAN)

                elif t == "prog":
                    _, proc, pend, omit, total, nombre = msg
                    self.pb["value"] = proc
                    self.lbl_num.config(text=f"{proc:,}", fg=GOLD)
                    self.lbl_archivo.config(text=f"  ⚙  {nombre[:80]}")
                    self._agregar_reciente(nombre)

                    # Velocidad y ETA
                    ahora = time.time()
                    dt    = ahora - self._t_vel
                    if dt >= 1.0:
                        vel = (proc - self._proc_prev) / dt
                        self._proc_prev = proc; self._t_vel = ahora
                        if vel > 0:
                            self.lbl_vel.config(text=f"⚡ {vel:.1f} archivos/seg")
                            restantes = max(0, pend - proc)
                            eta_s = int(restantes / vel)
                            m, s  = divmod(eta_s, 60)
                            h, m  = divmod(m, 60)
                            if h:   eta_txt = f"Tiempo restante: {h}h {m}m"
                            elif m: eta_txt = f"Tiempo restante: {m}m {s}s"
                            else:   eta_txt = f"Tiempo restante: {s}s"
                            self.lbl_eta.config(text=eta_txt)

                elif t == "optimizando":
                    self.lbl_archivo.config(text="  🔧  Optimizando índice FTS5…")
                    self.lbl_estado.config(text="Casi listo, optimizando…", fg=WARN)

                elif t == "fin":
                    _, nv, ac, om = msg
                    elapsed = time.time() - (self._t_ini or time.time())
                    self.pb["value"] = self._pendientes
                    m, s = divmod(int(elapsed), 60)
                    tiempo_txt = f"{m}m {s}s" if m else f"{s}s"
                    self.lbl_num.config(text=f"{nv+ac:,}", fg=GREEN)
                    self.lbl_estado.config(
                        text=f"✓  Completado en {tiempo_txt}  —  "
                             f"{nv} nuevos · {ac} actualizados · {om} sin cambios",
                        fg=GREEN)
                    self.lbl_archivo.config(text="")
                    self.lbl_eta.config(text="")
                    self.btn_det.config(state="disabled")
                    self.btn_cerrar.config(state="normal")
                    if self.callback: self.callback()

        except queue.Empty: pass
        self.after(100, self._poll)

# ══════════════════════════════════════════════════════════════════════════════
#  VENTANA — CONFIGURACIÓN
# ══════════════════════════════════════════════════════════════════════════════
class WinConfig(tk.Toplevel):
    def __init__(self, parent, carpeta_actual, callback=None):
        super().__init__(parent)
        self.title("Configuración — JurisBot")
        self.geometry("680x440"); self.configure(bg=BG2)
        self.resizable(False, False); self.grab_set()
        self.carpeta_actual = carpeta_actual
        self.callback       = callback
        self._ui()

    def _ui(self):
        tk.Frame(self, bg=GOLD, height=3).pack(fill="x")
        tk.Label(self, text="⚙  CONFIGURACIÓN", fg=GOLD, bg=BG2,
                 font=(FS, 13, "bold")).pack(pady=(14, 2))
        tk.Label(self, text=f"Motor FTS5 · {NUM_WORKERS} hilos de procesamiento",
                 fg=GREEN, bg=BG2, font=(FU, 8)).pack()
        _sep(self, pady=10)

        body = tk.Frame(self, bg=BG2, padx=30); body.pack(fill="x")

        # Info índice actual
        tk.Label(body, text="ÍNDICE ACTIVO", fg=GOLD, bg=BG2,
                 font=(FU, 9, "bold")).pack(anchor="w", pady=(0, 4))
        db_p    = db_path_para(self.carpeta_actual) if self.carpeta_actual else "—"
        db_info = f"{db_p}"
        if os.path.exists(db_p):
            db_info += f"   ({tamanio_fmt(os.path.getsize(db_p))})"
        tk.Label(body, text=db_info, fg=TEXT2, bg=BG2,
                 font=(FM, 8), wraplength=600).pack(anchor="w")

        fr = tk.Frame(body, bg=BG2); fr.pack(anchor="w", pady=6)
        _btn(fr, "📂  Abrir carpeta del índice", self._abrir_carpeta_idx,
             bg=BG3, fg=TEXT2, font=(FU, 8),
             tooltip="Abre en el Explorador la carpeta donde está guardado el índice").pack(side="left", padx=(0, 8))
        _btn(fr, "🔄  Cambiar carpeta de documentos", self._cambiar_carpeta,
             bg=BG3, fg=CYAN, font=(FU, 8),
             tooltip="Selecciona una carpeta diferente como origen de los documentos").pack(side="left")

        _sep(body, pady=10)

        # Reconstruir
        tk.Label(body, text="MANTENIMIENTO DEL ÍNDICE", fg=GOLD, bg=BG2,
                 font=(FU, 9, "bold")).pack(anchor="w", pady=(0, 4))
        tk.Label(body,
                 text="Si los resultados de búsqueda parecen incorrectos o el índice está dañado,\n"
                      "puede eliminarlo y reconstruirlo desde cero.",
                 fg=TEXT2, bg=BG2, font=(FU, 8)).pack(anchor="w")

        frec = tk.Frame(body, bg=BG2); frec.pack(anchor="w", pady=8)
        _btn(frec, "🗑  Reconstruir índice desde cero", self._reconstruir,
             bg="#3a0a0a", fg=RED, font=(FU, 9, "bold"),
             tooltip="ELIMINA el índice actual y lo reconstruye completamente.\n"
                     "Deberá esperar a que se re-indexen todos los documentos.\n"
                     "Use esto solo si hay problemas con los resultados.").pack(side="left")

        _sep(self, pady=(12, 0))
        fb = tk.Frame(self, bg=BG2); fb.pack(pady=10)
        _btn(fb, "Cerrar", self.destroy, bg=BG3, fg=TEXT2, font=(FU, 9)).pack()

    def _abrir_carpeta_idx(self):
        carpeta = self.carpeta_actual or ""
        if carpeta and os.path.isdir(carpeta):
            try: os.startfile(carpeta)
            except Exception as e: messagebox.showerror("Error", str(e), parent=self)
        else:
            messagebox.showinfo("Sin carpeta", "No hay una carpeta activa configurada.", parent=self)

    def _cambiar_carpeta(self):
        c = filedialog.askdirectory(title="Seleccionar nueva carpeta de documentos", parent=self)
        if c:
            self.destroy()
            if self.callback: self.callback("cambiar_carpeta", c)

    def _reconstruir(self):
        if not self.carpeta_actual:
            messagebox.showwarning("Sin carpeta", "No hay carpeta activa.", parent=self); return
        if not messagebox.askyesno("⚠  Reconstruir índice",
            f"Esto ELIMINARÁ completamente el índice:\n{db_path_para(self.carpeta_actual)}\n\n"
            "Tendrá que esperar a que se re-indexen TODOS los documentos desde cero.\n\n"
            "¿Está seguro de que desea continuar?", parent=self): return
        try:
            reconstruir_indice(self.carpeta_actual)
            messagebox.showinfo("Índice eliminado",
                "El índice fue eliminado correctamente.\n\n"
                "La aplicación iniciará la re-indexación automáticamente.", parent=self)
            self.destroy()
            if self.callback: self.callback("reconstruir", self.carpeta_actual)
        except Exception as e:
            messagebox.showerror("Error", str(e), parent=self)

# ══════════════════════════════════════════════════════════════════════════════
#  VENTANA — GUÍA RÁPIDA
# ══════════════════════════════════════════════════════════════════════════════
class WinGuia(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("¿Cómo buscar? — Guía rápida")
        self.geometry("620x560"); self.configure(bg=BG2)
        self.resizable(False, False); self._ui()

    def _ui(self):
        tk.Frame(self, bg=GOLD, height=3).pack(fill="x")
        tk.Label(self, text="¿CÓMO BUSCAR?", fg=GOLD, bg=BG2,
                 font=(FS, 14, "bold")).pack(pady=(16, 2))
        tk.Label(self, text="Use estos símbolos para hacer búsquedas más precisas:",
                 fg=TEXT2, bg=BG2, font=(FU, 9)).pack()
        _sep(self, pady=8)

        sf = tk.Frame(self, bg=BG2, padx=24); sf.pack(fill="both", expand=True)
        FILAS = [
            ("Búsqueda simple",           "contrato laboral",
             "Escribe las palabras y el sistema busca documentos que contengan TODAS.", GREEN),
            ("Símbolo  +  (Y también)",   "contrato + laboral + 2023",
             "Busca documentos que tengan TODAS las palabras que escribas.", GOLD),
            ("Símbolo  -  (Excluir)",     "contrato - civil",
             "Busca 'contrato' pero EXCLUYE los que también digan 'civil'.", RED),
            ('Comillas  "…"  (Frase exacta)', '"acción de tutela"',
             "Busca esa frase EXACTAMENTE como está escrita, en ese mismo orden.", BLUE),
            ("Coma  ,  (O cualquiera)",   "tutela, acción popular, nulidad",
             "Busca documentos que tengan CUALQUIERA de esas palabras.", CYAN),
            ("Asterisco  *  (Comodín)",   "contrat*",
             "Busca palabras que EMPIECEN así: contrato, contratar, contratación…", "#bc8cff"),
        ]
        for titulo, ejemplo, desc, color in FILAS:
            fila = tk.Frame(sf, bg=BG3, padx=14, pady=8); fila.pack(fill="x", pady=2)
            tk.Label(fila, text=titulo, fg=color, bg=BG3,
                     font=(FU, 9, "bold"), anchor="w").pack(anchor="w")
            tk.Label(fila, text=f"  Ejemplo:  {ejemplo}", fg=GOLD, bg=BG3,
                     font=(FM, 9), anchor="w").pack(anchor="w")
            tk.Label(fila, text=f"  {desc}", fg=TEXT2, bg=BG3,
                     font=(FU, 8), anchor="w", wraplength=540).pack(anchor="w")

        _sep(self, pady=8)
        _btn(self, "Entendido, cerrar", self.destroy).pack(pady=6)

# ══════════════════════════════════════════════════════════════════════════════
#  VENTANA — ETIQUETAS
# ══════════════════════════════════════════════════════════════════════════════
COLORES_ETQ = ["#e6b450","#3fb950","#58a6ff","#f85149","#bc8cff",
                "#39c5cf","#ff9580","#ffa500","#ff69b4","#90ee90"]

class WinEtiquetas(tk.Toplevel):
    def __init__(self, parent, carpeta, doc_id, doc_nombre, callback=None):
        super().__init__(parent)
        self.title("Etiquetas y favorito — JurisBot")
        self.geometry("420x500"); self.configure(bg=BG2)
        self.resizable(False, False); self.grab_set()
        self.carpeta    = carpeta
        self.doc_id     = doc_id
        self.doc_nombre = doc_nombre
        self.callback   = callback
        self._ci        = 0
        self._ui(); self._cargar()

    def _ui(self):
        tk.Frame(self, bg=GOLD, height=3).pack(fill="x")
        tk.Label(self, text="ETIQUETAS Y FAVORITO", fg=GOLD, bg=BG2,
                 font=(FS, 11, "bold")).pack(pady=(12, 2))
        tk.Label(self, text=self.doc_nombre[:60], fg=TEXT2, bg=BG2,
                 font=(FU, 8), wraplength=380).pack()
        _sep(self, pady=8)

        ff = tk.Frame(self, bg=BG2, padx=20); ff.pack(fill="x", pady=4)
        self.var_fav = tk.BooleanVar()
        cb = tk.Checkbutton(ff, text="  ⭐  Marcar como favorito",
                             variable=self.var_fav, fg=GOLD, bg=BG2,
                             selectcolor=BG3, activebackground=BG2,
                             font=(FU, 10, "bold"))
        cb.pack(side="left")
        _Tooltip(cb, "Los favoritos se pueden filtrar desde el panel lateral")
        _sep(self, pady=6)

        tk.Label(self, text="Etiquetas asignadas:", fg=TEXT2, bg=BG2,
                 font=(FU, 9, "bold")).pack(anchor="w", padx=20, pady=(4, 2))
        self.frame_etqs = tk.Frame(self, bg=BG2, padx=20); self.frame_etqs.pack(fill="x")

        _sep(self, pady=8)
        tk.Label(self, text="Crear nueva etiqueta:", fg=TEXT2, bg=BG2,
                 font=(FU, 9, "bold")).pack(anchor="w", padx=20)
        fn = tk.Frame(self, bg=BG2, padx=20); fn.pack(fill="x", pady=4)
        self.var_nueva = tk.StringVar()
        _entry(fn, self.var_nueva, width=22,
               tooltip="Nombre de la nueva etiqueta, ej: 'Urgente', 'Revisado'").pack(
            side="left", ipady=4, padx=(0, 6))
        self.btn_color = tk.Button(fn, bg=COLORES_ETQ[0], width=3, relief="flat",
                                    cursor="hand2", command=self._ciclar)
        self.btn_color.pack(side="left", padx=4)
        _Tooltip(self.btn_color, "Clic para cambiar el color")
        _btn(fn, "+ Crear", self._crear, bg=BG3, fg=GOLD, font=(FU, 9),
             tooltip="Crea la etiqueta y la asigna a este documento").pack(side="left", padx=4)

        _sep(self, pady=8)
        fb = tk.Frame(self, bg=BG2); fb.pack(pady=6)
        _btn(fb, "✓  Guardar", self._guardar).pack(side="left", padx=6)
        _btn(fb, "Cancelar", self.destroy, bg=BG3, fg=TEXT2).pack(side="left", padx=6)

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
            rf = tk.Frame(self.frame_etqs, bg=BG2); rf.pack(anchor="w", pady=1)
            tk.Label(rf, text="●", fg=e["color"], bg=BG2, font=(FU, 10)).pack(side="left")
            tk.Checkbutton(rf, text=f"  {e['nombre']}", variable=var,
                            fg=TEXT, bg=BG2, selectcolor=BG3,
                            activebackground=BG2, font=(FU, 9)).pack(side="left")

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
#  APP PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("JurisBot Relatoría")
        self.geometry("1440x880"); self.minsize(1100, 640)
        self.configure(bg=BG)

        self._carpeta        = None   # carpeta activa
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
        self._adv_visible    = False
        self._pdf_doc        = None
        self._pdf_page_idx   = 0
        self._modo_vista     = "texto"
        self._carpetas_check = {}   # ruta → BooleanVar (árbol con checkboxes)

        self._estilos()
        self._ui()
        self.after(300, self._inicio)
        self.after(200, self._poll_doc)

    # ── ESTILOS ───────────────────────────────────────────────────────────────
    def _estilos(self):
        s = ttk.Style(self); s.theme_use("clam")
        s.configure("Treeview", background=BG3, foreground=TEXT,
                    fieldbackground=BG3, rowheight=26, font=(FU, 9), borderwidth=0)
        s.configure("Treeview.Heading", background=BG2, foreground=GOLD,
                    font=(FU, 8, "bold"), relief="flat", borderwidth=0)
        s.map("Treeview", background=[("selected", BGSEL)], foreground=[("selected", GOLD2)])
        for orient in ("Vertical", "Horizontal"):
            s.configure(f"{orient}.TScrollbar", background=BG3, troughcolor=BG2,
                        borderwidth=0, arrowcolor=TEXT2, relief="flat")
        s.configure("TProgressbar", troughcolor=BG3, background=GOLD, borderwidth=0)

    # ── UI PRINCIPAL ──────────────────────────────────────────────────────────
    def _ui(self):
        tk.Frame(self, bg=GOLD, height=4).pack(fill="x")

        # HEADER
        hdr = tk.Frame(self, bg=BG2); hdr.pack(fill="x")
        hi  = tk.Frame(hdr, bg=BG2, pady=8, padx=16); hi.pack(fill="x")
        tk.Label(hi, text="⚖  JURISBOT RELATORÍA", fg=GOLD, bg=BG2,
                 font=(FS, 13, "bold")).pack(side="left")
        self.lbl_carpeta_hdr = tk.Label(hi, text="", fg=TEXT2, bg=BG2,
                                         font=(FU, 8)); self.lbl_carpeta_hdr.pack(side="left", padx=10)

        _btn(hi, "⚙", self._abrir_config, bg=BG3, fg=GOLD, font=(FU, 11),
             tooltip="Configuración: cambiar carpeta, reconstruir índice").pack(side="right", padx=2)
        _btn(hi, "▶  Actualizar índice", self._actualizar_idx, bg=GOLD, fg=BG,
             font=(FU, 8, "bold"),
             tooltip="Indexa los archivos nuevos o modificados.\nLos existentes sin cambios se omiten.").pack(side="right", padx=4)

        self.lbl_badge  = tk.Label(hi, text="● CARGANDO", fg=WARN, bg=BG2,
                                    font=(FU, 8, "bold")); self.lbl_badge.pack(side="right", padx=10)
        self.lbl_ultima = tk.Label(hi, text="", fg=TEXT2, bg=BG2,
                                    font=(FU, 7)); self.lbl_ultima.pack(side="right", padx=4)
        _sep(hdr)

        # BARRA BÚSQUEDA
        brow = tk.Frame(self, bg=BG2, pady=8, padx=16); brow.pack(fill="x")
        self.var_q = tk.StringVar()
        self.entry = tk.Entry(brow, textvariable=self.var_q, font=(FM, 12), bg=BG3, fg=TEXT,
                              insertbackground=GOLD, relief="flat", bd=0,
                              highlightthickness=2, highlightcolor=GOLD,
                              highlightbackground=BORDER)
        self.entry.pack(side="left", fill="x", expand=True, ipady=8, padx=(0, 8))
        self.entry.bind("<Return>",    lambda e: self._buscar_desde_cero())
        self.entry.bind("<KeyRelease>",self._debounce)
        _Tooltip(self.entry,
                 "Escriba las palabras a buscar.\n\n"
                 "Ejemplos:\n"
                 "  contrato + laboral    →  ambas palabras\n"
                 "  tutela - penal        →  tutela pero NO penal\n"
                 '  "acción de tutela"   →  frase exacta\n'
                 "  tutela, nulidad       →  cualquiera\n"
                 "  contrat*              →  comodín\n\n"
                 "Pulse F1 para la guía completa.")
        _btn(brow, "BUSCAR", self._buscar_desde_cero, font=(FU, 10, "bold"),
             tooltip="Buscar en todos los documentos indexados").pack(side="left")
        _btn(brow, "✕", self._limpiar, bg=BG3, fg=TEXT2, font=(FU, 10),
             tooltip="Limpiar búsqueda y resultados").pack(side="left", padx=3)
        _btn(brow, "▼ Avanzado", self._toggle_avanzada, bg=BG3, fg=CYAN,
             font=(FU, 8),
             tooltip="Despliega filtros adicionales: frase exacta, excluir, fecha, tipo").pack(side="left", padx=3)
        _btn(brow, "?", lambda: WinGuia(self), bg=BG3, fg=GOLD, font=(FU, 10, "bold"),
             tooltip="Guía de búsqueda — pulse F1").pack(side="left", padx=2)

        # Panel avanzado (oculto)
        self.frame_avanzada = tk.Frame(self, bg=BG5, padx=16, pady=10)
        self._build_avanzada(self.frame_avanzada)

        # Chips operadores
        chips = tk.Frame(self, bg=BG2, padx=16, pady=4); chips.pack(fill="x")
        tk.Label(chips, text="Operadores rápidos:", fg=TEXT2, bg=BG2,
                 font=(FU, 8)).pack(side="left", padx=(0, 6))
        for sym, nombre, ej, desc in OPERADORES:
            f = tk.Frame(chips, bg=BG3, padx=8, pady=3, cursor="hand2"); f.pack(side="left", padx=2)
            tk.Label(f, text=sym,      fg=GOLD, bg=BG3, font=(FM, 9, "bold")).pack(side="left")
            tk.Label(f, text=f" {nombre}", fg=TEXT2, bg=BG3, font=(FU, 8)).pack(side="left")
            _Tooltip(f, f"{desc}\n\nEjemplo:  {ej}\n\nHaga clic para insertar en la búsqueda.")
            f.bind("<Button-1>", lambda e, s=sym: self._insertar_op(s))
            for w in f.winfo_children():
                w.bind("<Button-1>", lambda e, s=sym: self._insertar_op(s))
        _sep(self)

        # CUERPO
        body  = tk.Frame(self, bg=BG); body.pack(fill="both", expand=True)
        outer = tk.PanedWindow(body, orient="horizontal", bg=BORDER,
                                sashwidth=5, sashrelief="flat", showhandle=False)
        outer.pack(fill="both", expand=True)

        sidebar = tk.Frame(outer, bg=BG2, width=230); outer.add(sidebar, minsize=200)
        self._build_sidebar(sidebar)

        right = tk.Frame(outer, bg=BG); outer.add(right, minsize=640)
        inner = tk.PanedWindow(right, orient="vertical", bg=BORDER,
                                sashwidth=5, sashrelief="flat", showhandle=False)
        inner.pack(fill="both", expand=True)

        ptabla = tk.Frame(inner, bg=BG);  inner.add(ptabla, minsize=130)
        self._build_tabla(ptabla)

        pdoc = tk.Frame(inner, bg=BG4); inner.add(pdoc, minsize=220)
        self._build_doc(pdoc)

        # STATUS BAR
        status = tk.Frame(self, bg=BG2, pady=3, padx=12); status.pack(fill="x", side="bottom")
        tk.Frame(status, bg=GOLD, height=1).pack(fill="x", side="top")
        sf = tk.Frame(status, bg=BG2); sf.pack(fill="x")
        self.lbl_status = tk.Label(sf, text="Iniciando…", fg=TEXT2, bg=BG2,
                                    font=(FU, 8), anchor="w"); self.lbl_status.pack(side="left")
        tk.Label(sf, text=f"⚡ FTS5 · {NUM_WORKERS} hilos", fg=GREEN, bg=BG2,
                 font=(FU, 7, "bold")).pack(side="right", padx=8)

        self.bind("<F1>", lambda e: WinGuia(self))

    # ── PANEL AVANZADO ────────────────────────────────────────────────────────
    def _build_avanzada(self, p):
        tk.Label(p, text="BÚSQUEDA AVANZADA", fg=CYAN, bg=BG5,
                 font=(FU, 8, "bold")).grid(row=0, column=0, columnspan=8, sticky="w", pady=(0, 6))

        self.var_adv_contiene = tk.StringVar()
        self.var_adv_frase    = tk.StringVar()
        self.var_adv_excluye  = tk.StringVar()
        self.var_adv_desde    = tk.StringVar()
        self.var_adv_hasta    = tk.StringVar()
        self.var_adv_tipo     = tk.StringVar(value="Todos")

        campos = [
            ("Contiene las palabras:", self.var_adv_contiene,
             "Documentos que contengan TODAS estas palabras"),
            ("Frase exacta:", self.var_adv_frase,
             "Busca esta frase exactamente como está escrita"),
            ("Excluye palabras:", self.var_adv_excluye,
             "Excluye documentos que contengan estas palabras"),
        ]
        for i, (lbl, var, tip) in enumerate(campos):
            col = (i % 2) * 3
            row = 1 + i // 2
            tk.Label(p, text=lbl, fg=TEXT2, bg=BG5, font=(FU, 8)).grid(
                row=row, column=col, sticky="w", padx=(0, 4), pady=3)
            _entry(p, var, width=26, tooltip=tip).grid(
                row=row, column=col+1, sticky="w", ipady=3, padx=(0, 20))

        tk.Label(p, text="Tipo:", fg=TEXT2, bg=BG5, font=(FU, 8)).grid(
            row=3, column=0, sticky="w", pady=3)
        ttk.Combobox(p, textvariable=self.var_adv_tipo,
                     values=["Todos", "PDF", "DOCX"],
                     state="readonly", width=7, font=(FU, 9)).grid(
            row=3, column=1, sticky="w", padx=(0, 20))

        tk.Label(p, text="Año desde:", fg=TEXT2, bg=BG5, font=(FU, 8)).grid(
            row=3, column=3, sticky="w", pady=3)
        _entry(p, self.var_adv_desde, width=6,
               tooltip="Año de inicio, ej: 2020").grid(row=3, column=4, sticky="w", ipady=3, padx=(0,6))
        tk.Label(p, text="hasta:", fg=TEXT2, bg=BG5, font=(FU, 8)).grid(
            row=3, column=5, sticky="w")
        _entry(p, self.var_adv_hasta, width=6,
               tooltip="Año de fin, ej: 2024").grid(row=3, column=6, sticky="w", ipady=3)

        fb = tk.Frame(p, bg=BG5); fb.grid(row=4, column=0, columnspan=8, sticky="w", pady=(8, 0))
        _btn(fb, "🔍 Aplicar filtros", self._buscar_avanzada,
             tooltip="Ejecuta la búsqueda con todos los filtros").pack(side="left", padx=(0, 6))
        _btn(fb, "Limpiar filtros", self._limpiar_avanzada, bg=BG3, fg=TEXT2,
             font=(FU, 8), tooltip="Limpia todos los campos").pack(side="left")

    def _toggle_avanzada(self):
        if self._adv_visible:
            self.frame_avanzada.pack_forget(); self._adv_visible = False
        else:
            self.frame_avanzada.pack(fill="x", after=self.entry.master)
            self._adv_visible = True

    def _limpiar_avanzada(self):
        for v in [self.var_adv_contiene, self.var_adv_frase,
                  self.var_adv_excluye, self.var_adv_desde, self.var_adv_hasta]:
            v.set("")
        self.var_adv_tipo.set("Todos")

    def _buscar_avanzada(self):
        partes = []
        if self.var_adv_contiene.get().strip():
            partes.append(self.var_adv_contiene.get().strip())
        if self.var_adv_frase.get().strip():
            partes.append(f'"{self.var_adv_frase.get().strip()}"')
        if self.var_adv_excluye.get().strip():
            for w in self.var_adv_excluye.get().strip().split():
                partes.append(f"- {w}")
        if partes:
            self.var_q.set(" ".join(partes))
        self._buscar_desde_cero()

    # ── SIDEBAR ───────────────────────────────────────────────────────────────
    def _build_sidebar(self, p):
        tk.Label(p, text="Filtros", fg=GOLD, bg=BG2,
                 font=(FU, 9, "bold")).pack(anchor="w", padx=10, pady=(10, 4))
        _sep(p)

        # Tipo
        tk.Label(p, text="Tipo de documento", fg=TEXT2, bg=BG2,
                 font=(FU, 8, "bold")).pack(anchor="w", padx=10, pady=(8, 2))
        self.var_pdf  = tk.BooleanVar(value=True)
        self.var_docx = tk.BooleanVar(value=True)
        for var, txt, color, tip in [
            (self.var_pdf,  "PDF",  RED,  "Incluir archivos PDF"),
            (self.var_docx, "DOCX", BLUE, "Incluir archivos Word (.docx)"),
        ]:
            f = tk.Frame(p, bg=BG2); f.pack(anchor="w", padx=10)
            cb = tk.Checkbutton(f, text=f"  {txt}", variable=var, fg=color,
                                 bg=BG2, selectcolor=BG3, activebackground=BG2,
                                 font=(FU, 9), command=self._buscar_desde_cero)
            cb.pack(side="left"); _Tooltip(cb, tip)

        _sep(p, pady=4)

        # Favoritos
        self.var_solo_fav = tk.BooleanVar(value=False)
        ff = tk.Frame(p, bg=BG2, padx=10); ff.pack(fill="x")
        cb_fav = tk.Checkbutton(ff, text="  ⭐ Solo favoritos",
                                  variable=self.var_solo_fav, fg=GOLD, bg=BG2,
                                  selectcolor=BG3, activebackground=BG2, font=(FU, 9),
                                  command=self._buscar_desde_cero)
        cb_fav.pack(side="left")
        _Tooltip(cb_fav, "Muestra solo documentos marcados como favoritos")

        _sep(p, pady=4)

        # Etiquetas
        tk.Label(p, text="Filtrar por etiqueta", fg=TEXT2, bg=BG2,
                 font=(FU, 8, "bold")).pack(anchor="w", padx=10, pady=(4, 2))
        self.var_etq = tk.StringVar(value="— Todas —")
        self.cb_etq  = ttk.Combobox(p, textvariable=self.var_etq,
                                     state="readonly", font=(FU, 8), width=24)
        self.cb_etq.pack(padx=10, pady=2)
        self.cb_etq.bind("<<ComboboxSelected>>", lambda e: self._buscar_desde_cero())
        _Tooltip(self.cb_etq, "Filtra por etiqueta asignada")
        self._etq_map = {"— Todas —": None}

        _sep(p, pady=4)

        # Historial
        tk.Label(p, text="Búsquedas recientes", fg=TEXT2, bg=BG2,
                 font=(FU, 8, "bold")).pack(anchor="w", padx=10, pady=(4, 2))
        hf = tk.Frame(p, bg=BG2); hf.pack(fill="x", padx=4)
        self.tree_hist = ttk.Treeview(hf, show="tree", selectmode="browse", height=5)
        vsb_h = ttk.Scrollbar(hf, orient="vertical", command=self.tree_hist.yview)
        self.tree_hist.configure(yscrollcommand=vsb_h.set)
        vsb_h.pack(side="right", fill="y"); self.tree_hist.pack(fill="x")
        self.tree_hist.bind("<Double-1>", self._repetir_hist)
        _Tooltip(self.tree_hist, "Doble clic para repetir una búsqueda anterior")

        _sep(p, pady=4)

        # ── Árbol de carpetas con Checkboxes ─────────────────────────────────
        tk.Label(p, text="Carpetas (marcar para filtrar)", fg=TEXT2, bg=BG2,
                 font=(FU, 8, "bold")).pack(anchor="w", padx=10, pady=(4, 2))
        _Tooltip(
            tk.Label(p, text="ⓘ Marcar incluye subcarpetas", fg=TEXT2, bg=BG2,
                     font=(FU, 7), padx=10),
            "Al marcar una carpeta, todos sus subdirectorios\nquedan incluidos automáticamente en el filtro."
        )

        self.frame_arbol = tk.Frame(p, bg=BG2); self.frame_arbol.pack(fill="both", expand=True, padx=4)

        # Canvas con scroll para el árbol de checkboxes
        self.canvas_arbol = tk.Canvas(self.frame_arbol, bg=BG2, highlightthickness=0)
        vsb_arbol = ttk.Scrollbar(self.frame_arbol, orient="vertical",
                                   command=self.canvas_arbol.yview)
        self.canvas_arbol.configure(yscrollcommand=vsb_arbol.set)
        vsb_arbol.pack(side="right", fill="y")
        self.canvas_arbol.pack(side="left", fill="both", expand=True)

        self.frame_arbol_inner = tk.Frame(self.canvas_arbol, bg=BG2)
        self._arbol_window = self.canvas_arbol.create_window(
            (0, 0), window=self.frame_arbol_inner, anchor="nw")
        self.frame_arbol_inner.bind("<Configure>", self._arbol_scroll_update)
        self.canvas_arbol.bind("<Configure>", self._arbol_canvas_resize)
        self.canvas_arbol.bind("<MouseWheel>", lambda e: self.canvas_arbol.yview_scroll(
            -1 * (e.delta // 120), "units"))

        fb_arbol = tk.Frame(p, bg=BG2); fb_arbol.pack(fill="x", padx=6, pady=4)
        _btn(fb_arbol, "✓ Todos", self._marcar_todos_arbol, bg=BG3, fg=GREEN,
             font=(FU, 7), tooltip="Marca todas las carpetas").pack(side="left", padx=2)
        _btn(fb_arbol, "✗ Ninguno", self._desmarcar_todos_arbol, bg=BG3, fg=RED,
             font=(FU, 7), tooltip="Desmarca todas las carpetas").pack(side="left", padx=2)
        _btn(fb_arbol, "Limpiar filtros", self._limpiar_filtros, bg=BG3, fg=TEXT2,
             font=(FU, 7), tooltip="Elimina todos los filtros activos").pack(side="right", padx=2)

    def _arbol_scroll_update(self, _=None):
        self.canvas_arbol.configure(scrollregion=self.canvas_arbol.bbox("all"))

    def _arbol_canvas_resize(self, event):
        self.canvas_arbol.itemconfig(self._arbol_window, width=event.width)

    # ── TABLA ─────────────────────────────────────────────────────────────────
    def _build_tabla(self, p):
        ch = tk.Frame(p, bg=BG2, pady=5, padx=10); ch.pack(fill="x")
        self.lbl_nres = tk.Label(ch, text="Resultados", fg=GOLD, bg=BG2,
                                  font=(FU, 9, "bold")); self.lbl_nres.pack(side="left")
        self.lbl_pag  = tk.Label(ch, text="", fg=TEXT2, bg=BG2,
                                  font=(FU, 8)); self.lbl_pag.pack(side="left", padx=8)

        pb_frame = tk.Frame(ch, bg=BG2); pb_frame.pack(side="right")
        self.btn_ant = _btn(pb_frame, "◀ Anterior", self._pag_ant, bg=BG3, fg=TEXT2,
                             font=(FU, 8), tooltip="Página anterior")
        self.btn_ant.pack(side="left", padx=2)
        self.btn_sig = _btn(pb_frame, "Siguiente ▶", self._pag_sig, bg=BG3, fg=TEXT2,
                             font=(FU, 8), tooltip="Página siguiente")
        self.btn_sig.pack(side="left", padx=2)
        _sep(p)

        tf   = tk.Frame(p, bg=BG); tf.pack(fill="both", expand=True)
        cols  = ("fav", "titulo", "tipo", "fecha", "ruta")
        heads = ("★",   "Nombre", "Tipo", "Modificado", "Ruta")
        widths= (25,    360,       50,     110,           460)

        self.tree = ttk.Treeview(tf, columns=cols, show="headings", selectmode="browse")
        for col, h, w in zip(cols, heads, widths):
            self.tree.heading(col, text=h, command=lambda c=col: self._ordenar(c))
            self.tree.column(col, width=w, minwidth=20, anchor="w")

        vsb = ttk.Scrollbar(tf, orient="vertical",   command=self.tree.yview)
        hsb = ttk.Scrollbar(tf, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        vsb.pack(side="right", fill="y"); hsb.pack(side="bottom", fill="x")
        self.tree.pack(fill="both", expand=True)

        self.tree.tag_configure("PDF",  foreground="#ff9580")
        self.tree.tag_configure("DOCX", foreground="#80b3ff")
        self.tree.tag_configure("par",  background="#1a2232")
        self.tree.tag_configure("fav",  foreground=GOLD)

        self.tree.bind("<<TreeviewSelect>>", self._on_select)
        self.tree.bind("<Double-1>",          lambda e: self._abrir_original())
        self.tree.bind("<Button-3>",          self._menu_ctx)
        _Tooltip(self.tree,
                 "Clic → ver documento en el panel inferior\n"
                 "Doble clic → abrir con programa predeterminado\n"
                 "Clic derecho → más opciones")
        self._orden_asc = {}

    # ── PANEL DOCUMENTO ───────────────────────────────────────────────────────
    def _build_doc(self, p):
        dh = tk.Frame(p, bg=BG2, pady=5, padx=10); dh.pack(fill="x")
        self.lbl_doc_nombre = tk.Label(dh,
                                        text="— Seleccione un resultado para ver el documento —",
                                        fg=GOLD, bg=BG2, font=(FU, 9, "bold"), anchor="w")
        self.lbl_doc_nombre.pack(side="left")

        nav = tk.Frame(dh, bg=BG2); nav.pack(side="right")
        self.lbl_hits = tk.Label(nav, text="", fg=TEXT2, bg=BG2, font=(FU, 8))
        self.lbl_hits.pack(side="left", padx=6)
        _btn(nav, "▲", self._hit_prev, bg=BG3, fg=GOLD, font=(FU, 8),
             tooltip="Coincidencia anterior").pack(side="left", padx=1)
        _btn(nav, "▼", self._hit_next, bg=BG3, fg=GOLD, font=(FU, 8),
             tooltip="Coincidencia siguiente").pack(side="left", padx=1)

        self.btn_vista_txt = _btn(nav, "📄 Texto", self._ver_texto, bg=GOLD, fg=BG,
                                   font=(FU, 8), tooltip="Ver texto con términos resaltados")
        self.btn_vista_txt.pack(side="left", padx=(8, 1))
        self.btn_vista_doc = _btn(nav, "🖼 Vista real", self._ver_doc_renderizado, bg=BG3, fg=CYAN,
                                   font=(FU, 8),
                                   tooltip="Ver el documento renderizado como imagen\n(PDF: renderizado directo · DOCX: requiere LibreOffice)")
        self.btn_vista_doc.pack(side="left", padx=1)

        self.btn_abrir = _btn(nav, "↗ Abrir", self._abrir_original, bg=BG3, fg=GREEN,
                               font=(FU, 8),
                               tooltip="Abrir con el programa predeterminado (Adobe, Word, etc.)")
        self.btn_abrir.pack(side="left", padx=(8, 1)); self.btn_abrir.config(state="disabled")
        _btn(nav, "📋 Ruta", self._copiar_ruta, bg=BG3, fg=TEXT2, font=(FU, 8),
             tooltip="Copiar ruta completa al portapapeles").pack(side="left", padx=1)
        _btn(nav, "⭐", self._abrir_etiquetas, bg=BG3, fg=GOLD, font=(FU, 9),
             tooltip="Marcar favorito / gestionar etiquetas").pack(side="left", padx=1)

        self.lbl_doc_meta = tk.Label(p, text="", fg=TEXT2, bg=BG2,
                                      font=(FU, 7), anchor="w", padx=10)
        self.lbl_doc_meta.pack(fill="x")

        # Navegación páginas (PDF/DOCX renderizado)
        self.frame_pag_nav = tk.Frame(p, bg=BG2)
        _btn(self.frame_pag_nav, "◀ Anterior", self._pag_doc_ant, bg=BG3, fg=TEXT,
             font=(FU, 8), tooltip="Página anterior").pack(side="left", padx=6)
        self.lbl_pag_doc = tk.Label(self.frame_pag_nav, text="", fg=TEXT2, bg=BG2,
                                     font=(FU, 8)); self.lbl_pag_doc.pack(side="left", padx=8)
        _btn(self.frame_pag_nav, "Siguiente ▶", self._pag_doc_sig, bg=BG3, fg=TEXT,
             font=(FU, 8), tooltip="Página siguiente").pack(side="left", padx=6)

        _sep(p)

        self.frame_contenido = tk.Frame(p, bg=BG4); self.frame_contenido.pack(fill="both", expand=True)

        # Vista texto
        self.frame_texto = tk.Frame(self.frame_contenido, bg=BG4)
        self.frame_texto.pack(fill="both", expand=True)
        tf2 = tk.Frame(self.frame_texto, bg=BG4); tf2.pack(fill="both", expand=True)
        self.txt = tk.Text(tf2, font=(FS, 11), bg=BG4, fg=TEXT, relief="flat", bd=0,
                            wrap="word", state="disabled", highlightthickness=0,
                            spacing1=2, spacing3=4, padx=20, pady=16,
                            selectbackground=BGSEL)
        vsb_t = ttk.Scrollbar(tf2, orient="vertical", command=self.txt.yview)
        self.txt.configure(yscrollcommand=vsb_t.set)
        vsb_t.pack(side="right", fill="y"); self.txt.pack(fill="both", expand=True)
        self.txt.tag_configure("normal",    foreground=TEXT)
        self.txt.tag_configure("resaltado", foreground=HL_FG, background=HL_BG,  font=(FS, 11, "bold"))
        self.txt.tag_configure("activo",    foreground="#000000", background=HL_ACT, font=(FS, 11, "bold"))
        self.txt.tag_configure("cargando",  foreground=TEXT2, font=(FS, 10, "italic"))

        # Vista renderizada (Canvas)
        self.frame_render = tk.Frame(self.frame_contenido, bg=BG4)
        rf = tk.Frame(self.frame_render, bg=BG4); rf.pack(fill="both", expand=True)
        self.canvas_doc = tk.Canvas(rf, bg=BG4, highlightthickness=0)
        vsb_r = ttk.Scrollbar(rf, orient="vertical",   command=self.canvas_doc.yview)
        hsb_r = ttk.Scrollbar(rf, orient="horizontal", command=self.canvas_doc.xview)
        self.canvas_doc.configure(yscrollcommand=vsb_r.set, xscrollcommand=hsb_r.set)
        vsb_r.pack(side="right", fill="y"); hsb_r.pack(side="bottom", fill="x")
        self.canvas_doc.pack(fill="both", expand=True)
        self.canvas_doc.bind("<MouseWheel>", lambda e: self.canvas_doc.yview_scroll(
            -1 * (e.delta // 120), "units"))
        self._img_ref = None

    # ── INICIO — verificar índice o pedir carpeta ────────────────────────────
    def _inicio(self):
        ultima = cfg_global_get("ultima_carpeta", "")
        if ultima and os.path.isdir(ultima):
            self._cargar_carpeta(ultima)
        else:
            WinBienvenida(self, self._cargar_carpeta)

    def _cargar_carpeta(self, carpeta):
        self._carpeta = carpeta
        cfg_global_set("ultima_carpeta", carpeta)
        self.lbl_carpeta_hdr.config(text=f"— {carpeta}")

        tiene, ultima_idx = hay_indice(carpeta)

        if tiene:
            self.lbl_badge.config(text="● ÍNDICE ACTIVO", fg=GREEN)
            self.lbl_ultima.config(text=f"Indexado: {ultima_idx}")
            self._poblar_arbol_check(carpeta)
            self._recargar_etiquetas()
            self._cargar_historial()
            self.lbl_status.config(text="Listo — escriba una palabra para buscar.", fg=TEXT2)

            # Verificar silenciosamente si hay archivos nuevos
            threading.Thread(target=self._verificar_nuevos_bg, daemon=True).start()
        else:
            self.lbl_badge.config(text="● SIN ÍNDICE", fg=RED)
            self.lbl_status.config(text="Esta carpeta no tiene índice. Iniciando indexación…", fg=WARN)
            self.after(600, lambda: WinIndexar(self, carpeta, callback=lambda: self._cargar_carpeta(carpeta)))

    def _verificar_nuevos_bg(self):
        """Corre en background. Solo avisa si encuentra archivos nuevos/modificados."""
        try:
            nuevos, modificados = verificar_nuevos(self._carpeta)
            if nuevos > 0 or modificados > 0:
                self.after(0, lambda: self._avisar_nuevos(nuevos, modificados))
        except: pass

    def _avisar_nuevos(self, nuevos, modificados):
        partes = []
        if nuevos:      partes.append(f"{nuevos} archivo(s) nuevo(s)")
        if modificados: partes.append(f"{modificados} archivo(s) modificado(s)")
        msg = " y ".join(partes)
        self.lbl_status.config(
            text=f"⚠  Se detectaron: {msg} — pulse 'Actualizar índice' para indexarlos.",
            fg=WARN)
        self.lbl_badge.config(text="● ÍNDICE DESACTUALIZADO", fg=WARN)

    def _actualizar_idx(self):
        if not self._carpeta:
            WinBienvenida(self, self._cargar_carpeta); return
        WinIndexar(self, self._carpeta,
                   callback=lambda: self._cargar_carpeta(self._carpeta))

    def _abrir_config(self):
        if not self._carpeta: return
        WinConfig(self, self._carpeta, callback=self._on_config_callback)

    def _on_config_callback(self, accion, carpeta):
        if accion == "cambiar_carpeta":
            self._cargar_carpeta(carpeta)
        elif accion == "reconstruir":
            WinIndexar(self, carpeta, callback=lambda: self._cargar_carpeta(carpeta))

    # ── ÁRBOL CON CHECKBOXES ─────────────────────────────────────────────────
    def _poblar_arbol_check(self, raiz):
        for w in self.frame_arbol_inner.winfo_children(): w.destroy()
        self._carpetas_check = {}
        if not raiz or not os.path.isdir(raiz): return
        # Nodo raíz siempre marcado y no se puede desmarcar
        self._agregar_nodo_check(self.frame_arbol_inner, raiz, nivel=0, es_raiz=True)

    def _agregar_nodo_check(self, parent, carpeta, nivel, es_raiz=False):
        nombre = os.path.basename(carpeta) or carpeta
        var    = tk.BooleanVar(value=True)
        self._carpetas_check[carpeta] = var

        fila = tk.Frame(parent, bg=BG2); fila.pack(anchor="w", fill="x")
        sangria = nivel * 14

        # Expand/collapse toggle
        try:
            hijos_dirs = [d for d in sorted(os.listdir(carpeta))
                          if os.path.isdir(os.path.join(carpeta, d)) and not d.startswith(".")]
        except: hijos_dirs = []

        self._frame_hijos = {}  # ruta → frame de hijos

        # Icono triángulo para expandir
        if hijos_dirs:
            estado = {"abierto": True}
            frame_hijos = tk.Frame(parent, bg=BG2)

            lbl_tri = tk.Label(fila, text="▾", fg=TEXT2, bg=BG2, font=(FU, 8),
                                cursor="hand2", padx=0)
            lbl_tri.place(x=sangria, rely=0.5, anchor="w")

            def _toggle(fh=frame_hijos, lt=lbl_tri, est=estado):
                if est["abierto"]:
                    fh.pack_forget(); lt.config(text="▸"); est["abierto"] = False
                else:
                    fh.pack(anchor="w", fill="x"); lt.config(text="▾"); est["abierto"] = True

            lbl_tri.bind("<Button-1>", lambda e: _toggle())

        cb = tk.Checkbutton(
            fila,
            text=f"  {'🏠 ' if es_raiz else '📁 '}{nombre}",
            variable=var,
            fg=GOLD if es_raiz else TEXT,
            bg=BG2, selectcolor=BG3, activebackground=BG2,
            font=(FU, 8, "bold" if es_raiz else "normal"),
            command=lambda c=carpeta, v=var: self._on_check_carpeta(c, v)
        )
        cb.pack(side="left", padx=(sangria + 14, 0))
        _Tooltip(cb, f"Marcar: incluye esta carpeta y subcarpetas en los resultados\n{carpeta}")

        if hijos_dirs:
            frame_hijos.pack(anchor="w", fill="x")
            for h in hijos_dirs[:60]:
                ruta_h = os.path.join(carpeta, h)
                self._agregar_nodo_check(frame_hijos, ruta_h, nivel + 1)

    def _on_check_carpeta(self, carpeta, var):
        """Al marcar/desmarcar, propagar a todas las subcarpetas."""
        estado = var.get()
        for ruta, v in self._carpetas_check.items():
            if ruta.startswith(carpeta):
                v.set(estado)
        self._buscar_desde_cero()

    def _marcar_todos_arbol(self):
        for v in self._carpetas_check.values(): v.set(True)
        self._buscar_desde_cero()

    def _desmarcar_todos_arbol(self):
        for ruta, v in self._carpetas_check.items():
            # No desmarcar la raíz
            if ruta != self._carpeta: v.set(False)
        self._buscar_desde_cero()

    def _get_carpetas_activas(self):
        """Devuelve lista de carpetas marcadas (excluyendo las desmarcadas)."""
        marcadas = [r for r, v in self._carpetas_check.items() if v.get()]
        # Si están todas marcadas, no hay filtro de carpeta
        if len(marcadas) == len(self._carpetas_check): return None
        return marcadas if marcadas else None

    # ── BÚSQUEDA ─────────────────────────────────────────────────────────────
    def _get_filtros(self):
        tipos = []
        if self.var_pdf.get():  tipos.append("pdf")
        if self.var_docx.get(): tipos.append("docx")
        tipo_adv = getattr(self, "var_adv_tipo", None)
        if tipo_adv and tipo_adv.get() != "Todos":
            tipos = [tipo_adv.get().lower()]

        anios = None
        try:
            d = int(self.var_adv_desde.get().strip())
            h = int(self.var_adv_hasta.get().strip())
            anios = list(range(d, h + 1))
        except: pass

        solo_fav = self.var_solo_fav.get()
        etq_nombre = self.var_etq.get()
        etq_id = self._etq_map.get(etq_nombre) if hasattr(self, "_etq_map") else None
        carpetas = self._get_carpetas_activas()

        return tipos if tipos else None, anios, carpetas, solo_fav, etq_id

    def _buscar_desde_cero(self):
        self._offset = 0; self._ejecutar_busqueda()

    def _ejecutar_busqueda(self):
        if not self._carpeta: return
        q = self.var_q.get().strip()
        if not q: return
        if not os.path.exists(db_path_para(self._carpeta)):
            messagebox.showwarning("Sin índice",
                "Esta carpeta no tiene índice.\nUse 'Actualizar índice' para crearlo.")
            return

        self.lbl_status.config(text="Buscando…", fg=GOLD)
        self.config(cursor="watch"); self.update()

        tipos, anios, carpetas, solo_fav, etq_id = self._get_filtros()
        t0 = time.time()
        try:
            res, total = buscar(self._carpeta, q, tipos, anios, carpetas,
                                 solo_fav, etq_id, self._offset, PAGE_SIZE)
        except Exception as e:
            messagebox.showerror("Error en la búsqueda", str(e))
            self.config(cursor=""); return

        dt = time.time() - t0
        self._resultados  = res
        self._total_res   = total
        self._query_actual = q
        self._terminos    = terminos_resaltar(q)
        self._mostrar_resultados(res)

        pag_actual = self._offset // PAGE_SIZE + 1
        pags_total = max(1, -(-total // PAGE_SIZE))
        self.lbl_pag.config(text=f"Página {pag_actual} de {pags_total}")
        self.btn_ant.config(state="normal" if self._offset > 0 else "disabled")
        self.btn_sig.config(state="normal" if (self._offset + PAGE_SIZE) < total else "disabled")

        if total == 0:
            self.lbl_status.config(
                text="Sin resultados — intente con menos palabras, verifique la ortografía "
                     "o use el comodín: contrat*", fg=WARN)
        else:
            self.lbl_status.config(
                text=f"⚡ {total:,} resultado(s) · {dt*1000:.0f}ms · Pág {pag_actual}/{pags_total}",
                fg=GREEN)
        self.config(cursor="")
        self._cargar_historial()

    def _debounce(self, _=None):
        if self._debounce_id: self.after_cancel(self._debounce_id)
        if len(self.var_q.get().strip()) >= 3:
            self._debounce_id = self.after(400, self._buscar_desde_cero)

    def _pag_ant(self):
        if self._offset >= PAGE_SIZE:
            self._offset -= PAGE_SIZE; self._ejecutar_busqueda()

    def _pag_sig(self):
        if (self._offset + PAGE_SIZE) < self._total_res:
            self._offset += PAGE_SIZE; self._ejecutar_busqueda()

    def _mostrar_resultados(self, rs):
        self.tree.delete(*self.tree.get_children())
        self._limpiar_doc(); self.btn_abrir.config(state="disabled")
        for i, r in enumerate(rs):
            tags = [r["tipo"]]
            if i % 2 == 1: tags.append("par")
            if r["favorito"]: tags.append("fav")
            etq_str = " · ".join(e[0] for e in r["etiquetas"]) if r["etiquetas"] else ""
            self.tree.insert("", "end", iid=str(r["id"]), values=(
                "⭐" if r["favorito"] else "",
                r["nombre"] + (f"  [{etq_str}]" if etq_str else ""),
                r["tipo"], r["fecha"], r["ruta"],
            ), tags=tags)
        self.lbl_nres.config(text=f"{self._total_res:,} resultado(s)")

    def _on_select(self, _=None):
        sel = self.tree.selection()
        if not sel: return
        doc_id = int(sel[0])
        r = next((x for x in self._resultados if x["id"] == doc_id), None)
        if not r: return
        self._ruta_sel = r["ruta"]; self._id_sel = doc_id
        self.btn_abrir.config(state="normal")
        self.lbl_doc_nombre.config(text=r["nombre"])
        self.lbl_doc_meta.config(
            text=f"Tipo: {r['tipo']}  ·  Tamaño: {tamanio_fmt(r['tam'])}  ·  Modificado: {r['fecha']}")
        if r["snippet"]:
            self.lbl_status.config(text=f"…{r['snippet']}…", fg=TEXT2)

        if doc_id in self._cache_doc:
            # Ya estaba en caché de sesión — instantáneo
            self._renderizar_texto(self._cache_doc[doc_id], self._terminos)
        else:
            # Intentar desde DB primero (milisegundos)
            texto = texto_desde_db(self._carpeta, doc_id)
            if texto is not None:
                # ✓ Leído desde DB — instantáneo, sin hilo
                self._cache_doc[doc_id] = texto
                self._renderizar_texto(texto, self._terminos)
            else:
                # Fallback: leer del archivo físico en background
                # (solo ocurre si el índice no tiene el texto, ej: doc muy nuevo)
                self.txt.config(state="normal"); self.txt.delete("1.0", "end")
                self.txt.insert("end", "⏳  Cargando desde archivo…", "cargando")
                self.txt.config(state="disabled")
                threading.Thread(target=self._cargar_doc_hilo,
                                 args=(doc_id, r["ruta"], self._terminos),
                                 daemon=True).start()

        if self._modo_vista == "render":
            threading.Thread(target=self._cargar_render_bg, args=(r["ruta"],),
                             daemon=True).start()

    def _cargar_doc_hilo(self, doc_id, ruta, terminos):
        texto = extraer(ruta)
        self._cache_doc[doc_id] = texto
        self._cola_doc.put(("texto", doc_id, texto, terminos))

    def _poll_doc(self):
        try:
            while True:
                msg = self._cola_doc.get_nowait(); t = msg[0]
                if t == "texto":
                    _, doc_id, texto, terminos = msg
                    sel = self.tree.selection()
                    if sel and int(sel[0]) == doc_id:
                        self._renderizar_texto(texto, terminos)
                elif t == "render_listo":
                    _, doc, ruta = msg
                    if self._ruta_sel == ruta:
                        self._pdf_doc = doc; self._pdf_page_idx = 0
                        self._mostrar_pagina_render()
                elif t == "render_error":
                    _, err = msg
                    self.canvas_doc.delete("all")
                    self.canvas_doc.create_text(20, 20, anchor="nw",
                                                text=f"⚠  {err}", fill=WARN, font=(FU, 10))
        except queue.Empty: pass
        self.after(150, self._poll_doc)

    # ── VISTA RENDERIZADA (PDF y DOCX) ────────────────────────────────────────
    def _ver_texto(self):
        self._modo_vista = "texto"
        self.frame_render.pack_forget()
        self.frame_pag_nav.pack_forget()
        self.frame_texto.pack(fill="both", expand=True)
        self.btn_vista_txt.config(bg=GOLD, fg=BG)
        self.btn_vista_doc.config(bg=BG3, fg=CYAN)

    def _ver_doc_renderizado(self):
        if not self._ruta_sel: return
        ext = Path(self._ruta_sel).suffix.lower()

        if ext == ".pdf" and not PDF_OK:
            messagebox.showwarning("No disponible",
                "La vista renderizada de PDF requiere PyMuPDF.\n"
                "Reinicie la aplicación para instalarlo."); return

        if ext == ".docx":
            lo = _encontrar_libreoffice()
            if not lo:
                messagebox.showinfo("LibreOffice no encontrado",
                    "Para ver DOCX renderizado se necesita LibreOffice instalado.\n\n"
                    "Descárguelo en: https://www.libreoffice.org\n\n"
                    "Mientras tanto, use la vista de texto."); return

        self._modo_vista = "render"
        self.frame_texto.pack_forget()
        self.frame_render.pack(fill="both", expand=True)
        self.frame_pag_nav.pack(fill="x", before=self.frame_contenido)
        self.btn_vista_doc.config(bg=GOLD, fg=BG)
        self.btn_vista_txt.config(bg=BG3, fg=GOLD)

        self.canvas_doc.delete("all")
        self.canvas_doc.create_text(20, 20, anchor="nw",
                                     text="⏳  Renderizando documento…",
                                     fill=TEXT2, font=(FU, 11, "italic"))
        threading.Thread(target=self._cargar_render_bg,
                         args=(self._ruta_sel,), daemon=True).start()

    def _cargar_render_bg(self, ruta):
        try:
            ext = Path(ruta).suffix.lower()
            if ext == ".pdf":
                doc = fitz.open(ruta)
                self._cola_doc.put(("render_listo", doc, ruta))
            elif ext == ".docx":
                pdf_tmp = docx_a_pdf_temporal(ruta)
                if pdf_tmp:
                    doc = fitz.open(pdf_tmp)
                    self._cola_doc.put(("render_listo", doc, ruta))
                else:
                    self._cola_doc.put(("render_error",
                        "No se pudo convertir el DOCX a PDF.\n"
                        "Verifique que LibreOffice esté instalado.", ruta))
        except Exception as e:
            self._cola_doc.put(("render_error", str(e)))

    def _mostrar_pagina_render(self):
        if not self._pdf_doc: return
        try:
            page = self._pdf_doc[self._pdf_page_idx]
            # Escalar al ancho disponible del canvas
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
                                         text=f"Error al renderizar: {e}", fill=RED, font=(FU, 10))

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
        for i, parte in enumerate(patron.split(texto)):
            self.txt.insert("end", parte, "resaltado" if i % 2 == 1 else "normal")
        self.txt.config(state="disabled")

        self._hits_pos = []; idx = "1.0"
        while True:
            idx = self.txt.search(patron.pattern, idx, stopindex="end", regexp=True, nocase=True)
            if not idx: break
            self._hits_pos.append(idx)
            line, col = idx.split(".")
            idx = f"{line}.{int(col)+1}"

        n = len(self._hits_pos); self._hit_actual = 0
        self.lbl_hits.config(
            text=f"{n} coincidencia(s)" if n else "Sin coincidencias",
            fg=GOLD if n else TEXT2)
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
        self.lbl_hits.config(text=f"{idx+1} / {len(self._hits_pos)} coincidencia(s)", fg=GOLD)

    def _hit_next(self):
        if self._hits_pos: self._saltar_hit((self._hit_actual+1) % len(self._hits_pos))

    def _hit_prev(self):
        if self._hits_pos: self._saltar_hit((self._hit_actual-1) % len(self._hits_pos))

    def _limpiar_doc(self):
        self.txt.config(state="normal"); self.txt.delete("1.0", "end")
        self.txt.config(state="disabled")
        self.lbl_doc_nombre.config(text="— Seleccione un resultado para ver el documento —")
        self.lbl_doc_meta.config(text=""); self.lbl_hits.config(text="")
        self._hits_pos = []; self._hit_actual = 0
        self._pdf_doc  = None; self._pdf_page_idx = 0

    def _limpiar(self):
        self.var_q.set(); self.tree.delete(*self.tree.get_children())
        self._limpiar_doc(); self.lbl_nres.config(text="Resultados")
        self.lbl_pag.config(text="")
        self.lbl_status.config(text="Listo — escriba una palabra para buscar.", fg=TEXT2)
        self._resultados = []; self._cache_doc = {}
        self._total_res = 0; self._offset = 0
        self.btn_abrir.config(state="disabled"); self._limpiar_avanzada()

    def _limpiar_filtros(self):
        self.var_solo_fav.set(False); self.var_etq.set("— Todas —")
        self._marcar_todos_arbol(); self._limpiar_avanzada()

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
        if vals: self.var_q.set(vals[0]); self._buscar_desde_cero()

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
        pos = self.entry.index(tk.INSERT)
        if sym == '"…"': self.entry.insert(pos, '""'); self.entry.icursor(pos+1)
        else: self.entry.insert(pos, f" {sym} ")
        self.entry.focus_set()

    # ── MENÚ CONTEXTUAL ───────────────────────────────────────────────────────
    def _menu_ctx(self, event):
        item = self.tree.identify_row(event.y)
        if not item: return
        self.tree.selection_set(item)
        doc_id = int(item)
        r = next((x for x in self._resultados if x["id"] == doc_id), None)
        if not r: return
        menu = tk.Menu(self, tearoff=0, bg=BG3, fg=TEXT,
                       activebackground=BGSEL, activeforeground=GOLD,
                       font=(FU, 9), relief="flat", bd=0)
        menu.add_command(label="↗  Abrir documento",    command=self._abrir_original)
        menu.add_command(label="📋  Copiar ruta",          command=self._copiar_ruta)
        menu.add_separator()
        fav_lbl = "★  Quitar de favoritos" if r["favorito"] else "⭐  Marcar como favorito"
        menu.add_command(label=fav_lbl, command=lambda: self._toggle_fav(doc_id))
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

    def _ordenar(self, col):
        mapa = {"fav": "favorito","titulo": "nombre","tipo": "tipo","fecha": "fecha","ruta": "ruta"}
        key  = mapa.get(col, "nombre")
        asc  = not self._orden_asc.get(col, False); self._orden_asc[col] = asc
        self._resultados.sort(key=lambda r: (r.get(key) or ""), reverse=not asc)
        self._mostrar_resultados(self._resultados)

    def _abrir_original(self):
        if self._ruta_sel:
            try: os.startfile(self._ruta_sel)
            except Exception as e: messagebox.showerror("Error al abrir", str(e))

    def _copiar_ruta(self):
        if self._ruta_sel:
            self.clipboard_clear(); self.clipboard_append(self._ruta_sel)
            self.lbl_status.config(text=f"✓ Ruta copiada: {self._ruta_sel}", fg=GOLD)

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
