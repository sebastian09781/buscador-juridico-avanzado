"""
JurisBot Relatoría v11
======================
Motor de búsqueda jurídico de alto rendimiento.

MEJORAS vs versión anterior:
 - Arranque INSTANTÁNEO: NO escanea carpetas al iniciar
 - Índice lazy cargado desde caché en disco (gzip JSON)
 - Búsqueda en hilo separado → UI nunca se congela
 - Indexación con modal de progreso + botón Cancelar
 - Vista previa con resaltado de coincidencias (en hilo)
 - 3 paneles: Resultados / Preview / Metadatos
 - Paleta oscura profesional
 - Atajos: Ctrl+F, Ctrl+I, Escape
 - Sin scrolls anidados, sin recreación de widgets
"""

import os
import re
import json
import gzip
import time
import queue
import threading
import subprocess
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime
from pathlib import Path

# ─── Paleta ───────────────────────────────────────────────────────────────────
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
}

EXTENSIONS  = {".docx", ".doc", ".pdf", ".txt", ".rtf", ".odt"}
INDEX_FILE  = Path.home() / ".jurisbot_index.json.gz"
MAX_RESULTS = 500
PREVIEW_MAX = 50_000


# ─── Lectura segura de archivos ───────────────────────────────────────────────

def read_file_safe(path: str, max_chars=100_000) -> str:
    """Lee texto de un archivo sin lanzar excepciones."""
    try:
        ext = Path(path).suffix.lower()
        if ext == ".txt":
            for enc in ("utf-8", "latin-1", "cp1252"):
                try:
                    with open(path, "r", encoding=enc, errors="replace") as f:
                        return f.read(max_chars)
                except Exception:
                    continue
        elif ext == ".docx":
            try:
                from docx import Document
                doc = Document(path)
                return "\n".join(p.text for p in doc.paragraphs)[:max_chars]
            except Exception:
                pass
        elif ext == ".pdf":
            try:
                import pdfplumber
                text = []
                with pdfplumber.open(path) as pdf:
                    for page in pdf.pages[:30]:
                        t = page.extract_text()
                        if t:
                            text.append(t)
                        if sum(len(s) for s in text) > max_chars:
                            break
                return "\n".join(text)[:max_chars]
            except Exception:
                pass
        # fallback texto crudo
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read(max_chars)
    except Exception:
        return ""


# ─── Motor de índice ──────────────────────────────────────────────────────────

class SearchIndex:

    def __init__(self):
        self.docs: dict[str, dict] = {}
        self.folders: list[str] = []
        self._lock = threading.Lock()

    def save(self):
        try:
            data = {"folders": self.folders, "docs": self.docs}
            with gzip.open(INDEX_FILE, "wt", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
        except Exception as e:
            print(f"[Index] save error: {e}")

    def load(self) -> bool:
        try:
            if not INDEX_FILE.exists():
                return False
            with gzip.open(INDEX_FILE, "rt", encoding="utf-8") as f:
                data = json.load(f)
            self.folders = data.get("folders", [])
            self.docs    = data.get("docs", {})
            return True
        except Exception:
            return False

    def build(self, folders: list[str],
              progress_cb=None,
              cancel_event: threading.Event = None) -> int:
        """
        Escanea carpetas e indexa documentos.
        Llama progress_cb(current, total, filename) cada 5 archivos.
        """
        files = []
        for folder in folders:
            if not os.path.isdir(folder):
                continue
            for root, dirs, fnames in os.walk(folder):
                dirs[:] = [d for d in dirs if not d.startswith(".")]
                for fname in fnames:
                    if Path(fname).suffix.lower() in EXTENSIONS:
                        files.append(os.path.join(root, fname))

        total = len(files)
        if total == 0:
            return 0

        new_docs = {}
        for i, path in enumerate(files):
            if cancel_event and cancel_event.is_set():
                break
            try:
                stat = os.stat(path)
                text = read_file_safe(path)
                p = Path(path)
                new_docs[path] = {
                    "name":       p.name,
                    "ext":        p.suffix.lower(),
                    "size":       stat.st_size,
                    "mtime":      stat.st_mtime,
                    "text_lower": text.lower(),
                    "snippet":    text[:300].replace("\n", " "),
                }
            except Exception:
                pass

            if progress_cb and i % 5 == 0:
                progress_cb(i + 1, total, Path(path).name)

        with self._lock:
            self.docs    = new_docs
            self.folders = [f for f in folders if os.path.isdir(f)]

        self.save()
        return len(new_docs)

    def search(self, query: str,
               year_filter: str = "",
               ext_filter: str = "") -> list[dict]:
        if not query.strip() and not year_filter and not ext_filter:
            return []

        terms = [t.lower().strip() for t in query.split() if t.strip()]
        year  = year_filter.strip()
        ext   = ext_filter.strip().lower()
        if ext and not ext.startswith("."):
            ext = "." + ext

        results = []
        with self._lock:
            snapshot = list(self.docs.items())

        for path, meta in snapshot:
            if ext and meta["ext"] != ext:
                continue
            if year and year not in path:
                continue
            text = meta["text_lower"]
            name = meta["name"].lower()
            if terms and not all((t in text or t in name) for t in terms):
                continue
            results.append({
                "path":    path,
                "name":    meta["name"],
                "ext":     meta["ext"],
                "size":    meta["size"],
                "mtime":   meta["mtime"],
                "snippet": meta["snippet"],
            })
            if len(results) >= MAX_RESULTS:
                break

        return results


# ─── Aplicación ───────────────────────────────────────────────────────────────

class JurisBotApp(tk.Tk):

    def __init__(self):
        super().__init__()
        self.title("JurisBot Relatoría v11")
        self.geometry("1400x800")
        self.minsize(900, 600)
        self.configure(bg=C["bg"])

        self.index            = SearchIndex()
        self._search_thread   = None
        self._selected_path   = tk.StringVar()
        self._status_var      = tk.StringVar(value="Sin índice  🔴")
        self._results: list   = []
        self._placeholder_on  = True

        self._build_ui()
        self._bind_shortcuts()
        self.after(100, self._load_index_async)

    # ── UI principal ──────────────────────────────────────────────────────

    def _build_ui(self):
        self._build_topbar()
        self._build_main()
        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)

    def _build_topbar(self):
        bar = tk.Frame(self, bg=C["panel"], height=60)
        bar.grid(row=0, column=0, sticky="ew")
        bar.grid_propagate(False)
        bar.columnconfigure(2, weight=1)

        tk.Label(bar, text="⚖ JurisBot", bg=C["panel"],
                 fg=C["accent"], font=("Segoe UI", 13, "bold")).grid(
            row=0, column=0, padx=(16,8), pady=10)

        tk.Frame(bar, bg=C["border"], width=1).grid(
            row=0, column=1, sticky="ns", pady=8)

        self._search_var = tk.StringVar()
        self._entry = tk.Entry(
            bar, textvariable=self._search_var,
            bg=C["bg"], fg=C["text2"],
            insertbackground=C["text"], relief="flat",
            font=C["font_lg"], highlightthickness=1,
            highlightcolor=C["accent"],
            highlightbackground=C["border"])
        self._entry.grid(row=0, column=2, sticky="ew", padx=8, ipady=6)
        self._entry.insert(0, "Buscar por texto, radicado, año, palabra clave…")
        self._entry.bind("<Return>",   lambda e: self._do_search())
        self._entry.bind("<FocusIn>",  self._ph_clear)
        self._entry.bind("<FocusOut>", self._ph_restore)

        # Filtros
        ff = tk.Frame(bar, bg=C["panel"])
        ff.grid(row=0, column=3, padx=4)
        tk.Label(ff, text="Año:", bg=C["panel"], fg=C["text2"],
                 font=C["font_sm"]).pack(side="left")
        self._year_var = tk.StringVar()
        tk.Entry(ff, textvariable=self._year_var, width=5,
                 bg=C["bg"], fg=C["text"],
                 insertbackground=C["text"], relief="flat",
                 font=C["font_sm"]).pack(side="left", padx=2)
        tk.Label(ff, text="Tipo:", bg=C["panel"], fg=C["text2"],
                 font=C["font_sm"]).pack(side="left", padx=(6,0))
        self._ext_var = tk.StringVar()
        ttk.Combobox(ff, textvariable=self._ext_var,
                     values=["", ".docx", ".pdf", ".txt", ".doc"],
                     width=7, state="readonly").pack(side="left", padx=2)

        self._btn_search = self._mk_btn(
            bar, "🔍 Buscar", self._do_search, C["accent"], "#1a1a1a")
        self._btn_search.grid(row=0, column=4, padx=4, pady=8)

        self._mk_btn(bar, "📂 Indexar", self._open_index_dialog,
                     C["accent2"], C["text"]).grid(row=0, column=5, padx=4, pady=8)

        self._searching_lbl = tk.Label(bar, text="",
                                       bg=C["panel"], fg=C["accent"],
                                       font=C["font_sm"])
        self._searching_lbl.grid(row=0, column=6, padx=4)

        tk.Label(bar, textvariable=self._status_var,
                 bg=C["panel"], fg=C["text2"],
                 font=C["font_sm"]).grid(row=0, column=7, padx=12)

    def _mk_btn(self, parent, text, cmd, bg, fg):
        b = tk.Button(parent, text=text, command=cmd,
                      bg=bg, fg=fg,
                      activebackground=C["hover"],
                      activeforeground=C["text"],
                      relief="flat", font=C["font"],
                      padx=10, pady=4, cursor="hand2", bd=0)
        b.bind("<Enter>", lambda e, _bg=bg: b.configure(bg=C["hover"]))
        b.bind("<Leave>", lambda e, _bg=bg: b.configure(bg=_bg))
        return b

    def _build_main(self):
        m = tk.Frame(self, bg=C["bg"])
        m.grid(row=1, column=0, sticky="nsew")
        m.rowconfigure(0, weight=1)
        m.columnconfigure(0, weight=3)
        m.columnconfigure(1, weight=5)
        m.columnconfigure(2, weight=2)
        self._build_results_panel(m)
        self._build_preview_panel(m)
        self._build_meta_panel(m)

    def _build_results_panel(self, parent):
        f = tk.Frame(parent, bg=C["panel"])
        f.grid(row=0, column=0, sticky="nsew", padx=(8,4), pady=8)
        f.rowconfigure(1, weight=1)
        f.columnconfigure(0, weight=1)

        hdr = tk.Frame(f, bg=C["panel"])
        hdr.grid(row=0, column=0, sticky="ew", padx=8, pady=(8,0))
        self._results_lbl = tk.Label(
            hdr, text="Resultados", bg=C["panel"], fg=C["text"],
            font=("Segoe UI", 10, "bold"))
        self._results_lbl.pack(side="left")

        lf = tk.Frame(f, bg=C["panel"])
        lf.grid(row=1, column=0, sticky="nsew", padx=4, pady=4)
        lf.rowconfigure(0, weight=1)
        lf.columnconfigure(0, weight=1)

        self._listbox = tk.Listbox(
            lf, bg=C["bg"], fg=C["text"],
            selectbackground=C["sel"], selectforeground=C["text"],
            font=C["font"], relief="flat", bd=0,
            activestyle="none", highlightthickness=0)
        self._listbox.grid(row=0, column=0, sticky="nsew")
        self._listbox.bind("<<ListboxSelect>>", self._on_select)

        sb = ttk.Scrollbar(lf, orient="vertical",
                           command=self._listbox.yview)
        sb.grid(row=0, column=1, sticky="ns")
        self._listbox.configure(yscrollcommand=sb.set)

    def _build_preview_panel(self, parent):
        f = tk.Frame(parent, bg=C["panel"])
        f.grid(row=0, column=1, sticky="nsew", padx=4, pady=8)
        f.rowconfigure(1, weight=1)
        f.columnconfigure(0, weight=1)

        self._preview_lbl = tk.Label(
            f, text="Vista previa", bg=C["panel"], fg=C["text"],
            font=("Segoe UI", 10, "bold"))
        self._preview_lbl.grid(row=0, column=0, sticky="w", padx=10, pady=(8,0))

        tf = tk.Frame(f, bg=C["panel"])
        tf.grid(row=1, column=0, sticky="nsew", padx=4, pady=4)
        tf.rowconfigure(0, weight=1)
        tf.columnconfigure(0, weight=1)

        self._preview_txt = tk.Text(
            tf, bg=C["bg"], fg=C["text"],
            font=C["font_mono"], relief="flat", bd=0,
            wrap="word", state="disabled",
            highlightthickness=0, padx=12, pady=8,
            spacing1=2, spacing3=2)
        self._preview_txt.grid(row=0, column=0, sticky="nsew")
        self._preview_txt.tag_configure(
            "highlight", background=C["accent"],
            foreground="#1a1a1a",
            font=("Consolas", 10, "bold"))

        sb = ttk.Scrollbar(tf, orient="vertical",
                           command=self._preview_txt.yview)
        sb.grid(row=0, column=1, sticky="ns")
        self._preview_txt.configure(yscrollcommand=sb.set)

    def _build_meta_panel(self, parent):
        f = tk.Frame(parent, bg=C["panel"])
        f.grid(row=0, column=2, sticky="nsew", padx=(4,8), pady=8)
        f.columnconfigure(0, weight=1)

        tk.Label(f, text="Metadatos", bg=C["panel"], fg=C["text"],
                 font=("Segoe UI", 10, "bold")).grid(
            row=0, column=0, sticky="w", padx=10, pady=(8,4))

        self._meta_vars = {}
        fields = [("Nombre","name"),("Tipo","ext"),
                  ("Tamaño","size"),("Modificado","mtime"),("Ruta","path")]
        for i, (lbl, key) in enumerate(fields):
            tk.Label(f, text=lbl+":", bg=C["panel"], fg=C["text2"],
                     font=C["font_sm"], anchor="w").grid(
                row=i*2+1, column=0, sticky="w", padx=10, pady=(4,0))
            var = tk.StringVar()
            self._meta_vars[key] = var
            tk.Label(f, textvariable=var, bg=C["panel"], fg=C["text"],
                     font=C["font_sm"], wraplength=180,
                     justify="left", anchor="w").grid(
                row=i*2+2, column=0, sticky="w", padx=10, pady=(0,2))

        bf = tk.Frame(f, bg=C["panel"])
        bf.grid(row=20, column=0, sticky="ew", padx=8, pady=16)
        self._mk_btn(bf, "📂 Abrir",        self._open_file,
                     C["accent2"], C["text"]).pack(fill="x", pady=2)
        self._mk_btn(bf, "📁 Ver ubicación", self._open_location,
                     C["border"], C["text"]).pack(fill="x", pady=2)
        self._mk_btn(bf, "⭐ Favorito",      self._toggle_favorite,
                     C["border"], C["text"]).pack(fill="x", pady=2)

        tk.Frame(f, bg=C["border"], height=1).grid(
            row=21, column=0, sticky="ew", pady=8)
        self._index_info_var = tk.StringVar(value="")
        tk.Label(f, textvariable=self._index_info_var,
                 bg=C["panel"], fg=C["text2"],
                 font=C["font_sm"], wraplength=180,
                 justify="left").grid(row=22, column=0, sticky="w", padx=10)

    # ── Placeholder ───────────────────────────────────────────────────────

    _PH = "Buscar por texto, radicado, año, palabra clave…"

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

    # ── Atajos ────────────────────────────────────────────────────────────

    def _bind_shortcuts(self):
        for seq in ("<Control-f>", "<Control-F>"):
            self.bind(seq, lambda e: (
                self._entry.focus(),
                self._ph_clear(),
                self._entry.select_range(0, "end")))
        for seq in ("<Control-i>", "<Control-I>"):
            self.bind(seq, lambda e: self._open_index_dialog())
        self.bind("<Escape>", lambda e: (
            self._entry.delete(0, "end"),
            self._ph_restore()))

    # ── Carga índice ──────────────────────────────────────────────────────

    def _load_index_async(self):
        def _worker():
            ok = self.index.load()
            self.after(0, self._after_load, ok)
        threading.Thread(target=_worker, daemon=True).start()

    def _after_load(self, ok: bool):
        if ok:
            n = len(self.index.docs)
            self._status_var.set(f"Índice activo  🟢  ({n:,} docs)")
            self._index_info_var.set(
                f"Documentos: {n:,}\n"
                f"Carpetas: {len(self.index.folders)}\n"
                f"Caché: {INDEX_FILE.name}")
        else:
            self._status_var.set("Sin índice  🔴")
            self._index_info_var.set(
                "Sin índice.\nUsa 📂 Indexar\npara construirlo.")

    # ── Búsqueda ──────────────────────────────────────────────────────────

    def _do_search(self):
        if self._placeholder_on:
            query = ""
        else:
            query = self._search_var.get().strip()

        if not self.index.docs:
            messagebox.showwarning(
                "Sin índice",
                "No hay documentos indexados.\n"
                "Usa '📂 Indexar' para seleccionar una carpeta.")
            return

        if self._search_thread and self._search_thread.is_alive():
            return  # ya hay búsqueda activa

        self._btn_search.configure(state="disabled")
        self._searching_lbl.configure(text="Buscando…")
        self._listbox.delete(0, "end")
        self._clear_preview()

        year = self._year_var.get()
        ext  = self._ext_var.get()

        def _worker():
            t0 = time.perf_counter()
            results = self.index.search(query, year, ext)
            elapsed = time.perf_counter() - t0
            self.after(0, self._show_results, results, elapsed)

        self._search_thread = threading.Thread(target=_worker, daemon=True)
        self._search_thread.start()

    def _show_results(self, results: list, elapsed: float):
        self._btn_search.configure(state="normal")
        self._results = results
        n = len(results)
        self._results_lbl.configure(
            text=f"Resultados ({n}{'+' if n >= MAX_RESULTS else ''})")
        self._searching_lbl.configure(text=f"{elapsed*1000:.0f} ms")

        icons = {"pdf":"📕","docx":"📘","doc":"📗","txt":"📄"}
        for r in results:
            ico = icons.get(r["ext"].lstrip("."), "📎")
            self._listbox.insert("end", f" {ico}  {r['name']}")

    # ── Selección ─────────────────────────────────────────────────────────

    def _on_select(self, event=None):
        sel = self._listbox.curselection()
        if not sel or sel[0] >= len(self._results):
            return
        doc = self._results[sel[0]]
        self._selected_path.set(doc["path"])
        self._update_meta(doc)

        query = "" if self._placeholder_on else self._search_var.get()
        threading.Thread(
            target=self._load_preview,
            args=(doc["path"], query),
            daemon=True).start()

    def _update_meta(self, doc: dict):
        self._meta_vars["name"].set(doc["name"])
        self._meta_vars["ext"].set(doc["ext"])
        sz = doc["size"]
        self._meta_vars["size"].set(
            f"{sz/1_048_576:.1f} MB" if sz > 1_048_576 else
            f"{sz/1024:.1f} KB"      if sz > 1024        else
            f"{sz} B")
        self._meta_vars["mtime"].set(
            datetime.fromtimestamp(doc["mtime"]).strftime("%Y-%m-%d  %H:%M"))
        self._meta_vars["path"].set(doc["path"])

    def _load_preview(self, path: str, query: str):
        text = read_file_safe(path, PREVIEW_MAX)
        self.after(0, self._render_preview, path, text, query)

    def _render_preview(self, path: str, text: str, query: str):
        self._preview_lbl.configure(text=Path(path).name)
        self._preview_txt.configure(state="normal")
        self._preview_txt.delete("1.0", "end")
        self._preview_txt.insert("end", text[:PREVIEW_MAX] or
                                  "[No se pudo leer el contenido]")
        terms = [t.strip() for t in query.split() if t.strip()]
        for term in terms:
            start = "1.0"
            while True:
                pos = self._preview_txt.search(
                    term, start, nocase=True, stopindex="end")
                if not pos:
                    break
                end = f"{pos}+{len(term)}c"
                self._preview_txt.tag_add("highlight", pos, end)
                start = end
        if terms:
            first = self._preview_txt.search(
                terms[0], "1.0", nocase=True, stopindex="end")
            if first:
                self._preview_txt.see(first)
        self._preview_txt.configure(state="disabled")

    def _clear_preview(self):
        self._preview_lbl.configure(text="Vista previa")
        self._preview_txt.configure(state="normal")
        self._preview_txt.delete("1.0", "end")
        self._preview_txt.configure(state="disabled")
        for v in self._meta_vars.values():
            v.set("")

    # ── Abrir archivos ────────────────────────────────────────────────────

    def _open_file(self):
        path = self._selected_path.get()
        if not path or not os.path.exists(path):
            messagebox.showwarning("Sin selección",
                                   "Selecciona un documento primero.")
            return
        try:
            if os.name == "nt":
                os.startfile(path)
            else:
                subprocess.Popen(
                    ["open" if os.uname().sysname == "Darwin"
                     else "xdg-open", path])
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo abrir:\n{e}")

    def _open_location(self):
        path = self._selected_path.get()
        if not path:
            return
        try:
            if os.name == "nt":
                subprocess.Popen(["explorer", "/select,", path])
            else:
                subprocess.Popen(
                    ["open" if os.uname().sysname == "Darwin"
                     else "xdg-open", str(Path(path).parent)])
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo abrir ubicación:\n{e}")

    def _toggle_favorite(self):
        path = self._selected_path.get()
        if path:
            messagebox.showinfo("Favorito",
                                f"⭐ Marcado:\n{Path(path).name}")

    # ── Indexación ────────────────────────────────────────────────────────

    def _open_index_dialog(self):
        if self.index.folders:
            resp = messagebox.askyesnocancel(
                "Indexar",
                "Carpetas actuales:\n" +
                "\n".join(f"  • {f}" for f in self.index.folders) +
                "\n\n¿Reindexar las mismas?\n(No = elegir nuevas)")
            if resp is None:
                return
            folders = self.index.folders[:] if resp else self._pick_folders()
        else:
            folders = self._pick_folders()

        if folders:
            IndexProgressDialog(self, self.index, folders,
                                on_done=self._after_index)

    def _pick_folders(self) -> list:
        folders = []
        while True:
            f = filedialog.askdirectory(
                title="Selecciona una carpeta para indexar")
            if not f:
                break
            folders.append(f)
            if not messagebox.askyesno("Más carpetas",
                                       "¿Agregar otra carpeta?"):
                break
        return folders

    def _after_index(self, n: int):
        self._status_var.set(f"Índice actualizado  🟢  ({n:,} docs)")
        self._index_info_var.set(
            f"Documentos: {n:,}\n"
            f"Carpetas: {len(self.index.folders)}\n"
            f"Caché: {INDEX_FILE.name}")


# ─── Modal de progreso ────────────────────────────────────────────────────────

class IndexProgressDialog(tk.Toplevel):

    def __init__(self, master, index, folders, on_done=None):
        super().__init__(master)
        self.index    = index
        self.folders  = folders
        self.on_done  = on_done
        self._cancel  = threading.Event()

        self.title("Indexando…")
        self.geometry("480x260")
        self.resizable(False, False)
        self.configure(bg=C["bg"])
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self._cancel_op)

        tk.Label(self, text="📂 Construyendo índice…",
                 bg=C["bg"], fg=C["text"],
                 font=("Segoe UI", 11, "bold")).pack(pady=(20,8))

        tk.Label(self,
                 text="\n".join(folders[:3]),
                 bg=C["bg"], fg=C["text2"],
                 font=C["font_sm"], wraplength=440,
                 justify="center").pack()

        self._file_lbl = tk.Label(self, text="Preparando…",
                                  bg=C["bg"], fg=C["accent"],
                                  font=C["font_sm"], wraplength=440)
        self._file_lbl.pack(pady=4)

        self._pv = tk.DoubleVar()
        ttk.Progressbar(self, variable=self._pv,
                        maximum=100, length=420,
                        mode="determinate").pack(pady=8)

        self._count_lbl = tk.Label(self, text="0 / ?",
                                   bg=C["bg"], fg=C["text2"],
                                   font=C["font_sm"])
        self._count_lbl.pack()

        tk.Button(self, text="Cancelar", command=self._cancel_op,
                  bg=C["danger"], fg="white", relief="flat",
                  font=C["font"], padx=12, pady=4).pack(pady=12)

        threading.Thread(target=self._worker, daemon=True).start()

    def _worker(self):
        def progress(cur, total, fname):
            pct = cur / total * 100 if total else 0
            self.after(0, self._upd, cur, total, fname, pct)
        n = self.index.build(self.folders, progress, self._cancel)
        self.after(0, self._done, n)

    def _upd(self, cur, total, fname, pct):
        if not self.winfo_exists():
            return
        self._file_lbl.configure(text=fname[:70])
        self._pv.set(pct)
        self._count_lbl.configure(text=f"{cur:,} / {total:,}")

    def _done(self, n):
        if self.winfo_exists():
            self.destroy()
        if self.on_done:
            self.on_done(n)

    def _cancel_op(self):
        self._cancel.set()
        self.destroy()


# ─── Estilos ttk ─────────────────────────────────────────────────────────────

def _apply_style():
    s = ttk.Style()
    s.theme_use("default")
    s.configure("TScrollbar",
                background=C["border"],
                troughcolor=C["bg"],
                arrowcolor=C["text2"],
                borderwidth=0)
    s.configure("TCombobox",
                fieldbackground=C["bg"],
                background=C["bg"],
                foreground=C["text"],
                selectbackground=C["sel"],
                arrowcolor=C["text2"])
    s.map("TCombobox",
          fieldbackground=[("readonly", C["bg"])],
          foreground=[("readonly", C["text"])])


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = JurisBotApp()
    _apply_style()
    app.mainloop()