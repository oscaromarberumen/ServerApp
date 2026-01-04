# file_server_gui_tabs_fast.py
# GUI (Tkinter) con pestañas + servidor Flask + cliente (subir/descargar)
# - Pestañas: Servidor / Archivos
# - Barra de estado inferior (incluye "Computadoras conectadas")
# - Rendimiento: NO bloquea la UI (poll de clientes en thread + cola + update con after)
#
# Requisitos:
#   pip install flask requests
#
# Ejecutar:
#   python file_server_gui_tabs_fast.py

import os
import threading
import socket
import time
import queue
from pathlib import Path
from datetime import datetime

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

import requests
from flask import Flask, jsonify, request, send_from_directory, abort
from werkzeug.serving import make_server
from werkzeug.utils import secure_filename


# ----------------------------
# Utilidades
# ----------------------------
def get_local_ip() -> str:
    """Obtiene IP LAN para mostrar URL útil."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def fmt_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(n)
    for u in units:
        if size < 1024.0:
            return f"{size:.1f} {u}" if u != "B" else f"{int(size)} {u}"
        size /= 1024.0
    return f"{size:.1f} PB"


def fmt_mtime(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def now_ts() -> float:
    return time.time()


# ----------------------------
# Servidor Flask controlable + tracking de clientes
# ----------------------------
class FlaskServer:
    """
    Servidor HTTP para listar/subir/descargar archivos.
    Tracking de "computadoras conectadas": IPs que han hecho requests en los últimos CLIENT_TTL segundos.
    """
    CLIENT_TTL = 120  # segundos activos para contar como conectado

    def __init__(self):
        self._server = None
        self._thread = None
        self._running = False

        self.base_dir = Path.cwd()
        self.host = "0.0.0.0"
        self.port = 8000
        self.token = ""  # opcional

        self._clients_lock = threading.Lock()
        self._clients = {}  # ip -> {"last_seen": ts}

        self.app = None

    def _purge_clients_locked(self):
        cutoff = now_ts() - self.CLIENT_TTL
        dead = [ip for ip, meta in self._clients.items() if meta.get("last_seen", 0) < cutoff]
        for ip in dead:
            self._clients.pop(ip, None)

    def clients_snapshot(self):
        """Devuelve (count, [ips]) de clientes activos."""
        with self._clients_lock:
            self._purge_clients_locked()
            ips = sorted(self._clients.keys())
            return len(ips), ips

    def _build_app(self) -> Flask:
        app = Flask(__name__)

        base_dir = self.base_dir
        token = self.token.strip()

        def require_token():
            if token:
                got = request.headers.get("X-Token", "")
                if got != token:
                    abort(401, description="Token inválido o faltante.")

        @app.before_request
        def track_client():
            ip = request.remote_addr or "unknown"
            with self._clients_lock:
                self._clients[ip] = {"last_seen": now_ts()}
                self._purge_clients_locked()

        @app.get("/")
        def home():
            return (
                "<h2>Servidor de Archivos Activo</h2>"
                "<ul>"
                "<li><a href='/api/health'>/api/health</a></li>"
                "<li><a href='/api/list'>/api/list</a></li>"
                "<li><a href='/api/clients'>/api/clients</a></li>"
                "</ul>"
                "<p>Usa la app GUI para subir/descargar.</p>"
            )

        @app.get("/api/health")
        def health():
            return jsonify(ok=True)

        @app.get("/api/clients")
        def api_clients():
            require_token()
            count, ips = self.clients_snapshot()
            return jsonify(count=count, ips=ips, ttl_seconds=self.CLIENT_TTL)

        @app.get("/api/list")
        def api_list():
            require_token()
            items = []
            base_dir.mkdir(parents=True, exist_ok=True)
            for p in sorted(base_dir.iterdir()):
                if p.is_file():
                    st = p.stat()
                    items.append({"name": p.name, "size": st.st_size, "mtime": st.st_mtime})
            return jsonify(items=items, base=str(base_dir))

        @app.get("/api/download/<path:filename>")
        def api_download(filename):
            require_token()
            safe = secure_filename(filename)
            if not safe:
                abort(400, description="Nombre de archivo inválido.")

            full = (base_dir / safe).resolve()
            if not str(full).startswith(str(base_dir.resolve())):
                abort(400, description="Ruta inválida.")
            if not full.exists() or not full.is_file():
                abort(404, description="Archivo no encontrado.")

            return send_from_directory(base_dir, safe, as_attachment=True)

        @app.post("/api/upload")
        def api_upload():
            require_token()
            if "file" not in request.files:
                abort(400, description="No se recibió 'file' en multipart/form-data.")
            f = request.files["file"]
            if not f.filename:
                abort(400, description="Nombre de archivo vacío.")

            safe = secure_filename(f.filename)
            if not safe:
                abort(400, description="Nombre de archivo inválido.")

            base_dir.mkdir(parents=True, exist_ok=True)
            target = (base_dir / safe).resolve()
            if not str(target).startswith(str(base_dir.resolve())):
                abort(400, description="Ruta inválida.")

            f.save(target)
            return jsonify(ok=True, saved=safe)

        return app

    def start(self, base_dir: str, host: str, port: int, token: str = ""):
        if self._running:
            raise RuntimeError("El servidor ya está corriendo.")

        self.base_dir = Path(base_dir).resolve()
        self.host = host
        self.port = int(port)
        self.token = token or ""

        self.app = self._build_app()
        self._server = make_server(self.host, self.port, self.app)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        self._running = True

    def stop(self):
        if not self._running:
            return
        try:
            self._server.shutdown()
        except Exception:
            pass
        self._server = None
        self._thread = None
        self._running = False
        with self._clients_lock:
            self._clients.clear()

    @property
    def running(self) -> bool:
        return self._running


# ----------------------------
# GUI con pestañas + status bar + polling rápido (NO bloquea UI)
# ----------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("QuickTransfer Server v1.0")
        self.geometry("1040x700")
        self.minsize(980, 620)

        self.server = FlaskServer()
        self.local_ip = get_local_ip()

        # Vars
        self.share_dir_var = tk.StringVar(value=str((Path.home() / "ServidorCompartido").resolve()))
        self.host_var = tk.StringVar(value="0.0.0.0")
        self.port_var = tk.StringVar(value="8000")
        self.server_token_var = tk.StringVar(value="")  # opcional

        self.client_url_var = tk.StringVar(value="http://127.0.0.1:8000")
        self.client_token_var = tk.StringVar(value="")

        # Status vars
        self.status_left_var = tk.StringVar(value="Listo.")
        self.status_mid_var = tk.StringVar(value="")
        self.status_right_var = tk.StringVar(value="Computadoras conectadas: 0")

        # Poller (background)
        self._poll_stop = threading.Event()
        self._poll_q = queue.Queue(maxsize=1)
        self._poll_thread = None
        self._session = requests.Session()
        self._last_clients_snapshot = None  # (count, tuple(ips))

        self._build_styles()
        self._build_ui()

        self._sync_urls()
        self._start_clients_poller()
        self.after(200, self._process_clients_poll_results)

        self.protocol("WM_DELETE_WINDOW", self._on_close)

    # ----------------------------
    # Estilo
    # ----------------------------
    def _build_styles(self):
        style = ttk.Style()
        preferred = ["clam", "vista", "xpnative"]
        for th in preferred:
            if th in style.theme_names():
                style.theme_use(th)
                break

        style.configure("TLabelframe.Label", font=("Segoe UI", 10, "bold"))
        style.configure("Header.TLabel", font=("Segoe UI", 11, "bold"))
        style.configure("Hint.TLabel", font=("Segoe UI", 9))
        style.configure("Status.TLabel", font=("Segoe UI", 9))
        style.configure("TButton", padding=(10, 6))

    # ----------------------------
    # UI
    # ----------------------------
    def _build_ui(self):
        root = ttk.Frame(self, padding=12)
        root.pack(fill="both", expand=True)

        self.nb = ttk.Notebook(root)
        self.nb.pack(fill="both", expand=True)

        self.tab_server = ttk.Frame(self.nb, padding=10)
        self.tab_files = ttk.Frame(self.nb, padding=10)

        self.nb.add(self.tab_server, text="Servidor")
        self.nb.add(self.tab_files, text="Archivos")

        self._build_tab_server()
        self._build_tab_files()

        ttk.Separator(root, orient="horizontal").pack(fill="x", pady=(10, 6))

        sb = ttk.Frame(root, padding=(6, 0))
        sb.pack(fill="x")
        ttk.Label(sb, textvariable=self.status_left_var, style="Status.TLabel").pack(side="left")
        ttk.Label(sb, textvariable=self.status_mid_var, style="Status.TLabel").pack(side="left", padx=(18, 0))
        ttk.Label(sb, textvariable=self.status_right_var, style="Status.TLabel").pack(side="right")

    def _build_tab_server(self):
        box = ttk.LabelFrame(self.tab_server, text="Configuración del Servidor")
        box.pack(fill="x", pady=(0, 10))

        row1 = ttk.Frame(box)
        row1.pack(fill="x", pady=(0, 8))
        ttk.Label(row1, text="Carpeta compartida:", style="Header.TLabel").pack(side="left")
        ttk.Entry(row1, textvariable=self.share_dir_var).pack(side="left", fill="x", expand=True, padx=10)
        ttk.Button(row1, text="Buscar...", command=self._browse_dir).pack(side="left")

        row2 = ttk.Frame(box)
        row2.pack(fill="x", pady=(0, 8))
        ttk.Label(row2, text="Host:", style="Hint.TLabel").pack(side="left")
        ttk.Entry(row2, width=14, textvariable=self.host_var).pack(side="left", padx=(6, 18))
        ttk.Label(row2, text="Puerto:", style="Hint.TLabel").pack(side="left")
        ttk.Entry(row2, width=10, textvariable=self.port_var).pack(side="left", padx=(6, 18))
        ttk.Label(row2, text="Token (opcional):", style="Hint.TLabel").pack(side="left")
        ttk.Entry(row2, width=22, textvariable=self.server_token_var).pack(side="left", padx=(6, 18))

        row3 = ttk.Frame(box)
        row3.pack(fill="x")
        ttk.Button(row3, text="Iniciar servidor", command=self._start_server).pack(side="left")
        ttk.Button(row3, text="Detener", command=self._stop_server).pack(side="left", padx=(10, 0))
        ttk.Button(row3, text="Copiar URL LAN", command=self._copy_lan_url).pack(side="left", padx=(20, 0))
        ttk.Button(row3, text="Abrir /api/health", command=self._open_health).pack(side="left", padx=(10, 0))

        info = ttk.LabelFrame(self.tab_server, text="Información")
        info.pack(fill="x", pady=(0, 10))

        self.url_local_var = tk.StringVar(value="")
        self.url_lan_var = tk.StringVar(value="")

        r = ttk.Frame(info)
        r.pack(fill="x", pady=(4, 2))
        ttk.Label(r, text="URL Local:", width=12).pack(side="left")
        ttk.Entry(r, textvariable=self.url_local_var, state="readonly").pack(side="left", fill="x", expand=True, padx=8)

        r2 = ttk.Frame(info)
        r2.pack(fill="x", pady=(4, 2))
        ttk.Label(r2, text="URL LAN:", width=12).pack(side="left")
        ttk.Entry(r2, textvariable=self.url_lan_var, state="readonly").pack(side="left", fill="x", expand=True, padx=8)

        clients_box = ttk.LabelFrame(self.tab_server, text="Computadoras conectadas (IPs activas)")
        clients_box.pack(fill="both", expand=True)

        self.clients_list = tk.Listbox(clients_box, height=10)
        self.clients_list.pack(fill="both", expand=True, padx=6, pady=6)

    def _build_tab_files(self):
        top = ttk.LabelFrame(self.tab_files, text="Conexión al Servidor")
        top.pack(fill="x", pady=(0, 10))

        row = ttk.Frame(top)
        row.pack(fill="x", pady=(2, 2))
        ttk.Label(row, text="URL del servidor:").pack(side="left")
        ttk.Entry(row, textvariable=self.client_url_var, width=46).pack(side="left", padx=8)
        ttk.Label(row, text="Token:").pack(side="left")
        ttk.Entry(row, textvariable=self.client_token_var, width=18).pack(side="left", padx=8)
        ttk.Button(row, text="Conectar / Refrescar", command=self._refresh_list).pack(side="left", padx=(8, 0))

        mid = ttk.LabelFrame(self.tab_files, text="Administrador de Archivos (Subir / Descargar)")
        mid.pack(fill="both", expand=True)

        columns = ("name", "size", "mtime")
        self.tree = ttk.Treeview(mid, columns=columns, show="headings", height=18)
        self.tree.heading("name", text="Archivo")
        self.tree.heading("size", text="Tamaño")
        self.tree.heading("mtime", text="Modificado")
        self.tree.column("name", width=580, anchor="w")
        self.tree.column("size", width=120, anchor="e")
        self.tree.column("mtime", width=230, anchor="center")

        vsb = ttk.Scrollbar(mid, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)

        self.tree.pack(side="left", fill="both", expand=True, padx=(6, 0), pady=6)
        vsb.pack(side="left", fill="y", pady=6)

        actions = ttk.Frame(mid, padding=6)
        actions.pack(side="left", fill="y", padx=(10, 6), pady=6)

        ttk.Button(actions, text="Subir archivo...", command=self._upload_file).pack(fill="x", pady=(0, 8))
        ttk.Button(actions, text="Descargar seleccionado...", command=self._download_selected).pack(fill="x", pady=(0, 8))
        ttk.Button(actions, text="Refrescar lista", command=self._refresh_list).pack(fill="x", pady=(0, 14))

        ttk.Label(actions, text="Tip:", font=("Segoe UI", 9, "bold")).pack(anchor="w")
        ttk.Label(actions, text="Si usas Token en el servidor,\npon el mismo aquí.", justify="left").pack(anchor="w", pady=(4, 0))

    # ----------------------------
    # Helpers GUI
    # ----------------------------
    def _browse_dir(self):
        d = filedialog.askdirectory(title="Selecciona la carpeta a compartir")
        if d:
            self.share_dir_var.set(d)

    def _sync_urls(self):
        port = (self.port_var.get().strip() or "8000")
        local_url = f"http://127.0.0.1:{port}"
        lan_url = f"http://{self.local_ip}:{port}"
        self.url_local_var.set(local_url)
        self.url_lan_var.set(lan_url)
        self.status_mid_var.set(f"URL LAN: {lan_url}")

        if self.client_url_var.get().strip() in ("", "http://127.0.0.1:8000", local_url):
            self.client_url_var.set(local_url)

    def _set_status(self, msg: str):
        self.status_left_var.set(msg)

    def _headers(self):
        t = self.client_token_var.get().strip()
        return {"X-Token": t} if t else {}

    def _api_base(self) -> str:
        return self.client_url_var.get().strip().rstrip("/")

    # ----------------------------
    # Servidor: start/stop
    # ----------------------------
    def _start_server(self):
        if self.server.running:
            messagebox.showinfo("Servidor", "El servidor ya está corriendo.")
            return

        base_dir = self.share_dir_var.get().strip()
        host = self.host_var.get().strip() or "0.0.0.0"
        port_str = self.port_var.get().strip() or "8000"
        token = self.server_token_var.get()

        try:
            port = int(port_str)
            if not (1 <= port <= 65535):
                raise ValueError
        except ValueError:
            messagebox.showerror("Error", "Puerto inválido. Usa un número entre 1 y 65535.")
            return

        try:
            Path(base_dir).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo usar/crear la carpeta:\n{e}")
            return

        try:
            self.server.start(base_dir=base_dir, host=host, port=port, token=token)
            self._sync_urls()

            if token.strip() and not self.client_token_var.get().strip():
                self.client_token_var.set(token.strip())

            self._set_status(f"Servidor corriendo. Carpeta: {Path(base_dir).resolve()}")
        except OSError as e:
            messagebox.showerror("Error", f"No se pudo iniciar el servidor (¿puerto ocupado?):\n{e}")
        except Exception as e:
            messagebox.showerror("Error", f"Fallo al iniciar servidor:\n{e}")

    def _stop_server(self):
        if not self.server.running:
            self._set_status("Servidor detenido.")
            self.status_right_var.set("Computadoras conectadas: 0")
            self.clients_list.delete(0, tk.END)
            return
        self.server.stop()
        self._set_status("Servidor detenido.")
        self.status_right_var.set("Computadoras conectadas: 0")
        self.clients_list.delete(0, tk.END)

    def _copy_lan_url(self):
        self.clipboard_clear()
        self.clipboard_append(self.url_lan_var.get())
        messagebox.showinfo("Copiado", f"URL LAN copiada:\n{self.url_lan_var.get()}")

    def _open_health(self):
        import webbrowser
        base = (self.url_local_var.get().strip().rstrip("/")) or "http://127.0.0.1:8000"
        webbrowser.open(f"{base}/api/health")

    # ----------------------------
    # Cliente: listar / subir / descargar
    # ----------------------------
    def _refresh_list(self, silent: bool = False):
        base = self._api_base()
        try:
            r = self._session.get(f"{base}/api/list", headers=self._headers(), timeout=8)
            if r.status_code == 401:
                if not silent:
                    messagebox.showerror("Auth", "Token inválido o faltante.")
                self._set_status("Error: Token inválido o faltante.")
                return
            r.raise_for_status()
            data = r.json()
            items = data.get("items", [])

            for iid in self.tree.get_children():
                self.tree.delete(iid)

            for it in items:
                name = it.get("name", "")
                size = fmt_bytes(int(it.get("size", 0)))
                mtime = fmt_mtime(float(it.get("mtime", 0)))
                self.tree.insert("", "end", values=(name, size, mtime))

            self._set_status(f"Lista actualizada: {len(items)} archivo(s).")
        except requests.exceptions.ConnectionError:
            if not silent:
                messagebox.showerror("Conexión", "No se pudo conectar al servidor.\nVerifica URL, puerto y firewall.")
            self._set_status("Error: no se pudo conectar al servidor.")
        except Exception as e:
            if not silent:
                messagebox.showerror("Error", f"Fallo al refrescar lista:\n{e}")
            self._set_status("Error al refrescar lista.")

    def _upload_file(self):
        base = self._api_base()
        file_path = filedialog.askopenfilename(title="Selecciona un archivo para subir")
        if not file_path:
            return

        try:
            with open(file_path, "rb") as f:
                files = {"file": (os.path.basename(file_path), f)}
                r = self._session.post(f"{base}/api/upload", headers=self._headers(), files=files, timeout=60)

            if r.status_code == 401:
                messagebox.showerror("Auth", "Token inválido o faltante.")
                self._set_status("Error: Token inválido o faltante.")
                return
            r.raise_for_status()

            self._refresh_list(silent=True)
            self._set_status("Archivo subido correctamente.")
            messagebox.showinfo("Subida", "Archivo subido correctamente.")
        except requests.exceptions.ConnectionError:
            messagebox.showerror("Conexión", "No se pudo conectar al servidor.")
            self._set_status("Error: no se pudo conectar al servidor.")
        except Exception as e:
            messagebox.showerror("Error", f"Fallo al subir:\n{e}")
            self._set_status("Error al subir archivo.")

    def _download_selected(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showwarning("Descarga", "Selecciona un archivo primero.")
            return

        base = self._api_base()
        filename = self.tree.item(sel[0], "values")[0]

        save_path = filedialog.asksaveasfilename(
            title="Guardar como...",
            initialfile=filename,
            defaultextension="",
        )
        if not save_path:
            return

        try:
            with self._session.get(
                f"{base}/api/download/{filename}",
                headers=self._headers(),
                stream=True,
                timeout=60
            ) as r:
                if r.status_code == 401:
                    messagebox.showerror("Auth", "Token inválido o faltante.")
                    self._set_status("Error: Token inválido o faltante.")
                    return
                r.raise_for_status()

                with open(save_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)

            self._set_status("Archivo descargado correctamente.")
            messagebox.showinfo("Descarga", f"Archivo descargado:\n{save_path}")
        except requests.exceptions.ConnectionError:
            messagebox.showerror("Conexión", "No se pudo conectar al servidor.")
            self._set_status("Error: no se pudo conectar al servidor.")
        except Exception as e:
            messagebox.showerror("Error", f"Fallo al descargar:\n{e}")
            self._set_status("Error al descargar archivo.")

    # ----------------------------
    # Polling de clientes (NO bloquea la UI)
    # ----------------------------
    def _start_clients_poller(self):
        if self._poll_thread and self._poll_thread.is_alive():
            return
        self._poll_stop.clear()
        self._poll_thread = threading.Thread(target=self._clients_poller_loop, daemon=True)
        self._poll_thread.start()

    def _clients_poller_loop(self):
        while not self._poll_stop.is_set():
            base = self._api_base()
            timeout_s = 0.8

            count = 0
            ips = []

            # Fast path: lectura directa si aplica (mismo proceso)
            try:
                if self.server.running:
                    port = int(self.port_var.get().strip() or "8000")
                    if f":{port}" in base and ("127.0.0.1" in base or "localhost" in base or self.local_ip in base):
                        count, ips = self.server.clients_snapshot()
                    else:
                        raise RuntimeError("usar HTTP")
                else:
                    raise RuntimeError("usar HTTP")
            except Exception:
                try:
                    r = self._session.get(f"{base}/api/clients", headers=self._headers(), timeout=timeout_s)
                    if r.status_code == 200:
                        data = r.json()
                        count = int(data.get("count", 0))
                        ips = data.get("ips", []) or []
                    else:
                        count, ips = 0, []
                except Exception:
                    count, ips = 0, []

            snapshot = (count, tuple(ips))

            if snapshot != self._last_clients_snapshot:
                self._last_clients_snapshot = snapshot
                try:
                    while not self._poll_q.empty():
                        self._poll_q.get_nowait()
                    self._poll_q.put_nowait(snapshot)
                except Exception:
                    pass

            self._poll_stop.wait(1.5)

    def _process_clients_poll_results(self):
        try:
            snapshot = self._poll_q.get_nowait()
        except Exception:
            snapshot = None

        if snapshot:
            count, ips = snapshot
            self.status_right_var.set(f"Computadoras conectadas: {count}")

            # Solo repinta la lista si estás en pestaña Servidor
            try:
                tab_text = self.nb.tab(self.nb.select(), "text")
            except Exception:
                tab_text = ""

            if tab_text == "Servidor":
                self.clients_list.delete(0, tk.END)
                for ip in ips:
                    self.clients_list.insert(tk.END, ip)

        self.after(200, self._process_clients_poll_results)

    # ----------------------------
    # Cierre
    # ----------------------------
    def _on_close(self):
        try:
            self._poll_stop.set()
        except Exception:
            pass
        try:
            self.server.stop()
        except Exception:
            pass
        self.destroy()


if __name__ == "__main__":
    app = App()
    app.mainloop()
