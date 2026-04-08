"""
Tkinter-based configuration panel for the Windows executable.
Provides real-time execution feedback with a hacker-style UI.
"""
from __future__ import annotations

from datetime import datetime
import logging
import os
import queue
import threading
import tkinter as tk
from tkinter import messagebox
from zoneinfo import ZoneInfo

from config import DEFAULT_APPS_SCRIPT_URL, get_default_env_path, save_env_file
from loop_runner import LoopRunner


PANEL_FIELD_LABELS = {
    'APPS_SCRIPT_TOKEN': 'Apps Script Token',
    'GREENMILE_URL': 'GreenMile URL',
    'GREENMILE_USERNAME': 'GreenMile Usuario',
    'GREENMILE_PASSWORD': 'GreenMile Senha',
    'CHUNK_SIZE': 'Chunk Size',
    'REQUEST_TIMEOUT': 'Request Timeout',
    'MAX_RETRIES': 'Max Retries',
    'LOOP_INTERVAL': 'Loop Interval (s)',
    'SNAPSHOT_REUSE_TTL_SECONDS': 'TTL Cache Snapshots (s)',
}

PANEL_INDICATOR_LABELS = {
    'pending_rows': 'Linhas pendentes',
    'moved_rows': 'Movimentados',
    'last_post_succeeded_at': 'Ultimo POST OK',
}


def format_indicator_datetime_br(value) -> str:
    text = str(value or '').strip()
    if not text or text == '-':
        return '-'
    try:
        if text.endswith('Z'):
            dt = datetime.fromisoformat(text.replace('Z', '+00:00'))
        else:
            dt = datetime.fromisoformat(text)
        dt_br = dt.astimezone(ZoneInfo('America/Sao_Paulo'))
        return dt_br.strftime('%d/%m/%Y %H:%M:%S')
    except Exception:
        return text


class QueueLogHandler(logging.Handler):
    def __init__(self, target_queue: queue.Queue):
        super().__init__()
        self.target_queue = target_queue

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.target_queue.put_nowait(self.format(record))
        except Exception:
            pass


class ConfigPanel:
    def __init__(self, env_path: str | None = None, run_callback=None, auto_run: bool = False):
        self.env_path = env_path or get_default_env_path()
        self.run_callback = run_callback
        self.auto_run = auto_run
        self.root = tk.Tk()
        self.root.title('GREENMILE SYNC :: HACKER TERMINAL')
        self.root.geometry('980x760')
        self.root.configure(bg='#020602')
        self.root.minsize(900, 680)
        self._saved_values = {}
        self._running = False
        self._loop_runner: LoopRunner | None = None
        self._log_queue: queue.Queue[str] = queue.Queue()
        self._log_handler: QueueLogHandler | None = None
        self._build()
        self._load_existing_values()

    def _build(self) -> None:
        root = self.root

        title = tk.Label(
            root,
            text='[ GREENMILE SYNC :: LIVE OPS CONSOLE ]',
            bg='#020602',
            fg='#58ff7e',
            font=('Consolas', 20, 'bold'),
            pady=16,
        )
        title.pack(fill='x')

        subtitle = tk.Label(
            root,
            text='Painel local com configuracao persistente, log em tempo real e execucao assistida.',
            bg='#020602',
            fg='#b7ffbf',
            font=('Consolas', 10),
        )
        subtitle.pack(fill='x', padx=18)

        fixed_url = tk.Label(
            root,
            text=f'APPS_SCRIPT_URL [FIXO] => {DEFAULT_APPS_SCRIPT_URL}',
            bg='#061006',
            fg='#37ff63',
            anchor='w',
            justify='left',
            wraplength=920,
            padx=12,
            pady=10,
            font=('Consolas', 9),
        )
        fixed_url.pack(fill='x', padx=18, pady=(12, 10))

        form_shell = tk.Frame(root, bg='#020602')
        form_shell.pack(fill='x', padx=18)

        self.vars = {
            'APPS_SCRIPT_TOKEN': tk.StringVar(),
            'GREENMILE_URL': tk.StringVar(value='https://3coracoes.greenmile.com'),
            'GREENMILE_USERNAME': tk.StringVar(),
            'GREENMILE_PASSWORD': tk.StringVar(),
            'CHUNK_SIZE': tk.StringVar(value='50'),
            'REQUEST_TIMEOUT': tk.StringVar(value='75'),
            'MAX_RETRIES': tk.StringVar(value='3'),
            'LOOP_INTERVAL': tk.StringVar(value='300'),
            'SNAPSHOT_REUSE_TTL_SECONDS': tk.StringVar(value='600'),
        }

        self._add_field(form_shell, 0, PANEL_FIELD_LABELS['APPS_SCRIPT_TOKEN'], self.vars['APPS_SCRIPT_TOKEN'])
        self._add_field(form_shell, 1, PANEL_FIELD_LABELS['GREENMILE_URL'], self.vars['GREENMILE_URL'])
        self._add_field(form_shell, 2, PANEL_FIELD_LABELS['GREENMILE_USERNAME'], self.vars['GREENMILE_USERNAME'])
        self._add_field(form_shell, 3, PANEL_FIELD_LABELS['GREENMILE_PASSWORD'], self.vars['GREENMILE_PASSWORD'], show='*')
        self._add_field(form_shell, 4, PANEL_FIELD_LABELS['CHUNK_SIZE'], self.vars['CHUNK_SIZE'])
        self._add_field(form_shell, 5, PANEL_FIELD_LABELS['REQUEST_TIMEOUT'], self.vars['REQUEST_TIMEOUT'])
        self._add_field(form_shell, 6, PANEL_FIELD_LABELS['MAX_RETRIES'], self.vars['MAX_RETRIES'])
        self._add_field(form_shell, 7, PANEL_FIELD_LABELS['LOOP_INTERVAL'], self.vars['LOOP_INTERVAL'])
        self._add_field(form_shell, 8, PANEL_FIELD_LABELS['SNAPSHOT_REUSE_TTL_SECONDS'], self.vars['SNAPSHOT_REUSE_TTL_SECONDS'])

        indicators = tk.Frame(root, bg='#020602')
        indicators.pack(fill='x', padx=18, pady=(4, 10))
        self.indicator_vars = {
            'pending_rows': tk.StringVar(value='-'),
            'moved_rows': tk.StringVar(value='-'),
            'last_post_succeeded_at': tk.StringVar(value='-'),
        }
        self._add_indicator(indicators, 0, PANEL_INDICATOR_LABELS['pending_rows'], self.indicator_vars['pending_rows'])
        self._add_indicator(indicators, 1, PANEL_INDICATOR_LABELS['moved_rows'], self.indicator_vars['moved_rows'])
        self._add_indicator(indicators, 2, PANEL_INDICATOR_LABELS['last_post_succeeded_at'], self.indicator_vars['last_post_succeeded_at'])

        actions = tk.Frame(root, bg='#020602')
        actions.pack(fill='x', padx=18, pady=(16, 10))

        self.save_button = self._make_button(actions, 'SALVAR .ENV', self._save_only, '#0e3516', '#5dff7a')
        self.save_button.pack(side='left')

        self.execute_button = self._make_button(actions, 'INICIAR LOOP', self._execute_only, '#143a47', '#7ef7ff')
        self.execute_button.pack(side='left', padx=(10, 0))

        self.save_execute_button = self._make_button(actions, 'SALVAR + INICIAR', self._save_and_execute, '#174917', '#d8ffd9')
        self.save_execute_button.pack(side='left', padx=(10, 0))

        self.stop_button = self._make_button(actions, 'PARAR LOOP', self._stop_loop, '#3a1414', '#ff7070')
        self.stop_button.pack(side='left', padx=(10, 0))
        self.stop_button.config(state='disabled')

        clear_button = self._make_button(actions, 'LIMPAR LOG', self._clear_log, '#2b2b12', '#fff98c')
        clear_button.pack(side='left', padx=(10, 0))

        self.status_var = tk.StringVar(value='STATUS: aguardando configuracao')
        status = tk.Label(
            root,
            textvariable=self.status_var,
            bg='#020602',
            fg='#9dffb0',
            anchor='w',
            padx=18,
            font=('Consolas', 11, 'bold'),
        )
        status.pack(fill='x', pady=(6, 8))

        self.log = tk.Text(
            root,
            bg='#010401',
            fg='#53ff68',
            insertbackground='#53ff68',
            relief='flat',
            wrap='word',
            font=('Consolas', 10),
            padx=12,
            pady=12,
        )
        self.log.pack(fill='both', expand=True, padx=18, pady=(0, 18))
        self.log.tag_configure('INFO', foreground='#58ff7e')
        self.log.tag_configure('WARNING', foreground='#fff47a')
        self.log.tag_configure('ERROR', foreground='#ff7070')
        self.log.tag_configure('DEBUG', foreground='#7ed6ff')
        self.log.tag_configure('SYSTEM', foreground='#9dffb0')

        self._append_log('Painel iniciado.', 'SYSTEM')
        self._append_log(f'.env alvo: {self.env_path}', 'SYSTEM')

    def _make_button(self, parent, text: str, command, bg: str, fg: str) -> tk.Button:
        return tk.Button(
            parent,
            text=text,
            command=command,
            bg=bg,
            fg=fg,
            activebackground=bg,
            activeforeground='#ffffff',
            relief='flat',
            padx=14,
            pady=10,
            font=('Consolas', 11, 'bold'),
        )

    def _add_field(self, parent, row: int, label: str, variable: tk.StringVar, show: str | None = None) -> None:
        tk.Label(
            parent,
            text=label,
            bg='#020602',
            fg='#b7ffbf',
            anchor='w',
            font=('Consolas', 10),
        ).grid(row=row, column=0, sticky='w', pady=6, padx=(0, 14))

        entry = tk.Entry(
            parent,
            textvariable=variable,
            show=show or '',
            bg='#061006',
            fg='#58ff7e',
            insertbackground='#58ff7e',
            relief='flat',
            font=('Consolas', 11),
            width=60,
        )
        entry.grid(row=row, column=1, sticky='ew', pady=6)
        parent.grid_columnconfigure(1, weight=1)

    def _add_indicator(self, parent, column: int, label: str, variable: tk.StringVar) -> None:
        shell = tk.Frame(parent, bg='#061006', padx=12, pady=10)
        shell.grid(row=0, column=column, sticky='ew', padx=(0, 10))
        tk.Label(
            shell,
            text=label,
            bg='#061006',
            fg='#b7ffbf',
            anchor='w',
            font=('Consolas', 9),
        ).pack(anchor='w')
        tk.Label(
            shell,
            textvariable=variable,
            bg='#061006',
            fg='#58ff7e',
            anchor='w',
            font=('Consolas', 14, 'bold'),
        ).pack(anchor='w')
        parent.grid_columnconfigure(column, weight=1)

    def _load_existing_values(self) -> None:
        if not os.path.isfile(self.env_path):
            self._append_log('Nenhum .env encontrado. Preencha os campos e salve.', 'WARNING')
            return

        values = {}
        with open(self.env_path, 'r', encoding='utf-8') as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, _, value = line.partition('=')
                values[key.strip()] = value.strip()

        self._saved_values = values
        for key, var in self.vars.items():
            if key in values:
                var.set(values[key])

        self._append_log('.env existente carregado.', 'SYSTEM')
        self.status_var.set('STATUS: configuracao carregada')

    def _collect_values(self) -> dict:
        return {key: var.get().strip() for key, var in self.vars.items()}

    def _validate(self, values: dict) -> list[str]:
        missing = []
        for key in ('APPS_SCRIPT_TOKEN', 'GREENMILE_URL', 'GREENMILE_USERNAME', 'GREENMILE_PASSWORD'):
            if not values.get(key, '').strip():
                missing.append(key)
        return missing

    def _append_log(self, message: str, level: str = 'INFO') -> None:
        self.log.insert('end', message + '\n', level)
        self.log.see('end')
        self.root.update_idletasks()

    def _clear_log(self) -> None:
        self.log.delete('1.0', 'end')
        self._append_log('Log limpo.', 'SYSTEM')

    def _update_indicators_from_result(self, result: dict) -> None:
        result = result or {}
        self.indicator_vars['pending_rows'].set(str(result.get('pending_rows', '-')))
        self.indicator_vars['moved_rows'].set(str(result.get('moved_rows', '-')))
        self.indicator_vars['last_post_succeeded_at'].set(
            format_indicator_datetime_br(result.get('last_post_succeeded_at', '-'))
        )

    def _save_values(self, show_popup: bool) -> bool:
        values = self._collect_values()
        missing = self._validate(values)
        if missing:
            self.status_var.set('STATUS: configuracao incompleta')
            self._append_log('ERRO: faltam campos obrigatorios: ' + ', '.join(missing), 'ERROR')
            if show_popup:
                messagebox.showerror('Configuracao incompleta', 'Preencha: ' + ', '.join(missing))
            return False

        save_env_file(self.env_path, values)
        self._saved_values = values
        self.status_var.set('STATUS: .env salvo com sucesso')
        self._append_log('.env salvo com sucesso.', 'SYSTEM')
        if show_popup:
            messagebox.showinfo('Configuracao salva', f'.env criado em:\n{self.env_path}')
        return True

    def _save_only(self) -> None:
        self._save_values(show_popup=True)

    def _execute_only(self) -> None:
        self._start_execution(save_before=False)

    def _save_and_execute(self) -> None:
        self._start_execution(save_before=True)

    def _get_loop_interval(self) -> int:
        try:
            return max(10, int(self.vars['LOOP_INTERVAL'].get().strip()))
        except (ValueError, KeyError):
            return 300

    def _start_execution(self, save_before: bool) -> None:
        if self._running:
            self._append_log('Execucao ja em andamento.', 'WARNING')
            return
        if save_before and not self._save_values(show_popup=False):
            return
        if not save_before and not self._saved_values:
            if not self._save_values(show_popup=False):
                return

        if not callable(self.run_callback):
            self._append_log('Nenhum callback de execucao foi configurado.', 'ERROR')
            return

        self._running = True
        self._set_buttons_state('disabled')
        self.status_var.set('STATUS: iniciando loop de sincronizacao...')
        self._append_log('>>> Iniciando loop continuo...', 'SYSTEM')
        self._install_log_handler()

        worker = threading.Thread(target=self._run_loop_in_background, daemon=True)
        worker.start()

    def _run_loop_in_background(self) -> None:
        interval = self._get_loop_interval()
        env_path = self.env_path

        def run_cycle():
            return self.run_callback(env_path)

        def on_cycle_start(cycle_num):
            self.root.after(0, lambda: (
                self.status_var.set(f'STATUS: ciclo {cycle_num} em execucao...'),
                self._append_log(f'>>> [Ciclo {cycle_num}] Iniciado.', 'SYSTEM'),
            ))

        def on_cycle_done(result):
            found = result.get('route_keys_found', 0)
            updated = (result.get('push_result') or {}).get('updatedRows', '?')
            skipped = result.get('skipped', False)
            if skipped:
                msg = f'>>> Ciclo concluido: sem rotas pendentes.'
            else:
                msg = f'>>> Ciclo concluido: {found} rotas encontradas, {updated} linhas atualizadas.'
            self.root.after(0, lambda: (self._update_indicators_from_result(result), self._append_log(msg, 'INFO')))

        def on_cycle_error(exc):
            msg = f'>>> Erro no ciclo: {exc}'
            self.root.after(0, lambda: self._append_log(msg, 'ERROR'))

        def on_next_cycle(secs):
            if secs % 10 == 0 or secs <= 5:
                self.root.after(0, lambda: self.status_var.set(
                    f'STATUS: proxima execucao em {secs}s'
                ))

        self._loop_runner = LoopRunner(
            run_fn=run_cycle,
            interval=interval,
            on_cycle_start=on_cycle_start,
            on_cycle_done=on_cycle_done,
            on_cycle_error=on_cycle_error,
            on_next_cycle=on_next_cycle,
        )
        try:
            self._loop_runner.start_loop()
        except Exception as err:
            self.root.after(0, lambda: self._finish_loop(False, err))
            return
        self.root.after(0, lambda: self._finish_loop(True, None))

    def _install_log_handler(self) -> None:
        self._remove_log_handler()
        self._log_handler = QueueLogHandler(self._log_queue)
        self._log_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s'))
        logging.getLogger().addHandler(self._log_handler)
        logging.getLogger().setLevel(logging.INFO)
        self.root.after(100, self._drain_log_queue)

    def _remove_log_handler(self) -> None:
        if self._log_handler is not None:
            logging.getLogger().removeHandler(self._log_handler)
            self._log_handler = None

    def _drain_log_queue(self) -> None:
        while True:
            try:
                line = self._log_queue.get_nowait()
            except queue.Empty:
                break
            level = 'INFO'
            if ' ERROR ' in line:
                level = 'ERROR'
            elif ' WARNING ' in line:
                level = 'WARNING'
            elif ' DEBUG ' in line:
                level = 'DEBUG'
            self._append_log(line, level)

        if self._running or not self._log_queue.empty():
            self.root.after(100, self._drain_log_queue)

    def _finish_loop(self, ok: bool, err) -> None:
        self._running = False
        self._loop_runner = None
        self._set_buttons_state('normal')
        self.stop_button.config(state='disabled')
        self._remove_log_handler()
        if ok:
            self.status_var.set('STATUS: loop encerrado')
            self._append_log('>>> Loop encerrado.', 'SYSTEM')
        else:
            self.status_var.set('STATUS: loop falhou')
            self._append_log(f'>>> Loop falhou: {err}', 'ERROR')

    def _stop_loop(self) -> None:
        if self._loop_runner:
            self._loop_runner.stop()
            self._append_log('>>> Sinal de parada enviado ao loop...', 'WARNING')
            self.stop_button.config(state='disabled')

    def _set_buttons_state(self, state: str) -> None:
        self.save_button.config(state=state)
        self.execute_button.config(state=state)
        self.save_execute_button.config(state=state)
        if state == 'disabled':
            self.stop_button.config(state='normal')
        else:
            self.stop_button.config(state='disabled')

    def show(self) -> str | None:
        if self.auto_run and self._saved_values:
            self.root.after(250, self._execute_only)
        self.root.mainloop()
        self.root.destroy()
        if self._saved_values:
            return self.env_path
        return None


def launch_config_panel(env_path: str | None = None, run_callback=None, auto_run: bool = False) -> str | None:
    panel = ConfigPanel(env_path=env_path, run_callback=run_callback, auto_run=auto_run)
    return panel.show()
