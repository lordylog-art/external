"""
LoopRunner — executa ciclos de sync contínuos com callbacks de status.

Projetado para ser testável sem dependência de Tkinter ou threading:
basta passar max_cycles para limitar a execução em testes.
"""
from __future__ import annotations

import logging
import time
from typing import Callable, Any

logger = logging.getLogger(__name__)


class LoopRunner:
    """
    Executa run_fn em loop, notificando via callbacks opcionais.

    Parâmetros
    ----------
    run_fn      : callable sem argumentos, retorna dict com resultado do ciclo
    interval    : segundos entre ciclos (default 120)
    max_cycles  : número máximo de ciclos; None = infinito (default None)
    on_cycle_start  : callback(cycle_num: int)
    on_cycle_done   : callback(result: dict)
    on_cycle_error  : callback(exc: Exception) — ciclo continua após erro
    on_next_cycle   : callback(seconds_remaining: int) — chamado durante countdown
    countdown_step  : intervalo em segundos entre chamadas de on_next_cycle (default 1)
    """

    def __init__(
        self,
        run_fn: Callable[[], dict],
        interval: int = 120,
        max_cycles: int | None = None,
        on_cycle_start: Callable[[int], None] | None = None,
        on_cycle_done: Callable[[dict], Any] | None = None,
        on_cycle_error: Callable[[Exception], None] | None = None,
        on_next_cycle: Callable[[int], None] | None = None,
        countdown_step: int = 1,
    ):
        self._run_fn = run_fn
        self._interval = max(0, int(interval))
        self._max_cycles = max_cycles
        self.on_cycle_start = on_cycle_start
        self.on_cycle_done = on_cycle_done
        self.on_cycle_error = on_cycle_error
        self.on_next_cycle = on_next_cycle
        self._countdown_step = max(1, int(countdown_step))
        self._stopped = False

    def stop(self) -> None:
        """Sinaliza que o loop deve parar após o ciclo atual."""
        self._stopped = True

    def start_loop(self) -> None:
        """Inicia o loop síncrono. Retorna quando stop() é chamado ou max_cycles é atingido."""
        self._stopped = False
        cycle = 0

        while True:
            if self._stopped:
                break
            if self._max_cycles is not None and cycle >= self._max_cycles:
                break

            cycle += 1
            logger.info('[LoopRunner] === Ciclo %d iniciado ===', cycle)

            if self.on_cycle_start:
                try:
                    self.on_cycle_start(cycle)
                except Exception:
                    pass

            try:
                result = self._run_fn()
                logger.info('[LoopRunner] Ciclo %d concluído: %s', cycle, result)

                if self.on_cycle_done:
                    try:
                        self.on_cycle_done(result)
                    except Exception:
                        pass

            except Exception as exc:
                logger.error('[LoopRunner] Ciclo %d falhou: %s', cycle, exc)
                if self.on_cycle_error:
                    try:
                        self.on_cycle_error(exc)
                    except Exception:
                        pass

            # Countdown até próximo ciclo (executado mesmo no último ciclo com interval=0)
            remaining = self._interval
            while remaining >= 0:
                if self._stopped:
                    break
                if self.on_next_cycle:
                    try:
                        self.on_next_cycle(remaining)
                    except Exception:
                        pass
                if remaining == 0:
                    break
                sleep_secs = min(self._countdown_step, remaining)
                time.sleep(sleep_secs)
                remaining -= sleep_secs
