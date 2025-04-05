import os
import logging
from rich import progress, box
from rich.layout import Layout
from rich.live import Live
from rich.console import Console
from rich.panel import Panel
from rich.logging import RichHandler
from rich.text import Text
from rich.style import Style

logger = logging.getLogger("lux_pipeline")

class ConsolePanel(Console):
    def __init__(self,*args,**kwargs):
        console_file = open(os.devnull,'w')
        super().__init__(record=True,file=console_file,*args,**kwargs)

    def __rich_console__(self,console,options):
        texts = self.export_text(clear=False).split('\n')
        for line in texts[-options.height:]:
            yield line

class LuxHandler(RichHandler):

    def render_message(self, record: logging.LogRecord, message: str):
        #if record.levelname in ["ERROR", "CRITICAL"]:
        #    message = f":warning: {message}"
        return super().render_message(record, message)

    def get_level_text(self, record):
        level_name = record.levelname
        color = {
            "DEBUG": "white",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "bright_red",
            "CRITICAL": "bright_magenta"
        }
        level_text = Text(f"[{color[level_name]}]{level_name.ljust(8)}")
        return level_text

def get_layout(cfgs, max_workers, log_level):

    layout = Layout()
    layout.split_column(
        Layout(name="title", size=3),
        Layout(name="progress", minimum_size=18),
        Layout(name="log", minimum_size=8)
    )

    l2 = Layout()
    layout['progress'].update(Panel(l2, title="Progress"))
    l2.split_column(*[Layout(name=f"process {x}", size=1) for x in range(max_workers)])

    bars = []
    for x in range(max_workers):
        pbar = progress.Progress(
            "[progress.description]{task.description}",
            progress.BarColumn(),
            "[progress.completed]{task.completed}", "/", "[progress.total]{task.total}",
            "[progress.percentage]{task.percentage:>3.0f}%",
            progress.TimeRemainingColumn(),
            progress.TimeElapsedColumn(),
            refresh_per_second=4)
        task_id = pbar.add_task(f"[red]task {x}", total=10000)
        l2[f'process {x}'].update(pbar)
        bars.append([pbar, task_id])

    cp = ConsolePanel()
    layout['log'].update(Panel(cp, title="Log Messages"))
    # remove stream handler
    while logger.handlers:
        logger.removeHandler(logger.handlers[0])
    # markup=False lets [...] be parsed by Text()
    # show_path=False because 99% of the calls are from _task_ui_manager, which
    #           receives them from the remote processes and re-logs them

    handler = LuxHandler(console=cp, show_path=False)
    log_level = log_level.upper()
    if not log_level or not log_level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
        log_level = "INFO"
    handler.setLevel(getattr(logging, log_level))
    logger.addHandler(handler)

    layout._lux_bars = bars
    return layout


# FIXME: This is a nasty hack. Figure someway to manage this better
def get_bar_from_layout(layout, which):
    return layout._lux_bars[which]



