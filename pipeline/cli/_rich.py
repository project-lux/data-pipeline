import os
from rich import progress
from rich.layout import Layout
from rich.live import Live
from rich.console import Console
from rich.panel import Panel
from rich import box
import logging
from rich.logging import RichHandler

import multiprocessing, logging
logger = multiprocessing.get_logger()

class ConsolePanel(Console):
    def __init__(self,*args,**kwargs):
        console_file = open(os.devnull,'w')
        super().__init__(record=True,file=console_file,*args,**kwargs)

    def __rich_console__(self,console,options):
        texts = self.export_text(clear=False).split('\n')
        for line in texts[-options.height:]:
            yield line


def get_layout(cfgs, max_workers):

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


    logging.basicConfig(
        level="INFO",
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=cp)]
    )
    logger.addHandler(RichHandler(console=cp))

    layout._lux_bars = bars
    return layout


# FIXME: This is a nasty hack. Figure someway to manage this better
def get_bar_from_layout(layout, which):
    return layout._lux_bars[which]
