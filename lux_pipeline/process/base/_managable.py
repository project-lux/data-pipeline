class Managable:
    """Base for a class that performs an action that can be managed via a task manager"""

    def __init__(self, config):
        self.config = config
        self.configs = config["all_configs"]
        self.name = config["name"]
        self.max_slice = -1
        self.my_slice = -1
        self.manager = None
        self.total = -1
        self.divide_by_max_slice = True

    def prepare(self, manager, my_slice, max_slice):
        self.manager = manager
        self.my_slice = my_slice
        self.max_slice = max_slice

    def update_progress_bar(self, total=-1, increment_total=-1, desc=None):
        if total > 0:
            ttl = total
            self.total = ttl
        elif increment_total > 0:
            self.total += increment_total
            ttl = self.total
        else:
            ttl = self.total
        if self.divide_by_max_slice and self.max_slice > 1:
            ttl = ttl // self.max_slice
        self.manager.update_progress_bar(total=ttl, description=desc)

    def increment_progress_bar(self, amount):
        self.manager.update_progress_bar(advance=1)

    def close_progress_bar(self):
        # Could set visibility to false or something but better to just leave it
        # alone by default
        ttl = self.total // self.max_slice if self.divide_by_max_slice else self.total
        self.manager.update_progress_bar(completed=ttl)

    def prepare(self, mgr, my_slice=-1, max_slice=-1):
        self.manager = mgr
        self.my_slice = my_slice
        self.max_slice = max_slice

    def process(self, disable_ui=False, **kw):
        raise NotImplementedError()
