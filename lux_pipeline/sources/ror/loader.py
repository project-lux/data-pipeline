from lux_pipeline.process.base.loader import Loader, Pointer

class RorLoader(Loader):

    def should_process_item(self, child):
        # Filter out v2.8-bla-bla.csv also in the zip
        if isinstance(child, Pointer):
            name = child.get_name()
            return name.endswith('json')
        else:
            return True
