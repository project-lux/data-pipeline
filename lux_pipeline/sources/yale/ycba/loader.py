from lux_pipeline.process.base.loader import Loader, Pointer


class YcbaLoader(Loader):
    def make_identifier(self, value):

        if isinstance(value, Pointer):
            value = value.get_name()
        value = value.replace("home/ec2-user/reconciliation/data/ycba/linked_art/", "")
        value = value.replace(self.config["namespace"], "")
        return value
