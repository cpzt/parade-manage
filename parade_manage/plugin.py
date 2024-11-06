from typing import Any, Dict

from parade_manage.common.dag import DAG


class Plugin:

    @property
    def plugin_name(self) -> str:
        raise NotImplementedError

    def run(self, dag: DAG) -> Any:
        raise NotImplementedError


class PluginRegistry:
    PLUGINS: Dict[str, Plugin] = dict()

    @staticmethod
    def register(plugin: Plugin):
        PluginRegistry.PLUGINS[plugin.plugin_name] = plugin

    @staticmethod
    def unregister(plugin_name: str):
        PluginRegistry.PLUGINS.pop(plugin_name, None)

    @staticmethod
    def load():
        return PluginRegistry.PLUGINS
