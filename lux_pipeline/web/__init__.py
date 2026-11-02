"""The LUX pipeline's web UI, served by `lux serve` (webui_docs/).

Needs the ``web`` extra (``pip install -e .[web]``); nothing outside this
package and `lux_pipeline/cli/serve.py` imports FastAPI, uvicorn or
Jinja2. The pages read what the console reads -- the workers' status and
log files, the configuration files -- through the functions the ``lux``
commands print from; nothing of substance is implemented here.
"""
