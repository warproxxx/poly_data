import sys
import webbrowser, threading

from bokeh.application.handlers.function import FunctionHandler
from bokeh.application import Application
from bokeh.document import Document
from bokeh.server.server import Server
from bokeh.io import show

from jinja2 import Environment, PackageLoader

from backtrader_plotting.bokeh import utils


class BokehWebapp:
    def __init__(self, title, html_template, scheme, model_factory_fnc, on_session_destroyed=None, port=80):
        self._title = title
        self._html_template = html_template
        self._scheme = scheme
        self._model_factory_fnc = model_factory_fnc
        self._port = port
        self._on_session_destroyed = on_session_destroyed

    def start(self, ioloop=None):
        """Serves a backtrader result as a Bokeh application running on a web server"""
        def make_document(doc: Document):
            if self._on_session_destroyed is not None:
                doc.on_session_destroyed(self._on_session_destroyed)

            # set document title
            doc.title = self._title

            # set document template
            env = Environment(loader=PackageLoader('backtrader_plotting.bokeh', 'templates'))
            doc.template = env.get_template(self._html_template)
            doc.template_variables['stylesheet'] = utils.generate_stylesheet(self._scheme)

            # get root model
            model = self._model_factory_fnc(doc)
            doc.add_root(model)

        self._run_server(make_document, ioloop=ioloop, port=self._port)

    @staticmethod
    def _run_server(fnc_make_document, iplot=True, notebook_url="localhost:8889", port=80, ioloop=None):
        """Runs a Bokeh webserver application. Documents will be created using fnc_make_document"""
        handler = FunctionHandler(fnc_make_document)
        app = Application(handler)

        if iplot and 'ipykernel' in sys.modules:
            show(app, notebook_url=notebook_url)
        else:
            apps = {'/': app}

            print(f"Browser is launching at: http://localhost:{port}")
            threading.Timer(2, lambda: webbrowser.open(f'http://localhost:{port}')).start()
            
            server = Server(apps, port=port, io_loop=ioloop)
            if ioloop is None:
                server.run_until_shutdown()
            else:
                server.start()
                ioloop.start()

