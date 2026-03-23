from vm import VM, FlaskVM
from flask import Flask

flask_vm = FlaskVM(app=Flask(__name__), vm=VM())
flask_vm.play_and_debug()