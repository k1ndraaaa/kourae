from vm import VM, FlaskVM

flask_vm = FlaskVM(
    name = __name__,
    vm = VM()
)

@flask_vm.get("/")
def index():
    return "Bienvenido a la aplicación"

flask_vm.play_and_debug()