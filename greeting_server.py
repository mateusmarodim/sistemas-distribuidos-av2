import Pyro5.api
import threading

@Pyro5.api.expose
class GreetingMaker(object):
    def get_greeting(self, name):
        return f"Hello, {name}!"
    
id = input("Enter an id for the greeting server: ").strip()
    
def daemon_thread():
    daemon = Pyro5.api.Daemon()
    ns = Pyro5.api.locate_ns()
    uri = daemon.register(GreetingMaker)
    ns.register(f"greeting.maker.{id}", uri)

    print("Ready. Object uri =", uri)
    daemon.requestLoop()

def client_thread():
    while True:
        name = input("What is your name? ").strip()
        if name == 'list':
            ns = Pyro5.api.locate_ns()
            print("Registered greeting servers:")
            for name, uri in ns.list(prefix="greeting.maker.").items():
                print(f"  {name} -> {uri}")
            continue
        id = input("Enter the id of the greeting server to use: ").strip()
        greeting_maker = Pyro5.api.Proxy(f"PYRONAME:greeting.maker.{id}")    # use name server object lookup uri shortcut
        print(greeting_maker.get_greeting(name))
    
threading.Thread(target=daemon_thread, daemon=True).start()
client_thread()

#1. Requisitar recursos
# 2. Liberar recursos
# 3. Listar peers ativos