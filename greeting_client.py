import Pyro5.api

name = input("What is your name? ").strip()

greeting_maker = Pyro5.api.Proxy("PYRONAME:greeting.maker")    # use name server object lookup uri shortcut
print(greeting_maker.get_greeting(name))