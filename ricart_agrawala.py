from enum import Enum
import Pyro5.api
import threading
import datetime
import time
#1. Requisitar recursos
# 2. Liberar recursos
# 3. Listar peers ativos

class State(Enum):
    RELEASED = 0
    WANTED = 1
    HELD = 2

class Response(Enum):
    ACCEPT = 0
    DENY = 1

@Pyro5.api.expose
class RicartAgrawala(object):
    def __init__(self, process_id=None):
        self.process_id = process_id
        self.state = State.RELEASED
        self.queued_requests = []
        self.peers = {}

    def run(self):
        self.daemon = Pyro5.api.Daemon()
        self.ns = Pyro5.api.locate_ns()
        self.uri = self.daemon.register(self)
        self.process_id = input("Escolha um nome para este peer: ").strip()
        self.ns.register(f"peer.{self.process_id}", self.uri)
        print("Ready. Object uri =", self.uri)

        threading.Thread(target=self.daemon.requestLoop, daemon=True).start()
        self.interface()

    def interface(self):
        while True:
            print("\n1. Requisitar seção crítica")
            print("2. Liberar seção crítica")
            print("3. Listar peers ativos")
            choice = input("Escolha uma opção: ").strip()

            if choice == '1':
                if self.state == State.HELD:
                    print("Você já está na seção crítica.")
                    continue
                timestamp = datetime.datetime.now().timestamp()
                print(timestamp)
                self.request_critical_section(timestamp)
                print("Seção crítica adquirida.")
            elif choice == '2':
                if self.state != State.HELD:
                    print("Você não está na seção crítica.")
                    continue
                self.exit_critical_section()
                print("Seção crítica liberada.")
            elif choice == '3':
                self.list_peers()
            else:
                print("Opção inválida.")

    def request_critical_section(self, timestamp):
        self.state = State.WANTED
        self.timestamp = timestamp

        self.peers = {peer[0]: Response.DENY for peer in self.ns.list(prefix="peer.").items() if peer[0].split(".")[1] != self.process_id}

        for peer_id in self.peers.keys():
            print((f"Enviando pedido de seção crítica para {peer_id}"))
            peer = Pyro5.api.Proxy(f"PYRONAME:peer.{peer_id}")
            peer.receive_critical_section_request(self.timestamp, self.process_id)


        print(self.peers)

        while True:
            time.sleep(0.01)
            if self.peers and all(r == Response.ACCEPT for r in self.peers.values()):
                break

        self.state = State.HELD
        for peer_id in self.peers.keys():
            self.peers[peer_id] = Response.DENY


    def receive_critical_section_request(self, timestamp, sender_id):
        peer = Pyro5.api.Proxy(f"PYRONAME:peer.{sender_id}")

        if self.state == State.HELD or (self.state == State.WANTED and (self.timestamp < timestamp or (self.timestamp == timestamp and self.process_id < sender_id))):
            self.queued_requests.append((timestamp, sender_id))
            peer.reply_critical_section_request(self.process_id, Response.DENY)
        peer.reply_critical_section_request(self.process_id, Response.ACCEPT)
    

    def reply_critical_section_request(self, sender_id, response: Response):
        self.peers[sender_id] = response

    def exit_critical_section(self):
        self.state = State.RELEASED
        for request in self.queued_requests:
            peer = Pyro5.api.Proxy(f"PYRONAME:peer.{request[1]}")
            peer.reply_critical_section_request(self.process_id, Response.ACCEPT)
        self.queued_requests.clear()

    def list_peers(self):
        print("Active peers:" )
        for name, uri in self.ns.list(prefix="peer.").items():
            peer_id = name.split(".")[1]
            if peer_id != self.process_id:
                print(f"  {name} -> {uri}")

if __name__ == "__main__":
    RicartAgrawala().run()