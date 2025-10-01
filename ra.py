from enum import Enum
from apscheduler.schedulers.background import BackgroundScheduler
import Pyro5.api
import threading
import datetime
import time

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
        self.timestamp = None
        self.queued_requests = []
        self.peers = {}  # {peer_id: Response}

    def run(self):
        self.daemon = Pyro5.api.Daemon()
        try:
            self.ns = Pyro5.api.locate_ns()
        except:
            Pyro5.nameserver.start_ns()
            self.ns = Pyro5.api.locate_ns()
        self.uri = self.daemon.register(self)
        self.process_id = input("Escolha um nome para este peer: ").strip()

        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

        # registre **exatamente** com este prefixo
        objname = f"peer.{self.process_id}"
        self.ns.register(objname, self.uri)
        print("Ready. Object uri =", self.uri)
        print("Registrado como:", objname)

        threading.Thread(target=self.daemon.requestLoop, daemon=True).start()
        self.interface()

    def refresh_peers(self):
        """Atualiza a lista de peers ativos (exceto eu) e marca como DENY inicialmente."""
        entries = self.ns.list(prefix="peer.")
        self.peers.clear()
        for name in entries.keys():
            peer_id = name.split(".", 1)[1]
            if peer_id != self.process_id:
                self.peers[peer_id] = Response.DENY

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
                self.refresh_peers()
                if not self.peers:
                    # Se não há peers, entra direto
                    self.state = State.HELD
                    print("Seção crítica adquirida (sem peers).")
                    continue

                timestamp = datetime.datetime.now().timestamp()
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

    def _proxy_for(self, peer_id):
        # Use o esquema correto: PYRONAME:<nome>
        return Pyro5.api.Proxy(f"PYRONAME:peer.{peer_id}")

    def request_critical_section(self, timestamp):
        self.state = State.WANTED
        self.timestamp = timestamp

        # Primeiro, envia pedido a todos e zera status para DENY
        for peer_id in list(self.peers.keys()):
            self.peers[peer_id] = Response.DENY
            print(f"Enviando pedido de seção crítica para {peer_id}")
            peer = self._proxy_for(peer_id)
            peer.receive_critical_section_request(self.timestamp, self.process_id)

        # Espera até todos responderem ACCEPT
        while True:
            # pequena pausa para não ocupar 100% CPU
            time.sleep(0.01)
            if self.peers and all(r == Response.ACCEPT for r in self.peers.values()):
                break

        self.state = State.HELD
        self.scheduler.add_job(self.exit_critical_section, 'interval', seconds=10)

    def receive_critical_section_request(self, timestamp, sender_id):
        peer = self._proxy_for(sender_id)

        # Regra Ricart-Agrawala:
        # Se eu estou na SC (HELD) OU eu também quero (WANTED) e minha prioridade é maior,
        # então NEGAR e enfileirar; caso contrário, ACEITAR.
        should_deny = (
            self.state == State.HELD
            or (self.state == State.WANTED and
                (self.timestamp < timestamp
                 or (self.timestamp == timestamp and self.process_id < sender_id)))
        )

        if should_deny:
            self.queued_requests.append((timestamp, sender_id))
            peer.reply_critical_section_request(self.process_id, int(Response.DENY.value))
        else:
            peer.reply_critical_section_request(self.process_id, int(Response.ACCEPT.value))

    def reply_critical_section_request(self, sender_id, response_value):
        # Se quiser evitar problemas de serialização de Enum, receba como int
        try:
            response = Response(response_value)
        except Exception:
            # fallback se vier string
            response = Response.ACCEPT if str(response_value) == "ACCEPT" else Response.DENY

        self.peers[sender_id] = response

    def exit_critical_section(self):
        self.state = State.RELEASED
        # Ao sair, responda ACCEPT para todos que estavam enfileirados
        for ts, waiting_id in self.queued_requests:
            peer = self._proxy_for(waiting_id)
            peer.reply_critical_section_request(self.process_id, int(Response.ACCEPT.value))
        self.queued_requests.clear()
        self.timestamp = None

    def list_peers(self):
        print("Active peers:")
        ns = Pyro5.api.locate_ns()
        for name, uri in ns.list(prefix="peer.").items():
            peer_id = name.split(".", 1)[1]
            if peer_id != self.process_id:
                print(f"  {name} -> {uri}")

if __name__ == "__main__":
    RicartAgrawala().run()
