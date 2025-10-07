from enum import Enum
from apscheduler.schedulers.background import BackgroundScheduler
import Pyro5.api
import Pyro5.socketutil
import threading
import datetime
import time
import os
import sys
import socket
from concurrent.futures import ThreadPoolExecutor

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
        self.peers = {}


    def __del__(self):
        """Limpa o registro no nameserver e encerra o daemon."""

        if hasattr(self, 'ns') and self.ns and hasattr(self, 'process_id') and self.process_id:
            try:
                self.ns.remove(f"peer.{self.process_id}")
                print("Removido do nameserver.")
            except Exception as e:
                print("Erro ao remover do nameserver:", e)
        if hasattr(self, 'daemon') and self.daemon:
            self.daemon.shutdown()
            print("Daemon Pyro5 desligado.")
        if hasattr(self, 'nameserver_daemon') and self.nameserver_daemon:
            self.nameserver_daemon.shutdown()
            print("Nameserver daemon desligado.")
        if hasattr(self, 'scheduler') and self.scheduler:
            self.scheduler.shutdown()
            print("Scheduler desligado.")


    def run(self):
        """Inicia o loop do daemon, registra o objeto no nameserver, schedula os jobs e inicia a interface"""

        print("Iniciando Ricart-Agrawala Peer...")
        self.daemon = Pyro5.api.Daemon()
        
        nameserver_found = False
        self.ns = None
        
        try:
            print('Localizando nameserver...')
            self.ns = Pyro5.api.locate_ns(host='0.0.0.0', port=9090)
            print("Nameserver localizado em 0.0.0.0:9090")
            nameserver_found = True
        except Pyro5.errors.NamingError:
            try:
                my_ip = Pyro5.socketutil.get_ip_address(None, workaround127=True)
                print(f'Tentando localizar nameserver em {my_ip}:9090...')
                self.ns = Pyro5.api.locate_ns(host=my_ip, port=9090)
                print("Nameserver localizado em IP específico.")
                nameserver_found = True
            except Pyro5.errors.NamingError:
                pass
    
        if not nameserver_found:
            print("Nameserver não encontrado. Iniciando novo nameserver...")
            my_ip = Pyro5.socketutil.get_ip_address(None, workaround127=True)
            ns_uri, nameserver_daemon, _ = Pyro5.api.start_ns(host='0.0.0.0', port=9090)
            print(f"Nameserver iniciado em 0.0.0.0:9090")
            self.ns = nameserver_daemon.nameserver
            self.nameserver_daemon = nameserver_daemon
            print(f"Nameserver URI: {ns_uri}")
            
            threading.Thread(target=nameserver_daemon.requestLoop, daemon=True).start()
            
            time.sleep(2)

        self.uri = self.daemon.register(self)
        self.process_id = input("Escolha um nome para este peer: ").strip()

        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        self.refresh_peers()
        self.send_heartbeat()

        self.scheduler.add_job(self.send_heartbeat, 'interval', seconds=10, id="heartbeat")
        objname = f"peer.{self.process_id}"

        while objname in self.ns.list(prefix="peer.").keys():
            print(f"Nome '{self.process_id}' já está em uso. Escolha outro.")
            self.process_id = input("Escolha um nome para este peer: ").strip()
            objname = f"peer.{self.process_id}"

        self.ns.register(objname, self.uri)
        print("Pronto. URI do objeto: ", self.uri)
        print("Registrado como: ", objname)

        threading.Thread(target=self.daemon.requestLoop, daemon=True).start()
        self.interface()


    def refresh_peers(self):
        """Atualiza a lista de peers ativos (exceto o peer atual)."""

        entries = self.ns.list(prefix="peer.")
        for name in entries.keys():
            peer_id = name.split(".", 1)[1]
            if peer_id != self.process_id:
                print(self.peers.keys())
                if peer_id not in self.peers.keys():
                    self.peers[peer_id] = {'response': None, 'last_heartbeat': datetime.datetime.now().timestamp()}
                else:
                    self.peers[peer_id]['response'] = None


    def interface(self):
        """Loop da interface do usuário."""

        while True:
            print("\n1. Requisitar seção crítica")
            print("2. Liberar seção crítica")
            print("3. Listar peers ativos")
            choice = input("Escolha uma opção: ").strip()

            if choice == '1':
                if self.state == State.HELD:
                    print("Você já está na seção crítica.")
                    continue

                self.request_critical_section()
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


    def request_critical_section(self):
        """Faz o pedido para entrar na seção crítica."""

        self.state = State.WANTED
        self.timestamp = datetime.datetime.now().timestamp()

        self.check_heartbeats()

        if not self.peers:
            self.state = State.HELD
            print("Seção crítica adquirida (sem peers).")
            self.scheduler.add_job(self.exit_critical_section, 'date', 
                                 run_date=datetime.datetime.now() + datetime.timedelta(seconds=10), 
                                 id="exit_cs")
            return

        with ThreadPoolExecutor(max_workers=10) as executor:
            for peer_id in list(self.peers.keys()):
                self.peers[peer_id]['response'] = None
                print(f"Enviando pedido de seção crítica para {peer_id}")
                executor.submit(self.send_request, peer_id)
                print(peer_id)
                self.scheduler.add_job(self.check_response, 'date', run_date=datetime.datetime.now() + datetime.timedelta(seconds=10), args=[peer_id], id=f"check_{peer_id}")

        while True:
            time.sleep(0.01)
                
            if not self.peers:
                print("Todos os peers foram removidos. Entrando na seção crítica.")
                break
                
            all_accepted = True
            for peer_id, info in self.peers.items():
                if info['response'] != Response.ACCEPT:
                    all_accepted = False
                    break
                    
            if all_accepted:
                break

        self.state = State.HELD
        print("Seção crítica adquirida.")
        self.scheduler.add_job(self.exit_critical_section, 'date', 
                            run_date=datetime.datetime.now() + datetime.timedelta(seconds=10), 
                            id="exit_cs")

    def send_request(self, peer_id):
        """Envia um pedido de seção crítica para o peer especificado."""

        try:
            peer = self._proxy_for(peer_id)
            peer.receive_critical_section_request(self.timestamp, self.process_id)
            print(f"Pedido enviado com sucesso para {peer_id}")
        except Exception as e:
            print(f"Erro ao enviar pedido para {peer_id}: {e}")
            if peer_id in self.peers:
                print(f"Removendo peer {peer_id} devido a erro de comunicação")
                del self.peers[peer_id]
                try:
                    self.scheduler.remove_job(f"check_{peer_id}")
                except Exception:
                    pass

    def receive_critical_section_request(self, timestamp, sender_id):
        """Recebe um pedido de seção crítica de um peer."""

        if sender_id not in self.peers:
            print(f"Pedido ignorado: peer {sender_id} não está na lista de peers ativos")
            return
            
        now = datetime.datetime.now().timestamp()
        if now - self.peers[sender_id]['last_heartbeat'] > 15:
            print(f"Pedido ignorado: peer {sender_id} não enviou heartbeat recente")
            return

        peer = self._proxy_for(sender_id)

        should_deny = (
            self.state == State.HELD
            or (self.state == State.WANTED and
                (self.timestamp < timestamp
                 or (self.timestamp == timestamp and self.process_id < sender_id)))
        )

        if should_deny:
            self.queued_requests.append((timestamp, sender_id))
            try:
                peer.reply_critical_section_request(self.process_id, int(Response.DENY.value))
            except Exception as e:
                print(f"Erro ao enviar DENY para {sender_id}: {e}")
                if sender_id in self.peers:
                    del self.peers[sender_id]
        else:
            try:
                peer.reply_critical_section_request(self.process_id, int(Response.ACCEPT.value))
            except Exception as e:
                print(f"Erro ao enviar ACCEPT para {sender_id}: {e}")
                if sender_id in self.peers:
                    del self.peers[sender_id]


    def reply_critical_section_request(self, sender_id, response_value):
        """Recebe a resposta de um peer ao pedido de seção crítica."""

        try:
            response = Response(response_value)
        except Exception:
            response = Response.ACCEPT if str(response_value) == "ACCEPT" else Response.DENY

        self.peers[sender_id]['response'] = response


    def exit_critical_section(self):
        """Libera a seção crítica e responde aos pedidos enfileirados."""

        self.state = State.RELEASED
        for ts, waiting_id in self.queued_requests:
            if waiting_id in self.peers:
                now = datetime.datetime.now().timestamp()
                if now - self.peers[waiting_id]['last_heartbeat'] <= 15:
                    try:
                        peer = self._proxy_for(waiting_id)
                        peer.reply_critical_section_request(self.process_id, int(Response.ACCEPT.value))
                    except Exception as e:
                        print(f"Erro ao enviar resposta para peer enfileirado {waiting_id}: {e}")
                        if waiting_id in self.peers:
                            del self.peers[waiting_id]
                else:
                    print(f"Peer enfileirado {waiting_id} não está mais ativo")
                    del self.peers[waiting_id]

        self.queued_requests.clear()
        self.timestamp = None
        print("\nSeção crítica liberada.")


    def list_peers(self):
        """Lista os peers ativos."""

        print("Active peers:")
        for peer_id, info in self.peers.items():
            if peer_id != self.process_id:
                print(f"  {peer_id} -> Response: {info['response']}; Last heartbeat: {datetime.datetime.fromtimestamp(info['last_heartbeat'])}")


    def send_heartbeat(self):
        """Envia heartbeat para todos os peers e remove os inativos."""

        now = datetime.datetime.now().timestamp()
        self.check_heartbeats()
        for peer_id in list(self.peers.keys()):
            try:
                peer = self._proxy_for(peer_id)
                peer.receive_heartbeat(self.process_id)
            except Exception as e:
                print(f"Removendo peer inativo: {peer_id}")
                del self.peers[peer_id]


    def receive_heartbeat(self, sender_id):
        """Recebe heartbeat de um peer."""

        if sender_id not in self.peers:
            self.peers[sender_id] = {'response': Response.DENY, 'last_heartbeat': datetime.datetime.now().timestamp()}
        else:
            self.peers[sender_id]['last_heartbeat'] = datetime.datetime.now().timestamp()


    def check_heartbeats(self):
        """Remove peers que não enviaram heartbeat recentemente."""

        now = datetime.datetime.now().timestamp()
        to_remove = []
        
        for peer_id, info in self.peers.items():
            if now - info['last_heartbeat'] > 15:
                to_remove.append(peer_id)
        
        for peer_id in to_remove:
            print(f"Removendo peer inativo: {peer_id}")
            try:
                self.ns.remove(f"peer.{peer_id}")
            except Exception:
                pass
            del self.peers[peer_id]


    def check_response(self, peer_id):
        """Verifica se o peer respondeu dentro do tempo limite."""

        if peer_id in self.peers:
            if self.peers[peer_id]['response'] is None:
                print(f'Limite de tempo atingido para {peer_id}. Removendo peer.')
                del self.peers[peer_id]
                try:
                    self.scheduler.remove_job(f"check_{peer_id}")
                except Exception:
                    pass


    def _proxy_for(self, peer_id):
        proxy = Pyro5.api.Proxy(f"PYRONAME:peer.{peer_id}")
        proxy._pyroTimeout = 5
        return proxy
        

if __name__ == "__main__":
    try:
        peer = RicartAgrawala()
        peer.run()
    except KeyboardInterrupt as e:
        try:
            print("\nSaindo...")
            if peer:
                peer.__del__()
            sys.exit(0)
        except Exception:
            if peer:
                peer.__del__()
            os._exit(0)

