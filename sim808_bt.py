"""
SIM808 Bluetooth Bridge — WebSocket <-> RFCOMM (WinRT API)

Usa a API Windows Runtime (WinRT) para conexão RFCOMM no Windows 10/11.
Funciona mesmo sem SPP registrado no SDP do dispositivo.

Uso:
  pip install winrt-Windows.Devices.Bluetooth winrt-Windows.Devices.Bluetooth.Rfcomm
  pip install winrt-Windows.Networking.Sockets winrt-Windows.Storage.Streams websockets
  python sim808_bt.py
"""

import asyncio
import sys
import json
import subprocess

# ── Dependências ──────────────────────────────────────────────────────────────
def pip_install(*pkgs):
    subprocess.check_call([sys.executable, "-m", "pip", "install", *pkgs],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

for pkg in [
    "websockets",
    "winrt-Windows.Devices.Bluetooth",
    "winrt-Windows.Devices.Bluetooth.Rfcomm",
    "winrt-Windows.Networking.Sockets",
    "winrt-Windows.Storage.Streams",
]:
    try:
        __import__(pkg.replace("-", ".").replace("winrt.", "").lower()
                   if "winrt" not in pkg else pkg.split("-", 1)[1].replace(".", "_").lower())
    except (ImportError, ModuleNotFoundError):
        pass  # verificação real abaixo

try:
    import websockets
except ImportError:
    print("Instalando websockets..."); pip_install("websockets"); import websockets  # type: ignore

try:
    from winrt.windows.devices.bluetooth import BluetoothDevice
    from winrt.windows.devices.bluetooth.rfcomm import RfcommServiceId
    from winrt.windows.networking.sockets import StreamSocket
    from winrt.windows.storage.streams import DataReader, DataWriter, InputStreamOptions
except ImportError:
    print("Instalando winrt...")
    pip_install(
        "winrt-Windows.Devices.Bluetooth",
        "winrt-Windows.Devices.Bluetooth.Rfcomm",
        "winrt-Windows.Networking",
        "winrt-Windows.Networking.Sockets",
        "winrt-Windows.Storage.Streams",
    )
    from winrt.windows.devices.bluetooth import BluetoothDevice
    from winrt.windows.devices.bluetooth.rfcomm import RfcommServiceId
    from winrt.windows.networking.sockets import StreamSocket
    from winrt.windows.storage.streams import DataReader, DataWriter, InputStreamOptions

# ── Configuração ──────────────────────────────────────────────────────────────
BT_MAC    = "38:1C:4A:CE:FA:55"
WS_HOST   = "127.0.0.1"
WS_PORT   = 8765

# ── Estado global ─────────────────────────────────────────────────────────────
bt_socket: StreamSocket | None = None
bt_writer: DataWriter  | None = None
bt_reader: DataReader  | None = None
bt_connected   = False
ws_clients: set = set()
rx_queue:  asyncio.Queue | None = None
loop:      asyncio.AbstractEventLoop | None = None


# ── Helpers MAC ──────────────────────────────────────────────────────────────

def mac_to_int(mac: str) -> int:
    return int(mac.replace(":", "").replace("-", ""), 16)


# ── Conexão WinRT RFCOMM ──────────────────────────────────────────────────────

async def winrt_connect(mac: str) -> StreamSocket:
    mac_int = mac_to_int(mac)
    print(f"[WinRT] Obtendo dispositivo {mac}...")
    device = await BluetoothDevice.from_bluetooth_address_async(mac_int)

    if device is None:
        raise Exception("Dispositivo não encontrado. Verifique se está pareado.")

    print(f"[WinRT] Dispositivo: {device.name}")

    # Tentativa 1: SPP via SDP (UUID 0x1101)
    spp_id = RfcommServiceId.from_short_id(0x1101)
    print("[WinRT] Buscando SPP (0x1101) via SDP...")
    result = await device.get_rfcomm_services_for_id_async(spp_id)

    if result and result.services and len(result.services) > 0:
        svc = result.services[0]
        print("[WinRT] SPP encontrado! Conectando...")
        sock = StreamSocket()
        await sock.connect_async(svc.connection_host_name, svc.connection_service_name)
        return sock

    # Tentativa 2: Todos os serviços RFCOMM do dispositivo
    print("[WinRT] SPP não está no SDP. Listando todos os serviços RFCOMM...")
    all_result = await device.get_rfcomm_services_async()

    if all_result and all_result.services and len(all_result.services) > 0:
        for svc in all_result.services:
            svc_str = svc.service_id.as_string()
            print(f"[WinRT] Tentando serviço: {svc_str}")
            try:
                sock = StreamSocket()
                await sock.connect_async(svc.connection_host_name, svc.connection_service_name)
                print(f"[WinRT] Conectado via {svc_str}!")
                return sock
            except Exception as e:
                print(f"[WinRT] Falhou ({svc_str}): {e}")

    raise Exception(
        "Nenhum serviço RFCOMM encontrado. "
        "Confirme AT+BTPOWER=1 e AT+BTVIS=1 no SIM808."
    )


# ── Reader assíncrono ─────────────────────────────────────────────────────────

async def winrt_reader_loop():
    global bt_connected
    try:
        reader = DataReader(bt_socket.input_stream)
        reader.input_stream_options = InputStreamOptions.PARTIAL
    except Exception as e:
        print(f"[WinRT] Erro ao criar DataReader: {e}")
        bt_connected = False
        if rx_queue:
            await rx_queue.put("__DISCONNECTED__")
        return

    buf = ""
    print("[WinRT] Reader iniciado.")
    while bt_connected:
        try:
            # load_async com PARTIAL retorna imediatamente com dados disponíveis
            loaded = await reader.load_async(256)
            if loaded == 0:
                await asyncio.sleep(0.05)
                continue
            raw = bytearray(loaded)
            reader.read_bytes(raw)
            chunk = raw.decode("utf-8", errors="replace")
            print(f"[WinRT] RX {loaded}b: {chunk!r}")
            buf += chunk
            while "\n" in buf:
                line, buf = buf.split("\n", 1)
                line = line.strip()
                if line and rx_queue:
                    await rx_queue.put(line)
        except Exception as e:
            print(f"[WinRT] Erro de leitura: {e}")
            break

    bt_connected = False
    if rx_queue:
        await rx_queue.put("__DISCONNECTED__")
    print("[WinRT] Reader encerrado.")


async def winrt_send(text: str):
    if bt_writer and bt_connected:
        try:
            bt_writer.write_string(text.strip() + "\r\n")
            await bt_writer.store_async()
        except Exception as e:
            print(f"[WinRT] Erro ao enviar: {e}")


# ── WebSocket ─────────────────────────────────────────────────────────────────

async def broadcast(message: str):
    if ws_clients:
        data = json.dumps({"type": "rx", "data": message})
        await asyncio.gather(
            *[ws.send(data) for ws in list(ws_clients)],
            return_exceptions=True
        )


async def ws_handler(websocket):
    global bt_socket, bt_writer, bt_reader, bt_connected, rx_queue

    ws_clients.add(websocket)
    print(f"[WS] Cliente conectado. Total: {len(ws_clients)}")

    status = "connected" if bt_connected else "disconnected"
    await websocket.send(json.dumps({"type": "status", "bt": status, "mac": BT_MAC}))

    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except Exception:
                msg = {"type": "send", "data": raw}

            mtype = msg.get("type", "send")

            if mtype == "connect_bt":
                if bt_connected:
                    await websocket.send(json.dumps({"type": "status", "bt": "connected"}))
                    continue

                mac = msg.get("mac", BT_MAC)
                await websocket.send(json.dumps({"type": "rx", "data": f"[WinRT] Conectando a {mac}... (aguarda até 30s)"}))
                await websocket.send(json.dumps({"type": "rx", "data": "[WinRT] ⚠ No Serial do SIM808: aguarde +BTCONNECTING e envie AT+BTACPT=1"}))
                try:
                    bt_socket = await asyncio.wait_for(winrt_connect(mac), timeout=30.0)
                    bt_writer  = DataWriter(bt_socket.output_stream)
                    rx_queue   = asyncio.Queue()
                    bt_connected = True
                    asyncio.create_task(winrt_reader_loop())
                    await websocket.send(json.dumps({"type": "status", "bt": "connected", "mac": mac}))
                    await broadcast("[WinRT] Conexão BT estabelecida!")
                except Exception as e:
                    err = str(e)
                    await websocket.send(json.dumps({"type": "status", "bt": "error", "error": err}))
                    await websocket.send(json.dumps({"type": "rx", "data": f"[WinRT] Erro: {err}"}))

            elif mtype == "disconnect_bt":
                bt_connected = False
                if bt_socket:
                    try: bt_socket.close()
                    except: pass
                    bt_socket = None
                bt_writer = None
                await websocket.send(json.dumps({"type": "status", "bt": "disconnected"}))

            elif mtype == "send":
                cmd = msg.get("data", "").strip()
                if cmd and bt_connected:
                    await winrt_send(cmd)
                elif cmd:
                    await websocket.send(json.dumps({"type": "rx", "data": "[WinRT] Sem conexão BT ativa"}))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        ws_clients.discard(websocket)
        print(f"[WS] Cliente desconectado. Total: {len(ws_clients)}")


async def rx_dispatcher():
    global bt_connected
    while True:
        if rx_queue:
            try:
                line = await asyncio.wait_for(rx_queue.get(), timeout=0.5)
                if line == "__DISCONNECTED__":
                    bt_connected = False
                    await broadcast("[WinRT] SIM808 desconectado.")
                    await asyncio.gather(
                        *[ws.send(json.dumps({"type": "status", "bt": "disconnected"}))
                          for ws in list(ws_clients)],
                        return_exceptions=True
                    )
                else:
                    await broadcast(line)
            except asyncio.TimeoutError:
                pass
        else:
            await asyncio.sleep(0.1)


def free_port(port: int):
    try:
        r = subprocess.run(["netstat", "-ano"], capture_output=True, text=True, timeout=5)
        for line in r.stdout.splitlines():
            if f":{port}" in line and "LISTENING" in line:
                pid = line.strip().split()[-1]
                if pid.isdigit():
                    subprocess.run(["taskkill", "/PID", pid, "/F"],
                                   capture_output=True, timeout=5)
                    print(f"[PORT] Processo {pid} encerrado (porta {port} liberada)")
    except Exception:
        pass


async def main():
    global loop
    loop = asyncio.get_event_loop()

    print(f"""
╔══════════════════════════════════════════╗
║   SIM808 Bluetooth Bridge (WinRT API)    ║
╠══════════════════════════════════════════╣
║  WebSocket : ws://{WS_HOST}:{WS_PORT}       ║
║  SIM808 MAC: {BT_MAC}     ║
╠══════════════════════════════════════════╣
║  No index.html: Interface →              ║
║  "WebSocket Bridge" → Conectar           ║
╚══════════════════════════════════════════╝
""")

    asyncio.create_task(rx_dispatcher())

    async with websockets.serve(ws_handler, WS_HOST, WS_PORT):
        print(f"[WS] Servidor em ws://{WS_HOST}:{WS_PORT}")
        print("[WS] Aguardando browser... (Ctrl+C para sair)\n")
        await asyncio.Future()


if __name__ == "__main__":
    free_port(WS_PORT)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Bridge] Encerrado.")
