import sys
import socket
import json
import base64
import os
import tempfile
from datetime import datetime
import threading
import time
import struct
import queue
import ipaddress
import subprocess
import platform
from typing import Optional, Dict, List, Tuple

# PyQt5 импорты
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.QtCore import QEvent


class ReverseConnectionManager:
    """Менеджер обратного подключения (клиент ждет подключения от сервера)"""

    def __init__(self):
        self.listener: Optional[socket.socket] = None
        self.client_socket: Optional[socket.socket] = None
        self.connected = False
        self.host = ""
        self.port = 0
        self.lock = threading.Lock()
        self.mouse_lock = threading.Lock()  # отдельный lock для команд мыши
        self.is_waiting = False
        self.connection_queue = queue.Queue(maxsize=1)

    def _ensure_attrs(self):
        """Гарантирует наличие всех атрибутов — защита от неполной инициализации"""
        if not hasattr(self, 'listener'):
            self.listener = None
        if not hasattr(self, 'client_socket'):
            self.client_socket = None
        if not hasattr(self, 'connected'):
            self.connected = False
        if not hasattr(self, 'host'):
            self.host = ""
        if not hasattr(self, 'port'):
            self.port = 0
        if not hasattr(self, 'lock'):
            self.lock = threading.Lock()
        if not hasattr(self, 'is_waiting'):
            self.is_waiting = False
        if not hasattr(self, 'connection_queue'):
            self.connection_queue = queue.Queue(maxsize=1)

    def start_listening(self, port: int = 5000) -> Tuple[bool, str]:
        """Начать прослушивание порта для обратного подключения"""
        self._ensure_attrs()

        # Останавливаем предыдущее прослушивание если есть
        if self.is_waiting:
            self.stop_listening()

        # Сбрасываем очередь
        self.connection_queue = queue.Queue(maxsize=1)

        try:
            self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.listener.bind(('0.0.0.0', port))
            self.listener.listen(1)
            self.listener.settimeout(1)
            self.port = port
            self.is_waiting = True

            # Запускаем поток для принятия подключений
            threading.Thread(target=self._accept_connections, daemon=True).start()
            return True, f"Ожидание подключения на порту {port}"
        except Exception as e:
            return False, f"Ошибка запуска прослушивания: {str(e)}"

    def _accept_connections(self):
        """Принимаем подключения в отдельном потоке"""
        while self.is_waiting:
            try:
                client, address = self.listener.accept()
                print(f"[DEBUG] Подключение от {address}")

                # Если уже есть подключение, закрываем старое
                if self.connected and self.client_socket:
                    try:
                        self.client_socket.close()
                    except:
                        pass

                self.client_socket = client
                self.connected = True
                self.host = address[0]
                self.is_waiting = False

                # Уведомляем основной поток
                try:
                    if hasattr(self, 'connection_queue'):
                        self.connection_queue.put_nowait(True)
                except queue.Full:
                    pass

                # Закрываем listener
                if self.listener:
                    self.listener.close()
                    self.listener = None

                break

            except socket.timeout:
                continue
            except Exception as e:
                print(f"[DEBUG] Ошибка принятия подключения: {e}")
                break

    def stop_listening(self):
        """Остановить прослушивание"""
        self.is_waiting = False
        if self.listener:
            try:
                self.listener.close()
            except:
                pass
            self.listener = None

    def disconnect(self):
        """Отключиться от сервера"""
        self.connected = False
        self.is_waiting = False
        if self.client_socket:
            try:
                self.client_socket.close()
            except:
                pass
            self.client_socket = None
        print("[DEBUG] Отключено от сервера")

    def _send_all(self, data: bytes) -> bool:
        """Надежная отправка всех данных"""
        try:
            total_size = len(data)
            self.client_socket.sendall(struct.pack('!I', total_size))
            self.client_socket.sendall(data)
            return True
        except Exception as e:
            print(f"[DEBUG] Ошибка отправки данных: {e}")
            return False

    def _receive_all(self, timeout: int = 120) -> Optional[bytes]:
        """Надежное получение всех данных"""
        if not self.client_socket:
            return None

        self.client_socket.settimeout(timeout)

        try:
            size_data = b""
            while len(size_data) < 4:
                chunk = self.client_socket.recv(4 - len(size_data))
                if not chunk:
                    return None
                size_data += chunk

            total_size = struct.unpack('!I', size_data)[0]

            data = b""
            while len(data) < total_size:
                chunk = self.client_socket.recv(min(4096, total_size - len(data)))
                if not chunk:
                    return None
                data += chunk

            return data
        except socket.timeout:
            return None
        except Exception as e:
            print(f"[DEBUG] Ошибка получения данных: {e}")
            return None

    def send_command(self, command_type: str, **kwargs) -> Tuple[Optional[Dict], Optional[str]]:
        """Отправка команды на сервер"""
        with self.lock:
            if not self.connected or not self.client_socket:
                return None, "Нет подключения к серверу"

            try:
                request = {'type': command_type, **kwargs}
                request_json = json.dumps(request, ensure_ascii=False)
                request_data = request_json.encode('utf-8')

                if not self._send_all(request_data):
                    self.connected = False  # реальная ошибка отправки — соединение разорвано
                    return None, "Ошибка отправки данных"

                response_data = self._receive_all(timeout=120)
                if response_data is None:
                    # Проверяем жив ли сокет прежде чем сбрасывать connected
                    try:
                        self.client_socket.getpeername()
                        # Сокет жив — просто таймаут ответа
                        return None, "Таймаут ожидания ответа"
                    except:
                        # Сокет мёртв
                        self.connected = False
                        return None, "Соединение разорвано"

                try:
                    response_json = response_data.decode('utf-8', errors='ignore')
                    response = json.loads(response_json)
                    return response, None
                except json.JSONDecodeError as e:
                    return None, f"Неверный формат ответа: {str(e)}"

            except Exception as e:
                return None, f"Ошибка отправки команды: {str(e)}"

    def is_connected(self) -> bool:
        """Проверка подключения"""
        self._ensure_attrs()
        return self.connected and self.client_socket is not None

    def send_fire_and_forget(self, command_type: str, **kwargs) -> bool:
        """Отправить команду мыши — отдельный lock, без ожидания ответа."""
        try:
            if not self.connected or not self.client_socket:
                return False
            data = json.dumps({'type': command_type, **kwargs}, ensure_ascii=False).encode('utf-8')
            with self.mouse_lock:
                self.client_socket.sendall(struct.pack('!I', len(data)))
                self.client_socket.sendall(data)
            return True
        except Exception as e:
            print(f"[mouse] error: {e}")
            return False

    def open_mouse_socket(self):
        """Для обратного подключения отдельный сокет невозможен — используем основной."""
        return None  # сигнал что надо использовать send_fire_and_forget




class CommandWorker(QThread):
    """Рабочий поток для выполнения команд"""
    result_ready = pyqtSignal(dict, str)
    progress = pyqtSignal(str)

    def __init__(self, connection_manager):
        super().__init__()
        self.connection_manager = connection_manager
        self.command_type = None
        self.command_kwargs = {}

    def set_command(self, command_type, **kwargs):
        """Установка команды для выполнения"""
        self.command_type = command_type
        self.command_kwargs = kwargs

    def run(self):
        """Выполнение команды"""
        if not self.command_type:
            return

        self.progress.emit(f"Выполнение {self.command_type}...")
        result, error = self.connection_manager.send_command(
            self.command_type,
            **self.command_kwargs
        )

        if result:
            result['_command_type'] = self.command_type

        self.result_ready.emit(result or {}, error or "")


class ConnectionManager:
    """Менеджер подключения (клиент подключается к серверу)"""

    def __init__(self):
        self.socket: Optional[socket.socket] = None
        self.connected = False
        self.host = ""
        self.port = 0
        self.lock = threading.Lock()
        self.mouse_lock = threading.Lock()
        self.receive_timeout = 120
        self.connect_timeout = 10

    def connect(self, host: str, port: int) -> Tuple[bool, str]:
        """Подключение к серверу"""
        try:
            print(f"[DEBUG] Пытаюсь подключиться к {host}:{port}")
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(self.connect_timeout)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self.socket.connect((host, port))
            self.socket.settimeout(self.receive_timeout)
            self.connected = True
            self.host = host
            self.port = port
            print(f"[DEBUG] Успешно подключено к {host}:{port}")
            return True, "Успешно подключено"
        except socket.timeout:
            self._cleanup_socket()
            return False, "Таймаут подключения (сервер не отвечает)"
        except ConnectionRefusedError:
            self._cleanup_socket()
            return False, "Сервер отказал в подключении"
        except Exception as e:
            self._cleanup_socket()
            return False, f"Ошибка подключения: {str(e)}"

    def _cleanup_socket(self):
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
            self.socket = None
        self.connected = False

    def _send_all(self, data: bytes) -> bool:
        try:
            self.socket.sendall(struct.pack('!I', len(data)))
            self.socket.sendall(data)
            return True
        except Exception as e:
            print(f"[DEBUG] Ошибка отправки: {e}")
            return False

    def _receive_all(self, timeout: int = 120) -> Optional[bytes]:
        if not self.socket:
            return None
        self.socket.settimeout(timeout)
        try:
            size_data = b""
            while len(size_data) < 4:
                chunk = self.socket.recv(4 - len(size_data))
                if not chunk:
                    return None
                size_data += chunk
            total_size = struct.unpack('!I', size_data)[0]
            data = b""
            while len(data) < total_size:
                chunk = self.socket.recv(min(4096, total_size - len(data)))
                if not chunk:
                    return None
                data += chunk
            return data
        except socket.timeout:
            return None
        except Exception as e:
            print(f"[DEBUG] Ошибка получения: {e}")
            return None

    def send_command(self, command_type: str, **kwargs) -> Tuple[Optional[Dict], Optional[str]]:
        """Отправка команды на сервер"""
        with self.lock:
            if not self.connected or not self.socket:
                return None, "Нет подключения к серверу"
            try:
                request = {'type': command_type, **kwargs}
                data = json.dumps(request, ensure_ascii=False).encode('utf-8')
                if not self._send_all(data):
                    self.connected = False  # реальная ошибка — соединение разорвано
                    return None, "Ошибка отправки данных"
                response_data = self._receive_all(timeout=120)
                if response_data is None:
                    try:
                        self.socket.getpeername()
                        return None, "Таймаут ожидания ответа"
                    except:
                        self.connected = False
                        return None, "Соединение разорвано"
                try:
                    return json.loads(response_data.decode('utf-8', errors='ignore')), None
                except json.JSONDecodeError as e:
                    return None, f"Неверный формат ответа: {str(e)}"
            except Exception as e:
                return None, f"Ошибка отправки команды: {str(e)}"

    def disconnect(self):
        self.connected = False
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
            self.socket = None
        print("[DEBUG] Отключено от сервера")

    def is_connected(self) -> bool:
        return self.connected and self.socket is not None

    def send_fire_and_forget(self, command_type: str, **kwargs) -> bool:
        """Отправить команду мыши — отдельный lock, без ожидания ответа."""
        try:
            if not self.connected or not self.socket:
                return False
            data = json.dumps({'type': command_type, **kwargs}, ensure_ascii=False).encode('utf-8')
            with self.mouse_lock:
                self.socket.sendall(struct.pack('!I', len(data)))
                self.socket.sendall(data)
            return True
        except Exception as e:
            print(f"[mouse] error: {e}")
            return False

    def open_mouse_socket(self):
        """Открыть выделенный сокет для мыши — не мешает основному и трансляции."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            s.connect((self.host, self.port))
            s.settimeout(None)  # блокирующий, но мы только пишем
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # без буферизации
            return s
        except Exception as e:
            print(f"[mouse_socket] error: {e}")
            return None


class RdtLabel(QLabel):
    """QLabel с перехватом событий мыши для удалённого управления."""
    mouse_moved  = pyqtSignal(float, float)          # нормализованные 0..1
    mouse_clicked = pyqtSignal(float, float, int, bool)  # x, y, button(1/2/3), double
    mouse_scrolled = pyqtSignal(float, float, int)   # x, y, delta
    mouse_dragged  = pyqtSignal(float, float, float, float, int)  # x1,y1,x2,y2,btn

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMouseTracking(True)
        self._mouse_control = False
        self._drag_start = None
        self._drag_button = None

    def set_mouse_control(self, enabled: bool):
        self._mouse_control = enabled
        self.setCursor(Qt.CrossCursor if enabled else Qt.ArrowCursor)

    def _norm(self, pos):
        """Нормализовать координаты клика относительно изображения внутри label."""
        lw, lh = self.width(), self.height()
        pm = self.pixmap()
        if pm is None or pm.isNull():
            return None, None
        # Pixmap отцентрирован внутри label — найдём его реальные границы
        pw, ph = pm.width(), pm.height()
        ox = (lw - pw) / 2
        oy = (lh - ph) / 2
        rx = (pos.x() - ox) / pw
        ry = (pos.y() - oy) / ph
        if not (0 <= rx <= 1 and 0 <= ry <= 1):
            return None, None
        return rx, ry

    def mouseMoveEvent(self, event):
        if self._mouse_control:
            rx, ry = self._norm(event.pos())
            if rx is not None:
                self.mouse_moved.emit(rx, ry)
        super().mouseMoveEvent(event)

    def mousePressEvent(self, event):
        if self._mouse_control:
            rx, ry = self._norm(event.pos())
            if rx is not None:
                self._drag_start = (rx, ry)
                self._drag_button = event.button()
        super().mousePressEvent(event)

    def mouseReleaseEvent(self, event):
        if self._mouse_control:
            rx, ry = self._norm(event.pos())
            if rx is not None:
                btn = {Qt.LeftButton: 1, Qt.RightButton: 3, Qt.MiddleButton: 2}.get(event.button(), 1)
                if self._drag_start:
                    dx = abs(rx - self._drag_start[0])
                    dy = abs(ry - self._drag_start[1])
                    if dx > 0.01 or dy > 0.01:
                        # Это перетаскивание
                        self.mouse_dragged.emit(self._drag_start[0], self._drag_start[1], rx, ry, btn)
                    else:
                        self.mouse_clicked.emit(rx, ry, btn, False)
                self._drag_start = None
        super().mouseReleaseEvent(event)

    def mouseDoubleClickEvent(self, event):
        if self._mouse_control:
            rx, ry = self._norm(event.pos())
            if rx is not None:
                btn = {Qt.LeftButton: 1, Qt.RightButton: 3, Qt.MiddleButton: 2}.get(event.button(), 1)
                self.mouse_clicked.emit(rx, ry, btn, True)
        super().mouseDoubleClickEvent(event)

    def wheelEvent(self, event):
        if self._mouse_control:
            rx, ry = self._norm(event.pos())
            if rx is not None:
                delta = event.angleDelta().y() // 120
                self.mouse_scrolled.emit(rx, ry, delta)
        super().wheelEvent(event)


class ScreenStreamThread(QThread):
    """Выделенный поток трансляции — использует собственный сокет, не блокирует основной."""
    frame_ready = pyqtSignal(bytes)
    error_occurred = pyqtSignal(str)

    def __init__(self, connection_manager, fps=5, quality=40, scale=0.5):
        super().__init__()
        self.connection_manager = connection_manager
        self.interval = max(0.1, 1.0 / fps)
        self.quality = quality
        self.scale = scale
        self._running = False
        self._sock = None

    def stop(self):
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except:
                pass
            self._sock = None
        self.wait(2000)

    def _connect_own_socket(self):
        """Открыть собственный сокет к серверу."""
        mgr = self.connection_manager
        host = mgr.host
        # Для ConnectionManager порт в .port, для ReverseConnectionManager тоже
        port = mgr.port
        if not host or not port:
            return False
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            s.connect((host, port))
            s.settimeout(10)
            self._sock = s
            return True
        except Exception as e:
            self.error_occurred.emit(f"Не удалось подключить поток трансляции: {e}")
            return False

    def _send_recv(self, request: dict):
        """Отправить запрос и получить ответ через собственный сокет."""
        data = json.dumps(request, ensure_ascii=False).encode('utf-8')
        self._sock.sendall(struct.pack('!I', len(data)))
        self._sock.sendall(data)

        size_data = b''
        while len(size_data) < 4:
            chunk = self._sock.recv(4 - len(size_data))
            if not chunk:
                raise ConnectionError("Сокет трансляции разорван")
            size_data += chunk
        total = struct.unpack('!I', size_data)[0]
        resp = b''
        while len(resp) < total:
            chunk = self._sock.recv(min(65536, total - len(resp)))
            if not chunk:
                raise ConnectionError("Сокет трансляции разорван")
            resp += chunk
        return json.loads(resp.decode('utf-8', errors='ignore'))

    def run(self):
        self._running = True

        # Открываем собственный сокет только если это прямое подключение
        # (для обратного подключения у сервера нет listen-сокета — используем основной)
        use_own_socket = hasattr(self.connection_manager, 'socket')  # ConnectionManager
        if use_own_socket:
            if not self._connect_own_socket():
                return
        else:
            use_own_socket = False

        while self._running:
            t_start = time.time()
            try:
                if use_own_socket and self._sock:
                    result = self._send_recv({
                        'type': 'screen_frame',
                        'quality': self.quality,
                        'scale': self.scale
                    })
                    error = None
                else:
                    result, error = self.connection_manager.send_command(
                        'screen_frame', quality=self.quality, scale=self.scale)
            except Exception as e:
                if self._running:
                    self.error_occurred.emit(str(e))
                break

            if not self._running:
                break

            if error:
                self.error_occurred.emit(error)
                break

            if result and result.get('success') and result.get('frame'):
                try:
                    img_data = base64.b64decode(result['frame'])
                    self.frame_ready.emit(img_data)
                except Exception as e:
                    self.error_occurred.emit(str(e))
                    break

            elapsed = time.time() - t_start
            sleep_time = self.interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

        if self._sock:
            try:
                self._sock.close()
            except:
                pass
            self._sock = None


class MouseSyncThread(QThread):
    """Выделенный поток для синхронизации мыши с минимальной задержкой.
    Держит постоянное соединение и отправляет команды из очереди."""
    error_occurred = pyqtSignal(str)

    def __init__(self, connection_manager):
        super().__init__()
        self.connection_manager = connection_manager
        self._queue = queue.Queue(maxsize=2)  # maxsize=2 — старые движения выбрасываем
        self._running = False
        self._sock = None

    def stop(self):
        self._running = False
        # Разблокируем очередь
        try:
            self._queue.put_nowait(None)
        except:
            pass
        self.wait(1000)

    def send(self, command_type: str, **kwargs):
        """Поставить команду в очередь. Для mouse_move — заменяем старую."""
        cmd = {'type': command_type, **kwargs}
        if command_type == 'mouse_move':
            # Выбрасываем старые движения — нужна только последняя позиция
            while not self._queue.empty():
                try:
                    self._queue.get_nowait()
                except:
                    break
        try:
            self._queue.put_nowait(cmd)
        except queue.Full:
            pass

    def _connect(self):
        """Подключиться к серверу."""
        # Для прямого подключения — открываем выделенный сокет
        sock = self.connection_manager.open_mouse_socket()
        if sock:
            self._sock = sock
            return True
        # Для обратного подключения — используем основной сокет через send_fire_and_forget
        self._sock = None
        return True  # работаем через fallback

    def _send_cmd(self, cmd: dict) -> bool:
        """Отправить одну команду."""
        data = json.dumps(cmd, ensure_ascii=False).encode('utf-8')
        header = struct.pack('!I', len(data))
        if self._sock:
            try:
                self._sock.sendall(header + data)
                # Читаем ответ чтобы не переполнить буфер сервера
                size_data = b''
                self._sock.settimeout(0.5)
                try:
                    while len(size_data) < 4:
                        chunk = self._sock.recv(4 - len(size_data))
                        if not chunk:
                            return False
                        size_data += chunk
                    if len(size_data) == 4:
                        total = struct.unpack('!I', size_data)[0]
                        resp = b''
                        while len(resp) < total:
                            chunk = self._sock.recv(min(4096, total - len(resp)))
                            if not chunk:
                                return False
                            resp += chunk
                except socket.timeout:
                    pass
                finally:
                    self._sock.settimeout(None)
                return True
            except Exception as e:
                print(f"[mouse_thread] socket error: {e}")
                return False
        else:
            # Fallback для обратного подключения
            return self.connection_manager.send_fire_and_forget(cmd['type'], **{k: v for k, v in cmd.items() if k != 'type'})

    def run(self):
        self._running = True
        if not self._connect():
            self.error_occurred.emit("Не удалось подключить mouse socket")
            return

        while self._running:
            try:
                cmd = self._queue.get(timeout=1.0)
                if cmd is None:  # сигнал остановки
                    break
                if not self._send_cmd(cmd):
                    # Пробуем переподключиться один раз
                    if self._sock:
                        try:
                            self._sock.close()
                        except:
                            pass
                    self._sock = self.connection_manager.open_mouse_socket()
                    if self._sock:
                        self._send_cmd(cmd)  # повторяем
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[mouse_thread] error: {e}")

        if self._sock:
            try:
                self._sock.close()
            except:
                pass


class RemoteClientGUI(QMainWindow):
    """Графический интерфейс клиента с поддержкой обратного подключения"""

    def __init__(self):
        super().__init__()
        # Оба менеджера подключений
        self.connection_manager = ConnectionManager()  # Оригинальный
        self.reverse_manager = ReverseConnectionManager()  # Обратный
        self.current_mode = "direct"  # "direct" или "reverse"

        self.worker_thread = None
        self.current_dir = "."
        self.current_screenshot = None
        self.temp_files = []  # Список временных файлов для очистки
        self.command_history_list = []  # История команд
        self.history_index = -1  # Текущая позиция в истории
        self.rdt_last_frame = None  # Последний кадр экрана
        self.rdt_mouse_enabled = False
        self.rdt_server_screen_w = 1920
        self.rdt_server_screen_h = 1080
        self._last_mouse_move_time = 0  # дросселинг движения
        self._mouse_thread = None  # поток синхронизации мыши
        self.init_ui()
        self.setup_dark_theme()

    def setup_dark_theme(self):
        """Настройка темной темы"""
        # Создаем темную палитру
        dark_palette = QPalette()

        # Устанавливаем цвета для темной темы
        dark_palette.setColor(QPalette.Window, QColor(30, 30, 30))
        dark_palette.setColor(QPalette.WindowText, QColor(220, 220, 220))
        dark_palette.setColor(QPalette.Base, QColor(25, 25, 25))
        dark_palette.setColor(QPalette.AlternateBase, QColor(35, 35, 35))
        dark_palette.setColor(QPalette.ToolTipBase, QColor(40, 40, 40))
        dark_palette.setColor(QPalette.ToolTipText, QColor(220, 220, 220))
        dark_palette.setColor(QPalette.Text, QColor(220, 220, 220))
        dark_palette.setColor(QPalette.Button, QColor(45, 45, 45))
        dark_palette.setColor(QPalette.ButtonText, QColor(220, 220, 220))
        dark_palette.setColor(QPalette.BrightText, QColor(255, 100, 100))
        dark_palette.setColor(QPalette.Link, QColor(80, 160, 240))
        dark_palette.setColor(QPalette.Highlight, QColor(80, 160, 240))
        dark_palette.setColor(QPalette.HighlightedText, QColor(30, 30, 30))

        # Устанавливаем цвета для состояний
        dark_palette.setColor(QPalette.Disabled, QPalette.WindowText, QColor(120, 120, 120))
        dark_palette.setColor(QPalette.Disabled, QPalette.Text, QColor(120, 120, 120))
        dark_palette.setColor(QPalette.Disabled, QPalette.ButtonText, QColor(120, 120, 120))
        dark_palette.setColor(QPalette.Disabled, QPalette.Highlight, QColor(60, 60, 60))
        dark_palette.setColor(QPalette.Disabled, QPalette.HighlightedText, QColor(120, 120, 120))

        self.setPalette(dark_palette)

        # Расширенный CSS для темной темы
        self.setStyleSheet("""
            /* Основное окно */
            QMainWindow {
                background-color: #1e1e1e;
                color: #d4d4d4;
            }

            /* Текст и метки */
            QLabel {
                color: #d4d4d4;
                font-family: 'Segoe UI', 'Arial', sans-serif;
            }

            QLabel#statusLabel {
                font-weight: bold;
                font-size: 11pt;
                padding: 5px;
                border-radius: 3px;
                background-color: #252526;
            }

            /* Поля ввода */
            QLineEdit, QTextEdit, QPlainTextEdit, QTextBrowser {
                background-color: #252526;
                color: #d4d4d4;
                border: 1px solid #3e3e42;
                border-radius: 3px;
                padding: 5px;
                font-family: 'Consolas', 'Monaco', monospace;
                font-size: 10pt;
                selection-background-color: #007acc;
                selection-color: #ffffff;
            }

            QLineEdit:focus, QTextEdit:focus, QPlainTextEdit:focus {
                border: 1px solid #007acc;
                background-color: #2d2d30;
            }

            /* Кнопки */
            QPushButton {
                background-color: #3e3e42;
                color: #ffffff;
                border: 1px solid #555;
                border-radius: 4px;
                padding: 8px 16px;
                font-weight: bold;
                font-family: 'Segoe UI', 'Arial', sans-serif;
                min-height: 28px;
            }

            QPushButton:hover {
                background-color: #4e4e52;
                border: 1px solid #666;
            }

            QPushButton:pressed {
                background-color: #2e2e32;
            }

            QPushButton:disabled {
                background-color: #2d2d30;
                color: #666;
                border: 1px solid #444;
            }

            /* Специальные кнопки */
            QPushButton#connectBtn {
                background-color: #0e7c0e;
            }

            QPushButton#connectBtn:hover {
                background-color: #0f9c0f;
            }

            QPushButton#connectBtn:pressed {
                background-color: #0c6c0c;
            }

            QPushButton#disconnectBtn {
                background-color: #c42b1c;
            }

            QPushButton#disconnectBtn:hover {
                background-color: #e43523;
            }

            QPushButton#disconnectBtn:pressed {
                background-color: #a32115;
            }

            QPushButton#listenBtn {
                background-color: #2b579a;
            }

            QPushButton#listenBtn:hover {
                background-color: #2f6fcf;
            }

            QPushButton#listenBtn:pressed {
                background-color: #234781;
            }

            /* Комбобокс и спинбокс */
            QComboBox, QSpinBox {
                background-color: #3e3e42;
                color: #ffffff;
                border: 1px solid #555;
                border-radius: 3px;
                padding: 5px;
                min-height: 24px;
            }

            QComboBox:editable, QSpinBox:editable {
                background-color: #252526;
            }

            QComboBox::drop-down {
                border: none;
                background-color: #3e3e42;
            }

            QComboBox::down-arrow {
                image: none;
                border-left: 4px solid transparent;
                border-right: 4px solid transparent;
                border-top: 5px solid #ffffff;
            }

            QComboBox QAbstractItemView {
                background-color: #252526;
                color: #d4d4d4;
                selection-background-color: #007acc;
                selection-color: #ffffff;
                border: 1px solid #3e3e42;
            }

            /* Фреймы и групповые рамки */
            QFrame {
                background-color: #2d2d30;
                border: 1px solid #3e3e42;
                border-radius: 4px;
            }

            QGroupBox {
                background-color: #2d2d30;
                border: 1px solid #3e3e42;
                border-radius: 4px;
                margin-top: 12px;
                padding-top: 12px;
                font-weight: bold;
                color: #d4d4d4;
                font-size: 10pt;
            }

            QGroupBox::title {
                subcontrol-origin: margin;
                left: 12px;
                padding: 0 6px 0 6px;
                background-color: #2d2d30;
            }

            /* Табы */
            QTabWidget::pane {
                background-color: #2d2d30;
                border: 1px solid #3e3e42;
                border-radius: 4px;
            }

            QTabBar::tab {
                background-color: #3e3e42;
                color: #d4d4d4;
                padding: 8px 16px;
                margin-right: 2px;
                border-top-left-radius: 4px;
                border-top-right-radius: 4px;
                font-family: 'Segoe UI', 'Arial', sans-serif;
            }

            QTabBar::tab:selected {
                background-color: #007acc;
                color: #ffffff;
                font-weight: bold;
            }

            QTabBar::tab:hover:!selected {
                background-color: #4e4e52;
            }

            /* Таблица */
            QTableWidget {
                background-color: #252526;
                border: 1px solid #3e3e42;
                gridline-color: #3e3e42;
                color: #d4d4d4;
                selection-background-color: #007acc;
                selection-color: #ffffff;
                font-family: 'Consolas', 'Monaco', monospace;
                font-size: 10pt;
            }

            QHeaderView::section {
                background-color: #3e3e42;
                color: #d4d4d4;
                padding: 6px;
                border: 1px solid #3e3e42;
                font-weight: bold;
            }

            QTableWidget::item {
                padding: 4px;
            }

            QTableWidget::item:selected {
                background-color: #007acc;
                color: #ffffff;
            }

            /* Списки */
            QListWidget {
                background-color: #252526;
                border: 1px solid #3e3e42;
                color: #d4d4d4;
                selection-background-color: #007acc;
                selection-color: #ffffff;
                font-family: 'Consolas', 'Monaco', monospace;
                font-size: 10pt;
            }

            QListWidget::item {
                padding: 6px;
                border-bottom: 1px solid #3e3e42;
            }

            QListWidget::item:selected {
                background-color: #007acc;
                color: #ffffff;
            }

            QListWidget::item:hover {
                background-color: #2d2d30;
            }

            /* Прогресс-бар */
            QProgressBar {
                background-color: #252526;
                border: 1px solid #3e3e42;
                border-radius: 3px;
                text-align: center;
                color: #d4d4d4;
                font-family: 'Segoe UI', 'Arial', sans-serif;
            }

            QProgressBar::chunk {
                background-color: #007acc;
                border-radius: 3px;
            }

            /* Статус бар */
            QStatusBar {
                background-color: #2d2d30;
                color: #d4d4d4;
                border-top: 1px solid #3e3e42;
                font-family: 'Segoe UI', 'Arial', sans-serif;
            }

            /* Splitter */
            QSplitter::handle {
                background-color: #3e3e42;
            }

            QSplitter::handle:hover {
                background-color: #007acc;
            }

            /* Скроллбары */
            QScrollBar:vertical {
                background-color: #2d2d30;
                width: 12px;
                border: 1px solid #3e3e42;
            }

            QScrollBar::handle:vertical {
                background-color: #3e3e42;
                border-radius: 4px;
                min-height: 20px;
            }

            QScrollBar::handle:vertical:hover {
                background-color: #4e4e52;
            }

            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                height: 0px;
            }

            QScrollBar:horizontal {
                background-color: #2d2d30;
                height: 12px;
                border: 1px solid #3e3e42;
            }

            QScrollBar::handle:horizontal {
                background-color: #3e3e42;
                border-radius: 4px;
                min-width: 20px;
            }

            QScrollBar::handle:horizontal:hover {
                background-color: #4e4e52;
            }

            QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
                width: 0px;
            }

            /* Меню */
            QMenu {
                background-color: #2d2d30;
                color: #d4d4d4;
                border: 1px solid #3e3e42;
                font-family: 'Segoe UI', 'Arial', sans-serif;
            }

            QMenu::item {
                padding: 6px 24px 6px 12px;
            }

            QMenu::item:selected {
                background-color: #007acc;
                color: #ffffff;
            }

            QMenu::separator {
                height: 1px;
                background-color: #3e3e42;
                margin: 4px 0px;
            }

            /* Диалоговые окна */
            QDialog {
                background-color: #2d2d30;
                color: #d4d4d4;
            }

            QMessageBox {
                background-color: #2d2d30;
                color: #d4d4d4;
            }

            QMessageBox QLabel {
                color: #d4d4d4;
            }

            /* Дерево */
            QTreeWidget {
                background-color: #252526;
                border: 1px solid #3e3e42;
                color: #d4d4d4;
                selection-background-color: #007acc;
                selection-color: #ffffff;
            }

            QTreeWidget::item {
                padding: 4px;
            }

            QTreeWidget::item:selected {
                background-color: #007acc;
                color: #ffffff;
            }

            /* Заголовки секций */
            QHeaderView::section:checked {
                background-color: #007acc;
                color: #ffffff;
            }

            /* Проверка и радиокнопки */
            QCheckBox, QRadioButton {
                color: #d4d4d4;
                spacing: 8px;
            }

            QCheckBox::indicator, QRadioButton::indicator {
                width: 16px;
                height: 16px;
            }

            QCheckBox::indicator:unchecked {
                border: 1px solid #555;
                background-color: #252526;
            }

            QCheckBox::indicator:checked {
                border: 1px solid #007acc;
                background-color: #007acc;
            }

            QRadioButton::indicator:unchecked {
                border: 1px solid #555;
                border-radius: 8px;
                background-color: #252526;
            }

            QRadioButton::indicator:checked {
                border: 1px solid #007acc;
                border-radius: 8px;
                background-color: #007acc;
            }
        """)

    def init_ui(self):
        """Инициализация интерфейса"""
        self.setWindowTitle("🔗 Удаленный клиент управления - ОБРАТНОЕ ПОДКЛЮЧЕНИЕ")
        self.setGeometry(100, 100, 1400, 900)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(12, 12, 12, 12)
        main_layout.setSpacing(8)

        # 1. Панель выбора режима подключения
        mode_frame = QGroupBox("Режим подключения")
        mode_layout = QHBoxLayout(mode_frame)

        self.mode_combo = QComboBox()
        self.mode_combo.addItem("📡 Прямое подключение (клиент → сервер)")
        self.mode_combo.addItem("🔄 Обратное подключение (сервер → клиент)")
        self.mode_combo.setCurrentIndex(0)
        self.mode_combo.currentIndexChanged.connect(self.on_mode_changed)

        mode_layout.addWidget(self.mode_combo)
        mode_layout.addStretch()

        main_layout.addWidget(mode_frame)

        # 2. Панель прямого подключения
        self.direct_frame = QGroupBox("Прямое подключение")
        direct_layout = QGridLayout(self.direct_frame)

        # Строка 1: Поля ввода
        direct_layout.addWidget(QLabel("IP адрес сервера:"), 0, 0)
        self.host_input = QLineEdit("127.0.0.1")
        self.host_input.setPlaceholderText("Введите IP адрес...")
        direct_layout.addWidget(self.host_input, 0, 1)

        direct_layout.addWidget(QLabel("Порт:"), 0, 2)
        self.port_input = QSpinBox()
        self.port_input.setRange(1, 65535)
        self.port_input.setValue(5000)
        direct_layout.addWidget(self.port_input, 0, 3)

        direct_layout.addWidget(QLabel("Пароль:"), 1, 0)
        self.password_input = QLineEdit()
        self.password_input.setPlaceholderText("Оставьте пустым если нет пароля...")
        self.password_input.setEchoMode(QLineEdit.Password)
        direct_layout.addWidget(self.password_input, 1, 1)

        # Строка 3: Кнопки
        self.connect_btn = QPushButton("🔗 Подключиться")
        self.connect_btn.setObjectName("connectBtn")
        self.connect_btn.clicked.connect(self.connect_to_server)
        self.connect_btn.setMinimumHeight(35)
        direct_layout.addWidget(self.connect_btn, 2, 0, 1, 2)

        self.test_direct_btn = QPushButton("🔍 Тест подключения")
        self.test_direct_btn.clicked.connect(lambda: self.send_command('test'))
        self.test_direct_btn.setMinimumHeight(35)
        direct_layout.addWidget(self.test_direct_btn, 2, 2, 1, 2)

        direct_layout.setColumnStretch(1, 1)
        main_layout.addWidget(self.direct_frame)

        # 3. Панель обратного подключения
        self.reverse_frame = QGroupBox("Обратное подключение")
        self.reverse_frame.setVisible(False)
        reverse_layout = QGridLayout(self.reverse_frame)

        # Строка 1: Поля ввода
        reverse_layout.addWidget(QLabel("Порт для прослушивания:"), 0, 0)
        self.listen_port_input = QSpinBox()
        self.listen_port_input.setRange(1, 65535)
        self.listen_port_input.setValue(5001)  # Другой порт для обратного подключения
        reverse_layout.addWidget(self.listen_port_input, 0, 1)

        # Строка 2: Информация
        self.reverse_info_label = QLabel("Ожидание подключения от сервера...")
        self.reverse_info_label.setWordWrap(True)
        reverse_layout.addWidget(self.reverse_info_label, 1, 0, 1, 2)

        # Строка 3: Кнопки
        self.listen_btn = QPushButton("👂 Начать ожидание")
        self.listen_btn.setObjectName("listenBtn")
        self.listen_btn.setCheckable(False)
        self.listen_btn.clicked.connect(self.start_listening)
        self.listen_btn.setMinimumHeight(35)
        reverse_layout.addWidget(self.listen_btn, 2, 0)

        self.stop_listen_btn = QPushButton("⏹️ Остановить")
        self.stop_listen_btn.clicked.connect(self.stop_listening)
        self.stop_listen_btn.setEnabled(False)
        self.stop_listen_btn.setMinimumHeight(35)
        reverse_layout.addWidget(self.stop_listen_btn, 2, 1)

        reverse_layout.setColumnStretch(1, 1)
        main_layout.addWidget(self.reverse_frame)

        # 4. Панель управления подключением
        control_frame = QGroupBox("Управление подключением")
        control_layout = QHBoxLayout(control_frame)

        self.disconnect_btn = QPushButton("🔌 Отключиться")
        self.disconnect_btn.setObjectName("disconnectBtn")
        self.disconnect_btn.clicked.connect(self.disconnect_from_server)
        self.disconnect_btn.setEnabled(False)
        self.disconnect_btn.setMinimumHeight(35)

        self.status_label = QLabel("🚫 Не подключено")
        self.status_label.setObjectName("statusLabel")

        control_layout.addWidget(self.disconnect_btn)
        control_layout.addWidget(self.status_label)
        control_layout.addStretch()

        # Индикатор подключения
        self.connection_indicator = QLabel("●")
        self.connection_indicator.setStyleSheet("""
            QLabel {
                color: #ff4444;
                font-size: 16pt;
                font-weight: bold;
            }
        """)
        control_layout.addWidget(self.connection_indicator)

        main_layout.addWidget(control_frame)

        # 5. Основная область
        splitter = QSplitter(Qt.Vertical)

        # Верхняя часть - командная строка
        top_widget = QWidget()
        top_layout = QVBoxLayout(top_widget)
        top_layout.setContentsMargins(0, 0, 0, 0)

        # Панель команд
        command_frame = QGroupBox("Командная строка")
        command_layout = QHBoxLayout(command_frame)

        self.command_input = QLineEdit()
        self.command_input.setPlaceholderText("Введите команду (help для справки)...")
        self.command_input.returnPressed.connect(self.execute_command)
        self.command_input.installEventFilter(self)

        self.execute_btn = QPushButton("▶️ Выполнить")
        self.execute_btn.clicked.connect(self.execute_command)
        self.execute_btn.setMinimumWidth(120)

        self.clear_output_btn = QPushButton("🗑️ Очистить")
        self.clear_output_btn.clicked.connect(self.clear_output)
        self.clear_output_btn.setMinimumWidth(120)

        command_layout.addWidget(self.command_input)
        command_layout.addWidget(self.execute_btn)
        command_layout.addWidget(self.clear_output_btn)

        # Поле вывода
        self.output_text = QTextEdit()
        self.output_text.setReadOnly(True)
        self.output_text.setFont(QFont("Consolas", 10))

        top_layout.addWidget(command_frame)
        top_layout.addWidget(self.output_text)

        # Нижняя часть - табы
        self.tabs = QTabWidget()
        self.tabs.setTabPosition(QTabWidget.North)

        # Вкладка 1: Файловый менеджер
        files_tab = QWidget()
        files_layout = QVBoxLayout(files_tab)

        # Панель навигации
        nav_frame = QGroupBox("Навигация")
        nav_layout = QHBoxLayout(nav_frame)

        # Выбор диска
        self.drive_combo = QComboBox()
        self.drive_combo.setMinimumWidth(90)
        self.drive_combo.setToolTip("Выбрать диск")
        self.drive_combo.currentIndexChanged.connect(self.on_drive_selected)

        self.path_label = QLabel("📁 Путь: /")
        self.up_btn = QPushButton("⬆️ Наверх")
        self.up_btn.clicked.connect(self.navigate_up)
        self.home_btn = QPushButton("🏠 Домашняя")
        self.home_btn.clicked.connect(self.navigate_home)
        self.refresh_btn = QPushButton("🔄 Обновить")
        self.refresh_btn.clicked.connect(self.refresh_files)

        nav_layout.addWidget(QLabel("💾 Диск:"))
        nav_layout.addWidget(self.drive_combo)
        nav_layout.addWidget(self.path_label)
        nav_layout.addStretch()
        nav_layout.addWidget(self.home_btn)
        nav_layout.addWidget(self.up_btn)
        nav_layout.addWidget(self.refresh_btn)

        # Таблица файлов
        self.files_table = QTableWidget(0, 4)
        self.files_table.setHorizontalHeaderLabels(["📄 Имя", "📊 Тип", "📏 Размер", "📅 Изменен"])
        self.files_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.files_table.doubleClicked.connect(self.on_file_double_click)
        self.files_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.files_table.customContextMenuRequested.connect(self.show_file_context_menu)

        # Панель действий с файлами
        actions_frame = QGroupBox("Действия с файлами")
        actions_layout = QHBoxLayout(actions_frame)

        # Новые кнопки
        self.open_remote_btn = QPushButton("▶️ Открыть удаленно")
        self.open_remote_btn.clicked.connect(self.open_file_remotely)
        self.open_remote_btn.setToolTip("Запустить выбранный файл на удаленном компьютере")

        # Существующие кнопки
        self.download_btn = QPushButton("📥 Скачать")
        self.download_btn.clicked.connect(self.download_file)
        self.upload_btn = QPushButton("📤 Загрузить")
        self.upload_btn.clicked.connect(self.upload_file)
        self.rename_btn = QPushButton("✏️ Переименовать")
        self.rename_btn.clicked.connect(self.rename_file)
        self.delete_btn = QPushButton("🗑️ Удалить")
        self.delete_btn.clicked.connect(self.delete_file)
        self.create_folder_btn = QPushButton("📁 Создать папку")
        self.create_folder_btn.clicked.connect(self.create_folder)
        self.copy_file_btn = QPushButton("📋 Копировать")
        self.copy_file_btn.clicked.connect(self.copy_file_on_server)
        self.zip_btn = QPushButton("📦 В архив")
        self.zip_btn.clicked.connect(self.zip_selected_file)
        self.unzip_btn = QPushButton("📂 Распаковать")
        self.unzip_btn.clicked.connect(self.unzip_selected_file)
        self.file_info_btn = QPushButton("ℹ️ Инфо")
        self.file_info_btn.clicked.connect(self.show_file_info_detailed)

        # Добавляем кнопки в правильном порядке
        actions_layout.addWidget(self.open_remote_btn)
        actions_layout.addWidget(self.download_btn)
        actions_layout.addWidget(self.upload_btn)
        actions_layout.addWidget(self.copy_file_btn)
        actions_layout.addWidget(self.zip_btn)
        actions_layout.addWidget(self.unzip_btn)
        actions_layout.addWidget(self.rename_btn)
        actions_layout.addWidget(self.delete_btn)
        actions_layout.addWidget(self.create_folder_btn)
        actions_layout.addWidget(self.file_info_btn)
        actions_layout.addStretch()

        files_layout.addWidget(nav_frame)
        files_layout.addWidget(self.files_table)
        files_layout.addWidget(actions_frame)

        # Вкладка 2: Системная информация
        sysinfo_tab = QWidget()
        sysinfo_layout = QVBoxLayout(sysinfo_tab)

        self.sysinfo_text = QTextEdit()
        self.sysinfo_text.setReadOnly(True)
        self.sysinfo_text.setFont(QFont("Consolas", 10))

        sysinfo_buttons = QHBoxLayout()
        self.sysinfo_btn = QPushButton("🖥️ Получить информацию")
        self.sysinfo_btn.clicked.connect(self.get_system_info)
        self.copy_sysinfo_btn = QPushButton("📋 Копировать")
        self.copy_sysinfo_btn.clicked.connect(self.copy_system_info)

        sysinfo_buttons.addWidget(self.sysinfo_btn)
        sysinfo_buttons.addWidget(self.copy_sysinfo_btn)
        sysinfo_buttons.addStretch()

        sysinfo_layout.addWidget(self.sysinfo_text)
        sysinfo_layout.addLayout(sysinfo_buttons)

        # Вкладка 3: Скриншот
        screenshot_tab = QWidget()
        screenshot_layout = QVBoxLayout(screenshot_tab)

        # Область для скриншота
        self.screenshot_label = QLabel("📸 Скриншот появится здесь")
        self.screenshot_label.setAlignment(Qt.AlignCenter)
        self.screenshot_label.setMinimumHeight(400)
        self.screenshot_label.setStyleSheet("""
            QLabel {
                border: 2px solid #3e3e42;
                border-radius: 5px;
                background-color: #252526;
                padding: 20px;
                color: #888;
                font-size: 12pt;
                font-style: italic;
            }
        """)

        # Кнопки для скриншотов
        screenshot_buttons = QHBoxLayout()
        self.screenshot_btn = QPushButton("📸 Сделать скриншот")
        self.screenshot_btn.clicked.connect(self.take_screenshot)
        self.save_screenshot_btn = QPushButton("💾 Сохранить скриншот")
        self.save_screenshot_btn.clicked.connect(self.save_screenshot)
        self.save_screenshot_btn.setEnabled(False)
        self.clipboard_screenshot_btn = QPushButton("📋 В буфер обмена")
        self.clipboard_screenshot_btn.clicked.connect(self.copy_screenshot_to_clipboard)
        self.clipboard_screenshot_btn.setEnabled(False)

        screenshot_buttons.addWidget(self.screenshot_btn)
        screenshot_buttons.addWidget(self.save_screenshot_btn)
        screenshot_buttons.addWidget(self.clipboard_screenshot_btn)
        screenshot_buttons.addStretch()

        screenshot_layout.addWidget(self.screenshot_label)
        screenshot_layout.addLayout(screenshot_buttons)

        # Вкладка 4: Управление процессами (ИСПРАВЛЕННАЯ)
        processes_tab = QWidget()
        processes_layout = QVBoxLayout(processes_tab)

        # Панель управления процессами
        processes_frame = QGroupBox("Управление процессами")
        processes_controls = QHBoxLayout(processes_frame)

        self.process_list_btn = QPushButton("📋 Получить список процессов")
        self.process_list_btn.clicked.connect(self.get_process_list)
        self.process_kill_btn = QPushButton("🔪 Завершить процесс")
        self.process_kill_btn.clicked.connect(self.kill_process_dialog)

        processes_controls.addWidget(self.process_list_btn)
        processes_controls.addWidget(self.process_kill_btn)
        processes_controls.addStretch()

        # Таблица процессов
        self.processes_table = QTableWidget(0, 3)
        self.processes_table.setHorizontalHeaderLabels(["PID", "Имя", "Память"])
        self.processes_table.setColumnWidth(0, 100)  # Ширина для PID
        self.processes_table.setColumnWidth(1, 1000)  # Ширина для имени
        self.processes_table.setColumnWidth(2, 150)  # Ширина для памяти
        self.processes_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.processes_table.setSelectionMode(QTableWidget.SingleSelection)
        self.processes_table.setSortingEnabled(True)
        self.processes_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.processes_table.customContextMenuRequested.connect(self.show_process_context_menu)

        processes_layout.addWidget(processes_frame)
        processes_layout.addWidget(self.processes_table)

        # Вкладка 5: Быстрые команды
        quick_tab = QWidget()
        quick_layout = QVBoxLayout(quick_tab)

        quick_frame = QGroupBox("Быстрые команды")
        quick_grid = QGridLayout(quick_frame)

        # Создаем кнопки быстрых команд
        quick_commands = [
            ("🔍 Тест подключения", "test"),
            ("🖥️ Информация о системе", "sysinfo"),
            ("📁 Список файлов", "dir"),
            ("🌐 Сетевые настройки", "ipconfig"),
            ("👤 Текущий пользователь", "whoami"),
            ("📡 Сетевые соединения", "netstat -an"),
            ("🔄 Перезагрузка", "shutdown /r /t 5"),
            ("⏹️ Выключение", "shutdown /s /t 5"),
            ("📖 Помощь", "help")
        ]
        row, col = 0, 0
        for name, cmd in quick_commands:
            btn = QPushButton(name)
            btn.clicked.connect(lambda checked, c=cmd: self.execute_quick_command(c))
            btn.setMinimumHeight(40)
            quick_grid.addWidget(btn, row, col)
            col += 1
            if col > 2:
                col = 0
                row += 1

        quick_layout.addWidget(quick_frame)
        quick_layout.addStretch()


        # Вкладка 6: Поиск файлов
        search_tab = QWidget()
        search_layout = QVBoxLayout(search_tab)

        search_ctrl = QGroupBox("Поиск файлов")
        search_ctrl_layout = QGridLayout(search_ctrl)

        search_ctrl_layout.addWidget(QLabel("Папка для поиска:"), 0, 0)
        self.search_path_input = QLineEdit()
        self.search_path_input.setPlaceholderText("Оставьте пустым для текущей папки...")
        search_ctrl_layout.addWidget(self.search_path_input, 0, 1)

        search_ctrl_layout.addWidget(QLabel("Маска (например *.txt):"), 1, 0)
        self.search_pattern_input = QLineEdit("*")
        search_ctrl_layout.addWidget(self.search_pattern_input, 1, 1)

        self.search_recursive_chk = QCheckBox("Рекурсивный поиск")
        self.search_recursive_chk.setChecked(True)
        search_ctrl_layout.addWidget(self.search_recursive_chk, 2, 0)

        self.search_btn = QPushButton("🔍 Найти")
        self.search_btn.clicked.connect(self.search_files_on_server)
        self.search_btn.setMinimumHeight(35)
        search_ctrl_layout.addWidget(self.search_btn, 2, 1)

        search_ctrl_layout.setColumnStretch(1, 1)

        self.search_results_table = QTableWidget(0, 3)
        self.search_results_table.setHorizontalHeaderLabels(["📄 Имя", "📁 Путь", "📏 Размер"])
        self.search_results_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.search_results_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.search_results_table.customContextMenuRequested.connect(self.show_search_context_menu)
        self.search_results_table.doubleClicked.connect(self.navigate_to_search_result)

        self.search_status_label = QLabel("Введите параметры поиска и нажмите 'Найти'")

        search_layout.addWidget(search_ctrl)
        search_layout.addWidget(self.search_status_label)
        search_layout.addWidget(self.search_results_table)

        # Вкладка 7: История команд
        history_tab = QWidget()
        history_layout = QVBoxLayout(history_tab)

        history_ctrl = QHBoxLayout()
        self.history_refresh_btn = QPushButton("🔄 Загрузить историю с сервера")
        self.history_refresh_btn.clicked.connect(self.load_server_history)
        self.history_clear_btn = QPushButton("🗑️ Очистить локальную историю")
        self.history_clear_btn.clicked.connect(self.clear_local_history)
        history_ctrl.addWidget(self.history_refresh_btn)
        history_ctrl.addWidget(self.history_clear_btn)
        history_ctrl.addStretch()

        self.history_table = QTableWidget(0, 3)
        self.history_table.setHorizontalHeaderLabels(["⏰ Время", "💻 Команда", "📁 Рабочая папка"])
        self.history_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.history_table.doubleClicked.connect(self.run_from_history)

        history_layout.addLayout(history_ctrl)
        history_layout.addWidget(QLabel("Двойной клик — выполнить команду снова"))
        history_layout.addWidget(self.history_table)

        # Вкладка 8: Удалённый рабочий стол
        rdt = QWidget()
        rdt_layout = QVBoxLayout(rdt)

        rdt_ctrl = QGroupBox("Управление трансляцией")
        rdt_ctrl_layout = QHBoxLayout(rdt_ctrl)

        self.rdt_quality_label = QLabel("Качество:")
        self.rdt_quality = QSpinBox()
        self.rdt_quality.setRange(10, 95)
        self.rdt_quality.setValue(40)
        self.rdt_quality.setSuffix("%")

        self.rdt_fps_label = QLabel("FPS:")
        self.rdt_fps = QSpinBox()
        self.rdt_fps.setRange(1, 15)
        self.rdt_fps.setValue(5)

        self.rdt_scale_label = QLabel("Масштаб:")
        self.rdt_scale = QSpinBox()
        self.rdt_scale.setRange(10, 100)
        self.rdt_scale.setValue(50)
        self.rdt_scale.setSuffix("%")

        self.rdt_start_btn = QPushButton("▶️ Начать трансляцию")
        self.rdt_start_btn.setObjectName("connectBtn")
        self.rdt_start_btn.clicked.connect(self.start_remote_desktop)

        self.rdt_stop_btn = QPushButton("⏹️ Стоп")
        self.rdt_stop_btn.setObjectName("disconnectBtn")
        self.rdt_stop_btn.clicked.connect(self.stop_remote_desktop)
        self.rdt_stop_btn.setEnabled(False)

        self.rdt_save_btn = QPushButton("💾 Сохранить кадр")
        self.rdt_save_btn.clicked.connect(self.save_rdt_frame)
        self.rdt_save_btn.setEnabled(False)

        self.rdt_mouse_btn = QPushButton("🖱️ Управление мышью: ВЫКЛ")
        self.rdt_mouse_btn.setCheckable(True)
        self.rdt_mouse_btn.setObjectName("listenBtn")
        self.rdt_mouse_btn.clicked.connect(self.toggle_mouse_control)

        self.rdt_status_label = QLabel("⏸️ Трансляция остановлена")

        rdt_ctrl_layout.addWidget(self.rdt_quality_label)
        rdt_ctrl_layout.addWidget(self.rdt_quality)
        rdt_ctrl_layout.addWidget(self.rdt_fps_label)
        rdt_ctrl_layout.addWidget(self.rdt_fps)
        rdt_ctrl_layout.addWidget(self.rdt_scale_label)
        rdt_ctrl_layout.addWidget(self.rdt_scale)
        rdt_ctrl_layout.addWidget(self.rdt_start_btn)
        rdt_ctrl_layout.addWidget(self.rdt_stop_btn)
        rdt_ctrl_layout.addWidget(self.rdt_save_btn)
        rdt_ctrl_layout.addWidget(self.rdt_mouse_btn)
        rdt_ctrl_layout.addWidget(self.rdt_status_label)
        rdt_ctrl_layout.addStretch()

        # Область отображения экрана (с перехватом мыши)
        self.rdt_label = RdtLabel(self)
        self.rdt_label.setAlignment(Qt.AlignCenter)
        self.rdt_label.setMinimumHeight(400)
        self.rdt_label.setStyleSheet("""
            QLabel {
                border: 2px solid #007acc;
                border-radius: 5px;
                background-color: #111;
                color: #555;
                font-size: 13pt;
                font-style: italic;
            }
        """)
        self.rdt_label.setText("🖥️ Экран удалённого компьютера появится здесь")

        # Подключаем сигналы мыши
        self.rdt_label.mouse_moved.connect(self._on_rdt_mouse_move)
        self.rdt_label.mouse_clicked.connect(self._on_rdt_mouse_click)
        self.rdt_label.mouse_scrolled.connect(self._on_rdt_mouse_scroll)
        self.rdt_label.mouse_dragged.connect(self._on_rdt_mouse_drag)

        self.rdt_info_label = QLabel("")
        self.rdt_info_label.setAlignment(Qt.AlignCenter)

        rdt_layout.addWidget(rdt_ctrl)
        rdt_layout.addWidget(self.rdt_label, 1)
        rdt_layout.addWidget(self.rdt_info_label)

        # Состояние трансляции
        self.rdt_frame_count = 0
        self.rdt_last_frame = None
        self.rdt_thread = None

        # Вкладка 9: Планировщик задач
        sched_tab = QWidget()
        sched_layout = QVBoxLayout(sched_tab)

        # Форма создания задачи
        sched_form = QGroupBox("Новая задача")
        sched_form_layout = QGridLayout(sched_form)

        sched_form_layout.addWidget(QLabel("Команда:"), 0, 0)
        self.sched_cmd_input = QLineEdit()
        self.sched_cmd_input.setPlaceholderText("Например: shutdown /s /t 0")
        sched_form_layout.addWidget(self.sched_cmd_input, 0, 1, 1, 3)

        sched_form_layout.addWidget(QLabel("Название:"), 1, 0)
        self.sched_name_input = QLineEdit()
        self.sched_name_input.setPlaceholderText("Необязательно...")
        sched_form_layout.addWidget(self.sched_name_input, 1, 1)

        sched_form_layout.addWidget(QLabel("Задержка:"), 1, 2)
        self.sched_delay_spin = QSpinBox()
        self.sched_delay_spin.setRange(1, 86400)
        self.sched_delay_spin.setValue(60)

        self.sched_delay_unit = QComboBox()
        self.sched_delay_unit.addItems(["секунд", "минут", "часов"])

        sched_form_layout.addWidget(self.sched_delay_spin, 1, 3)
        sched_form_layout.addWidget(self.sched_delay_unit, 1, 4)

        sched_form_layout.addWidget(QLabel("Повтор:"), 2, 0)
        self.sched_repeat_chk = QCheckBox("Повторять каждые")
        self.sched_repeat_chk.stateChanged.connect(self.on_repeat_toggled)
        self.sched_repeat_spin = QSpinBox()
        self.sched_repeat_spin.setRange(1, 86400)
        self.sched_repeat_spin.setValue(60)
        self.sched_repeat_spin.setEnabled(False)

        self.sched_repeat_unit = QComboBox()
        self.sched_repeat_unit.addItems(["секунд", "минут", "часов"])
        self.sched_repeat_unit.setEnabled(False)

        sched_form_layout.addWidget(self.sched_repeat_chk, 2, 1)
        sched_form_layout.addWidget(self.sched_repeat_spin, 2, 2)
        sched_form_layout.addWidget(self.sched_repeat_unit, 2, 3)

        sched_form_layout.setColumnStretch(1, 1)

        sched_btns = QHBoxLayout()
        self.sched_add_btn = QPushButton("➕ Добавить задачу")
        self.sched_add_btn.setObjectName("connectBtn")
        self.sched_add_btn.clicked.connect(self.add_scheduled_task)
        self.sched_add_btn.setMinimumHeight(35)

        self.sched_refresh_btn = QPushButton("🔄 Обновить список")
        self.sched_refresh_btn.clicked.connect(self.refresh_tasks)
        self.sched_refresh_btn.setMinimumHeight(35)

        self.sched_cancel_btn = QPushButton("❌ Отменить выбранную")
        self.sched_cancel_btn.setObjectName("disconnectBtn")
        self.sched_cancel_btn.clicked.connect(self.cancel_selected_task)
        self.sched_cancel_btn.setMinimumHeight(35)

        sched_btns.addWidget(self.sched_add_btn)
        sched_btns.addWidget(self.sched_refresh_btn)
        sched_btns.addWidget(self.sched_cancel_btn)
        sched_btns.addStretch()

        # Таблица задач
        self.sched_table = QTableWidget(0, 6)
        self.sched_table.setHorizontalHeaderLabels(
            ["#", "📋 Название", "💻 Команда", "⏰ Выполнится в", "🔄 Повтор", "📊 Статус"])
        self.sched_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.sched_table.horizontalHeader().setStretchLastSection(True)

        sched_layout.addWidget(sched_form)
        sched_layout.addLayout(sched_btns)
        sched_layout.addWidget(self.sched_table)

        # Таймер автообновления задач
        self.sched_auto_timer = QTimer()
        self.sched_auto_timer.timeout.connect(self.refresh_tasks)

        # Добавляем вкладки
        self.tabs.addTab(files_tab, "📁 Файлы")
        self.tabs.addTab(sysinfo_tab, "🖥️ Система")
        self.tabs.addTab(screenshot_tab, "📸 Скриншот")
        self.tabs.addTab(processes_tab, "⚙️ Процессы")
        self.tabs.addTab(quick_tab, "🚀 Быстрые команды")
        self.tabs.addTab(search_tab, "🔍 Поиск")
        self.tabs.addTab(history_tab, "🕒 История")
        self.tabs.addTab(rdt, "🖥️ Рабочий стол")
        self.tabs.addTab(sched_tab, "📅 Планировщик")

        splitter.addWidget(top_widget)
        splitter.addWidget(self.tabs)
        splitter.setSizes([400, 500])
        main_layout.addWidget(splitter)

        # Статус бар
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("✅ Готов к работе")

        self.progress_bar = QProgressBar()
        self.progress_bar.setMaximumWidth(300)
        self.progress_bar.setMaximumHeight(20)
        self.progress_bar.hide()
        self.status_bar.addPermanentWidget(self.progress_bar)

        # Иконки
        self.setWindowIcon(QIcon())

        # Блокируем элементы до подключения
        self.set_connected_state(False)

        # Выводим инструкцию
        self.log_message("=" * 70, "info")
        self.log_message("🚀 УДАЛЕННЫЙ КЛИЕНТ УПРАВЛЕНИЯ С ОБРАТНЫМ ПОДКЛЮЧЕНИЕМ", "info")
        self.log_message("=" * 70, "info")
        self.log_message("📖 Инструкция:", "info")
        self.log_message("1. Для прямого подключения:", "info")
        self.log_message("   - Введите IP и порт сервера", "info")
        self.log_message("   - Нажмите 'Подключиться'", "info")
        self.log_message("2. Для обратного подключения:", "info")
        self.log_message("   - Выберите режим 'Обратное подключение'", "info")
        self.log_message("   - Нажмите 'Начать ожидание'", "info")
        self.log_message("   - Сервер подключится к вам", "info")
        self.log_message("=" * 70, "info")

        # Таймер для проверки обратного подключения
        self.reverse_check_timer = QTimer()
        self.reverse_check_timer.timeout.connect(self.check_reverse_connection)
        self.reverse_check_timer.setInterval(500)

    def eventFilter(self, source, event):
        """Фильтр событий для навигации по истории команд (↑↓)"""
        if source is self.command_input and event.type() == QEvent.KeyPress:
            if event.key() == Qt.Key_Up:
                if self.command_history_list and self.history_index < len(self.command_history_list) - 1:
                    self.history_index += 1
                    self.command_input.setText(self.command_history_list[-(self.history_index + 1)])
                return True
            elif event.key() == Qt.Key_Down:
                if self.history_index > 0:
                    self.history_index -= 1
                    self.command_input.setText(self.command_history_list[-(self.history_index + 1)])
                elif self.history_index == 0:
                    self.history_index = -1
                    self.command_input.clear()
                return True
        return super().eventFilter(source, event)

    def connect_to_server(self):
        """Прямое подключение к серверу"""
        host = self.host_input.text().strip()
        port = self.port_input.value()

        if not host:
            QMessageBox.warning(self, "Ошибка", "Введите IP адрес сервера")
            return

        self.connect_btn.setEnabled(False)
        self.progress_bar.show()
        self.progress_bar.setRange(0, 0)
        self.status_bar.showMessage(f"🔗 Подключение к {host}:{port}...")

        self.connect_thread = threading.Thread(
            target=self._connect_thread_func,
            args=(host, port),
            daemon=True
        )
        self.connect_thread.start()

        self.connect_timer = QTimer()
        self.connect_timer.timeout.connect(self._check_connect_status)
        self.connect_timer.start(100)

    def copy_file_on_server(self):
        """Копировать файл на сервере"""
        selected = self.files_table.selectedItems()
        if not selected:
            QMessageBox.warning(self, "Ошибка", "Выберите файл для копирования")
            return
        row = selected[0].row()
        name_item = self.files_table.item(row, 0)
        if not name_item:
            return
        filename = name_item.text().split(' ', 1)[-1]
        src_path = os.path.join(self.current_dir, filename)

        dst_name, ok = QInputDialog.getText(
            self, "Копирование файла",
            f"Укажите путь назначения для '{filename}':\n(Оставьте пустым для копии в текущую папку)",
            text=os.path.join(self.current_dir, f"копия_{filename}")
        )
        if ok and dst_name.strip():
            self.log_message(f"📋 Копирование: {filename}", "info")
            self.send_command('copy_file', src=src_path, dst=dst_name.strip())

    def zip_selected_file(self):
        """Архивировать выбранный файл"""
        selected = self.files_table.selectedItems()
        if not selected:
            QMessageBox.warning(self, "Ошибка", "Выберите файл/папку для архивирования")
            return
        row = selected[0].row()
        name_item = self.files_table.item(row, 0)
        if not name_item:
            return
        filename = name_item.text().split(' ', 1)[-1]
        filepath = os.path.join(self.current_dir, filename)
        archive_name = filename + '.zip'
        self.log_message(f"📦 Архивирование: {filename}", "info")
        self.send_command('zip_files', paths=[filepath], archive_name=archive_name)

    def unzip_selected_file(self):
        """Распаковать выбранный zip файл"""
        selected = self.files_table.selectedItems()
        if not selected:
            QMessageBox.warning(self, "Ошибка", "Выберите zip архив для распаковки")
            return
        row = selected[0].row()
        name_item = self.files_table.item(row, 0)
        if not name_item:
            return
        filename = name_item.text().split(' ', 1)[-1]
        if not filename.lower().endswith('.zip'):
            QMessageBox.warning(self, "Ошибка", "Выберите файл с расширением .zip")
            return
        filepath = os.path.join(self.current_dir, filename)
        self.log_message(f"📂 Распаковка: {filename}", "info")
        self.send_command('unzip_file', archive_path=filepath, extract_to=self.current_dir)

    def show_file_info_detailed(self):
        """Показать подробную информацию о файле с MD5"""
        selected = self.files_table.selectedItems()
        if not selected:
            QMessageBox.warning(self, "Ошибка", "Выберите файл")
            return
        row = selected[0].row()
        name_item = self.files_table.item(row, 0)
        if not name_item:
            return
        filename = name_item.text().split(' ', 1)[-1]
        filepath = os.path.join(self.current_dir, filename)
        self.log_message(f"ℹ️ Запрос информации о файле: {filename}", "info")
        self.send_command('file_info', path=filepath)

    def search_files_on_server(self):
        """Поиск файлов на сервере"""
        pattern = self.search_pattern_input.text().strip()
        if not pattern:
            pattern = '*'
        path = self.search_path_input.text().strip() or self.current_dir
        recursive = self.search_recursive_chk.isChecked()
        self.search_status_label.setText(f"🔍 Поиск '{pattern}' в '{path}'...")
        self.log_message(f"🔍 Поиск файлов: '{pattern}'", "info")
        self.send_command('search_files', path=path, pattern=pattern, recursive=recursive)

    def show_search_context_menu(self, position):
        """Контекстное меню для результатов поиска"""
        if not self.search_results_table.selectedItems():
            return
        menu = QMenu()
        nav_action = menu.addAction("📁 Перейти в папку")
        nav_action.triggered.connect(self.navigate_to_search_result)
        copy_action = menu.addAction("📋 Копировать путь")
        copy_action.triggered.connect(self.copy_search_result_path)
        menu.exec_(self.search_results_table.viewport().mapToGlobal(position))

    def navigate_to_search_result(self, index=None):
        """Перейти в папку найденного файла"""
        selected = self.search_results_table.selectedItems()
        if not selected:
            return
        row = selected[0].row()
        path_item = self.search_results_table.item(row, 1)
        if path_item:
            folder = os.path.dirname(path_item.text())
            self.send_command('cd', path=folder)
            self.tabs.setCurrentIndex(0)

    def copy_search_result_path(self):
        """Копировать путь из результатов поиска"""
        selected = self.search_results_table.selectedItems()
        if not selected:
            return
        row = selected[0].row()
        path_item = self.search_results_table.item(row, 1)
        if path_item:
            QApplication.clipboard().setText(path_item.text())
            self.log_message(f"📋 Путь скопирован: {path_item.text()}", "success")

    def load_server_history(self):
        """Загрузить историю команд с сервера"""
        self.send_command('command_history', limit=200)

    def clear_local_history(self):
        """Очистить локальную историю команд"""
        self.command_history_list.clear()
        self.history_index = -1
        self.history_table.setRowCount(0)
        self.log_message("🗑️ Локальная история команд очищена", "info")

    def run_from_history(self, index):
        """Выполнить команду из истории"""
        row = index.row()
        cmd_item = self.history_table.item(row, 1)
        if cmd_item:
            self.command_input.setText(cmd_item.text())
            self.execute_command()

    def open_file_remotely(self):
        """Запустить выбранный файл на удаленном компьютере"""
        selected = self.files_table.selectedItems()
        if not selected:
            QMessageBox.warning(self, "Ошибка", "Выберите файл для запуска на удаленном компьютере")
            return

        row = selected[0].row()
        name_item = self.files_table.item(row, 0)
        type_item = self.files_table.item(row, 1)

        if not name_item:
            return

        # Проверяем, что это не папка
        if "Папка" in type_item.text():
            QMessageBox.warning(self, "Ошибка", "Нельзя запустить папку")
            return

        filename = name_item.text().split(' ', 1)[-1]  # Убираем эмодзи
        filepath = os.path.join(self.current_dir, filename)

        # Запрашиваем подтверждение
        reply = QMessageBox.question(
            self, "Подтверждение",
            f"Вы уверены, что хотите запустить файл '{filename}' на удаленном компьютере?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )

        if reply == QMessageBox.Yes:
            self.log_message(f"▶️ Запуск файла на удаленном компьютере: {filename}", "info")

            # Для Windows
            if os.name == 'nt':
                command = f'start "" "{filepath}"'
            # Для Linux/Mac
            else:
                command = f'xdg-open "{filepath}"' if platform.system() != 'Darwin' else f'open "{filepath}"'

            self.send_command('command', command=command)

            self.log_message(f"✅ Команда запуска отправлена: {command}", "success")

    def cleanup_temp_files(self, temp_dir):
        """Очистка старых временных файлов"""
        try:
            current_time = time.time()
            for filename in os.listdir(temp_dir):
                filepath = os.path.join(temp_dir, filename)
                if os.path.isfile(filepath):
                    # Удаляем файлы старше 1 часа
                    if current_time - os.path.getmtime(filepath) > 3600:
                        try:
                            os.remove(filepath)
                        except:
                            pass
        except:
            pass

    def on_mode_changed(self, index):
        """Изменение режима подключения"""
        # Сначала останавливаем всё активное в ТЕКУЩЕМ режиме
        # до изменения current_mode
        if self.current_mode == "direct":
            self.connection_manager.disconnect()
        else:
            self.reverse_manager.stop_listening()
            self.reverse_manager.disconnect()

        # Останавливаем трансляцию и планировщик
        if hasattr(self, 'rdt_thread') and self.rdt_thread is not None:
            self.stop_remote_desktop()
        if hasattr(self, '_mouse_thread') and self._mouse_thread is not None:
            self._mouse_thread.stop()
            self._mouse_thread = None
        if hasattr(self, 'rdt_mouse_btn') and self.rdt_mouse_btn.isChecked():
            self.rdt_mouse_btn.setChecked(False)
            self.toggle_mouse_control(False)
        if hasattr(self, 'sched_auto_timer') and self.sched_auto_timer.isActive():
            self.sched_auto_timer.stop()

        # Теперь переключаем режим
        if index == 0:
            self.current_mode = "direct"
            self.direct_frame.setVisible(True)
            self.reverse_frame.setVisible(False)
        else:
            self.current_mode = "reverse"
            self.direct_frame.setVisible(False)
            self.reverse_frame.setVisible(True)
            self.reverse_info_label.setText("🚫 Не активно")
            self.reverse_info_label.setStyleSheet("color: #888888;")
            self.listen_btn.setEnabled(True)
            self.stop_listen_btn.setEnabled(False)

        # Сбрасываем UI
        self.status_label.setText("🚫 Не подключено")
        self.status_label.setStyleSheet("color: #888888; font-weight: bold;")
        self.connection_indicator.setStyleSheet("color: #ff4444; font-size: 16pt; font-weight: bold;")
        self.connect_btn.setEnabled(True)
        self.disconnect_btn.setEnabled(False)
        self.set_connected_state(False)

        # Очищаем данные
        self.drive_combo.blockSignals(True)
        self.drive_combo.clear()
        self.drive_combo.blockSignals(False)
        self.sysinfo_text.clear()
        self.files_table.setRowCount(0)
        self.processes_table.setRowCount(0)
        self.screenshot_label.setText("📸 Скриншот появится здесь")
        self.save_screenshot_btn.setEnabled(False)
        self.clipboard_screenshot_btn.setEnabled(False)

    def get_current_manager(self):
        """Получить текущий менеджер подключения"""
        if self.current_mode == "direct":
            return self.connection_manager
        else:
            return self.reverse_manager

    def start_listening(self):
        """Начать ожидание обратного подключения"""
        # Защита от повторного входа
        if getattr(self, '_listening_starting', False):
            return
        self._listening_starting = True

        try:
            port = self.listen_port_input.value()
            success, message = self.reverse_manager.start_listening(port)

            if success:
                self.listen_btn.setEnabled(False)
                self.stop_listen_btn.setEnabled(True)
                self.reverse_info_label.setText(f"✅ Ожидание подключения на порту {port}...")
                self.reverse_info_label.setStyleSheet("color: #44ff44;")
                self.log_message(f"👂 Начато ожидание подключения на порту {port}", "info")
                self.reverse_check_timer.start()
            else:
                self.log_message(f"❌ Ошибка: {message}", "error")
                QMessageBox.warning(self, "Ошибка", f"Не удалось начать прослушивание:\n\n{message}")
        finally:
            self._listening_starting = False

    def stop_listening(self):
        """Остановить ожидание обратного подключения"""
        self.reverse_manager.stop_listening()
        self.listen_btn.setEnabled(True)
        self.stop_listen_btn.setEnabled(False)
        self.reverse_info_label.setText("⏹️ Ожидание остановлено")
        self.reverse_info_label.setStyleSheet("color: #ffaa44;")
        self.reverse_check_timer.stop()
        self.log_message("⏹️ Ожидание подключения остановлено", "info")

    def check_reverse_connection(self):
        """Проверить установку обратного подключения"""
        if self.reverse_manager.connected:
            self.reverse_check_timer.stop()
            self.listen_btn.setEnabled(False)
            self.stop_listen_btn.setEnabled(False)
            self.reverse_info_label.setText(f"✅ Подключено от {self.reverse_manager.host}")
            self.reverse_info_label.setStyleSheet("color: #44ff44; font-weight: bold;")

            self.status_label.setText(f"🔗 Подключено (обратное от {self.reverse_manager.host})")
            self.status_label.setStyleSheet("color: #44ff44; font-weight: bold;")
            self.connection_indicator.setStyleSheet("color: #44ff44; font-size: 16pt; font-weight: bold;")
            self.disconnect_btn.setEnabled(True)
            self.set_connected_state(True)

            self.log_message(f"✅ Подключение установлено от {self.reverse_manager.host}", "success")
            self.status_bar.showMessage(f"✅ Обратное подключение установлено от {self.reverse_manager.host}")

            # Отправляем тестовую команду
            QTimer.singleShot(500, lambda: self.send_command('test'))
            QTimer.singleShot(1000, self.load_drives)

    def _connect_thread_func(self, host, port):
        """Функция для потока подключения"""
        self._connect_result = self.connection_manager.connect(host, port)

    def _check_connect_status(self):
        """Проверка статуса подключения"""
        if hasattr(self, '_connect_result'):
            self.connect_timer.stop()
            success, message = self._connect_result
            delattr(self, '_connect_result')

            QTimer.singleShot(0, lambda: self._on_connect_complete(success, message))

    def _on_connect_complete(self, success, message):
        """Завершение подключения"""
        self.progress_bar.hide()

        if success:
            self.status_label.setText(
                f"✅ Подключено (прямое к {self.connection_manager.host}:{self.connection_manager.port})")
            self.status_label.setStyleSheet("color: #44ff44; font-weight: bold;")
            self.connection_indicator.setStyleSheet("color: #44ff44; font-size: 16pt; font-weight: bold;")
            self.connect_btn.setEnabled(False)
            self.disconnect_btn.setEnabled(True)
            self.set_connected_state(True)

            self.log_message(f"✅ Подключено к {self.host_input.text()}:{self.port_input.value()}", "success")
            self.status_bar.showMessage(f"✅ Подключено к {self.host_input.text()}:{self.port_input.value()}")

            # Загружаем информацию о системе и файлы
            password = self.password_input.text()
            if password:
                import hashlib
                pwd_hash = hashlib.sha256(password.encode()).hexdigest()
                QTimer.singleShot(200, lambda: self.send_command('auth', password_hash=pwd_hash))
            QTimer.singleShot(500, lambda: self.send_command('test'))
            QTimer.singleShot(1000, self.get_system_info)
            QTimer.singleShot(1500, self.refresh_files)
            QTimer.singleShot(2000, self.load_drives)
        else:
            self.status_label.setText("❌ Не подключено")
            self.status_label.setStyleSheet("color: #ff4444; font-weight: bold;")
            self.connection_indicator.setStyleSheet("color: #ff4444; font-size: 16pt; font-weight: bold;")
            self.log_message(f"❌ Ошибка подключения: {message}", "error")
            self.connect_btn.setEnabled(True)

            QMessageBox.warning(self, "❌ Ошибка подключения",
                                f"Не удалось подключиться к серверу:\n\n{message}")

    def disconnect_from_server(self):
        """Отключение от сервера"""
        manager = self.get_current_manager()
        manager.disconnect()

        # Останавливаем ожидание если это обратное подключение
        if self.current_mode == "reverse":
            self.stop_listening()
            self.reverse_info_label.setText("🚫 Не активно")
            self.reverse_info_label.setStyleSheet("color: #888888;")

        # Останавливаем трансляцию и планировщик при отключении
        if hasattr(self, 'rdt_thread') and self.rdt_thread is not None:
            self.stop_remote_desktop()
        if hasattr(self, '_mouse_thread') and self._mouse_thread is not None:
            self._mouse_thread.stop()
            self._mouse_thread = None
        if hasattr(self, 'rdt_mouse_btn') and self.rdt_mouse_btn.isChecked():
            self.rdt_mouse_btn.setChecked(False)
            self.toggle_mouse_control(False)
        if hasattr(self, 'sched_auto_timer') and self.sched_auto_timer.isActive():
            self.sched_auto_timer.stop()

        self.status_label.setText("🚫 Не подключено")
        self.status_label.setStyleSheet("color: #888888; font-weight: bold;")
        self.connection_indicator.setStyleSheet("color: #ff4444; font-size: 16pt; font-weight: bold;")
        self.connect_btn.setEnabled(True)
        self.disconnect_btn.setEnabled(False)
        self.set_connected_state(False)

        self.log_message("🔌 Отключено от сервера", "info")
        self.status_bar.showMessage("🔌 Отключено")

        # Очищаем данные
        self.drive_combo.blockSignals(True)
        self.drive_combo.clear()
        self.drive_combo.blockSignals(False)
        self.sysinfo_text.clear()
        self.files_table.setRowCount(0)
        self.screenshot_label.setText("📸 Скриншот появится здесь")
        self.save_screenshot_btn.setEnabled(False)
        self.clipboard_screenshot_btn.setEnabled(False)
        self.processes_table.setRowCount(0)

    def set_connected_state(self, connected):
        """Установка состояния подключения"""
        enabled_color = "#d4d4d4" if connected else "#666666"

        # Основные элементы
        self.command_input.setEnabled(connected)
        self.execute_btn.setEnabled(connected)
        self.clear_output_btn.setEnabled(connected)
        self.tabs.setEnabled(connected)

        self.drive_combo.setEnabled(connected)
        # Элементы вкладки файлов
        self.open_remote_btn.setEnabled(connected)
        self.up_btn.setEnabled(connected)
        self.home_btn.setEnabled(connected)
        self.refresh_btn.setEnabled(connected)
        self.download_btn.setEnabled(connected)
        self.upload_btn.setEnabled(connected)
        self.rename_btn.setEnabled(connected)
        self.delete_btn.setEnabled(connected)
        self.create_folder_btn.setEnabled(connected)
        self.copy_file_btn.setEnabled(connected)
        self.zip_btn.setEnabled(connected)
        self.unzip_btn.setEnabled(connected)
        self.file_info_btn.setEnabled(connected)

        # Элементы вкладки системы
        self.sysinfo_btn.setEnabled(connected)
        self.copy_sysinfo_btn.setEnabled(connected)

        # Элементы вкладки скриншота
        self.screenshot_btn.setEnabled(connected)

        # Элементы вкладки процессов
        self.process_list_btn.setEnabled(connected)
        self.process_kill_btn.setEnabled(connected)

        # Элементы поиска и истории
        self.search_btn.setEnabled(connected)
        self.history_refresh_btn.setEnabled(connected)

        # Удалённый рабочий стол
        self.rdt_start_btn.setEnabled(connected)
        self.rdt_save_btn.setEnabled(connected and self.rdt_last_frame is not None)

        # Планировщик
        self.sched_add_btn.setEnabled(connected)
        self.sched_refresh_btn.setEnabled(connected)
        self.sched_cancel_btn.setEnabled(connected)

    def send_command(self, command_type, silent=False, **kwargs):
        """Отправка команды на сервер"""
        manager = self.get_current_manager()

        if not manager.is_connected():
            if not silent:
                self.log_message("❌ Нет подключения к серверу", "error")
            return

        # Каждая команда — свой поток, не перезаписываем self.worker_thread
        worker = CommandWorker(manager)
        worker.set_command(command_type, **kwargs)
        worker.result_ready.connect(self.on_command_result)
        worker.progress.connect(self.on_progress)
        worker.start()

        # Держим ссылку чтобы GC не уничтожил поток
        if not hasattr(self, '_workers'):
            self._workers = []
        self._workers.append(worker)
        # Чистим завершённые потоки
        self._workers = [w for w in self._workers if not w.isFinished()]

        self.progress_bar.show()
        self.progress_bar.setRange(0, 0)

    def get_process_list(self):
        """Получить список процессов"""
        if os.name == 'nt':  # Windows
            self.send_command('command', command='tasklist /FO CSV /NH')
        else:  # Linux/Mac
            self.send_command('command', command='ps aux')

    def kill_process_dialog(self):
        """Диалог завершения процесса"""
        selected = self.processes_table.selectedItems()
        if not selected:
            QMessageBox.warning(self, "Ошибка", "Выберите процесс для завершения")
            return

        row = selected[0].row()
        pid_item = self.processes_table.item(row, 0)
        name_item = self.processes_table.item(row, 1)

        if pid_item and name_item:
            pid = pid_item.text()
            name = name_item.text()

            reply = QMessageBox.question(
                self, "Подтверждение",
                f"Вы уверены, что хотите завершить процесс?\n\nPID: {pid}\nИмя: {name}",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )

            if reply == QMessageBox.Yes:
                self.kill_process(pid)

    def kill_process(self, pid):
        """Завершить процесс"""
        if os.name == 'nt':  # Windows
            command = f'taskkill /PID {pid} /F'
        else:  # Linux/Mac
            command = f'kill -9 {pid}'

        self.send_command('command', command=command)
        QTimer.singleShot(1000, self.get_process_list)  # Обновляем список через секунду

    def show_process_context_menu(self, position):
        """Контекстное меню для процессов"""
        if not self.processes_table.selectedItems():
            return

        menu = QMenu()

        kill_action = menu.addAction("🔪 Завершить процесс")
        kill_action.triggered.connect(self.kill_process_dialog)

        menu.addSeparator()

        copy_pid_action = menu.addAction("📋 Копировать PID")
        copy_pid_action.triggered.connect(self.copy_process_pid)

        copy_name_action = menu.addAction("📋 Копировать имя")
        copy_name_action.triggered.connect(self.copy_process_name)

        menu.exec_(self.processes_table.viewport().mapToGlobal(position))

    def copy_process_pid(self):
        """Копировать PID процесса"""
        selected = self.processes_table.selectedItems()
        if selected:
            pid_item = self.processes_table.item(selected[0].row(), 0)
            if pid_item:
                clipboard = QApplication.clipboard()
                clipboard.setText(pid_item.text())
                self.log_message(f"📋 PID скопирован: {pid_item.text()}", "success")

    def copy_process_name(self):
        """Копировать имя процесса"""
        selected = self.processes_table.selectedItems()
        if selected:
            name_item = self.processes_table.item(selected[0].row(), 1)
            if name_item:
                clipboard = QApplication.clipboard()
                clipboard.setText(name_item.text())
                self.log_message(f"📋 Имя процесса скопировано: {name_item.text()}", "success")

    def create_folder(self):
        """Создать папку"""
        name, ok = QInputDialog.getText(self, "Создание папки", "Введите имя папки:")
        if ok and name:
            path = os.path.join(self.current_dir, name)
            self.send_command('command', command=f'mkdir "{path}"')

    def copy_system_info(self):
        """Копировать информацию о системе в буфер обмена"""
        clipboard = QApplication.clipboard()
        clipboard.setText(self.sysinfo_text.toPlainText())
        self.log_message("📋 Информация о системе скопирована в буфер обмена", "success")

    def copy_screenshot_to_clipboard(self):
        """Копировать скриншот в буфер обмена"""
        if self.current_screenshot:
            clipboard = QApplication.clipboard()
            pixmap = QPixmap()
            pixmap.loadFromData(self.current_screenshot)
            clipboard.setPixmap(pixmap)
            self.log_message("📋 Скриншот скопирован в буфер обмена", "success")

    def on_drive_selected(self, index):
        """Переход на выбранный диск"""
        if index < 0 or not self.get_current_manager().is_connected():
            return
        mountpoint = self.drive_combo.itemData(index)
        if mountpoint:
            self.send_command('cd', path=mountpoint)

    def load_drives(self):
        """Запросить список дисков с сервера"""
        self.send_command('get_drives', silent=True)

    def update_drive_combo(self, drives):
        """Обновить выпадающий список дисков"""
        self.drive_combo.blockSignals(True)
        self.drive_combo.clear()
        for d in drives:
            mp = d['mountpoint']
            device = d['device']
            total = d['total_gb']
            free = d['free_gb']
            if total > 0:
                label = f"💾 {device}  ({free}/{total} GB)"
            else:
                label = f"💾 {device}"
            self.drive_combo.addItem(label, mp)
        self.drive_combo.blockSignals(False)

        # Выделяем диск текущей директории
        for i in range(self.drive_combo.count()):
            mp = self.drive_combo.itemData(i)
            if mp and self.current_dir.startswith(mp):
                self.drive_combo.setCurrentIndex(i)
                break

    def navigate_home(self):
        """Перейти в домашнюю директорию"""
        self.send_command('cd', path='~')

    def show_file_context_menu(self, position):
        """Показать контекстное меню для файлов"""
        if not self.files_table.selectedItems():
            return

        menu = QMenu()

        # Новые действия
        open_remote_action = menu.addAction("▶️ Открыть удаленно")
        open_remote_action.triggered.connect(self.open_file_remotely)

        menu.addSeparator()

        # Существующие действия
        download_action = menu.addAction("📥 Скачать")
        download_action.triggered.connect(self.download_file)

        upload_action = menu.addAction("📤 Загрузить сюда")
        upload_action.triggered.connect(self.upload_file_to_selected)

        rename_action = menu.addAction("✏️ Переименовать")
        rename_action.triggered.connect(self.rename_file)

        delete_action = menu.addAction("🗑️ Удалить")
        delete_action.triggered.connect(self.delete_file)

        menu.addSeparator()

        copy_path_action = menu.addAction("📋 Копировать путь")
        copy_path_action.triggered.connect(self.copy_file_path)

        properties_action = menu.addAction("📊 Свойства")
        properties_action.triggered.connect(self.show_file_properties)

        menu.exec_(self.files_table.viewport().mapToGlobal(position))

    def upload_file_to_selected(self):
        """Загрузить файл в выбранную директорию"""
        self.upload_file()

    def copy_file_path(self):
        """Копировать путь к файлу"""
        selected = self.files_table.selectedItems()
        if selected:
            row = selected[0].row()
            name_item = self.files_table.item(row, 0)
            if name_item:
                filepath = os.path.join(self.current_dir, name_item.text()[3:])
                clipboard = QApplication.clipboard()
                clipboard.setText(filepath)
                self.log_message(f"📋 Путь скопирован: {filepath}", "success")

    def show_file_properties(self):
        """Показать свойства файла"""
        selected = self.files_table.selectedItems()
        if not selected:
            return

        row = selected[0].row()
        name_item = self.files_table.item(row, 0)
        type_item = self.files_table.item(row, 1)
        size_item = self.files_table.item(row, 2)

        if name_item and type_item and size_item:
            info = f"Имя: {name_item.text()}\n"
            info += f"Тип: {type_item.text()}\n"
            info += f"Размер: {size_item.text()}\n"
            info += f"Путь: {os.path.join(self.current_dir, name_item.text())}"

            QMessageBox.information(self, "Свойства файла", info)

    def clear_output(self):
        """Очистить поле вывода"""
        self.output_text.clear()
        self.log_message("🗑️ Очистка вывода", "info")

    def execute_command_manual(self, command):
        """Выполнить команду вручную"""
        self.command_input.setText(command)
        self.execute_command()

    def log_message(self, message, message_type="info"):
        """Логирование сообщений"""
        timestamp = datetime.now().strftime("%H:%M:%S")

        if message_type == "error":
            color = "#ff4444"
            prefix = "❌"
        elif message_type == "success":
            color = "#44ff44"
            prefix = "✅"
        elif message_type == "warning":
            color = "#ffaa44"
            prefix = "⚠️"
        elif message_type == "command":
            color = "#4488ff"
            prefix = "💻"
        else:
            color = "#aaaaaa"
            prefix = "ℹ️"

        html = f'<span style="color: {color}"><b>{prefix}</b> [{timestamp}] {message}</span>'
        self.output_text.append(html)

        cursor = self.output_text.textCursor()
        cursor.movePosition(cursor.End)
        self.output_text.setTextCursor(cursor)

    def execute_command(self):
        """Выполнение команды"""
        command = self.command_input.text().strip()
        if not command:
            return

        self.command_input.clear()
        self.history_index = -1

        # Добавляем в локальную историю
        if not self.command_history_list or self.command_history_list[-1] != command:
            self.command_history_list.append(command)
            if len(self.command_history_list) > 500:
                self.command_history_list = self.command_history_list[-500:]

        self.log_message(f"▶️ > {command}", "command")

        if command.lower() == 'help':
            self.show_help()
            return
        elif command.lower() == 'clear':
            self.clear_output()
            return
        elif command.lower() == 'test':
            self.send_command('test')
            return
        elif command.lower() == 'sysinfo':
            self.get_system_info()
            return
        elif command.lower() == 'screenshot':
            self.take_screenshot()
            return

        self.send_command('command', command=command)

    def execute_quick_command(self, command):
        """Выполнение быстрой команды"""
        self.command_input.setText(command)
        self.execute_command()

    def show_help(self):
        """Показать справку"""
        help_text = """
==================== СПРАВКА ПО КОМАНДАМ ====================

📋 ОСНОВНЫЕ КОМАНДЫ:
  help          - показать эту справку
  clear         - очистить окно вывода
  test          - тест подключения
  sysinfo       - информацию о системе
  screenshot    - сделать скриншот

📁 ФАЙЛОВАЯ СИСТЕМА:
  dir / ls      - список файлов
  cd [путь]     - сменить директорию
  mkdir [имя]   - создать папку
  rm [имя]      - удалить файл/папку

🖥️ СИСТЕМА:
  tasklist / ps - список процессов
  ipconfig      - сетевые настройки
  systeminfo    - подробная информация
  whoami        - текущий пользователь
  netstat       - сетевые соединения

🆕 НОВЫЕ ФУНКЦИИ:
  ↑/↓           - навигация по истории команд
  📦 В архив    - архивировать выбранный файл в zip
  📂 Распаковать - распаковать выбранный zip
  📋 Копировать - копировать файл на сервере
  ℹ️ Инфо      - MD5, дата создания и подробная инфо
  🔍 Поиск      - поиск файлов по маске (вкладка)
  🕒 История    - история выполненных команд (вкладка)
  🔐 Пароль     - аутентификация при подключении

🔄 ОБРАТНОЕ ПОДКЛЮЧЕНИЕ:
  Сервер может подключиться к вам!
  Выберите режим "Обратное подключение"
  и нажмите "Начать ожидание"

============================================================
"""
        self.output_text.append(help_text)

    def on_progress(self, message):
        """Обновление прогресса"""
        self.status_bar.showMessage(message)

    def on_command_result(self, result, error):
        """Обработка результата команды"""
        self.progress_bar.hide()

        if error:
            self.log_message(f"❌ {error}", "error")
            self.status_bar.showMessage(f"❌ Ошибка: {error}", 3000)

            if "подключения" in error.lower() or "соединение" in error.lower():
                self.disconnect_from_server()
            return

        if not result:
            self.log_message("❌ Пустой ответ от сервера", "error")
            return

        if result.get("type") == "process_list":
            self.update_process_table(result.get("processes", []))
            return

        command_type = result.get('_command_type', 'unknown')

        if command_type == 'command':
            if 'output' in result:
                output = result['output']
                if output.strip():
                    self.output_text.append(output)

            elif 'error' in result:
                self.log_message(f"❌ {result['error']}", "error")

            self.status_bar.showMessage("✅ Команда выполнена", 3000)

        elif command_type == 'test':
            if result.get('success'):
                msg = result.get('message', 'Тест пройден')
                self.log_message(f"✅ {msg}", "success")
            else:
                self.log_message(f"❌ {result.get('error', 'Ошибка теста')}", "error")

        elif command_type == 'sysinfo':
            self.display_system_info(result)

        elif command_type == 'screenshot':
            self.display_screenshot(result)

        elif command_type == 'list_files':
            self.display_files(result)

        elif command_type == 'cd':
            if result.get('success'):
                self.current_dir = result.get('cwd', '.')
                self.refresh_files()
            else:
                self.log_message(f"❌ {result.get('error', 'Ошибка')}", "error")

        elif command_type == 'download':
            # Проверяем, есть ли параметр temp_path для локального просмотра
            temp_path = result.get('temp_path')
            if temp_path:
                self.handle_view_locally_result(result, temp_path)
            else:
                self.handle_download_result(result)

        elif command_type == 'upload':
            self.handle_upload_result(result)

        elif command_type == 'rename':
            self.handle_rename_result(result)

        elif command_type == 'delete':
            self.handle_delete_result(result)

        elif command_type == 'reverse_connect':
            self.handle_reverse_connect_result(result)

        elif command_type == 'list_clients':
            self.handle_list_clients_result(result)

        elif command_type == 'send_to_client':
            self.handle_send_to_client_result(result)

        elif command_type == 'disconnect_client':
            self.handle_disconnect_client_result(result)

        elif command_type == 'get_drives':
            if result.get('success'):
                self.update_drive_combo(result.get('drives', []))

        elif command_type == 'search_files':
            self.display_search_results(result)

        elif command_type == 'zip_files':
            self.handle_zip_result(result)

        elif command_type == 'unzip_file':
            self.handle_unzip_result(result)

        elif command_type == 'copy_file':
            self.handle_copy_file_result(result)

        elif command_type == 'file_info':
            self.handle_file_info_result(result)

        elif command_type == 'command_history':
            self.display_server_history(result)

        elif command_type == 'auth':
            if result.get('success'):
                self.log_message(f"🔐 {result.get('message', 'Аутентификация успешна')}", "success")
            else:
                self.log_message(f"❌ {result.get('error', 'Ошибка аутентификации')}", "error")
                QMessageBox.warning(self, "Ошибка аутентификации", result.get('error', 'Неверный пароль'))

        elif command_type == 'schedule_task':
            if result.get('success'):
                self.log_message(
                    f"📅 Задача добавлена: '{result.get('task_name')}' → {result.get('run_at_str')}", "success")
                QTimer.singleShot(500, self.refresh_tasks)
            else:
                self.log_message(f"❌ Планировщик: {result.get('error')}", "error")

        elif command_type == 'cancel_task':
            if result.get('success'):
                self.log_message(f"❌ Задача #{result.get('task_id')} отменена", "info")
            else:
                self.log_message(f"⚠️ {result.get('error')}", "warning")

        elif command_type == 'get_tasks':
            self.display_tasks(result)

        elif command_type == 'screen_frame':
            # Обрабатывается напрямую в _on_screen_frame
            pass

        elif command_type == 'screen_size':
            if result.get('success'):
                self.rdt_server_screen_w = result.get('width', 1920)
                self.rdt_server_screen_h = result.get('height', 1080)
                self.log_message(
                    f"🖥️ Размер экрана сервера: {self.rdt_server_screen_w}×{self.rdt_server_screen_h}", "info")

        elif command_type in ('mouse_move', 'mouse_click', 'mouse_scroll', 'mouse_drag'):
            if not result.get('success'):
                err = result.get('error', '')
                # Показываем ошибку только если pyautogui не установлен
                if 'pyautogui' in err:
                    self.log_message(f"⚠️ {err}", "warning")
                    self.rdt_mouse_btn.setChecked(False)
                    self.toggle_mouse_control(False)

    def handle_view_locally_result(self, result, temp_path):
        """Обработка результата для локального просмотра файла"""
        self.progress_bar.hide()

        if not result.get('success'):
            error_msg = result.get('error', 'Неизвестная ошибка')
            self.log_message(f"❌ Ошибка скачивания: {error_msg}", "error")
            QMessageBox.warning(self, "Ошибка", f"Не удалось скачать файл:\n\n{error_msg}")
            return

        file_data = result.get('data')
        if not file_data:
            self.log_message("❌ Файл не содержит данных", "error")
            return

        filename = result.get('filename', 'downloaded_file')

        try:
            decoded_data = base64.b64decode(file_data)

            with open(temp_path, 'wb') as f:
                f.write(decoded_data)

            file_size = len(decoded_data)
            size_text = self.format_file_size(file_size)

            self.log_message(f"✅ Файл сохранен во временную папку: {temp_path} ({size_text})", "success")

            # Пытаемся открыть файл
            try:
                if os.name == 'nt':  # Windows
                    os.startfile(temp_path)
                elif platform.system() == 'Darwin':  # macOS
                    subprocess.Popen(['open', temp_path])
                else:  # Linux
                    subprocess.Popen(['xdg-open', temp_path])

                self.log_message(f"✅ Файл открыт: {filename}", "success")
                self.temp_files.append(temp_path)  # Запоминаем для очистки

            except Exception as e:
                self.log_message(f"⚠️ Не удалось открыть файл автоматически: {str(e)}", "warning")

                # Предлагаем открыть вручную
                reply = QMessageBox.question(
                    self, "Файл сохранен",
                    f"Файл успешно сохранен:\n{temp_path}\nРазмер: {size_text}\n\nХотите открыть файл вручную?",
                    QMessageBox.Yes | QMessageBox.No
                )

                if reply == QMessageBox.Yes:
                    if os.name == 'nt':  # Windows
                        os.startfile(os.path.dirname(temp_path))
                    elif platform.system() == 'Darwin':  # macOS
                        subprocess.Popen(['open', os.path.dirname(temp_path)])
                    else:  # Linux
                        subprocess.Popen(['xdg-open', os.path.dirname(temp_path)])

        except Exception as e:
            self.log_message(f"❌ Ошибка сохранения файла: {str(e)}", "error")
            QMessageBox.critical(self, "Ошибка", f"Не удалось сохранить файл:\n\n{str(e)}")

    def handle_reverse_connect_result(self, result):
        """Обработка результата подключения к клиенту"""
        if result.get('success'):
            client_id = result.get('client_id')
            host = result.get('host')
            port = result.get('port')
            self.log_message(f"✅ Подключено к клиенту {host}:{port} (ID: {client_id})", "success")
        else:
            self.log_message(f"❌ {result.get('error', 'Ошибка подключения')}", "error")

    def handle_list_clients_result(self, result):
        """Обработка результата списка клиентов"""
        if result.get('success'):
            clients = result.get('clients', [])
            self.log_message(f"✅ Получено {len(clients)} клиентов", "success")
        else:
            self.log_message(f"❌ {result.get('error', 'Ошибка')}", "error")

    def handle_send_to_client_result(self, result):
        """Обработка результата отправки команды клиенту"""
        if result.get('success'):
            client_id = result.get('client_id')
            response = result.get('response', {})
            if 'output' in response:
                output = response['output']
                self.output_text.append(f"\n[Клиент {client_id}]:\n{output}")
            self.log_message(f"✅ Команда отправлена клиенту {client_id}", "success")
        else:
            self.log_message(f"❌ {result.get('error', 'Ошибка')}", "error")

    def handle_disconnect_client_result(self, result):
        """Обработка результата отключения клиента"""
        if result.get('success'):
            client_id = result.get('client_id')
            self.log_message(f"✅ Клиент {client_id} отключен", "success")
        else:
            self.log_message(f"❌ {result.get('error', 'Ошибка')}", "error")

    # ──────────── УДАЛЁННЫЙ РАБОЧИЙ СТОЛ ────────────

    def start_remote_desktop(self):
        """Начать трансляцию экрана"""
        manager = self.get_current_manager()
        if not manager.is_connected():
            self.log_message("❌ Нет подключения", "error")
            return

        fps = self.rdt_fps.value()
        quality = self.rdt_quality.value()
        scale = self.rdt_scale.value() / 100.0

        # Сохраняем поток как атрибут — иначе GC уничтожит его сразу
        self.rdt_thread = ScreenStreamThread(manager, fps, quality, scale)
        self.rdt_thread.frame_ready.connect(self._on_screen_frame)
        self.rdt_thread.error_occurred.connect(self._on_rdt_error)
        self.rdt_thread.start()

        self.rdt_frame_count = 0
        self.rdt_start_btn.setEnabled(False)
        self.rdt_stop_btn.setEnabled(True)
        self.rdt_save_btn.setEnabled(False)
        self.rdt_status_label.setText(f"▶️ Трансляция: {fps} FPS")
        self.log_message(f"🖥️ Трансляция экрана запущена ({fps} FPS)", "success")

    def stop_remote_desktop(self):
        """Остановить трансляцию"""
        if hasattr(self, 'rdt_thread') and self.rdt_thread is not None:
            self.rdt_thread.stop()
            self.rdt_thread = None
        self.rdt_start_btn.setEnabled(True)
        self.rdt_stop_btn.setEnabled(False)
        self.rdt_status_label.setText("⏸️ Трансляция остановлена")
        self.log_message("🖥️ Трансляция экрана остановлена", "info")

    def _on_rdt_error(self, msg):
        """Ошибка трансляции — останавливаем"""
        self.stop_remote_desktop()
        self.log_message(f"❌ Трансляция: {msg}", "error")

    def _on_screen_frame(self, img_data):
        """Отобразить пришедший кадр (вызывается из главного потока через сигнал)"""
        try:
            self.rdt_last_frame = img_data
            pixmap = QPixmap()
            pixmap.loadFromData(img_data)
            if pixmap.isNull():
                return

            # Реальный размер кадра = размер экрана * scale
            # Пересчитываем в исходные координаты сервера
            scale = self.rdt_scale.value() / 100.0
            if scale > 0:
                self.rdt_server_screen_w = int(pixmap.width() / scale)
                self.rdt_server_screen_h = int(pixmap.height() / scale)

            scaled = pixmap.scaled(
                self.rdt_label.size(),
                Qt.KeepAspectRatio,
                Qt.SmoothTransformation
            )
            self.rdt_label.setPixmap(scaled)
            self.rdt_label.setText("")
            self.rdt_frame_count += 1
            size_kb = len(img_data) // 1024
            self.rdt_info_label.setText(
                f"Кадр #{self.rdt_frame_count} | {size_kb} KB | "
                f"экран сервера: {self.rdt_server_screen_w}×{self.rdt_server_screen_h}"
            )
            if not self.rdt_save_btn.isEnabled():
                self.rdt_save_btn.setEnabled(True)
        except Exception as e:
            self.log_message(f"❌ Ошибка отображения кадра: {e}", "error")

    def save_rdt_frame(self):
        """Сохранить текущий кадр как изображение"""
        if not self.rdt_last_frame:
            return
        filename, _ = QFileDialog.getSaveFileName(
            self, "Сохранить кадр",
            f"frame_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg",
            "JPEG Files (*.jpg);;PNG Files (*.png)"
        )
        if filename:
            try:
                with open(filename, 'wb') as f:
                    f.write(self.rdt_last_frame)
                self.log_message(f"✅ Кадр сохранён: {filename}", "success")
            except Exception as e:
                self.log_message(f"❌ Ошибка сохранения: {e}", "error")

    def toggle_mouse_control(self, checked):
        """Включить/выключить управление мышью"""
        self.rdt_mouse_enabled = checked
        self.rdt_label.set_mouse_control(checked)
        if checked:
            self.rdt_mouse_btn.setText("🖱️ Управление мышью: ВКЛ")
            # Запускаем поток синхронизации мыши
            manager = self.get_current_manager()
            self._mouse_thread = MouseSyncThread(manager)
            self._mouse_thread.error_occurred.connect(
                lambda msg: self.log_message(f"⚠️ Mouse sync: {msg}", "warning"))
            self._mouse_thread.start()
            self.log_message("🖱️ Управление мышью включено", "success")
        else:
            self.rdt_mouse_btn.setText("🖱️ Управление мышью: ВЫКЛ")
            if self._mouse_thread:
                self._mouse_thread.stop()
                self._mouse_thread = None
            self.log_message("🖱️ Управление мышью выключено", "info")

    def _norm_to_server(self, rx, ry):
        """Перевести нормализованные координаты в пиксели экрана сервера."""
        return int(rx * self.rdt_server_screen_w), int(ry * self.rdt_server_screen_h)

    def _send_mouse(self, command_type, **kwargs):
        """Отправить команду мыши через выделенный поток."""
        if self._mouse_thread and self._mouse_thread.isRunning():
            self._mouse_thread.send(command_type, **kwargs)

    def _on_rdt_mouse_move(self, rx, ry):
        if not self.rdt_mouse_enabled:
            return
        x, y = self._norm_to_server(rx, ry)
        self._send_mouse('mouse_move', x=x, y=y)

    def _on_rdt_mouse_click(self, rx, ry, btn, double):
        if not self.rdt_mouse_enabled:
            return
        x, y = self._norm_to_server(rx, ry)
        button = {1: 'left', 2: 'middle', 3: 'right'}.get(btn, 'left')
        self._send_mouse('mouse_click', x=x, y=y, button=button, double=double)
        action = "двойной клик" if double else "клик"
        self.log_message(f"🖱️ {action} {button} ({x}, {y})", "info")

    def _on_rdt_mouse_scroll(self, rx, ry, delta):
        if not self.rdt_mouse_enabled:
            return
        x, y = self._norm_to_server(rx, ry)
        self._send_mouse('mouse_scroll', x=x, y=y, delta=delta)

    def _on_rdt_mouse_drag(self, rx1, ry1, rx2, ry2, btn):
        if not self.rdt_mouse_enabled:
            return
        x1, y1 = self._norm_to_server(rx1, ry1)
        x2, y2 = self._norm_to_server(rx2, ry2)
        button = {1: 'left', 2: 'middle', 3: 'right'}.get(btn, 'left')
        self._send_mouse('mouse_drag', x1=x1, y1=y1, x2=x2, y2=y2, button=button)
        self.log_message(f"🖱️ перетаскивание ({x1},{y1}) → ({x2},{y2})", "info")

    # ──────────── ПЛАНИРОВЩИК ЗАДАЧ ────────────

    def on_repeat_toggled(self, state):
        """Включить/выключить настройки повтора"""
        enabled = bool(state)
        self.sched_repeat_spin.setEnabled(enabled)
        self.sched_repeat_unit.setEnabled(enabled)

    def _unit_to_seconds(self, value, unit_index):
        """Перевести значение в секунды"""
        multipliers = [1, 60, 3600]
        return value * multipliers[unit_index]

    def add_scheduled_task(self):
        """Добавить задачу в планировщик"""
        command = self.sched_cmd_input.text().strip()
        if not command:
            QMessageBox.warning(self, "Ошибка", "Введите команду для выполнения")
            return

        delay = self._unit_to_seconds(
            self.sched_delay_spin.value(),
            self.sched_delay_unit.currentIndex()
        )
        repeat = 0
        if self.sched_repeat_chk.isChecked():
            repeat = self._unit_to_seconds(
                self.sched_repeat_spin.value(),
                self.sched_repeat_unit.currentIndex()
            )

        name = self.sched_name_input.text().strip()
        self.send_command('schedule_task',
                          command=command,
                          delay_seconds=delay,
                          repeat_interval=repeat,
                          task_name=name)
        self.sched_cmd_input.clear()
        self.sched_name_input.clear()

        # Запускаем автообновление
        if not self.sched_auto_timer.isActive():
            self.sched_auto_timer.start(2000)

    def refresh_tasks(self):
        """Обновить список задач"""
        self.send_command('get_tasks', silent=True)

    def cancel_selected_task(self):
        """Отменить выбранную задачу"""
        selected = self.sched_table.selectedItems()
        if not selected:
            QMessageBox.warning(self, "Ошибка", "Выберите задачу для отмены")
            return
        row = selected[0].row()
        id_item = self.sched_table.item(row, 0)
        if id_item:
            task_id = int(id_item.text())
            self.send_command('cancel_task', task_id=task_id)
            QTimer.singleShot(300, self.refresh_tasks)

    def display_tasks(self, result):
        """Отобразить список задач"""
        if not result.get('success'):
            return
        tasks = result.get('tasks', [])
        self.sched_table.setRowCount(len(tasks))

        status_colors = {
            'pending':   '#ffaa44',
            'running':   '#44aaff',
            'done':      '#44ff44',
            'cancelled': '#888888',
        }
        status_labels = {
            'pending':   '⏳ Ожидает',
            'running':   '▶️ Выполняется',
            'done':      '✅ Выполнено',
            'cancelled': '❌ Отменено',
        }

        all_done = all(t.get('status') in ('done', 'cancelled') for t in tasks)
        if all_done and self.sched_auto_timer.isActive():
            self.sched_auto_timer.stop()

        for row, task in enumerate(tasks):
            status = task.get('status', '')
            repeat = task.get('repeat_interval', 0)
            repeat_str = f"каждые {repeat}с" if repeat else "однократно"

            items = [
                str(task.get('id', '')),
                task.get('name', ''),
                task.get('command', ''),
                task.get('run_at_str', ''),
                repeat_str,
                status_labels.get(status, status),
            ]
            for col, text in enumerate(items):
                item = QTableWidgetItem(text)
                color = status_colors.get(status, '#d4d4d4')
                item.setForeground(QColor(color))
                self.sched_table.setItem(row, col, item)

        self.sched_table.resizeColumnsToContents()

    def display_search_results(self, result):
        """Отобразить результаты поиска"""
        if not result.get('success'):
            self.search_status_label.setText(f"❌ {result.get('error', 'Ошибка поиска')}")
            self.log_message(f"❌ Поиск: {result.get('error')}", "error")
            return
        results = result.get('results', [])
        count = result.get('count', 0)
        pattern = result.get('pattern', '')
        self.search_status_label.setText(f"✅ Найдено {count} файлов по маске '{pattern}'")
        self.search_results_table.setRowCount(len(results))
        for row, item in enumerate(results):
            self.search_results_table.setItem(row, 0, QTableWidgetItem(
                ("📁 " if item['is_dir'] else "📄 ") + item['name']))
            self.search_results_table.setItem(row, 1, QTableWidgetItem(item['path']))
            size_text = "<DIR>" if item['is_dir'] else self.format_file_size(item['size'])
            self.search_results_table.setItem(row, 2, QTableWidgetItem(size_text))
        self.search_results_table.resizeColumnsToContents()
        self.log_message(f"🔍 Найдено {count} файлов", "success")

    def handle_zip_result(self, result):
        """Обработка результата архивирования"""
        if not result.get('success'):
            self.log_message(f"❌ Архивирование: {result.get('error')}", "error")
            QMessageBox.warning(self, "Ошибка", f"Не удалось создать архив:\n{result.get('error')}")
            return
        archive = result.get('archive_name', '')
        size = self.format_file_size(result.get('size', 0))
        self.log_message(f"✅ Архив создан: {archive} ({size})", "success")
        self.refresh_files()

    def handle_unzip_result(self, result):
        """Обработка результата распаковки"""
        if not result.get('success'):
            self.log_message(f"❌ Распаковка: {result.get('error')}", "error")
            QMessageBox.warning(self, "Ошибка", f"Не удалось распаковать:\n{result.get('error')}")
            return
        count = result.get('files_count', 0)
        self.log_message(f"✅ Распаковано {count} файлов в {result.get('extract_to', '')}", "success")
        self.refresh_files()

    def handle_copy_file_result(self, result):
        """Обработка результата копирования"""
        if not result.get('success'):
            self.log_message(f"❌ Копирование: {result.get('error')}", "error")
            QMessageBox.warning(self, "Ошибка", f"Не удалось скопировать:\n{result.get('error')}")
            return
        self.log_message(f"✅ Скопировано: {result.get('dst', '')}", "success")
        self.refresh_files()

    def handle_file_info_result(self, result):
        """Отобразить подробную информацию о файле"""
        if not result.get('success'):
            self.log_message(f"❌ Инфо: {result.get('error')}", "error")
            return
        from datetime import datetime as dt
        def fmt_time(ts):
            try:
                return dt.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            except:
                return 'Н/Д'

        info = f"📄 Файл: {result.get('name', '')}\n"
        info += f"📁 Путь: {result.get('path', '')}\n"
        info += f"📊 Тип: {'Папка' if result.get('is_dir') else 'Файл'}\n"
        if not result.get('is_dir'):
            info += f"📏 Размер: {self.format_file_size(result.get('size', 0))}\n"
            info += f"🔑 MD5: {result.get('md5', 'Н/Д')}\n"
        else:
            info += f"📋 Элементов: {result.get('items_count', 0)}\n"
        info += f"📅 Создан: {fmt_time(result.get('created', 0))}\n"
        info += f"📅 Изменён: {fmt_time(result.get('modified', 0))}\n"
        info += f"📅 Открыт: {fmt_time(result.get('accessed', 0))}"
        QMessageBox.information(self, "ℹ️ Информация о файле", info)

    def display_server_history(self, result):
        """Отобразить историю команд с сервера"""
        if not result.get('success'):
            self.log_message(f"❌ История: {result.get('error')}", "error")
            return
        history = result.get('history', [])
        self.history_table.setRowCount(len(history))
        for row, item in enumerate(history):
            self.history_table.setItem(row, 0, QTableWidgetItem(item.get('timestamp', '')))
            self.history_table.setItem(row, 1, QTableWidgetItem(item.get('command', '')))
            self.history_table.setItem(row, 2, QTableWidgetItem(item.get('cwd', '')))
        self.history_table.resizeColumnsToContents()
        total = result.get('total', len(history))
        self.log_message(f"🕒 История загружена: {len(history)} из {total} команд", "success")

    def get_system_info(self):
        """Получить информацию о системе"""
        self.send_command('sysinfo', silent=True)

    def display_system_info(self, result):
        """Отобразить информацию о системе"""
        if not result.get('success'):
            self.log_message(f"❌ {result.get('error', 'Ошибка')}", "error")
            return

        info_text = "==================== ИНФОРМАЦИЯ О СИСТЕМЕ ====================\n\n"

        for key, value in result.items():
            if key not in ['type', 'success', '_command_type']:
                if key == 'disks' and isinstance(value, list):
                    info_text += f"\n💾 ДИСКИ:\n"
                    for disk in value:
                        info_text += f"  📀 {disk.get('device', '')} ({disk.get('mountpoint', '')}):\n"
                        info_text += f"     📊 {disk.get('used', '')} / {disk.get('total', '')} ({disk.get('percent', '')})\n"
                elif key == 'ip_addresses' and isinstance(value, list):
                    info_text += f"\n🌐 IP АДРЕСА:\n"
                    for ip in value:
                        info_text += f"  📡 {ip}\n"
                else:
                    display_key = {
                        'platform': '🏗️ Платформа',
                        'system': '💻 Система',
                        'system_version': '📈 Версия системы',
                        'hostname': '🏠 Имя хоста',
                        'username': '👤 Пользователь',
                        'python_version': '🐍 Версия Python',
                        'current_directory': '📁 Текущая директория',
                        'cpu_count': '⚙️ Количество ядер CPU',
                        'memory_total': '🧠 Общая память',
                        'memory_available': '🆓 Доступная память',
                        'memory_used_percent': '📊 Использовано памяти'
                    }.get(key, key)

                    info_text += f"🔸 {display_key}: {value}\n"

        info_text += "\n=============================================================="
        self.sysinfo_text.setText(info_text)
        self.log_message("✅ Информация о системе получена", "success")

    def take_screenshot(self):
        """Сделать скриншот"""
        self.send_command('screenshot')

    def display_screenshot(self, result):
        """Отобразить скриншот"""
        if not result.get('success'):
            self.log_message(f"❌ {result.get('error', 'Ошибка получения скриншота')}", "error")
            return

        screenshot_data = result.get('screenshot')
        if not screenshot_data:
            self.log_message("❌ Скриншот не получен", "error")
            return

        try:
            img_data = base64.b64decode(screenshot_data)
            self.current_screenshot = img_data

            pixmap = QPixmap()
            pixmap.loadFromData(img_data)

            if pixmap.isNull():
                raise Exception("Неверные данные изображения")

            scaled_pixmap = pixmap.scaled(
                self.screenshot_label.size(),
                Qt.KeepAspectRatio,
                Qt.SmoothTransformation
            )

            self.screenshot_label.setPixmap(scaled_pixmap)
            self.screenshot_label.setText("")
            self.save_screenshot_btn.setEnabled(True)
            self.clipboard_screenshot_btn.setEnabled(True)

            size_bytes = len(img_data)
            size_text = self.format_file_size(size_bytes)
            self.log_message(f"✅ Скриншот получен ({size_text})", "success")
        except Exception as e:
            self.log_message(f"❌ Ошибка обработки скриншота: {str(e)}", "error")
            self.screenshot_label.setText("❌ Ошибка загрузки скриншота")

    def save_screenshot(self):
        """Сохранить скриншот"""
        if not self.current_screenshot:
            return

        filename, _ = QFileDialog.getSaveFileName(
            self, "Сохранить скриншот",
            f"screenshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png",
            "PNG Files (*.png);;JPEG Files (*.jpg *.jpeg);;All Files (*)"
        )

        if filename:
            try:
                with open(filename, 'wb') as f:
                    f.write(self.current_screenshot)
                self.log_message(f"✅ Скриншот сохранен: {filename}", "success")
            except Exception as e:
                self.log_message(f"❌ Ошибка сохранения: {str(e)}", "error")

    def refresh_files(self):
        """Обновить список файлов"""
        self.send_command('list_files', path=self.current_dir, silent=True)

    def display_files(self, result):
        """Отобразить список файлов"""
        if not result.get('success'):
            self.log_message(f"❌ {result.get('error', 'Ошибка')}", "error")
            return

        self.current_dir = result.get('path', '.')
        self.path_label.setText(f"📁 Путь: {self.current_dir}")

        files = result.get('files', [])
        self.files_table.setRowCount(len(files))

        for row, file_info in enumerate(files):
            name = file_info['name']
            is_dir = file_info['is_dir']
            size = file_info['size']
            modified = file_info.get('modified', 0)

            # Имя с иконкой
            if is_dir:
                icon_text = "📁"
            elif name.lower().endswith(('.exe', '.bat', '.cmd')):
                icon_text = "⚙️"
            elif name.lower().endswith(('.txt', '.log', '.ini', '.cfg')):
                icon_text = "📄"
            elif name.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp')):
                icon_text = "🖼️"
            elif name.lower().endswith(('.zip', '.rar', '.7z', '.tar', '.gz')):
                icon_text = "📦"
            elif name.lower().endswith(('.py', '.js', '.java', '.cpp', '.c', '.h')):
                icon_text = "📝"
            else:
                icon_text = "📄"

            name_item = QTableWidgetItem(f"{icon_text} {name}")

            # Тип
            type_item = QTableWidgetItem("📁 Папка" if is_dir else "📄 Файл")

            # Размер
            if is_dir:
                size_item = QTableWidgetItem("<DIR>")
            else:
                size_item = QTableWidgetItem(self.format_file_size(size))

            # Дата изменения
            if modified > 0:
                date_str = datetime.fromtimestamp(modified).strftime('%Y-%m-%d %H:%M:%S')
                date_item = QTableWidgetItem(date_str)
            else:
                date_item = QTableWidgetItem("Неизвестно")

            self.files_table.setItem(row, 0, name_item)
            self.files_table.setItem(row, 1, type_item)
            self.files_table.setItem(row, 2, size_item)
            self.files_table.setItem(row, 3, date_item)

        self.files_table.resizeColumnsToContents()
        self.log_message(f"✅ Получено {len(files)} файлов/папок", "success")

    def navigate_up(self):
        """Перейти на уровень вверх"""
        if self.current_dir and self.current_dir != "." and self.current_dir != "/" and self.current_dir != "\\":
            try:
                new_path = os.path.dirname(self.current_dir)
                if new_path:
                    self.current_dir = new_path
                    self.send_command('cd', path=new_path)
            except:
                pass

    def on_file_double_click(self, index):
        """Двойной клик по файлу"""
        row = index.row()
        if row >= 0:
            type_item = self.files_table.item(row, 1)
            name_item = self.files_table.item(row, 0)

            if type_item and name_item and "Папка" in type_item.text():
                folder_name = name_item.text().split(' ', 1)[-1]  # Убираем эмодзи
                new_path = os.path.join(self.current_dir, folder_name)
                self.send_command('cd', path=new_path)

    def download_file(self):
        """Скачать файл"""
        selected = self.files_table.selectedItems()
        if not selected:
            QMessageBox.warning(self, "Ошибка", "Выберите файл для скачивания")
            return

        row = selected[0].row()
        name_item = self.files_table.item(row, 0)
        type_item = self.files_table.item(row, 1)

        if not name_item or "Папка" in type_item.text():
            QMessageBox.warning(self, "Ошибка", "Нельзя скачать папку")
            return

        filename = name_item.text().split(' ', 1)[-1]  # Убираем эмодзи
        filepath = os.path.join(self.current_dir, filename)

        self.log_message(f"📥 Запрос скачивания файла: {filename}", "info")

        self.send_command('download', filepath=filepath)

    def update_process_table(self, processes):
        self.processes_table.setRowCount(0)

        for proc in processes:
            row = self.processes_table.rowCount()
            self.processes_table.insertRow(row)

            self.processes_table.setItem(row, 0, QTableWidgetItem(proc.get("pid", "")))
            self.processes_table.setItem(row, 1, QTableWidgetItem(proc.get("name", "")))
            self.processes_table.setItem(row, 2, QTableWidgetItem(proc.get("memory", "")))

        self.processes_table.resizeColumnsToContents()

    def handle_download_result(self, result):
        """Обработка результата скачивания файла"""
        self.progress_bar.hide()

        if not result.get('success'):
            error_msg = result.get('error', 'Неизвестная ошибка')
            self.log_message(f"❌ Ошибка скачивания: {error_msg}", "error")
            QMessageBox.warning(self, "Ошибка", f"Не удалось скачать файл:\n\n{error_msg}")
            return

        file_data = result.get('data')
        if not file_data:
            self.log_message("❌ Файл не содержит данных", "error")
            return

        filename = result.get('filename', 'downloaded_file')

        save_path, _ = QFileDialog.getSaveFileName(
            self, "Сохранить файл",
            os.path.join(os.path.expanduser("~"), "Downloads", filename),
            "All Files (*)"
        )

        if not save_path:
            return

        try:
            decoded_data = base64.b64decode(file_data)

            with open(save_path, 'wb') as f:
                f.write(decoded_data)

            file_size = len(decoded_data)
            size_text = self.format_file_size(file_size)

            self.log_message(f"✅ Файл сохранен: {save_path} ({size_text})", "success")

            reply = QMessageBox.question(
                self, "Успех",
                f"Файл успешно сохранен:\n{save_path}\nРазмер: {size_text}\n\nОткрыть файл?",
                QMessageBox.Yes | QMessageBox.No
            )

            if reply == QMessageBox.Yes:
                os.startfile(save_path)

        except Exception as e:
            self.log_message(f"❌ Ошибка сохранения файла: {str(e)}", "error")
            QMessageBox.critical(self, "Ошибка", f"Не удалось сохранить файл:\n\n{str(e)}")

    def upload_file(self):
        """Загрузить файл на сервер"""
        filename, _ = QFileDialog.getOpenFileName(
            self, "Выберите файл для загрузки",
            os.path.expanduser("~"),
            "All Files (*)"
        )

        if not filename:
            return

        try:
            file_size = os.path.getsize(filename)
            if file_size > 100 * 1024 * 1024:
                QMessageBox.warning(self, "Ошибка",
                                    f"Файл слишком большой: {self.format_file_size(file_size)}\nЛимит: 100MB")
                return
        except:
            pass

        try:
            with open(filename, 'rb') as f:
                file_data = f.read()

            encoded_data = base64.b64encode(file_data).decode('utf-8')

            self.log_message(f"📤 Загрузка файла на сервер: {os.path.basename(filename)}", "info")

            self.send_command('upload',
                              filename=os.path.basename(filename),
                              data=encoded_data,
                              path=self.current_dir)

        except Exception as e:
            self.log_message(f"❌ Ошибка чтения файла: {str(e)}", "error")
            QMessageBox.critical(self, "Ошибка", f"Не удалось прочитать файл:\n\n{str(e)}")

    def handle_upload_result(self, result):
        """Обработка результата загрузки файла на сервер"""
        if not result.get('success'):
            error_msg = result.get('error', 'Неизвестная ошибка')
            self.log_message(f"❌ Ошибка загрузки: {error_msg}", "error")
            QMessageBox.warning(self, "Ошибка", f"Не удалось загрузить файл:\n\n{error_msg}")
            return

        self.refresh_files()

        filename = result.get('filename', 'файл')
        size = result.get('size', 0)

        self.log_message(f"✅ Файл успешно загружен на сервер: {filename} ({self.format_file_size(size)})", "success")

    def rename_file(self):
        """Переименовать файл"""
        selected = self.files_table.selectedItems()
        if not selected:
            QMessageBox.warning(self, "Ошибка", "Выберите файл или папку для переименования")
            return

        row = selected[0].row()
        name_item = self.files_table.item(row, 0)

        if not name_item:
            return

        old_name = name_item.text().split(' ', 1)[-1]  # Убираем эмодзи

        new_name, ok = QInputDialog.getText(
            self, "Переименовать",
            f"Введите новое имя для '{old_name}':",
            text=old_name
        )

        if not ok or not new_name.strip() or new_name == old_name:
            return

        old_path = os.path.join(self.current_dir, old_name)
        new_path = os.path.join(self.current_dir, new_name)

        self.log_message(f"✏️ Переименование: {old_name} -> {new_name}", "info")
        self.send_command('rename', old_path=old_path, new_path=new_path)

    def handle_rename_result(self, result):
        """Обработка результата переименования"""
        if not result.get('success'):
            error_msg = result.get('error', 'Неизвестная ошибка')
            self.log_message(f"❌ Ошибка переименования: {error_msg}", "error")
            QMessageBox.warning(self, "Ошибка", f"Не удалось переименовать:\n\n{error_msg}")
            return

        self.refresh_files()

        old_name = result.get('old_name', '')
        new_name = result.get('new_name', '')

        self.log_message(f"✅ Успешно переименовано: {old_name} -> {new_name}", "success")

    def delete_file(self):
        """Удалить файл"""
        selected = self.files_table.selectedItems()
        if not selected:
            QMessageBox.warning(self, "Ошибка", "Выберите файл или папку для удаления")
            return

        row = selected[0].row()
        name_item = self.files_table.item(row, 0)
        type_item = self.files_table.item(row, 1)

        if not name_item:
            return

        filename = name_item.text().split(' ', 1)[-1]  # Убираем эмодзи
        file_type = "папку" if "Папка" in type_item.text() else "файл"

        reply = QMessageBox.question(
            self, "Подтверждение удаления",
            f"Вы уверены, что хотите удалить {file_type} '{filename}'?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )

        if reply == QMessageBox.Yes:
            filepath = os.path.join(self.current_dir, filename)

            self.log_message(f"🗑️ Удаление: {filepath}", "info")
            self.send_command('delete', path=filepath)

    def handle_delete_result(self, result):
        """Обработка результата удаления"""
        if not result.get('success'):
            error_msg = result.get('error', 'Неизвестная ошибка')
            self.log_message(f"❌ Ошибка удаления: {error_msg}", "error")
            QMessageBox.warning(self, "Ошибка", f"Не удалось удалить:\n\n{error_msg}")
            return

        self.refresh_files()

        filename = result.get('filename', '')
        self.log_message(f"✅ Успешно удалено: {filename}", "success")

    def format_file_size(self, size_bytes):
        """Форматирование размера файла"""
        if size_bytes >= 1024 ** 3:
            return f"{size_bytes / (1024 ** 3):.2f} GB"
        elif size_bytes >= 1024 ** 2:
            return f"{size_bytes / (1024 ** 2):.2f} MB"
        elif size_bytes >= 1024:
            return f"{size_bytes / 1024:.2f} KB"
        else:
            return f"{size_bytes} B"

    def closeEvent(self, event):
        """Закрытие окна"""
        # Очищаем временные файлы
        for temp_file in self.temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except:
                pass

        if self.get_current_manager().is_connected():
            reply = QMessageBox.question(
                self, "Подтверждение",
                "Вы подключены к серверу. Вы уверены, что хотите выйти?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )

            if reply == QMessageBox.Yes:
                self.disconnect_from_server()
                event.accept()
            else:
                event.ignore()
        else:
            event.accept()


def main():
    """Главная функция"""
    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    # Устанавливаем иконку приложения
    app.setWindowIcon(QIcon())

    # Устанавливаем стиль темной темы
    app.setStyleSheet("""
        QToolTip {
            background-color: #2d2d30;
            color: #d4d4d4;
            border: 1px solid #3e3e42;
            padding: 5px;
        }
    """)

    window = RemoteClientGUI()
    window.show()

    sys.exit(app.exec_())


if __name__ == "__main__":
    print("=" * 70)
    print("🚀 УДАЛЕННЫЙ КЛИЕНТ УПРАВЛЕНИЯ")
    print("📡 Режимы: Прямое и обратное подключение")
    print("🎨 Интерфейс: Темная тема")
    print("📂 НОВОЕ: Кнопка 'Открыть удаленно'")
    print("⚙️  Вкладка 'Процессы' исправлена и работает")
    print("=" * 70)

    main()