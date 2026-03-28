#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
СЕРВЕР С ВЫБОРОМ РЕЖИМА РАБОТЫ И ОБРАТНЫМ ПОДКЛЮЧЕНИЕМ
"""

import socket
import subprocess
import json
import platform
import os
import threading
import time
import sys
import traceback
import psutil
import getpass
import ctypes
import shutil
import struct
import queue
import select
import zipfile
import fnmatch
import hashlib

try:
    import pyautogui
    pyautogui.FAILSAFE = False
    HAS_PYAUTOGUI = True
except ImportError:
    HAS_PYAUTOGUI = False

try:
    from PIL import ImageGrab
    import io
    import base64

    HAS_PILLOW = True
except ImportError:
    HAS_PILLOW = False


class ServerMode(Enum):
    """Режимы работы сервера"""
    STANDALONE = "standalone"  # Автономный сервер (принимает подключения)
    REVERSE_CLIENT = "reverse"  # Подключается к клиентам (обратный режим)
    HYBRID = "hybrid"  # Гибридный режим (оба варианта)


class ReverseClientManager:
    """Менеджер для подключения к клиентам (обратное подключение)"""

    def __init__(self, server_instance):
        self.server = server_instance
        self.clients = {}  # client_id -> socket
        self.client_info = {}  # client_id -> info
        self.lock = threading.Lock()
        self.next_client_id = 1
        self.running = True

    def connect_to_client(self, host, port):
        """Подключиться к клиенту (обратное подключение)"""
        try:
            self.server.log(f"🔄 Подключение к клиенту {host}:{port}...")

            # Создаем сокет для подключения
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.settimeout(10)

            # Пытаемся подключиться
            client_socket.connect((host, port))

            # Устанавливаем таймауты
            client_socket.settimeout(120)

            # Получаем ID клиента
            with self.lock:
                client_id = self.next_client_id
                self.next_client_id += 1
                self.clients[client_id] = client_socket
                self.client_info[client_id] = {
                    'host': host,
                    'port': port,
                    'connected_at': time.time(),
                    'status': 'connected',
                    'socket': client_socket
                }

            # Запускаем обработчик клиента в отдельном потоке
            threading.Thread(
                target=self._handle_reverse_client,
                args=(client_id,),
                daemon=True
            ).start()

            self.server.log(f"✅ Успешно подключено к клиенту {host}:{port} (ID: {client_id})")

            return client_id, f"Подключено к клиенту {host}:{port}"

        except socket.timeout:
            error_msg = f"Таймаут подключения к клиенту {host}:{port}"
            self.server.log(f"❌ {error_msg}")
            return None, error_msg
        except ConnectionRefusedError:
            error_msg = f"Клиент {host}:{port} отказал в подключении"
            self.server.log(f"❌ {error_msg}")
            return None, error_msg
        except Exception as e:
            error_msg = f"Ошибка подключения к клиенту {host}:{port}: {str(e)}"
            self.server.log(f"❌ {error_msg}")
            traceback.print_exc()
            return None, error_msg

    def _handle_reverse_client(self, client_id):
        """Обработка обратного клиента"""
        with self.lock:
            if client_id not in self.clients:
                return

            client_socket = self.clients[client_id]
            client_info = self.client_info[client_id]

        host = client_info['host']
        port = client_info['port']

        self.server.log(f"🔗 Обработка обратного клиента {host}:{port} (ID: {client_id})")

        try:
            while self.running:
                # Получаем запрос от клиента
                request_data = self._receive_all(client_socket)
                if request_data is None:
                    break

                # Парсим JSON запрос
                try:
                    request = json.loads(request_data.decode('utf-8', errors='ignore'))
                except:
                    self.server.log(f"❌ Неверный JSON от клиента {client_id}")
                    break

                # Обрабатываем запрос
                response = self.server.process_request(request)

                # Отправляем ответ
                if not self._send_response(client_socket, response):
                    break

                self.server.log(f"✅ Запрос от клиента {client_id} обработан: {request.get('type', 'unknown')}")

        except Exception as e:
            self.server.log(f"❌ Ошибка обработки обратного клиента {client_id}: {e}")
            traceback.print_exc()
        finally:
            self._disconnect_client(client_id)

    def _send_and_receive(self, sock, request, timeout=30):
        """Отправить запрос и получить ответ"""
        try:
            # Отправляем запрос
            if not self._send_response(sock, request):
                return None, "Ошибка отправки"

            # Получаем ответ
            response_data = self._receive_all(sock, timeout)
            if response_data is None:
                return None, "Таймаут получения ответа"

            # Парсим ответ
            response = json.loads(response_data.decode('utf-8', errors='ignore'))
            return response, None

        except Exception as e:
            return None, str(e)

    def _receive_all(self, sock, timeout=120):
        """Получение данных от клиента"""
        sock.settimeout(timeout)

        try:
            # Получаем размер данных
            size_data = b""
            while len(size_data) < 4:
                chunk = sock.recv(4 - len(size_data))
                if not chunk:
                    return None
                size_data += chunk

            # Распаковываем размер
            total_size = struct.unpack('!I', size_data)[0]

            # Получаем данные
            data = b""
            while len(data) < total_size:
                chunk = sock.recv(min(4096, total_size - len(data)))
                if not chunk:
                    return None
                data += chunk

            return data
        except socket.timeout:
            return None
        except:
            return None

    def _send_response(self, sock, response):
        """Отправка данных клиенту"""
        try:
            response_json = json.dumps(response, ensure_ascii=False)
            response_data = response_json.encode('utf-8')

            # Отправляем размер данных
            total_size = len(response_data)
            sock.sendall(struct.pack('!I', total_size))

            # Отправляем данные
            sock.sendall(response_data)
            return True
        except:
            return False

    def send_command_to_client(self, client_id, command_type, **kwargs):
        """Отправить команду конкретному клиенту"""
        with self.lock:
            if client_id not in self.clients:
                return None, f"Клиент {client_id} не найден"

            client_socket = self.clients[client_id]

        try:
            # Создаем запрос
            request = {'type': command_type, **kwargs}

            # Отправляем и получаем ответ
            response, error = self._send_and_receive(client_socket, request)

            if error:
                self._disconnect_client(client_id)
                return None, f"Ошибка связи с клиентом: {error}"

            return response, None

        except Exception as e:
            self._disconnect_client(client_id)
            return None, f"Ошибка: {str(e)}"

    def _disconnect_client(self, client_id):
        """Отключить клиента"""
        with self.lock:
            if client_id in self.clients:
                try:
                    self.clients[client_id].close()
                except:
                    pass
                del self.clients[client_id]

            if client_id in self.client_info:
                info = self.client_info[client_id]
                self.server.log(f"🚪 Отключен обратный клиент {info['host']}:{info['port']} (ID: {client_id})")
                self.client_info[client_id]['status'] = 'disconnected'
                self.client_info[client_id]['disconnected_at'] = time.time()

    def get_connected_clients(self):
        """Получить список подключенных клиентов"""
        with self.lock:
            clients = []
            for client_id, info in self.client_info.items():
                if info['status'] == 'connected' and client_id in self.clients:
                    clients.append({
                        'id': client_id,
                        'host': info['host'],
                        'port': info['port'],
                        'connected_at': info['connected_at']
                    })
            return clients

    def disconnect_all(self):
        """Отключить всех клиентов"""
        with self.lock:
            self.running = False
            for client_id in list(self.clients.keys()):
                self._disconnect_client(client_id)

    def start_auto_connect(self, target_list):
        """Автоматическое подключение к списку целей"""
        self.running = True

        def auto_connect_worker():
            while self.running:
                for target in target_list:
                    host, port = target
                    if not self.running:
                        break

                    # Проверяем, не подключены ли уже к этому хосту
                    already_connected = False
                    with self.lock:
                        for info in self.client_info.values():
                            if info['host'] == host and info['port'] == port and info['status'] == 'connected':
                                already_connected = True
                                break

                    if not already_connected:
                        self.server.log(f"🔄 Попытка авто-подключения к {host}:{port}")
                        self.connect_to_client(host, port)

                time.sleep(10)  # Ждем 10 секунд перед следующей попыткой

        threading.Thread(target=auto_connect_worker, daemon=True).start()


class CompleteServer:
    def __init__(self, host='127.0.0.1', port=5000, mode=ServerMode.STANDALONE, password=None):
        self.host = host
        self.port = port
        self.mode = mode
        self.running = False
        self.server_socket = None
        self.clients = []
        self.lock = threading.Lock()
        self.password = hashlib.sha256(password.encode()).hexdigest() if password else None
        self.command_history = []  # История выполненных команд

        # Планировщик задач
        self.scheduled_tasks = {}   # task_id -> info
        self.next_task_id = 1
        self.scheduler_lock = threading.Lock()

        # Трансляция экрана
        self.screen_stream_clients = {}  # client_addr -> socket
        self.screen_stream_running = False

        # Менеджер обратного подключения
        self.reverse_manager = ReverseClientManager(self)

        print("=" * 70)
        print(f"🚀 СЕРВЕР УПРАВЛЕНИЯ - РЕЖИМ: {mode.value.upper()}")
        print("=" * 70)
        print(f"📍 Хост: {host}")
        print(f"🔌 Порт: {port}")
        print(f"🎛️  Режим: {self.get_mode_description()}")
        print("=" * 70)

    def get_mode_description(self):
        """Получить описание режима"""
        descriptions = {
            ServerMode.STANDALONE: "Автономный сервер (принимает подключения)",
            ServerMode.REVERSE_CLIENT: "Активный клиент (подключается к другим)",
            ServerMode.HYBRID: "Гибридный (принимает и подключается)"
        }
        return descriptions.get(self.mode, "Неизвестный режим")

    def log(self, message):
        """Логирование"""
        timestamp = time.strftime("%H:%M:%S")
        print(f"[{timestamp}] {message}")

    # Оригинальные методы остаются без изменений
    def execute_command(self, command):
        """Выполнение команды"""
        try:
            self.log(f"Выполняю команду: {command[:100]}...")
            encoding = 'cp866' if platform.system() == 'Windows' else 'utf-8'

            process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding=encoding
            )

            stdout, stderr = process.communicate(timeout=30)
            returncode = process.returncode
            output = stdout + stderr

            response = {
                'type': 'command',
                'command': command,
                'output': output,
                'returncode': returncode,
                'cwd': os.getcwd(),
                'success': True
            }

            self.log(f"Команда выполнена успешно, вывод: {len(output)} символов")
            return response

        except Exception as e:
            error_msg = f"Ошибка выполнения: {str(e)}"
            self.log(f"Ошибка: {error_msg}")
            return {
                'type': 'command',
                'command': command,
                'error': error_msg,
                'success': False
            }

    def get_system_info(self):
        """Полная информация о системе"""
        try:
            self.log("Получение информации о системе...")

            info = {
                'type': 'sysinfo',
                'platform': platform.platform(),
                'system': platform.system(),
                'system_version': platform.version(),
                'hostname': socket.gethostname(),
                'username': getpass.getuser(),
                'python_version': platform.python_version(),
                'current_directory': os.getcwd(),
                'cpu_count': os.cpu_count(),
                'success': True
            }

            # IP адреса
            try:
                ip_addresses = []
                for interface, addrs in psutil.net_if_addrs().items():
                    for addr in addrs:
                        if addr.family == socket.AF_INET and not addr.address.startswith('127.'):
                            ip_addresses.append(f"{interface}: {addr.address}")
                info['ip_addresses'] = ip_addresses
            except:
                info['ip_addresses'] = ["Не удалось получить IP адреса"]

            # Память
            try:
                memory = psutil.virtual_memory()
                info['memory_total'] = f"{memory.total // (1024 ** 3)} GB"
                info['memory_available'] = f"{memory.available // (1024 ** 3)} GB"
                info['memory_used_percent'] = f"{memory.percent}%"
            except:
                info['memory_info'] = "Недоступно"

            # Диски
            try:
                disks = []
                for partition in psutil.disk_partitions():
                    try:
                        usage = psutil.disk_usage(partition.mountpoint)
                        disks.append({
                            'device': partition.device,
                            'mountpoint': partition.mountpoint,
                            'total': f"{usage.total // (1024 ** 3)} GB",
                            'used': f"{usage.used // (1024 ** 3)} GB",
                            'free': f"{usage.free // (1024 ** 3)} GB",
                            'percent': f"{usage.percent}%"
                        })
                    except:
                        continue
                info['disks'] = disks
            except:
                info['disks'] = []

            self.log(f"Информация о системе получена: {len(info)} параметров")
            return info

        except Exception as e:
            error_msg = f"Ошибка получения информации: {str(e)}"
            self.log(f"Ошибка: {error_msg}")
            return {
                'type': 'sysinfo',
                'error': error_msg,
                'success': False
            }

    def take_screenshot(self):
        """Создание скриншота"""
        if not HAS_PILLOW:
            error_msg = "Pillow не установлен. Установите: pip install pillow"
            return {
                'type': 'screenshot',
                'error': error_msg,
                'success': False
            }

        try:
            self.log("Создание скриншота...")
            screenshot = ImageGrab.grab()

            img_byte_arr = io.BytesIO()
            screenshot.save(img_byte_arr, format='PNG')
            img_byte_arr.seek(0)
            img_data = img_byte_arr.read()
            screenshot_b64 = base64.b64encode(img_data).decode('utf-8')

            self.log(f"Скриншот создан успешно, размер: {len(screenshot_b64)} символов")

            return {
                'type': 'screenshot',
                'screenshot': screenshot_b64,
                'format': 'PNG',
                'size_bytes': len(img_data),
                'size_base64': len(screenshot_b64),
                'success': True
            }

        except Exception as e:
            error_msg = f"Ошибка создания скриншота: {str(e)}"
            self.log(f"Ошибка: {error_msg}")
            return {
                'type': 'screenshot',
                'error': error_msg,
                'success': False
            }

    def list_files(self, path='.'):
        """Список файлов"""
        try:
            self.log(f"Получение файлов для пути: {path}")

            if not path or path == '.':
                path = os.getcwd()

            if path.startswith('~'):
                path = os.path.expanduser(path)

            abs_path = os.path.abspath(path)

            if not os.path.exists(abs_path):
                return {
                    'type': 'list_files',
                    'error': f'Путь не существует: {abs_path}',
                    'success': False
                }

            if not os.path.isdir(abs_path):
                return {
                    'type': 'list_files',
                    'error': f'Не является директорией: {abs_path}',
                    'success': False
                }

            files = []
            try:
                items = os.listdir(abs_path)

                for item in items:
                    try:
                        item_path = os.path.join(abs_path, item)
                        is_dir = os.path.isdir(item_path)
                        size = 0

                        if os.path.exists(item_path) and not is_dir:
                            try:
                                size = os.path.getsize(item_path)
                            except:
                                size = 0

                        files.append({
                            'name': item,
                            'is_dir': is_dir,
                            'size': size,
                            'path': item_path
                        })
                    except:
                        continue

                files.sort(key=lambda x: (not x['is_dir'], x['name'].lower()))

            except PermissionError as e:
                return {
                    'type': 'list_files',
                    'error': f'Нет доступа к директории: {abs_path}',
                    'success': False
                }

            self.log(f"Получено {len(files)} файлов/папок из {abs_path}")

            return {
                'type': 'list_files',
                'files': files,
                'path': abs_path,
                'count': len(files),
                'success': True
            }

        except Exception as e:
            error_msg = f"Ошибка получения файлов: {str(e)}"
            self.log(f"Ошибка: {error_msg}")
            return {
                'type': 'list_files',
                'error': error_msg,
                'success': False
            }


    def change_directory(self, path):
        """Смена директории"""
        try:
            self.log(f"Попытка смены директории на: {path}")

            if not path:
                path = os.path.expanduser('~')

            if path.startswith('~'):
                path = os.path.expanduser(path)

            abs_path = os.path.abspath(path)

            if not os.path.exists(abs_path):
                return {
                    'type': 'cd',
                    'error': f'Путь не существует: {abs_path}',
                    'success': False
                }

            if not os.path.isdir(abs_path):
                return {
                    'type': 'cd',
                    'error': f'Не является директорией: {abs_path}',
                    'success': False
                }

            os.chdir(abs_path)
            current_dir = os.getcwd()

            self.log(f"Директория изменена на: {current_dir}")
            return {
                'type': 'cd',
                'cwd': current_dir,
                'success': True
            }

        except Exception as e:
            error_msg = f"Ошибка смены директории: {str(e)}"
            self.log(f"Ошибка: {error_msg}")
            return {
                'type': 'cd',
                'error': error_msg,
                'success': False
            }

    def download_file(self, filepath):
        """Скачивание файла"""
        try:
            self.log(f"Запрос на скачивание файла: {filepath}")

            if not os.path.exists(filepath):
                return {
                    'type': 'download',
                    'error': f'Файл не существует: {filepath}',
                    'success': False
                }

            if not os.path.isfile(filepath):
                return {
                    'type': 'download',
                    'error': f'Не является файлом: {filepath}',
                    'success': False
                }

            file_size = os.path.getsize(filepath)
            if file_size > 100 * 1024 * 1024:
                return {
                    'type': 'download',
                    'error': f'Файл слишком большой: {file_size // (1024 * 1024)}MB (лимит 100MB)',
                    'success': False
                }

            with open(filepath, 'rb') as f:
                file_data = f.read()

            encoded_data = base64.b64encode(file_data).decode('utf-8')

            return {
                'type': 'download',
                'filename': os.path.basename(filepath),
                'data': encoded_data,
                'size': file_size,
                'success': True
            }

        except Exception as e:
            error_msg = f"Ошибка чтения файла: {str(e)}"
            self.log(f"Ошибка: {error_msg}")
            return {
                'type': 'download',
                'error': error_msg,
                'success': False
            }

    def upload_file(self, filename, data, path='.'):
        """Загрузка файла на сервер"""
        try:
            self.log(f"Загрузка файла на сервер: {filename} в {path}")

            if not path or path == '.':
                path = os.getcwd()

            full_path = os.path.join(path, filename)

            if len(data) > 100 * 1024 * 1024:
                return {
                    'type': 'upload',
                    'error': f'Файл слишком большой: {len(data)} символов (лимит 100MB)',
                    'success': False
                }

            try:
                file_data = base64.b64decode(data)
            except Exception as e:
                return {
                    'type': 'upload',
                    'error': f'Неверный формат данных: {str(e)}',
                    'success': False
                }

            with open(full_path, 'wb') as f:
                f.write(file_data)

            file_size = len(file_data)

            self.log(f"Файл успешно загружен: {full_path} ({file_size} байт)")

            return {
                'type': 'upload',
                'filename': filename,
                'path': full_path,
                'size': file_size,
                'success': True
            }

        except Exception as e:
            error_msg = f'Ошибка загрузки файла: {str(e)}'
            self.log(f"Ошибка: {error_msg}")
            return {
                'type': 'upload',
                'error': error_msg,
                'success': False
            }

    def rename_file(self, old_path, new_path):
        """Переименование файла"""
        try:
            self.log(f"Переименование: {old_path} -> {new_path}")

            old_path = os.path.abspath(old_path)
            new_path = os.path.abspath(new_path)

            if not os.path.exists(old_path):
                return {
                    'type': 'rename',
                    'error': f'Исходный файл не существует: {old_path}',
                    'success': False
                }

            if os.path.exists(new_path):
                return {
                    'type': 'rename',
                    'error': f'Файл с именем {os.path.basename(new_path)} уже существует',
                    'success': False
                }

            os.rename(old_path, new_path)

            self.log(f"Успешно переименовано: {old_path} -> {new_path}")

            return {
                'type': 'rename',
                'old_path': old_path,
                'new_path': new_path,
                'success': True
            }

        except Exception as e:
            error_msg = f'Ошибка переименования: {str(e)}'
            self.log(f"Ошибка: {error_msg}")
            return {
                'type': 'rename',
                'error': error_msg,
                'success': False
            }

    def delete_file(self, path):
        """Удаление файла"""
        try:
            self.log(f"Удаление: {path}")

            path = os.path.abspath(path)

            if not os.path.exists(path):
                return {
                    'type': 'delete',
                    'error': f'Файл не существует: {path}',
                    'success': False
                }

            is_dir = os.path.isdir(path)

            if is_dir:
                shutil.rmtree(path)
            else:
                os.remove(path)

            filename = os.path.basename(path)

            self.log(f"Успешно удалено: {path}")

            return {
                'type': 'delete',
                'path': path,
                'filename': filename,
                'is_dir': is_dir,
                'success': True
            }

        except Exception as e:
            error_msg = f'Ошибка удаления: {str(e)}'
            self.log(f"Ошибка: {error_msg}")
            return {
                'type': 'delete',
                'error': error_msg,
                'success': False
            }

    def authenticate(self, password_hash):
        """Проверка пароля"""
        if self.password is None:
            return {'type': 'auth', 'success': True, 'message': 'Аутентификация не требуется'}
        if password_hash == self.password:
            return {'type': 'auth', 'success': True, 'message': 'Аутентификация успешна'}
        return {'type': 'auth', 'success': False, 'error': 'Неверный пароль'}

    def get_command_history(self, limit=50):
        """Получить историю команд"""
        history = self.command_history[-limit:] if len(self.command_history) > limit else self.command_history
        return {
            'type': 'command_history',
            'history': history,
            'total': len(self.command_history),
            'success': True
        }

    def search_files(self, path, pattern, recursive=True):
        """Поиск файлов по маске"""
        try:
            self.log(f"Поиск файлов: '{pattern}' в '{path}', рекурсивно={recursive}")
            if not path or path == '.':
                path = os.getcwd()
            if path.startswith('~'):
                path = os.path.expanduser(path)
            abs_path = os.path.abspath(path)

            if not os.path.exists(abs_path):
                return {'type': 'search_files', 'error': f'Путь не существует: {abs_path}', 'success': False}

            found = []
            if recursive:
                for root, dirs, files in os.walk(abs_path):
                    for name in files + dirs:
                        if fnmatch.fnmatch(name.lower(), pattern.lower()):
                            full = os.path.join(root, name)
                            try:
                                is_dir = os.path.isdir(full)
                                size = 0 if is_dir else os.path.getsize(full)
                                found.append({'name': name, 'path': full, 'is_dir': is_dir, 'size': size})
                            except:
                                pass
                    if len(found) >= 500:
                        break
            else:
                for name in os.listdir(abs_path):
                    if fnmatch.fnmatch(name.lower(), pattern.lower()):
                        full = os.path.join(abs_path, name)
                        try:
                            is_dir = os.path.isdir(full)
                            size = 0 if is_dir else os.path.getsize(full)
                            found.append({'name': name, 'path': full, 'is_dir': is_dir, 'size': size})
                        except:
                            pass

            self.log(f"Найдено {len(found)} файлов")
            return {'type': 'search_files', 'results': found, 'count': len(found),
                    'pattern': pattern, 'path': abs_path, 'success': True}
        except Exception as e:
            return {'type': 'search_files', 'error': str(e), 'success': False}

    def zip_files(self, paths, archive_name=None):
        """Архивирование файлов/папок в zip"""
        try:
            if not paths:
                return {'type': 'zip_files', 'error': 'Не указаны файлы для архивирования', 'success': False}

            if not archive_name:
                base = os.path.basename(paths[0].rstrip('/\\'))
                archive_name = base + '.zip'

            # Сохраняем архив рядом с первым файлом
            archive_dir = os.path.dirname(os.path.abspath(paths[0]))
            archive_path = os.path.join(archive_dir, archive_name)

            self.log(f"Создание архива: {archive_path}")
            with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for path in paths:
                    path = os.path.abspath(path)
                    if not os.path.exists(path):
                        continue
                    if os.path.isdir(path):
                        for root, dirs, files in os.walk(path):
                            for file in files:
                                full = os.path.join(root, file)
                                arcname = os.path.relpath(full, os.path.dirname(path))
                                zf.write(full, arcname)
                    else:
                        zf.write(path, os.path.basename(path))

            size = os.path.getsize(archive_path)
            self.log(f"Архив создан: {archive_path} ({size} байт)")
            return {
                'type': 'zip_files', 'archive_path': archive_path,
                'archive_name': archive_name, 'size': size, 'success': True
            }
        except Exception as e:
            return {'type': 'zip_files', 'error': str(e), 'success': False}

    def unzip_file(self, archive_path, extract_to=None):
        """Разархивирование zip файла"""
        try:
            archive_path = os.path.abspath(archive_path)
            if not os.path.exists(archive_path):
                return {'type': 'unzip_file', 'error': f'Архив не существует: {archive_path}', 'success': False}

            if not extract_to:
                extract_to = os.path.dirname(archive_path)
            extract_to = os.path.abspath(extract_to)
            os.makedirs(extract_to, exist_ok=True)

            self.log(f"Разархивирование {archive_path} в {extract_to}")
            with zipfile.ZipFile(archive_path, 'r') as zf:
                names = zf.namelist()
                zf.extractall(extract_to)

            self.log(f"Разархивировано {len(names)} файлов в {extract_to}")
            return {
                'type': 'unzip_file', 'extract_to': extract_to,
                'files_count': len(names), 'success': True
            }
        except zipfile.BadZipFile:
            return {'type': 'unzip_file', 'error': 'Файл не является корректным zip архивом', 'success': False}
        except Exception as e:
            return {'type': 'unzip_file', 'error': str(e), 'success': False}

    def copy_file(self, src, dst):
        """Копирование файла/папки"""
        try:
            src = os.path.abspath(src)
            dst = os.path.abspath(dst)
            if not os.path.exists(src):
                return {'type': 'copy_file', 'error': f'Источник не существует: {src}', 'success': False}

            self.log(f"Копирование: {src} -> {dst}")
            if os.path.isdir(src):
                if os.path.exists(dst):
                    dst = os.path.join(dst, os.path.basename(src))
                shutil.copytree(src, dst)
            else:
                if os.path.isdir(dst):
                    dst = os.path.join(dst, os.path.basename(src))
                shutil.copy2(src, dst)

            return {'type': 'copy_file', 'src': src, 'dst': dst, 'success': True}
        except Exception as e:
            return {'type': 'copy_file', 'error': str(e), 'success': False}

    def get_drives(self):
        """Получить список дисков/разделов"""
        try:
            drives = []
            for part in psutil.disk_partitions(all=False):
                try:
                    usage = psutil.disk_usage(part.mountpoint)
                    free_gb = usage.free // (1024 ** 3)
                    total_gb = usage.total // (1024 ** 3)
                    drives.append({
                        'device': part.device,
                        'mountpoint': part.mountpoint,
                        'fstype': part.fstype,
                        'free_gb': free_gb,
                        'total_gb': total_gb,
                    })
                except:
                    drives.append({
                        'device': part.device,
                        'mountpoint': part.mountpoint,
                        'fstype': part.fstype,
                        'free_gb': 0,
                        'total_gb': 0,
                    })
            return {'type': 'get_drives', 'drives': drives, 'success': True}
        except Exception as e:
            return {'type': 'get_drives', 'error': str(e), 'success': False}

    def get_file_info(self, path):
        """Получить подробную информацию о файле"""
        try:
            path = os.path.abspath(path)
            if not os.path.exists(path):
                return {'type': 'file_info', 'error': f'Файл не существует: {path}', 'success': False}

            stat = os.stat(path)
            is_dir = os.path.isdir(path)

            info = {
                'type': 'file_info',
                'path': path,
                'name': os.path.basename(path),
                'is_dir': is_dir,
                'size': stat.st_size,
                'created': stat.st_ctime,
                'modified': stat.st_mtime,
                'accessed': stat.st_atime,
                'success': True
            }

            if not is_dir:
                # MD5 для небольших файлов
                if stat.st_size < 50 * 1024 * 1024:
                    try:
                        md5 = hashlib.md5()
                        with open(path, 'rb') as f:
                            for chunk in iter(lambda: f.read(8192), b''):
                                md5.update(chunk)
                        info['md5'] = md5.hexdigest()
                    except:
                        info['md5'] = 'Ошибка вычисления'
                else:
                    info['md5'] = 'Файл слишком большой'
            else:
                # Количество файлов в папке
                try:
                    items = os.listdir(path)
                    info['items_count'] = len(items)
                except:
                    info['items_count'] = -1

            return info
        except Exception as e:
            return {'type': 'file_info', 'error': str(e), 'success': False}

    # ─────────────── ПЛАНИРОВЩИК ЗАДАЧ ───────────────

    def schedule_task(self, command, delay_seconds, repeat_interval=0, task_name=''):
        """Запланировать выполнение команды через N секунд"""
        with self.scheduler_lock:
            task_id = self.next_task_id
            self.next_task_id += 1

        run_at = time.time() + delay_seconds
        task_info = {
            'id': task_id,
            'name': task_name or f'Задача #{task_id}',
            'command': command,
            'delay_seconds': delay_seconds,
            'repeat_interval': repeat_interval,
            'run_at': run_at,
            'status': 'pending',
            'created_at': time.strftime("%Y-%m-%d %H:%M:%S"),
            'run_at_str': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(run_at)),
            'result': None,
        }

        with self.scheduler_lock:
            self.scheduled_tasks[task_id] = task_info

        # Запускаем поток-таймер
        t = threading.Thread(target=self._run_scheduled_task, args=(task_id,), daemon=True)
        t.start()

        self.log(f"📅 Задача #{task_id} запланирована: '{command}' через {delay_seconds}с")
        return task_id

    def _run_scheduled_task(self, task_id):
        """Внутренний поток планировщика"""
        with self.scheduler_lock:
            task = self.scheduled_tasks.get(task_id)
        if not task:
            return

        delay = task['run_at'] - time.time()
        if delay > 0:
            time.sleep(delay)

        with self.scheduler_lock:
            if task_id not in self.scheduled_tasks:
                return
            self.scheduled_tasks[task_id]['status'] = 'running'

        self.log(f"⏰ Выполняется запланированная задача #{task_id}: {task['command']}")
        result = self.execute_command(task['command'])

        with self.scheduler_lock:
            if task_id in self.scheduled_tasks:
                self.scheduled_tasks[task_id]['status'] = 'done'
                self.scheduled_tasks[task_id]['result'] = result.get('output', result.get('error', ''))
                self.scheduled_tasks[task_id]['finished_at'] = time.strftime("%Y-%m-%d %H:%M:%S")

        # Повторяющаяся задача
        if task.get('repeat_interval', 0) > 0:
            with self.scheduler_lock:
                if task_id in self.scheduled_tasks:
                    new_run_at = time.time() + task['repeat_interval']
                    self.scheduled_tasks[task_id]['run_at'] = new_run_at
                    self.scheduled_tasks[task_id]['run_at_str'] = time.strftime(
                        "%Y-%m-%d %H:%M:%S", time.localtime(new_run_at))
                    self.scheduled_tasks[task_id]['status'] = 'pending'
            # Перезапускаем
            t = threading.Thread(target=self._run_scheduled_task, args=(task_id,), daemon=True)
            t.start()

    def cancel_task(self, task_id):
        """Отменить задачу"""
        with self.scheduler_lock:
            if task_id in self.scheduled_tasks:
                if self.scheduled_tasks[task_id]['status'] == 'pending':
                    self.scheduled_tasks[task_id]['status'] = 'cancelled'
                    return True
        return False

    def get_tasks(self):
        """Получить список задач"""
        with self.scheduler_lock:
            tasks = list(self.scheduled_tasks.values())
        return tasks

    # ─────────────── ТРАНСЛЯЦИЯ ЭКРАНА ───────────────

    def get_screen_frame(self, quality=50, scale=0.5):
        """Снять один кадр экрана в JPEG с уменьшенным размером"""
        if not HAS_PILLOW:
            return None, "Pillow не установлен"
        try:
            screenshot = ImageGrab.grab()
            if scale != 1.0:
                new_w = int(screenshot.width * scale)
                new_h = int(screenshot.height * scale)
                screenshot = screenshot.resize((new_w, new_h))
            buf = io.BytesIO()
            screenshot.save(buf, format='JPEG', quality=quality, optimize=True)
            buf.seek(0)
            data = buf.read()
            return base64.b64encode(data).decode('utf-8'), None
        except Exception as e:
            return None, str(e)

    # ─────────────── УПРАВЛЕНИЕ МЫШЬЮ ───────────────

    def mouse_move(self, x, y):
        """Переместить курсор"""
        if not HAS_PYAUTOGUI:
            return {'type': 'mouse_move', 'error': 'pyautogui не установлен. Установите: pip install pyautogui', 'success': False}
        try:
            pyautogui.moveTo(x, y, duration=0)
            return {'type': 'mouse_move', 'x': x, 'y': y, 'success': True}
        except Exception as e:
            return {'type': 'mouse_move', 'error': str(e), 'success': False}

    def mouse_click(self, x, y, button='left', double=False):
        """Клик мышью"""
        if not HAS_PYAUTOGUI:
            return {'type': 'mouse_click', 'error': 'pyautogui не установлен', 'success': False}
        try:
            if double:
                pyautogui.doubleClick(x, y, button=button)
            else:
                pyautogui.click(x, y, button=button)
            return {'type': 'mouse_click', 'x': x, 'y': y, 'button': button, 'double': double, 'success': True}
        except Exception as e:
            return {'type': 'mouse_click', 'error': str(e), 'success': False}

    def mouse_scroll(self, x, y, delta):
        """Прокрутка колёсиком"""
        if not HAS_PYAUTOGUI:
            return {'type': 'mouse_scroll', 'error': 'pyautogui не установлен', 'success': False}
        try:
            pyautogui.moveTo(x, y, duration=0)
            pyautogui.scroll(delta)
            return {'type': 'mouse_scroll', 'x': x, 'y': y, 'delta': delta, 'success': True}
        except Exception as e:
            return {'type': 'mouse_scroll', 'error': str(e), 'success': False}

    def mouse_drag(self, x1, y1, x2, y2, button='left'):
        """Перетаскивание мышью"""
        if not HAS_PYAUTOGUI:
            return {'type': 'mouse_drag', 'error': 'pyautogui не установлен', 'success': False}
        try:
            pyautogui.mouseDown(x1, y1, button=button)
            pyautogui.moveTo(x2, y2, duration=0.1)
            pyautogui.mouseUp(button=button)
            return {'type': 'mouse_drag', 'success': True}
        except Exception as e:
            return {'type': 'mouse_drag', 'error': str(e), 'success': False}

    def get_screen_size(self):
        """Получить размер экрана сервера"""
        if not HAS_PYAUTOGUI:
            # Fallback через PIL
            if HAS_PILLOW:
                try:
                    img = ImageGrab.grab()
                    return {'type': 'screen_size', 'width': img.width, 'height': img.height, 'success': True}
                except:
                    pass
            return {'type': 'screen_size', 'error': 'pyautogui не установлен', 'success': False}
        try:
            w, h = pyautogui.size()
            return {'type': 'screen_size', 'width': w, 'height': h, 'success': True}
        except Exception as e:
            return {'type': 'screen_size', 'error': str(e), 'success': False}

    def process_test(self):
        """Обработка тестового запроса"""
        return {
            'type': 'test',
            'message': 'Сервер работает корректно!',
            'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
            'success': True
        }

    def process_ping(self):
        """Обработка ping-запроса"""
        return {
            'type': 'ping',
            'message': 'pong',
            'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
            'success': True
        }

    def process_get_processes(self):
        processes = []

        for p in psutil.process_iter(['pid', 'name', 'memory_info']):
            try:
                processes.append({
                    "pid": str(p.info['pid']),
                    "name": p.info['name'] or "",
                    "memory": f"{p.info['memory_info'].rss // (1024 * 1024)} MB"
                })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        return {
            "type": "process_list",
            "processes": processes
        }

    def process_kill_process(self, pid):
        """Завершить процесс"""
        try:
            self.log(f"Завершение процесса PID: {pid}")

            if platform.system() == 'Windows':
                cmd = f'taskkill /PID {pid} /F'
            else:
                cmd = f'kill -9 {pid}'

            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)

            if result.returncode == 0:
                return {
                    'type': 'kill_process',
                    'pid': pid,
                    'message': f'Процесс {pid} завершен успешно',
                    'success': True
                }
            else:
                return {
                    'type': 'kill_process',
                    'pid': pid,
                    'error': f'Ошибка завершения процесса: {result.stderr}',
                    'success': False
                }

        except Exception as e:
            error_msg = f"Ошибка завершения процесса: {str(e)}"
            self.log(f"Ошибка: {error_msg}")
            return {
                'type': 'kill_process',
                'pid': pid,
                'error': error_msg,
                'success': False
            }

    # НОВЫЕ КОМАНДЫ ДЛЯ ОБРАТНОГО ПОДКЛЮЧЕНИЯ
    def process_connect_to_client(self, request):
        """Подключиться к клиенту (обратное подключение)"""
        host = request.get('host')
        port = request.get('port')

        if not host or not port:
            return {
                'type': 'reverse_connect',
                'error': 'Не указан хост или порт',
                'success': False
            }

        try:
            port = int(port)
        except:
            return {
                'type': 'reverse_connect',
                'error': 'Неверный порт',
                'success': False
            }

        client_id, message = self.reverse_manager.connect_to_client(host, port)

        if client_id:
            return {
                'type': 'reverse_connect',
                'client_id': client_id,
                'message': message,
                'host': host,
                'port': port,
                'success': True
            }
        else:
            return {
                'type': 'reverse_connect',
                'error': message,
                'success': False
            }

    def process_list_clients(self):
        """Получить список подключенных клиентов"""
        clients = self.reverse_manager.get_connected_clients()

        return {
            'type': 'list_clients',
            'clients': clients,
            'count': len(clients),
            'success': True
        }

    def process_send_to_client(self, request):
        """Отправить команду клиенту"""
        client_id = request.get('client_id')
        command = request.get('command')

        if not client_id or not command:
            return {
                'type': 'send_to_client',
                'error': 'Не указан ID клиента или команда',
                'success': False
            }

        response, error = self.reverse_manager.send_command_to_client(
            client_id, 'command', command=command
        )

        if error:
            return {
                'type': 'send_to_client',
                'error': error,
                'success': False
            }
        else:
            return {
                'type': 'send_to_client',
                'client_id': client_id,
                'response': response,
                'success': True
            }

    def process_disconnect_client(self, request):
        """Отключить клиента"""
        client_id = request.get('client_id')

        if not client_id:
            return {
                'type': 'disconnect_client',
                'error': 'Не указан ID клиента',
                'success': False
            }

        self.reverse_manager._disconnect_client(client_id)

        return {
            'type': 'disconnect_client',
            'client_id': client_id,
            'message': f'Клиент {client_id} отключен',
            'success': True
        }

    def process_request(self, request):
        """Обработка запроса"""
        try:
            if not isinstance(request, dict):
                return {
                    'type': 'error',
                    'error': 'Неверный формат запроса',
                    'success': False
                }

            command_type = request.get('type', 'command')
            self.log(f"Обработка команды: {command_type}")

            # Обрабатываем команду
            if command_type == 'command':
                cmd = request.get('command', '')
                if not cmd:
                    return {
                        'type': 'command',
                        'error': 'Пустая команда',
                        'success': False
                    }

                # Сохраняем в историю
                self.command_history.append({
                    'command': cmd,
                    'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                    'cwd': os.getcwd()
                })
                if len(self.command_history) > 1000:
                    self.command_history = self.command_history[-1000:]

                # Специальная обработка для команд процессов
                if 'tasklist' in cmd.lower() or 'ps aux' in cmd.lower():
                    return self.process_get_processes()
                elif 'taskkill' in cmd.lower() or 'kill' in cmd.lower():
                    # Пытаемся извлечь PID из команды
                    import re
                    match = re.search(r'\b(\d+)\b', cmd)
                    if match:
                        pid = match.group(1)
                        return self.process_kill_process(pid)
                    else:
                        return self.execute_command(cmd)
                else:
                    return self.execute_command(cmd)

            elif command_type == 'sysinfo':
                return self.get_system_info()

            elif command_type == 'screenshot':
                return self.take_screenshot()

            elif command_type == 'list_files':
                path = request.get('path', '.')
                return self.list_files(path)

            elif command_type == 'cd':
                path = request.get('path', '.')
                return self.change_directory(path)

            elif command_type == 'download':
                filepath = request.get('filepath', '')
                if not filepath:
                    return {
                        'type': 'download',
                        'error': 'Не указан путь к файлу',
                        'success': False
                    }
                return self.download_file(filepath)

            elif command_type == 'upload':
                filename = request.get('filename', '')
                data = request.get('data', '')
                path = request.get('path', '.')
                if not filename or not data:
                    return {
                        'type': 'upload',
                        'error': 'Не указано имя файла или данные',
                        'success': False
                    }
                return self.upload_file(filename, data, path)

            elif command_type == 'rename':
                old_path = request.get('old_path', '')
                new_path = request.get('new_path', '')
                if not old_path or not new_path:
                    return {
                        'type': 'rename',
                        'error': 'Не указаны старый или новый путь',
                        'success': False
                    }
                return self.rename_file(old_path, new_path)

            elif command_type == 'delete':
                path = request.get('path', '')
                if not path:
                    return {
                        'type': 'delete',
                        'error': 'Не указан путь к файлу',
                        'success': False
                    }
                return self.delete_file(path)

            elif command_type == 'test':
                return self.process_test()

            elif command_type == 'ping':
                return self.process_ping()

            elif command_type == 'process_list':
                return self.process_get_processes()

            elif command_type == 'kill_process':
                pid = request.get('pid')
                if not pid:
                    return {
                        'type': 'kill_process',
                        'error': 'Не указан PID процесса',
                        'success': False
                    }
                return self.process_kill_process(pid)

            elif command_type == 'get_drives':
                return self.get_drives()

            elif command_type == 'auth':
                password_hash = request.get('password_hash', '')
                return self.authenticate(password_hash)

            elif command_type == 'command_history':
                limit = request.get('limit', 50)
                return self.get_command_history(limit)

            elif command_type == 'search_files':
                path = request.get('path', '.')
                pattern = request.get('pattern', '*')
                recursive = request.get('recursive', True)
                return self.search_files(path, pattern, recursive)

            elif command_type == 'zip_files':
                paths = request.get('paths', [])
                archive_name = request.get('archive_name', None)
                return self.zip_files(paths, archive_name)

            elif command_type == 'unzip_file':
                archive_path = request.get('archive_path', '')
                extract_to = request.get('extract_to', None)
                return self.unzip_file(archive_path, extract_to)

            elif command_type == 'copy_file':
                src = request.get('src', '')
                dst = request.get('dst', '')
                if not src or not dst:
                    return {'type': 'copy_file', 'error': 'Не указан источник или назначение', 'success': False}
                return self.copy_file(src, dst)

            elif command_type == 'file_info':
                path = request.get('path', '')
                if not path:
                    return {'type': 'file_info', 'error': 'Не указан путь', 'success': False}
                return self.get_file_info(path)

            elif command_type == 'mouse_move':
                x, y = int(request.get('x', 0)), int(request.get('y', 0))
                return self.mouse_move(x, y)

            elif command_type == 'mouse_click':
                x, y = int(request.get('x', 0)), int(request.get('y', 0))
                button = request.get('button', 'left')
                double = bool(request.get('double', False))
                return self.mouse_click(x, y, button, double)

            elif command_type == 'mouse_scroll':
                x, y = int(request.get('x', 0)), int(request.get('y', 0))
                delta = int(request.get('delta', 0))
                return self.mouse_scroll(x, y, delta)

            elif command_type == 'mouse_drag':
                x1, y1 = int(request.get('x1', 0)), int(request.get('y1', 0))
                x2, y2 = int(request.get('x2', 0)), int(request.get('y2', 0))
                button = request.get('button', 'left')
                return self.mouse_drag(x1, y1, x2, y2, button)

            elif command_type == 'screen_size':
                return self.get_screen_size()

            elif command_type == 'schedule_task':
                command = request.get('command', '')
                delay = int(request.get('delay_seconds', 0))
                repeat = int(request.get('repeat_interval', 0))
                name = request.get('task_name', '')
                if not command:
                    return {'type': 'schedule_task', 'error': 'Не указана команда', 'success': False}
                task_id = self.schedule_task(command, delay, repeat, name)
                task = self.scheduled_tasks[task_id]
                return {
                    'type': 'schedule_task',
                    'task_id': task_id,
                    'task_name': task['name'],
                    'run_at_str': task['run_at_str'],
                    'success': True
                }

            elif command_type == 'cancel_task':
                task_id = int(request.get('task_id', 0))
                cancelled = self.cancel_task(task_id)
                return {
                    'type': 'cancel_task',
                    'task_id': task_id,
                    'success': cancelled,
                    'error': None if cancelled else 'Задача не найдена или уже выполняется'
                }

            elif command_type == 'get_tasks':
                tasks = self.get_tasks()
                return {'type': 'get_tasks', 'tasks': tasks, 'success': True}

            elif command_type == 'screen_frame':
                quality = int(request.get('quality', 50))
                scale = float(request.get('scale', 0.5))
                frame, error = self.get_screen_frame(quality, scale)
                if error:
                    return {'type': 'screen_frame', 'error': error, 'success': False}
                return {
                    'type': 'screen_frame',
                    'frame': frame,
                    'width': int(ImageGrab.grab().width * scale) if HAS_PILLOW else 0,
                    'height': int(ImageGrab.grab().height * scale) if HAS_PILLOW else 0,
                    'success': True
                }

            # НОВЫЕ КОМАНДЫ ДЛЯ ОБРАТНОГО ПОДКЛЮЧЕНИЯ
            elif command_type == 'reverse_connect':
                return self.process_connect_to_client(request)

            elif command_type == 'list_clients':
                return self.process_list_clients()

            elif command_type == 'send_to_client':
                return self.process_send_to_client(request)

            elif command_type == 'disconnect_client':
                return self.process_disconnect_client(request)

            else:
                return {
                    'type': 'error',
                    'error': f'Неизвестная команда: {command_type}',
                    'success': False
                }

        except Exception as e:
            error_msg = f"Критическая ошибка обработки запроса: {str(e)}"
            self.log(f"Ошибка: {error_msg}")
            traceback.print_exc()
            return {
                'type': 'error',
                'error': error_msg,
                'success': False
            }

    def _receive_all(self, sock, timeout=30):
        """Надежное получение всех данных"""
        sock.settimeout(timeout)

        size_data = b""
        while len(size_data) < 4:
            try:
                chunk = sock.recv(4 - len(size_data))
                if not chunk:
                    return None
                size_data += chunk
            except socket.timeout:
                return None

        total_size = struct.unpack('!I', size_data)[0]

        data = b""
        while len(data) < total_size:
            try:
                chunk = sock.recv(min(4096, total_size - len(data)))
                if not chunk:
                    return None
                data += chunk
            except socket.timeout:
                return None

        return data

    def _send_all(self, sock, response):
        """Надежная отправка всех данных"""
        try:
            response_json = json.dumps(response, ensure_ascii=False, indent=None)
            response_data = response_json.encode('utf-8')

            total_size = len(response_data)
            sock.sendall(struct.pack('!I', total_size))
            sock.sendall(response_data)
            return True
        except Exception as e:
            self.log(f"ОШИБКА отправки ответа: {e}")
            return False

    def send_response(self, client_socket, response):
        """Отправка ответа клиенту"""
        return self._send_all(client_socket, response)

    def receive_request(self, client_socket, timeout=30):
        """Получение запроса от клиента"""
        try:
            data = self._receive_all(client_socket, timeout)
            if not data:
                return None

            decoded = data.decode('utf-8', errors='ignore')
            request = json.loads(decoded)
            return request

        except json.JSONDecodeError as e:
            self.log(f"Ошибка декодирования JSON: {e}")
            return None
        except Exception as e:
            self.log(f"Ошибка получения запроса: {e}")
            return None

    def handle_client(self, client_socket, address):
        """Обработка клиента"""
        self.log(f"🔗 Новый клиент: {address}")
        client_socket.settimeout(120)

        try:
            while self.running:
                request = self.receive_request(client_socket, timeout=120)
                if request is None:
                    self.log(f"📤 Клиент {address} отключился")
                    break

                response = self.process_request(request)

                if not self.send_response(client_socket, response):
                    self.log(f"❌ Не удалось отправить ответ клиенту {address}")
                    break

                self.log(f"✅ Запрос от {address} обработан: {request.get('type', 'unknown')}")

        except Exception as e:
            self.log(f"❌ Ошибка обработки клиента {address}: {e}")
        finally:
            try:
                client_socket.close()
            except:
                pass
            self.log(f"🚪 Соединение с {address} закрыто")

    def start_standalone_server(self):
        """Запуск автономного сервера"""
        try:
            self.log("🛠️  Создание сокета для автономного режима...")
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.settimeout(1)

            self.log(f"📍 Привязка к {self.host}:{self.port}...")
            self.server_socket.bind((self.host, self.port))

            self.log("👂 Начало прослушивания...")
            self.server_socket.listen(5)

            self.log("=" * 70)
            self.log("✅ АВТОНОМНЫЙ СЕРВЕР ЗАПУЩЕН УСПЕШНО!")
            self.log("=" * 70)
            self.log(f"🔗 Подключение: {self.host}:{self.port}")
            self.log("📋 Режим: Принимает подключения от клиентов")
            self.log("=" * 70)
            self.log("👂 Ожидание подключений...")
            self.log("🛑 Ctrl+C для остановки")

            while self.running:
                try:
                    client_socket, address = self.server_socket.accept()
                    client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, address),
                        daemon=True
                    )
                    client_thread.start()

                except socket.timeout:
                    continue
                except KeyboardInterrupt:
                    self.log("🛑 Остановка по запросу пользователя...")
                    break
                except Exception as e:
                    if self.running:
                        self.log(f"⚠️  Ошибка accept: {e}")
                    continue

        except Exception as e:
            self.log(f"❌ Ошибка запуска автономного сервера: {e}")

    def start_reverse_client(self, target_list):
        """Запуск в режиме активного клиента (подключается к другим)"""
        self.log("=" * 70)
        self.log("🔌 РЕЖИМ АКТИВНОГО КЛИЕНТА")
        self.log("=" * 70)
        self.log("📋 Цели для подключения:")
        for host, port in target_list:
            self.log(f"  • {host}:{port}")
        self.log("=" * 70)
        self.log("🔄 Начало авто-подключения к целям...")

        # Запускаем автоматическое подключение
        self.reverse_manager.start_auto_connect(target_list)

        self.log("✅ Режим активного клиента запущен")
        self.log("🔄 Автоматическое подключение к целям...")
        self.log("🛑 Ctrl+C для остановки")

        # Основной цикл
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.log("🛑 Остановка по запросу пользователя...")

    def start_hybrid_mode(self, target_list):
        """Запуск в гибридном режиме"""
        self.log("=" * 70)
        self.log("🔀 ГИБРИДНЫЙ РЕЖИМ")
        self.log("=" * 70)
        self.log("📋 Функции:")
        self.log("  • Принимает подключения как сервер")
        self.log("  • Подключается к целям как клиент")
        self.log("=" * 70)

        # Запускаем автономный сервер в отдельном потоке
        server_thread = threading.Thread(
            target=self.start_standalone_server,
            daemon=True
        )
        server_thread.start()

        # Запускаем активного клиента
        if target_list:
            self.start_reverse_client(target_list)
        else:
            self.log("⚠️  Список целей пуст, активный клиент не запущен")
            try:
                while self.running:
                    time.sleep(1)
            except KeyboardInterrupt:
                self.log("🛑 Остановка по запросу пользователя...")

    def start_server(self, target_list=None):
        """Запуск сервера в выбранном режиме"""
        self.running = True

        try:
            if self.mode == ServerMode.STANDALONE:
                self.start_standalone_server()
            elif self.mode == ServerMode.REVERSE_CLIENT:
                if not target_list:
                    self.log("❌ В режиме активного клиента требуется список целей")
                    return
                self.start_reverse_client(target_list)
            elif self.mode == ServerMode.HYBRID:
                self.start_hybrid_mode(target_list or [])

        except KeyboardInterrupt:
            self.log("🛑 Остановка сервера...")
        except Exception as e:
            self.log(f"❌ Критическая ошибка: {e}")
            traceback.print_exc()
        finally:
            self.running = False
            self.reverse_manager.disconnect_all()
            if self.server_socket:
                try:
                    self.server_socket.close()
                except:
                    pass
            self.log("🛑 Сервер остановлен")

    def stop_server(self):
        """Остановка сервера"""
        self.running = False


def main():
    """Главная функция с выбором режима"""
    print("\n" + "=" * 70)
    print("⚙️  НАСТРОЙКА СЕРВЕРА УПРАВЛЕНИЯ")
    print("=" * 70)

    print("\n🔍 Проверка зависимостей...")
    try:
        import psutil
        print("✅ psutil установлен")
    except ImportError:
        print("❌ psutil не установлен")
        print("   Установите: pip install psutil")
        return

    if not HAS_PILLOW:
        print("⚠️  Pillow не установлен (скриншоты недоступны)")
        print("   Установите: pip install pillow")

    print("\n🎛️  Выберите режим работы:")
    print("  1. Автономный сервер (принимает подключения)")
    print("  2. Активный клиент (подключается к другим)")
    print("  3. Гибридный режим (оба варианта)")

    mode_choice = input("\nВаш выбор (1/2/3) [1]: ").strip() or "1"

    if mode_choice == "1":
        mode = ServerMode.STANDALONE
    elif mode_choice == "2":
        mode = ServerMode.REVERSE_CLIENT
    elif mode_choice == "3":
        mode = ServerMode.HYBRID
    else:
        mode = ServerMode.STANDALONE

    print(f"\n🎛️  Выбран режим: {mode.value}")

    # Настройка хоста и порта
    if mode in [ServerMode.STANDALONE, ServerMode.HYBRID]:
        print("\n🌐 Настройка серверной части:")
        print("  1. Только этот компьютер (127.0.0.1)")
        print("  2. Все компьютеры в сети (0.0.0.0)")
        print("  3. Специальный IP адрес")

        host_choice = input("\nВаш выбор (1/2/3) [1]: ").strip() or "1"

        if host_choice == "1":
            host = "127.0.0.1"
        elif host_choice == "2":
            host = "0.0.0.0"
        elif host_choice == "3":
            host = input("Введите IP адрес: ").strip()
            if not host:
                host = "127.0.0.1"
        else:
            host = "127.0.0.1"

        port_str = input(f"\n🔢 Введите порт для сервера [5000]: ").strip() or "5000"
    else:
        host = "127.0.0.1"  # Для активного клиента не нужен серверный хост
        port = 0

    try:
        if 'port_str' in locals():
            port = int(port_str)
            if port < 1 or port > 65535:
                print("❌ Порт должен быть от 1 до 65535")
                return
    except:
        print("❌ Неверный порт")
        return

    # Настройка целей для подключения (если нужно)
    target_list = []
    if mode in [ServerMode.REVERSE_CLIENT, ServerMode.HYBRID]:
        print("\n🎯 Настройка целей для подключения:")
        print("  Формат: IP:PORT (например: 192.168.1.100:5001)")
        print("  Введите 'done' для завершения ввода")
        print("  Оставьте пустым для использования тестовых целей")

        use_test = input("\nИспользовать тестовые цели? (y/n) [y]: ").strip().lower() or "y"

        if use_test == "y":
            # Тестовые цели для демонстрации
            target_list = [
                ("127.0.0.1", 5001),
                ("127.0.0.1", 5002)
            ]
            print("\n📋 Установлены тестовые цели:")
            for host, port in target_list:
                print(f"  • {host}:{port}")
        else:
            print("\nВведите цели (одна на строку):")
            while True:
                target = input("  > ").strip()
                if target.lower() == 'done':
                    break
                if not target:
                    continue

                try:
                    if ':' in target:
                        host_part, port_part = target.split(':', 1)
                        port = int(port_part)
                        target_list.append((host_part.strip(), port))
                    else:
                        print("  ⚠️  Формат должен быть IP:PORT")
                except:
                    print("  ⚠️  Неверный формат")

    print("\n" + "=" * 70)
    print("✅ ВСЕ ГОТОВО К ЗАПУСКУ!")
    print("=" * 70)
    print(f"🎛️  Режим: {mode.value}")

    if mode in [ServerMode.STANDALONE, ServerMode.HYBRID]:
        print(f"📍 Адрес сервера: {host}")
        print(f"🔌 Порт сервера: {port}")

        if host == "0.0.0.0":
            print("\n📡 Сервер будет доступен по всем сетевым интерфейсам")
            print("   Для подключения с этого компьютера используйте 127.0.0.1")

    if mode in [ServerMode.REVERSE_CLIENT, ServerMode.HYBRID] and target_list:
        print(f"\n🎯 Цели для подключения ({len(target_list)}):")
        for i, (t_host, t_port) in enumerate(target_list, 1):
            print(f"  {i}. {t_host}:{t_port}")

    print("\n🔄 Запуск...")
    print("=" * 70)

    server = CompleteServer(host, port, mode, password=None)  # Установите пароль здесь если нужно

    try:
        server.start_server(target_list if target_list else None)
    except KeyboardInterrupt:
        print("\n\n🛑 Программа остановлена пользователем")
    except Exception as e:
        print(f"\n❌ Ошибка запуска: {e}")
        traceback.print_exc()


def is_admin():
    """Проверка прав администратора"""
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False


if __name__ == "__main__":
    if platform.system() == "Windows" and not is_admin():
        ctypes.windll.shell32.ShellExecuteW(
            None, "runas", sys.executable, " ".join(sys.argv), None, 1
        )
        sys.exit()

    main()