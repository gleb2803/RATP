# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_all
from PyInstaller.utils.hooks import copy_metadata

datas = []
binaries = []
hiddenimports = ['pyaudio', 'cv2', 'numpy', 'numpy._core', 'numpy._core._exceptions', 'numpy._core._multiarray_umath', 'numpy._core._multiarray_tests', 'numpy._core.multiarray', 'numpy._core._methods', 'numpy.core', 'numpy.core._exceptions', 'numpy.core.multiarray', 'numpy.core._multiarray_umath', 'PIL', 'PIL.ImageGrab', 'psutil', 'pyautogui', 'wave', 'zipfile', 'fnmatch', 'hashlib', 'getpass', 'ctypes', 'select']
datas += copy_metadata('numpy')
tmp_ret = collect_all('pyaudio')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('cv2')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('numpy')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]


a = Analysis(
    ['ServerV4.py'],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='ServerV4',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
