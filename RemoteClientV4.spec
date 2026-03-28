# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['ClientV4.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=['pyaudio', 'cv2', 'PIL', 'PIL.ImageGrab', 'PyQt5', 'PyQt5.QtWidgets', 'PyQt5.QtCore', 'PyQt5.QtGui', 'wave', 'hashlib', 'ipaddress'],
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
    name='RemoteClientV4',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
