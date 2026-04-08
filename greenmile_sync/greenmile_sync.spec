# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec file for greenmile_sync standalone Windows executable.
#
# Build:
#   pip install pyinstaller
#   cd external/greenmile_sync
#   pyinstaller greenmile_sync.spec
#
# Output: dist/greenmile_sync.exe (no Python installation required on target PC)

import os

src_dir = os.path.join(os.path.dirname(os.path.abspath(SPECPATH)), 'greenmile_sync', 'src')

a = Analysis(
    [os.path.join(src_dir, 'main.py')],
    pathex=[src_dir],
    binaries=[],
    datas=[],
    hiddenimports=[
        'config',
        'http_client',
        'apps_gateway',
        'greenmile_client',
        'snapshot_mapper',
        'sync_runner',
        'ui_panel',
        'tkinter',
        'tkinter.messagebox',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'unittest',
        'xml',
        'html',
        'http.server',
        'xmlrpc',
        'pydoc',
    ],
    noarchive=False,
    optimize=1,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='greenmile_sync',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,   # Keep console for log output
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,
)
