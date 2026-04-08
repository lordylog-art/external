# greenmile_sync - Worker externo GreenMile -> Apps Script

Worker Python para tirar do Apps Script o fetch pesado no GreenMile.

## Como usar no exe

1. Abra `greenmile_sync.exe`
2. Se ainda nao existir `.env`, o painel hacker abre automaticamente
3. Preencha:
   - `Apps Script Token`
   - `GreenMile URL`
   - `GreenMile Usuario`
   - `GreenMile Senha`
   - opcionais: `Chunk Size`, `Request Timeout`, `Max Retries`
4. Clique em `SALVAR .ENV` para apenas salvar
5. Clique em `SALVAR E EXECUTAR` para salvar e iniciar a sincronizacao

O `.env` e criado automaticamente na mesma pasta do `.exe`.

## URL fixa do Apps Script

O executavel ja usa internamente:

```text
https://script.google.com/macros/s/AKfycbwgyg51wEvQZFtWTHVIazEOXF3Kb6QgO6_WUchLzzPvaU8p3fRGv-e_PUJZjzIpK6eL/exec
```

Voce nao precisa digitar essa URL no painel.

## Reabrir o painel depois

Se o `.env` ja existir e voce quiser editar a configuracao:

```bat
greenmile_sync.exe --configure
```

## Execucao em desenvolvimento

```bash
python src/main.py --configure
python src/main.py --env .env
python src/main.py --env .env --loop 300
```

## Testes

```bash
python -m unittest discover -s external/greenmile_sync/tests -p "test_*.py" -v
```

## Build do exe

```bash
pip install pyinstaller
cd external/greenmile_sync
pyinstaller greenmile_sync.spec
```

Saida esperada:

```text
external/greenmile_sync/dist/greenmile_sync.exe
```

## Estrutura

```text
external/greenmile_sync/
|-- .env.example
|-- README.md
|-- greenmile_sync.spec
|-- src/
|   |-- config.py
|   |-- http_client.py
|   |-- apps_gateway.py
|   |-- greenmile_client.py
|   |-- snapshot_mapper.py
|   |-- sync_runner.py
|   |-- ui_panel.py
|   `-- main.py
`-- tests/
```
