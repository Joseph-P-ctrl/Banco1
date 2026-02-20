# Bancos - Deploy en Vercel

## Ejecutar local

1. Crear/activar entorno virtual.
2. Instalar dependencias:

```bash
pip install -r requirements.txt
```

3. Ejecutar app:

```bash
python app.py
```

## Deploy en Vercel

Este proyecto ya incluye:

- `vercel.json`
- `api/index.py`
- soporte de rutas de escritura para entorno serverless (`/tmp`) en `storage_paths.py`

### Pasos

1. Instalar Vercel CLI:

```bash
npm i -g vercel
```

2. Login:

```bash
vercel login
```

3. Deploy:

```bash
vercel --prod
```

## Nota importante

En Vercel, los archivos generados/subidos se guardan en almacenamiento temporal del runtime (`/tmp`).
Eso permite ejecutar el flujo, pero no es almacenamiento persistente entre ejecuciones o escalados.
